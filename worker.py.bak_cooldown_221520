import json
"""
Worker 执行模块 - 水军通过Bot转发广告
流程：
  1. 水军号给Bot发 /start
  2. Bot回复广告消息（图片+文案+URL按钮）
  3. 水军转发广告消息给目标用户
  4. 验证发送状态（检查消息是否出现在对方聊天中）
  5. 如果连续多次未送达，水军进入冷却
  6. 如果被TG限制，标记水军号

关键：使用 Telegram 的转发功能分享广告消息
"""
import os
import time
import asyncio
import logging
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.functions.contacts import ResolveUsernameRequest
from telethon.tl.functions.messages import (
    GetBotCallbackAnswerRequest,
    GetInlineBotResultsRequest,
    SendInlineBotResultRequest,
    GetHistoryRequest
)
from telethon.tl.types import (
    MessageMediaPhoto, MessageMediaDocument,
    ReplyInlineMarkup, KeyboardButtonCallback,
    KeyboardButtonUrl, KeyboardButtonSwitchInline,
    InputPeerUser, PeerUser
)
from telethon.errors import (
    FloodWaitError, UserPrivacyRestrictedError,
    PeerFloodError, UsernameNotOccupiedError,
    ChatWriteForbiddenError, UserBannedInChannelError,
    InputUserDeactivatedError, UserIsBlockedError,
    BotInlineDisabledError, QueryIdInvalidError,
    AuthKeyUnregisteredError, UserDeactivatedBanError,
    PhoneNumberBannedError
)
import socks

logger = logging.getLogger("Worker")

MAX_RECONNECT_ATTEMPTS = 3
RECONNECT_DELAY = 5
# 递增等待策略: 第1次20分钟, 第2次1小时, 第3次2小时, 第4次永久标记
PROGRESSIVE_WAIT = {1: 6*3600, 2: 12*3600, 3: 24*3600, 4: 24*3600, 5: 24*3600}
MAX_FAILURES = 10  # 第10次直接标记删除


class ShareWorker:
    """单个分享Worker - 通过Bot转发广告"""

    def __init__(self, worker_config, api_id, api_hash):
        self.config = worker_config
        self.worker_id = worker_config["id"]
        self.bot_token = worker_config.get("bot_token", "")
        # Handle multiple bot usernames (comma-separated from batch assign)
        raw_bot_username = worker_config.get("bot_username", "")
        if "," in raw_bot_username:
            self.bot_username = raw_bot_username.split(",")[0].strip().lstrip("@")
        else:
            self.bot_username = raw_bot_username.strip().lstrip("@")
        # Also store all bot usernames for rotation
        self.bot_usernames = [u.strip().lstrip("@").rstrip(".") for u in raw_bot_username.replace("...", "").split(",") if u.strip()]
        # Get bot_ids list for full bot info
        self.bot_ids = worker_config.get("bot_ids", [])
        self.bot_tokens = worker_config.get("bot_tokens", [])
        self.phone = worker_config["phone"]
        self.session_name = worker_config.get("session_name", self.phone.replace("+", ""))
        self.proxy = worker_config.get("proxy")
        self.api_id = api_id
        self.api_hash = api_hash
        self.client = None
        self.status = "idle"
        self._connected = False
        self.daily_sends = 0
        self.last_error = ""
        # 限制标记
        self.is_restricted = worker_config.get("is_restricted", False)
        self.restricted_at = None
        self.restricted_reason = ""
        self.is_dead = False
        self.needs_disconnect = False  # 永久死亡标记
        # 频率限制计数
        self.rate_limit_count = worker_config.get("rate_limit_count", 0)  # 从配置加载历史限制次数
        self.has_been_banned_24h = worker_config.get("has_been_banned_24h", False)  # 从配置加载
        self.banned_at = None  # 24小时禁止开始时间
        # 暂停机制
        self.cooldown_until = 0
        self._on_rate_limit_changed = None  # 回调: 限制次数变化时通知外部持久化
        self._on_dead_detected = None  # 回调: 检测到账号死亡时通知外部自动删除
        # 当前使用的Bot用户名（用于标记Bot限制）
        self.current_bot_username = ""

    def _get_proxy_tuple(self):
        """转换代理配置为Telethon格式（兼容python-socks）"""
        if not self.proxy:
            return None
        proxy_type = self.proxy.get("type", "socks5")
        
        # 尝试使用python-socks的dict格式（新版Telethon推荐）
        try:
            import python_socks
            if proxy_type == "socks5":
                p_type = python_socks.ProxyType.SOCKS5
            elif proxy_type == "socks4":
                p_type = python_socks.ProxyType.SOCKS4
            else:
                p_type = python_socks.ProxyType.HTTP
            
            proxy_dict = {
                'proxy_type': p_type,
                'addr': self.proxy["host"],
                'port': int(self.proxy["port"]),
            }
            if self.proxy.get("username"):
                proxy_dict['username'] = self.proxy["username"]
                proxy_dict['password'] = self.proxy.get("password", "")
            return proxy_dict
        except ImportError:
            # 回退到PySocks的tuple格式
            if proxy_type == "socks5":
                p_type = socks.SOCKS5
            elif proxy_type == "socks4":
                p_type = socks.SOCKS4
            else:
                p_type = socks.HTTP

            proxy_tuple = (p_type, self.proxy["host"], int(self.proxy["port"]))
            if self.proxy.get("username"):
                proxy_tuple = (p_type, self.proxy["host"], int(self.proxy["port"]),
                              True, self.proxy["username"], self.proxy.get("password", ""))
            return proxy_tuple

    async def connect(self):
        """连接水军号到Telegram"""
        from config import SESSIONS_DIR
        session_path = os.path.join(str(SESSIONS_DIR), self.session_name)
        proxy = self._get_proxy_tuple()

        self.client = TelegramClient(
            session_path,
            self.api_id,
            self.api_hash,
            proxy=proxy,
            auto_reconnect=True,
            connection_retries=5,
            retry_delay=5,
            timeout=30,
            request_retries=3
        )

        await self.client.connect()

        if not await self.client.is_user_authorized():
            logger.error(f"[Worker-{self.worker_id}] ☠️ 水军号 {self.phone} 未登录(session失效/账号被封)")
            self.status = "dead"
            self.is_dead = True
            self.last_error = "账号已死: session失效或被TG封禁"
            self._connected = False
            # 触发自动删除回调
            if hasattr(self, '_on_dead_detected') and self._on_dead_detected:
                self._on_dead_detected(self.worker_id, self.phone, "session失效/未登录")
            return False

        me = await self.client.get_me()
        logger.info(f"[Worker-{self.worker_id}] 水军号已连接: {me.first_name} ({self.phone})")
        self.status = "idle"
        self._connected = True
        return True

    async def ensure_connected(self):
        """确保连接"""
        if self.client and self.client.is_connected():
            return True

        logger.warning(f"[Worker-{self.worker_id}] 连接断开，尝试重连...")
        for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
            try:
                if self.client:
                    await asyncio.wait_for(self.client.connect(), timeout=20)
                    if self.client.is_connected() and await self.client.is_user_authorized():
                        logger.info(f"[Worker-{self.worker_id}] 重连成功")
                        self._connected = True
                        return True
                else:
                    return await self.connect()
            except asyncio.TimeoutError:
                logger.warning(f"[Worker-{self.worker_id}] 重连尝试 {attempt} 超时(20s)")
            except Exception as e:
                logger.warning(f"[Worker-{self.worker_id}] 重连尝试 {attempt} 失败: {e}")
            await asyncio.sleep(RECONNECT_DELAY)

        self._connected = False
        self.status = "error"
        self.last_error = "连接失败"
        return False

    async def disconnect(self):
        """断开连接"""
        if self.client:
            try:
                await self.client.disconnect()
            except:
                pass
            self.client = None
            self._connected = False

    def is_in_cooldown(self):
        """检查是否在冷却期"""
        if self.cooldown_until > time.time():
            return True
        return False

    def enter_cooldown(self, duration=None):
        """进入冷却期
        - 若显式传入 duration(如 TG FloodWait 要求的秒数), 优先使用该时长
        - 否则按递增策略: 20分钟 -> 1小时 -> 2小时 -> 永久标记
        """
        # 如果已死/已被永久限制，不做任何处理
        if self.is_dead:
            return
        # 增加限制计数
        self.rate_limit_count += 1
        logger.warning(f"[Worker-{self.worker_id}] 分享失败第 {self.rate_limit_count} 次")
        # 持久化限制次数
        if self._on_rate_limit_changed:
            self._on_rate_limit_changed(self.worker_id, self.rate_limit_count, self.has_been_banned_24h, self.is_restricted)
        # 决定本次等待时长: 显式传入的 duration 优先, 否则按递增表
        if duration is not None:
            wait_time = duration
        else:
            wait_time = PROGRESSIVE_WAIT.get(self.rate_limit_count, -1)
        if wait_time == -1 or self.rate_limit_count >= MAX_FAILURES:
            # 第4次(或更多): 永久标记限制
            self.is_dead = True
            self.is_restricted = True
            # 持久化限制状态
            if self._on_rate_limit_changed:
                self._on_rate_limit_changed(self.worker_id, self.rate_limit_count, self.has_been_banned_24h, self.is_restricted)
            self.restricted_reason = f"分享失败{self.rate_limit_count}次，标记删除"
            self.status = "dead"
            self.needs_disconnect = True
            logger.error(f"[Worker-{self.worker_id}] ☠️ 水军号达到10次限制，标记删除: 分享失败{self.rate_limit_count}次")
            return
        # 第1-3次: 按等待时长进入冷却
        self.cooldown_until = time.time() + wait_time
        # 自适应时长描述: >=1小时显示小时, 否则显示分钟(避免 0.33h 被显示为"0小时")
        wait_desc = f"{wait_time/3600:.1f}小时" if wait_time >= 3600 else f"{max(1, round(wait_time/60))}分钟"
        if self.rate_limit_count >= 3:
            self.status = "banned_24h"
            self.has_been_banned_24h = True
            self.banned_at = time.time()
            self.needs_disconnect = True
            logger.error(f"[Worker-{self.worker_id}] 🚫 分享失败第{self.rate_limit_count}次，等待{wait_desc}，断开释放资源")
        elif self.rate_limit_count >= 2:
            self.status = "cooldown"
            self.needs_disconnect = True
            logger.warning(f"[Worker-{self.worker_id}] ⚠️ 分享失败第{self.rate_limit_count}次，等待{wait_desc}，断开释放资源")
        else:
            self.status = "cooldown"
            logger.warning(f"[Worker-{self.worker_id}] 分享失败第{self.rate_limit_count}次，等待{wait_desc}后重新排队")

    def mark_restricted(self, reason=""):
        """标记水军号被限制"""
        self.is_restricted = True
        # 持久化限制状态
        if self._on_rate_limit_changed:
            self._on_rate_limit_changed(self.worker_id, self.rate_limit_count, self.has_been_banned_24h, self.is_restricted)
        self.restricted_at = datetime.now().isoformat()
        self.restricted_reason = reason
        self.status = "restricted"
        logger.error(f"[Worker-{self.worker_id}] ⚠️ 水军号被限制: {reason}")

    async def search_user(self, username):
        """搜索用户名并验证"""
        try:
            if not await self.ensure_connected():
                return None, False

            clean_username = username.lstrip("@")
            try:
                from telethon.tl.functions.contacts import SearchRequest
                sr = await asyncio.wait_for(
                    self.client(SearchRequest(q=clean_username, limit=20)),
                    timeout=30
                )
                users = list(getattr(sr, "users", []) or [])
                for u in users:
                    un = (getattr(u, "username", None) or "").lower()
                    if un == clean_username.lower():
                        logger.info(f"[Worker-{self.worker_id}] ✅ Search命中: @{un}")
                        return u, True
                result = await asyncio.wait_for(
                    self.client(ResolveUsernameRequest(clean_username)),
                    timeout=30
                )
            except asyncio.TimeoutError:
                logger.error(f"[Worker-{self.worker_id}] ⏰ 搜索用户超时(30s): @{username}")
                self._connected = False
                self.last_error = "搜索超时"
                raise
            except Exception as e:
                self.last_error = str(e)
                logger.error(f"[Worker-{self.worker_id}] 搜索异常: {e}")
                return None, False

            if not (result and result.peer):
                try:
                    user = await asyncio.wait_for(self.client.get_entity(clean_username), timeout=20)
                    logger.info(f"[Worker-{self.worker_id}] ✅ get_entity兜底找到: @{getattr(user,'username',clean_username)}")
                    return user, True
                except Exception as e:
                    es = type(e).__name__ + " " + str(e)
                    if "UsernameNotOccupied" in es or "UsernameInvalid" in es:
                        return None, False
                    logger.warning(f"[Worker-{self.worker_id}] Resolve空peer兜底失败: {e}")
                    raise
            if result and result.peer:
                try:
                    user = await asyncio.wait_for(
                        self.client.get_entity(result.peer),
                        timeout=15
                    )
                except asyncio.TimeoutError:
                    logger.error(f"[Worker-{self.worker_id}] ⏰ get_entity超时(15s): @{username}")
                    self._connected = False
                    raise
                if hasattr(user, 'username') and user.username:
                    if user.username.lower() == clean_username.lower():
                        logger.info(f"[Worker-{self.worker_id}] ✅ 找到用户: @{user.username}")
                        return user, True
                    else:
                        logger.warning(f"[Worker-{self.worker_id}] 用户名不匹配")
                        return user, False
                return user, True
            return None, False

        except UsernameNotOccupiedError:
            logger.warning(f"[Worker-{self.worker_id}] 用户名不存在: @{username}")
            return None, False
        except FloodWaitError as e:
            logger.warning(f"[Worker-{self.worker_id}] FloodWait: {e.seconds}秒")
            self.enter_cooldown(max(e.seconds, 60))
            return None, False
        except Exception as e:
            logger.error(f"[Worker-{self.worker_id}] 搜索用户出错: {e}")
            raise

    async def get_bot_ad_and_share(self, target_user, ad_index=0):
        """
        完整的Bot分享流程：
        1. 给Bot发 /start
        2. Bot回复广告消息（图片+文案+分享按钮）
        3. 水军点击分享按钮触发inline query，在目标用户聊天中发送
        4. 如果没有inline分享按钮，直接转发广告消息
        5. 验证发送状态

        返回: (success, message, bot_restricted)
        """
        bot_restricted = False
        try:
            if not await self.ensure_connected():
                return False, "连接失败", False

            # Try to get bot entity - use first valid bot username
            bot_entity = None
            bot_username_to_use = ""
            # === 全局Bot池轮换机制 ===
            # 不再使用固定绑定的bot_usernames，改为从全局Bot池依次选择
            import os as _os
            bots_file = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "data", "bots.json")
            restrictions_file = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "data", "restrictions.json")
            try:
                with open(bots_file, 'r') as bf:
                    bots_data = json.load(bf)
                all_bots = [b.get("username", "").lstrip("@") for b in bots_data.get("bots", []) if b.get("enabled", True) and not b.get("is_restricted", False) and b.get("username")]
                group_bots = None
                _pp = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "data", "proxy_pool.json")
                if not _os.path.exists(_pp):
                    _pp = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "proxy_pool.json")
                if _os.path.exists(_pp):
                    _pdata = json.load(open(_pp, "r"))
                    _proxies = _pdata.get("proxies", _pdata if isinstance(_pdata, list) else [])
                    my_host = ((self.config or {}).get("proxy") or {}).get("host")
                    my_port = str(((self.config or {}).get("proxy") or {}).get("port") or "")
                    for _px in _proxies:
                        if str(_px.get("host")) == str(my_host) and str(_px.get("port")) == my_port:
                            acc = _px.get("assigned_bot_accounts") or []
                            names = []
                            for item in acc:
                                if isinstance(item, dict):
                                    names.append(str(item.get("username") or item.get("id") or "").lstrip("@"))
                                else:
                                    names.append(str(item).lstrip("@"))
                            names = [x for x in names if x]
                            if names:
                                group_bots = [x for x in all_bots if x in names] or names
                            break
                if group_bots:
                    all_bots = group_bots
            except Exception as e:
                logger.warning(f"[Worker-{self.worker_id}] 加载Bot池失败: {e}")
                all_bots = []
            if not all_bots:
                return False, "全局Bot池为空，无可用Bot", False
            # 加载限制记录，跳过被banned的水军+Bot组合
            banned_combos = set()
            try:
                with open(restrictions_file, 'r') as rf:
                    restrictions_data = json.load(rf)
                for key, record in restrictions_data.get("records", {}).items():
                    if record.get("banned") and record.get("worker_phone") == self.phone:
                        banned_combos.add(record.get("bot_username", "").lstrip("@"))
            except Exception:
                pass
            # 使用全局轮换计数器选择Bot（基于类变量）
            if not hasattr(ShareWorker, '_global_bot_index'):
                ShareWorker._global_bot_index = 0
            # 从全局索引开始，找到第一个可用的Bot
            valid_usernames = []
            start_idx = ShareWorker._global_bot_index % len(all_bots)
            for i in range(len(all_bots)):
                idx = (start_idx + i) % len(all_bots)
                bot_name = all_bots[idx]
                if bot_name in banned_combos:
                    continue
                valid_usernames.append(bot_name)
                if len(valid_usernames) >= 5:
                    break
            # 推进全局索引（每次调用都推进，确保下次用不同Bot）
            ShareWorker._global_bot_index += 1
            if not valid_usernames:
                return False, f"所有Bot都被当前水军号banned，无可用Bot", False
            for uname in valid_usernames:
                try:
                    bot_entity = await self.client.get_entity(f"@{uname}")
                    bot_username_to_use = uname
                    break
                except Exception as e:
                    logger.debug(f"[Worker-{self.worker_id}] Bot @{uname} not found: {e}")
                    continue
            if bot_entity is None:
                return False, f"无法连接任何Bot: {', '.join(valid_usernames[:5])}", False

            self.current_bot_username = bot_username_to_use

            # === Step 1: 发送 /start ===
            await self.client.send_message(bot_entity, "/start")
            logger.info(f"[Worker-{self.worker_id}] 发送 /start 给 @{bot_username_to_use}")
            import random as _rnd; _wait = _rnd.randint(10, 20); logger.info(f"[Worker-{self.worker_id}] 等待 {_wait} 秒获取广告..."); await asyncio.sleep(_wait)

            # === Step 2: 获取Bot回复的广告消息 ===
            messages = await self.client.get_messages(bot_entity, limit=10)
            ad_msg = None
            share_query = None
            my_id = (await self.client.get_me()).id

            # 先找带分享按钮（KeyboardButtonSwitchInline）的消息
            for msg in messages:
                if msg.sender_id != my_id and msg.reply_markup:
                    if isinstance(msg.reply_markup, ReplyInlineMarkup):
                        for row in msg.reply_markup.rows:
                            for btn in row.buttons:
                                if isinstance(btn, KeyboardButtonSwitchInline):
                                    ad_msg = msg
                                    share_query = btn.query
                                    break
                            if share_query is not None:
                                break
                if share_query is not None:
                    break

            # 如果没有inline分享按钮，找带媒体的广告消息用于转发
            if ad_msg is None:
                for msg in messages:
                    if msg.sender_id != my_id and (msg.media or msg.message):
                        if isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
                            ad_msg = msg
                            break
                        elif msg.message and len(msg.message) > 10:
                            ad_msg = msg
                            break

            if ad_msg is None:
                # Bot可能响应慢，再等10秒重试
                logger.info(f"[Worker-{self.worker_id}] Bot未立即回复，再等10秒...")
                await asyncio.sleep(10)
                messages = await self.client.get_messages(bot_entity, limit=10)
                for msg in messages:
                    if msg.sender_id != my_id:
                        if msg.reply_markup and isinstance(msg.reply_markup, ReplyInlineMarkup):
                            for row in msg.reply_markup.rows:
                                for btn in row.buttons:
                                    if isinstance(btn, KeyboardButtonSwitchInline):
                                        ad_msg = msg
                                        share_query = btn.query
                                        break
                                if share_query is not None:
                                    break
                        if ad_msg is None and (msg.media or (msg.message and len(msg.message) > 10)):
                            ad_msg = msg
                            break
                if ad_msg is None:
                    return False, f"Bot @{bot_username_to_use} 未返回广告消息(重试后仍无)", False

            # === Step 3: 分享广告给目标用户 ===
            if share_query is not None:
                # 有inline分享按钮 → 使用 inline query 分享
                logger.info(f"[Worker-{self.worker_id}] 使用inline分享，query: '{share_query}'")
                try:
                    inline_results = await self.client(GetInlineBotResultsRequest(
                        bot=bot_entity,
                        peer=target_user,
                        query=share_query or str(ad_index),
                        offset=""
                    ))
                except BotInlineDisabledError:
                    # Bot被禁用inline mode → 标记Bot被限制
                    bot_restricted = True
                    return False, f"Bot @{bot_username_to_use} inline mode被禁用", True
                except Exception as e:
                    err_str = str(e)
                    if "bot_inline" in err_str.lower() or "restricted" in err_str.lower():
                        bot_restricted = True
                        return False, f"Bot @{bot_username_to_use} 被限制: {e}", True
                    return False, f"获取inline结果失败: {e}", False

                if not inline_results.results:
                    return False, "inline query无结果", False

                # 发送 inline 结果给目标用户
                result = inline_results.results[0]
                try:
                    await self.client(SendInlineBotResultRequest(
                        peer=target_user,
                        query_id=inline_results.query_id,
                        result_id=result.id,
                        random_id=int(time.time() * 1000000)
                    ))
                except QueryIdInvalidError:
                    return False, "query已过期，请重试", False
                except Exception as e:
                    err_str = str(e)
                    if "banned" in err_str.lower() or "restricted" in err_str.lower() or "forbidden" in err_str.lower():
                        bot_restricted = True
                        return False, f"Bot @{bot_username_to_use} 分享被限制: {e}", True
                    return False, f"发送失败: {e}", False

                logger.info(f"[Worker-{self.worker_id}] ✅ 通过inline分享成功")
            else:
                # 没有inline分享按钮 → 直接转发广告消息
                logger.info(f"[Worker-{self.worker_id}] 使用转发方式分享广告")
                try:
                    await self.client.forward_messages(target_user, ad_msg)
                except Exception as e:
                    err_str = str(e)
                    if "banned" in err_str.lower() or "restricted" in err_str.lower():
                        return False, f"转发被限制: {e}", False
                    if "too many" in err_str.lower() or "flood" in err_str.lower():
                        logger.warning(f"[Worker-{self.worker_id}] 转发频率限制(第{self.rate_limit_count+1}次): {e}")
                        self.enter_cooldown(3600)
                        return False, f"转发失败: Too many requests (已限制{self.rate_limit_count}次)", False
                    if "blocked" in err_str.lower() or "you blocked" in err_str.lower():
                        return False, f"You blocked this user", False
                    return False, f"转发失败: {e}", False
                logger.info(f"[Worker-{self.worker_id}] ✅ 转发广告消息成功")
            # === Step 4: 验证发送 - 检查消息是否出现在目标用户聊天中 ===
            await asyncio.sleep(5)  # 等待5秒让TG处理转发
            delivered = False
            try:
                history = await self.client.get_messages(target_user, limit=5)
                if history:
                    for msg in history:
                        if msg.out:  # 自己发出的消息
                            delivered = True
                            if not hasattr(self, '_consecutive_undelivered'):
                                self._consecutive_undelivered = 0
                            self._consecutive_undelivered = 0
                            logger.info(f"[Worker-{self.worker_id}] ✅ 确认消息已送达目标用户")
                            return True, "发送成功(已送达)", bot_restricted
                        # 转发消息可能不标记为out，检查最近的转发消息
                        if hasattr(msg, 'forward') and msg.forward and msg.date and (time.time() - msg.date.timestamp()) < 30:
                            delivered = True
                            if not hasattr(self, '_consecutive_undelivered'):
                                self._consecutive_undelivered = 0
                            self._consecutive_undelivered = 0
                            logger.info(f"[Worker-{self.worker_id}] ✅ 确认转发消息已送达")
                            return True, "发送成功(已送达)", bot_restricted
            except Exception as e:
                logger.debug(f"[Worker-{self.worker_id}] 验证发送状态出错: {e}")
                # 验证出错不代表没送达，视为成功
                return True, "发送成功(验证跳过)", bot_restricted
            if not delivered:
                if not hasattr(self, '_consecutive_undelivered'):
                    self._consecutive_undelivered = 0
                self._consecutive_undelivered += 1
                if self._consecutive_undelivered >= 3:
                    # 连续3次未送达 → 可能被TG静默限制，暂停1小时
                    cnt = self._consecutive_undelivered
                    logger.warning(f"[Worker-{self.worker_id}] ⚠️ 连续{cnt}次未送达，水军暂停1小时")
                    self.enter_cooldown(3600)
                    self._consecutive_undelivered = 0
                    return False, f"连续{cnt}次未送达(水军暂停)", bot_restricted
                else:
                    # 单次未送达 → 不冷却，继续下一个目标
                    logger.info(f"[Worker-{self.worker_id}] ⚠️ 消息可能未送达(第{self._consecutive_undelivered}次)，继续发送")
                    return True, "发送成功(未确认送达)", bot_restricted
            return True, "发送成功", bot_restricted

            return True, "发送成功", bot_restricted

        except FloodWaitError as e:
            self.enter_cooldown(e.seconds)
            return False, f"FloodWait: 需等待{e.seconds}秒", bot_restricted
        except PeerFloodError:
            # 被TG检测到频繁发送 → 标记水军号被限制
            self.mark_restricted("PeerFlood: 被TG检测到频繁分享广告")
            return False, "PeerFlood: 水军号被限制分享", bot_restricted
        except UserPrivacyRestrictedError:
            return False, "用户隐私设置限制", bot_restricted
        except ChatWriteForbiddenError:
            return False, "无法向该用户发送消息", bot_restricted
        except UserBannedInChannelError:
            # 账号被限制 → 标记
            self.mark_restricted("UserBannedInChannel: 账号被限制发送")
            return False, "账号被限制", bot_restricted
        except Exception as e:
            err_msg = str(e)
            if "disconnected" in err_msg.lower() or "connection" in err_msg.lower():
                self._connected = False
            # 检查是否是账号死亡（TG平台封禁）
            if "deactivated" in err_msg.lower() or "auth_key_unregistered" in err_msg.lower() or "phone_number_banned" in err_msg.lower():
                logger.error(f"[Worker-{self.worker_id}] ☠️ 水军号 {self.phone} 在发送中被检测到已死: {e}")
                self.status = "dead"
                self.is_dead = True
                self.last_error = f"账号已死: {e}"
                if hasattr(self, '_on_dead_detected') and self._on_dead_detected:
                    self._on_dead_detected(self.worker_id, self.phone, str(e))
                return False, f"账号已死: {e}", bot_restricted
            # 检查是否是限制相关的错误
            if "banned" in err_msg.lower() or "restricted" in err_msg.lower() or "spam" in err_msg.lower():
                self.mark_restricted(f"被限制: {e}")
            logger.error(f"[Worker-{self.worker_id}] 分享出错: {e}")
            return False, f"错误: {e}", bot_restricted

    async def execute_share_task(self, target_username, ad_index=0):
        """
        执行完整的分享任务：
        1. 检查是否在冷却期或被限制
        2. 先搜索目标用户确认存在
        3. 通过Bot分享广告给目标用户
        4. 验证发送状态
        5. 如果被限制，标记状态

        返回: (success, message)
        """
        # 检查是否已死
        if self.is_dead:
            return False, f"水军号已死亡: {self.restricted_reason}"
        # 检查是否被限制
        if self.is_restricted:
            return False, f"水军号已被限制: {self.restricted_reason}"

        # 检查是否在冷却期
        if self.is_in_cooldown():
            remaining = int(self.cooldown_until - time.time())
            return False, f"水军号冷却中，剩余{remaining}秒"

        self.status = "working"

        # Step 1: 搜索并确认目标用户
        logger.info(f"[Worker-{self.worker_id}] 搜索目标用户: @{target_username}")
        user, matched = await self.search_user(target_username)

        if not user:
            self.status = "idle"
            return False, f"目标用户 @{target_username} 不存在"

        if not matched:
            self.status = "idle"
            return False, f"用户名不匹配"

        # Step 2: 通过Bot分享广告
        success, msg, bot_restricted = await self.get_bot_ad_and_share(user, ad_index)

        if success:
            self.daily_sends += 1
            self.status = "idle"
        else:
            if self.status not in ("cooldown", "restricted"):
                self.status = "idle"
            self.last_error = msg

        # 返回结果，包含bot_restricted信息供调度器使用
        return success, msg

    def get_status_info(self):
        """获取水军号的完整状态信息"""
        return {
            "id": self.worker_id,
            "phone": self.phone,
            "status": self.status,
            "connected": self._connected,
            "daily_sends": self.daily_sends,
            "last_error": self.last_error,
            "is_restricted": self.is_restricted,
            "is_dead": self.is_dead,
            "restricted_at": self.restricted_at,
            "restricted_reason": self.restricted_reason,
            "rate_limit_count": self.rate_limit_count,
            "has_been_banned_24h": self.has_been_banned_24h,
            "in_cooldown": self.is_in_cooldown(),
            "cooldown_remaining": max(0, int(self.cooldown_until - time.time())) if self.is_in_cooldown() else 0,
            "current_bot": self.current_bot_username
        }

    async def send_start_and_wait(self, bot_username, timeout=15):
        try:
            if not await self.ensure_connected():
                return False
            bot = await self.client.get_entity(bot_username)
            await self.client.send_message(bot, "/start")
            await asyncio.sleep(3)
            deadline = time.time() + timeout
            my_id = (await self.client.get_me()).id
            while time.time() < deadline:
                msgs = await self.client.get_messages(bot, limit=5)
                for m in msgs:
                    if m.sender_id != my_id and (m.message or m.media or m.reply_markup):
                        return True
                await asyncio.sleep(1)
            return False
        except Exception as e:
            logger.warning(f"[Worker] send_start_and_wait 失败: {e}")
            return False

    async def read_bot_and_prepare_share(self, bot_username):
        try:
            bot = await self.client.get_entity(bot_username)
            msgs = await self.client.get_messages(bot, limit=10)
            my_id = (await self.client.get_me()).id
            for msg in msgs:
                if msg.sender_id != my_id and msg.reply_markup:
                    if isinstance(msg.reply_markup, ReplyInlineMarkup):
                        for row in msg.reply_markup.rows:
                            for btn in row.buttons:
                                if isinstance(btn, KeyboardButtonSwitchInline):
                                    return btn.query
            for msg in msgs:
                if msg.sender_id != my_id and (msg.media or msg.message):
                    return msg
            return None
        except Exception as e:
            logger.warning(f"[Worker] read_bot_and_prepare_share 失败: {e}")
            return None

    def mark_rate_limit_and_check_delete(self):
        self.rate_limit_count = getattr(self, "rate_limit_count", 0) + 1
        if self.rate_limit_count >= 10:
            self.is_dead = True
            self.restricted_reason = "限制满10次自动删除"
            logger.warning(f"[Worker-{self.worker_id}] 限制满10次，自动删除")
            return True
        return False

    async def forward_share_to_user(self, user_entity, bot_msg=None):
        """转发分享内容到目标用户"""
        try:
            if not user_entity:
                return False, "无目标用户"
            # 如果有 bot 消息，直接转发
            if bot_msg and hasattr(self, "client") and self.client:
                await self.client.forward_messages(user_entity, bot_msg)
                return True, "转发成功"
            # 否则发一条简单消息
            await self.client.send_message(user_entity, "你好")
            return True, "发送成功"
        except Exception as e:
            logger.warning(f"[Worker] forward_share_to_user 失败: {e}")
            return False, str(e)
