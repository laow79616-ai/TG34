"""
tg_share_v2 主程序 - 优化版
功能：
1. Bot 服务（处理 /start、预览、inline query）
2. Worker 管理（水军号连接、分享任务）
3. IP 代理池管理
4. Web 管理面板 API
5. 定时任务调度

内存优化：
- 按需连接水军号（不全部同时在线）
- 最多5个同时连接
- 连接间隔10秒
- 内存超80%自动清理
"""
import os
import signal
import sys
import json
import time
import asyncio
import logging
import random
import hashlib
import psutil
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque

from aiohttp import web
import aiohttp_cors

# Telegram Bot
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, InlineQueryHandler,
    filters, ContextTypes
)
from telegram import (
    InlineQueryResultCachedPhoto, InlineQueryResultArticle,
    InputTextMessageContent, InlineQueryResultPhoto
)
from telethon.tl.types import (
    ReplyInlineMarkup, KeyboardButtonCallback, KeyboardButtonSwitchInline
)

# 本地模块
from worker import ShareWorker
import auth
from config import (
    BASE_DIR, DATA_DIR, SESSIONS_DIR, LOGS_DIR,
    load_json, save_json,
    AD_CONFIG_FILE, BOT_CONFIG_FILE, WORKERS_CONFIG_FILE,
BOTS_FILE, TARGETS_FILE, PROXY_POOL_FILE, SCHEDULE_FILE, STATS_FILE
)

# ============ 日志配置 ============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(str(LOGS_DIR / "app.log"), encoding='utf-8')
    ]
)
logger = logging.getLogger("App")

# 降低 Telethon 日志级别，避免日志撑到数 GB
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("telethon.network").setLevel(logging.WARNING)
logging.getLogger("telethon.network.mtprotosender").setLevel(logging.WARNING)


# ============ 全局状态 ============
workers = {}  # worker_id -> ShareWorker

def _persist_worker_rate_limit(worker_id, rate_limit_count, has_been_banned_24h, is_restricted):
    """持久化水军的限制次数到配置文件"""
    try:
        data = load_json(WORKERS_CONFIG_FILE)
        worker_list = data.get("workers", [])
        for w in worker_list:
            if w["id"] == worker_id:
                w["rate_limit_count"] = rate_limit_count
                w["has_been_banned_24h"] = has_been_banned_24h
                w["is_restricted"] = is_restricted
                break
        save_json(WORKERS_CONFIG_FILE, {"workers": worker_list})
    except Exception as e:
        logger.error(f"持久化限制次数失败: {e}")


bot_app = None  # Telegram Bot Application
scheduler_task = None
_manual_stopped = False  # 手动停止后禁止自动再启动
# --- 2号面板：长时间无发送自动续跑 / 上报 ---
PANEL_NAME = "2号面板"
STALL_MINUTES = 15
STALL_AFTER_RESTART_MINUTES = 10
_stall_monitor_task = None
_last_success_send_ts = 0.0
_last_attempt_ts = 0.0  # 全局间隔控制
_last_auto_restart_ts = 0.0
_stall_alerted_after_restart = False

_worker_low_notified_at = 0  # 水军不足上报时间戳，防刷
reconnect_task = None  # 后台自动重连任务
# 工作流活动日志
activity_log = []  # 最近50条活动记录
current_activity = {}  # 当前正在进行的操作
MAX_ACTIVITY_LOG = 50

async def check_workers_running_low(worker_configs, interval_min=120):
    """可用水军是否会在约5小时内不够用；是则上报（5小时内最多一次）"""
    global _worker_low_notified_at
    import time as _time
    now = _time.time()
    if now - _worker_low_notified_at < 5 * 3600:
        return
    usable = 0
    for wc in worker_configs:
        wid = wc.get("id")
        if wid in workers:
            w = workers[wid]
            if getattr(w, "is_restricted", False):
                continue
            if getattr(w, "is_dead", False):
                continue
            if getattr(w, "has_been_banned_24h", False):
                continue
            if getattr(w, "rate_limit_count", 0) >= 5:
                continue
            if w.is_in_cooldown():
                continue
        else:
            # 未连接的配置号，若配置未标记限制也算潜在可用
            if wc.get("is_restricted") or wc.get("has_been_banned_24h"):
                continue
            if int(wc.get("rate_limit_count") or 0) >= 5:
                continue
        usable += 1
    # 5小时按当前间隔能发多少条
    avg_interval = max(int(interval_min), 60)
    need_for_5h = int((5 * 3600) / avg_interval)  # 约一条接一条
    # 经验：健康号平均在再次限制前能撑的条数有限，按每号约8条保守估计
    capacity = usable * 8
    if usable <= 15 or capacity < need_for_5h:
        st = _today_send_stats() if "_today_send_stats" in globals() or True else {"today": 0, "total": 0}
        try:
            st = _today_send_stats()
        except Exception:
            st = {"today": 0, "total": 0}
        msg = (
            f"【旧面板】水军预计5小时内可能用完\\n"
            f"当前可用水军约: {usable}\\n"
            f"预估5小时需求约: {need_for_5h} 次发送\\n"
            f"保守产能约: {capacity} 次\\n"
            f"今日成功: {st.get('today', 0)} 条\\n"
            f"请及时补仓水军\\n"
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_panel_notify(msg)
        _worker_low_notified_at = now
        logger.warning(f"[预警] 水军不足已上报: usable={usable}, capacity={capacity}, need_5h={need_for_5h}")



async def send_panel_notify(text: str):
    """任何状态上报；失败写日志"""
    logger.info(f"[通知] 准备发送: {str(text)[:80]}")
    try:
        import aiohttp, json as _json
        from pathlib import Path as _P
        cfg_path = _P(__file__).resolve().parent / "data" / "notify_config.json"
        if not cfg_path.exists():
            logger.warning("[通知] 缺少 data/notify_config.json")
            return False
        cfg = _json.loads(cfg_path.read_text(encoding="utf-8"))
        token = (cfg.get("bot_token") or "").strip()
        chat_id = str(cfg.get("chat_id") or "").strip()
        if not token or not chat_id:
            logger.warning("[通知] token 或 chat_id 为空")
            return False
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": str(text)[:3500]},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                body = await resp.text()
                if resp.status != 200:
                    logger.warning(f"[通知] 失败 HTTP {resp.status}: {body[:200]}")
                    return False
                logger.info("[通知] 发送成功")
                return True
    except Exception as e:
        logger.warning(f"[通知] 异常: {e}")
        return False

def _today_send_stats():
    try:
        stats = load_json(STATS_FILE) or {}
        return {
            "today": int(stats.get("today_sends") or stats.get("today") or 0),
            "total": int(stats.get("total_sends") or stats.get("total") or 0),
        }
    except Exception:
        return {"today": 0, "total": 0}








def ensure_daily_reset():
    """每日 0 点重置 today_sends（累计不重置），并重置内存水军 daily_sends"""
    try:
        stats = load_json(STATS_FILE) or {}
        today = datetime.now().strftime("%Y-%m-%d")
        if stats.get("today_date") != today:
            old = stats.get("today_sends", 0)
            stats["today_sends"] = 0
            stats["today_date"] = today
            save_json(STATS_FILE, stats)
            logger.info(f"[统计] 今日发送数已重置 {old} -> 0，日期={today}")
        for w in list(workers.values()):
            if getattr(w, "_daily_date", None) != today:
                w.daily_sends = 0
                w._daily_date = today
    except Exception as e:
        logger.warning(f"ensure_daily_reset 失败: {e}")

from datetime import timezone, timedelta
BJ_TZ = timezone(timedelta(hours=8))

def _bj_now():
    return datetime.now(BJ_TZ)

def log_step(step, action, worker_phone="", target="", details=""):
    msg = f"[步骤{step}] {action}"
    if worker_phone:
        msg += f" | 水军:{worker_phone}"
    if target:
        msg += f" | 目标:{target}"
    if details:
        msg += f" | {details}"
    logger.info(msg)
    log_activity(f"步骤{step}: {action}", details, worker_phone, target, status="info")

def log_activity(action, details="", worker_phone="", target="", status="info"):
    """记录工作流活动"""
    global activity_log
    entry = {
        "time": _bj_now().strftime("%H:%M:%S"),
        "action": action,
        "details": details,
        "worker": worker_phone,
        "target": target,
        "status": status  # info, success, warning, error
    }
    activity_log.append(entry)
    if len(activity_log) > MAX_ACTIVITY_LOG:
        activity_log = activity_log[-MAX_ACTIVITY_LOG:]

connected_count = 0
MAX_CONCURRENT_CONNECTIONS = 10
def get_max_online():
    try:
        return int(load_json(BOT_CONFIG_FILE).get("max_online_workers", 20) or 20)
    except:
        return 10
CONNECTION_INTERVAL = 10  # 秒
MEMORY_THRESHOLD = 80  # %

# ============ IP 代理池管理 ============
class ProxyPool:
    """IP代理池管理"""

    def __init__(self):
        self.proxies = []  # [{host, port, type, username, password, assigned_bots: []}]
        self.load()

    def load(self):
        data = load_json(PROXY_POOL_FILE)
        self.proxies = data.get("proxies", [])

    def save(self):
        save_json(PROXY_POOL_FILE, {"proxies": self.proxies})

    def add_proxy(self, host, port, proxy_type="socks5", username="", password=""):
        proxy = {
            "id": hashlib.md5(f"{host}:{port}".encode()).hexdigest()[:8],
            "host": host,
            "port": int(port),
            "type": proxy_type,
            "username": username,
            "password": password,
            "assigned_bots": [],
            "status": "active"
        }
        self.proxies.append(proxy)
        self.save()
        return proxy

    def remove_proxy(self, proxy_id):
        self.proxies = [p for p in self.proxies if p["id"] != proxy_id]
        self.save()

    def get_proxy_for_worker(self, worker_id):
        """获取分配给某个worker的代理"""
        for proxy in self.proxies:
            if worker_id in proxy.get("assigned_bots", []):
                return proxy
        return None

    def auto_assign(self, worker_ids, bot_ids=None, workers_per_proxy=5, bots_per_proxy=40):
        """一键均匀分配：水军和Bot均匀分配到所有IP"""
        num_proxies = len(self.proxies)
        if num_proxies == 0:
            return {"error": "没有可用的代理IP"}
        # 先清除所有分配
        for proxy in self.proxies:
            proxy["assigned_bots"] = []
            proxy["assigned_bot_accounts"] = []
        # 均匀分配水军号（轮询方式）
        for i, wid in enumerate(worker_ids):
            self.proxies[i % num_proxies]["assigned_bots"].append(wid)
        # 均匀分配Bot（轮询方式）
        if bot_ids:
            for i, bid in enumerate(bot_ids):
                self.proxies[i % num_proxies]["assigned_bot_accounts"].append(bid)
        self.save()
        return {
            "total_workers": len(worker_ids),
            "total_bots": len(bot_ids) if bot_ids else 0,
            "total_proxies": num_proxies,
            "workers_per_proxy": f"{len(worker_ids)//num_proxies}-{len(worker_ids)//num_proxies+1}",
            "bots_per_proxy": f"{len(bot_ids)//num_proxies}-{len(bot_ids)//num_proxies+1}" if bot_ids else "0",
            "assigned_workers": sum(len(p.get("assigned_bots", [])) for p in self.proxies),
            "assigned_bots": sum(len(p.get("assigned_bot_accounts", [])) for p in self.proxies)
        }


    def get_all(self):
        return self.proxies


proxy_pool = ProxyPool()

# ============ Bot 处理器 ============
def load_ads():
    data = load_json(AD_CONFIG_FILE)
    return data.get("ads", [])


def save_ads(ads):
    save_json(AD_CONFIG_FILE, {"ads": ads})


async def bot_start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("👁 预览消息", callback_data="preview_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "欢迎使用快约到家推广系统！\n\n请点击下方按钮预览广告消息：",
        reply_markup=reply_markup
    )


async def bot_preview_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ads = load_ads()
    if not ads:
        await update.message.reply_text("暂无广告，请在管理面板添加广告。")
        return
    keyboard = []
    for i, ad in enumerate(ads):
        keyboard.append([InlineKeyboardButton(
            ad.get("name", f"广告{i+1}"),
            callback_data=f"preview_{i}"
        )])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("请选择要预览的广告：", reply_markup=reply_markup)


async def bot_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "preview_menu":
        ads = load_ads()
        if not ads:
            await query.edit_message_text("暂无广告，请在管理面板添加广告。")
            return
        keyboard = []
        for i, ad in enumerate(ads):
            keyboard.append([InlineKeyboardButton(
                ad.get("name", f"广告{i+1}"),
                callback_data=f"preview_{i}"
            )])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("请选择要预览的广告：", reply_markup=reply_markup)

    elif data.startswith("preview_"):
        idx = int(data.split("_")[1])
        ads = load_ads()
        if idx >= len(ads):
            await query.edit_message_text("广告不存在")
            return

        ad = ads[idx]
        caption = ad.get("message", "")
        image_url = ad.get("image_url", "")
        image_file_id = ad.get("image_file_id", "")

        keyboard = []
        url_buttons = ad.get("url_buttons", [])
        for btn in url_buttons:
            keyboard.append([InlineKeyboardButton(btn["text"], url=btn["url"])])

        # 分享按钮
        keyboard.append([InlineKeyboardButton(
            "📤 分享给用户",
            switch_inline_query=str(idx)
        )])
        reply_markup = InlineKeyboardMarkup(keyboard)

        if image_file_id:
            await query.message.reply_photo(
                photo=image_file_id,
                caption=caption,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
        elif image_url:
            await query.message.reply_photo(
                photo=image_url,
                caption=caption,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
        else:
            await query.message.reply_text(
                caption,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )


async def bot_inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.inline_query
    query_text = query.query.strip()
    ads = load_ads()
    results = []

    if query_text.isdigit():
        idx = int(query_text)
        if idx < len(ads):
            ads_to_show = [(idx, ads[idx])]
        else:
            ads_to_show = list(enumerate(ads))
    else:
        ads_to_show = list(enumerate(ads))

    for i, ad in ads_to_show:
        caption = ad.get("message", "")
        image_file_id = ad.get("image_file_id", "")
        image_url = ad.get("image_url", "")

        keyboard = []
        url_buttons = ad.get("url_buttons", [])
        for btn in url_buttons:
            keyboard.append([InlineKeyboardButton(btn["text"], url=btn["url"])])
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None

        if image_file_id:
            from telegram import InlineQueryResultCachedPhoto
            result = InlineQueryResultCachedPhoto(
                id=str(i),
                photo_file_id=image_file_id,
                title=ad.get("name", f"广告{i+1}"),
                caption=caption,
                parse_mode="HTML",
                reply_markup=reply_markup
            )
            results.append(result)
        elif image_url:
            result = InlineQueryResultPhoto(
                id=str(i),
                photo_url=image_url,
                thumbnail_url=image_url,
                title=ad.get("name", f"广告{i+1}"),
                caption=caption,
                parse_mode="HTML",
                reply_markup=reply_markup
            )
            results.append(result)
        else:
            result = InlineQueryResultArticle(
                id=str(i),
                title=ad.get("name", f"广告{i+1}"),
                description=caption[:100],
                input_message_content=InputTextMessageContent(
                    message_text=caption,
                    parse_mode="HTML"
                ),
                reply_markup=reply_markup
            )
            results.append(result)

    await query.answer(results, cache_time=5, is_personal=True)


# ============ Web API 路由 ============
routes = web.RouteTableDef()
# 批量导入模块
from batch_import import register_batch_import_routes


@routes.get("/")
async def index(request):
    """前端页面"""
    frontend_path = BASE_DIR / "frontend" / "index.html"
    if frontend_path.exists():
        resp = web.FileResponse(frontend_path)
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp
    return web.Response(text="tg_share_v2 running", content_type="text/html")


@web.middleware
async def auth_middleware(request, handler):
    """鉴权中间件: 放行登录端点与静态资源, 其余 /api/* 需有效 token"""
    path = request.path
    if path == "/api/login" or path.startswith("/static/") or not path.startswith("/api/"):
        return await handler(request)
    if not auth.verify_token(request.headers.get("X-Auth-Token", "")):
        return web.json_response({"ok": False, "error": "未授权或登录已过期, 请重新登录"}, status=401)
    return await handler(request)


@routes.post("/api/login")
async def api_login(request):
    """管理员登录, 校验通过返回 token"""
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "请求格式错误"}, status=400)
    username = body.get("username", "")
    password = body.get("password", "")
    if auth.verify_credentials(username, password):
        return web.json_response({"ok": True, "token": auth.create_token(username)})
    return web.json_response({"ok": False, "error": "用户名或密码错误"}, status=401)


@routes.get("/api/status")
async def api_status(request):
    try:
        ensure_daily_reset()
    except Exception:
        pass
    """系统状态"""
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.5)
    stats = load_json(STATS_FILE)
    # 总数从配置文件读，在线数从内存连接状态读
    cfg_workers = load_json(WORKERS_CONFIG_FILE).get("workers", [])
    workers_total = len(cfg_workers)
    workers_connected = sum(1 for w in workers.values() if getattr(w, "_connected", False))
    healthy_count = sum(1 for w in cfg_workers if int(w.get("rate_limit_count") or 0) == 0 and not w.get("is_restricted") and not w.get("is_dead"))
    limited_count = sum(1 for w in cfg_workers if int(w.get("rate_limit_count") or 0) > 0 or w.get("is_restricted"))
    health_percent = round(healthy_count / workers_total * 100, 1) if workers_total > 0 else 0
    return web.json_response({
        "status": "running",
        "memory_percent": mem.percent,
        "memory_used_mb": round(mem.used / 1024 / 1024, 1),
        "memory_total_mb": round(mem.total / 1024 / 1024, 1),
        "cpu_percent": cpu,
        "workers_total": workers_total,
        "workers_connected": sum(1 for w in workers.values() if getattr(w, "_connected", False) and not w.is_in_cooldown() and not getattr(w, "is_restricted", False) and not getattr(w, "is_dead", False)),
        "workers_cooling": sum(1 for w in workers.values() if w.is_in_cooldown()),
        "workers_connected_raw": workers_connected,
        "healthy_count": healthy_count,
        "limited_count": limited_count,
        "health_percent": health_percent,
        "total_sends": stats.get("total_sends", 0),
        "today_sends": stats.get("today_sends", 0),
        "uptime": time.time() - stats.get("start_time", time.time())
    })


# --- 水军管理 ---
@routes.get("/api/workers")
async def api_workers_list(request):
    try:
        ensure_daily_reset()
    except Exception:
        pass
    """获取水军列表"""
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    # 附加运行时状态（中文显示；冷却不计入在线；系统未运行时也正确）
    now_ts = time.time()
    status_cn = {
        "idle": "空闲", "cooldown": "冷却中", "working": "工作中",
        "restricted": "已限制", "dead": "死号", "offline": "离线",
        "banned_24h": "24h禁止", "error": "错误", "connected": "在线",
        "pending_login": "待登录"
    }
    for w in worker_list:
        wid = w["id"]
        cfg_cd = float(w.get("cooldown_until") or 0)
        in_cd_cfg = cfg_cd > now_ts
        if wid in workers:
            mem = workers[wid]
            in_cd = in_cd_cfg or mem.is_in_cooldown()
            le = (getattr(mem, "last_error", "") or "").lower()
            if "floodwait" in le or "wait of" in le:
                in_cd = True
            if "frozen" in le or getattr(mem, "is_frozen", False):
                w["is_restricted"] = True
                w["is_frozen"] = True
                in_cd = False
            connected = bool(getattr(mem, "_connected", False)) and not in_cd and not w.get("is_restricted")
            if in_cd:
                st = "冷却中"
            elif getattr(mem, "is_dead", False):
                st = "死号"
            elif getattr(mem, "is_restricted", False):
                st = "已限制"
            elif connected:
                st = "在线"
            else:
                raw = getattr(mem, "status", "") or "offline"
                st = status_cn.get(raw, raw if raw else "离线")
            w["runtime_status"] = st
            w["connected"] = connected
            w["daily_sends"] = getattr(mem, "daily_sends", 0)
            w["last_error"] = getattr(mem, "last_error", "")
            w["is_restricted"] = getattr(mem, "is_restricted", False)
            w["is_dead"] = getattr(mem, "is_dead", False)
            w["restricted_at"] = getattr(mem, "restricted_at", "") or ""
            w["restricted_reason"] = getattr(mem, "restricted_reason", "")
            w["rate_limit_count"] = getattr(mem, "rate_limit_count", 0)
            w["has_been_banned_24h"] = getattr(mem, "has_been_banned_24h", False)
            w["in_cooldown"] = in_cd
            w["cooldown_remaining"] = max(0, int((mem.cooldown_until if mem.is_in_cooldown() else cfg_cd) - now_ts)) if in_cd else 0
            w["skip_auto_connect"] = w.get("skip_auto_connect", False)
            w["marked_restricted_or_cooldown"] = w.get("marked_restricted_or_cooldown", False)
        else:
            # 未在内存中：从配置判断冷却/限制
            in_cd = in_cd_cfg
            rate = int(w.get("rate_limit_count") or 0)
            reason = w.get("restricted_reason") or w.get("last_error") or ""
            # 冷却已过期且非真正受限的，清除标记，恢复可调度
            truly_blocked = bool(w.get("is_restricted") or w.get("is_dead") or w.get("is_frozen") or (rate >= 10))
            if not in_cd and w.get("marked_restricted_or_cooldown") and not truly_blocked:
                w["marked_restricted_or_cooldown"] = False
                w["skip_auto_connect"] = False
            if in_cd:
                rem = max(0, int(cfg_cd - now_ts))
                w["runtime_status"] = "冷却中"
                w["in_cooldown"] = True
                w["cooldown_remaining"] = rem
                if not reason:
                    reason = f"平台冷却中，剩余 {rem//3600}小时{(rem%3600)//60}分钟"
                w["restricted_reason"] = reason
                w["is_restricted"] = False
            else:
                w["runtime_status"] = "离线"
                w["in_cooldown"] = False
                w["cooldown_remaining"] = 0
                w["is_restricted"] = bool(w.get("is_restricted"))
                if truly_blocked and not reason:
                    reason = "平台限制" + (f"（{rate}次）" if rate else "")
                elif rate > 0 and not reason:
                    reason = f"历史限制 {rate} 次（冷却已结束，可调度）"
                w["restricted_reason"] = reason
                w["schedulable"] = not truly_blocked and not in_cd
            w["connected"] = False
            w["is_dead"] = bool(w.get("is_dead"))
            w["rate_limit_count"] = rate
            w["has_been_banned_24h"] = bool(w.get("has_been_banned_24h"))
            w["daily_sends"] = 0
            w["last_error"] = w.get("last_error", "") or reason
    # 附加限制记录
    for w in worker_list:
        w["restriction_count"] = w.get("rate_limit_count", 0)
    return web.json_response(worker_list)


@routes.post("/api/workers/add")
async def api_worker_add(request):
    body = await request.json()
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    proxy_data = load_json(PROXY_POOL_FILE)
    proxies = proxy_data.get("proxies", proxy_data if isinstance(proxy_data, list) else [])
    active_proxies = [p for p in proxies if p.get("status") != "disabled"][:20]
    group_counts = {i: 0 for i in range(len(active_proxies))}
    for w in worker_list:
        g = w.get("group_no")
        if isinstance(g, int) and 1 <= g <= len(active_proxies):
            group_counts[g - 1] = group_counts.get(g - 1, 0) + 1
    min_group = min(group_counts, key=group_counts.get) if group_counts else 0
    assigned_proxy = active_proxies[min_group] if active_proxies else None
    group_no = min_group + 1
    new_worker = {
        "id": "w_" + str(int(time.time())) + "_" + str(random.randint(100,999)),
        "phone": body["phone"],
        "bot_token": body.get("bot_token", ""),
        "bot_username": body.get("bot_username", ""),
        "session_name": body["phone"].replace("+", ""),
        "proxy": None,
        "group_no": group_no,
        "status": "pending_login",
        "created_at": datetime.now().isoformat()
    }
    if assigned_proxy:
        new_worker["proxy"] = {
            "type": assigned_proxy.get("type", "socks5"),
            "host": assigned_proxy.get("host"),
            "port": assigned_proxy.get("port"),
            "username": assigned_proxy.get("username", ""),
            "password": assigned_proxy.get("password", "")
        }
        assigned_bots = list(assigned_proxy.get("assigned_bots") or [])
        if new_worker["id"] not in assigned_bots:
            assigned_bots.append(new_worker["id"])
            assigned_proxy["assigned_bots"] = assigned_bots
        save_json(PROXY_POOL_FILE, {"proxies": proxies})
    worker_list.append(new_worker)
    save_json(WORKERS_CONFIG_FILE, {"workers": worker_list})
    logger.info("[add worker] " + body["phone"] + " assigned to group " + str(group_no))
    return web.json_response({"ok": True, "worker": new_worker, "group": group_no})


@routes.post("/api/workers/{worker_id}/connect")
async def api_worker_connect(request):
    """连接水军号"""
    worker_id = request.match_info["worker_id"]
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    wconfig = next((w for w in worker_list if w["id"] == worker_id), None)

    if not wconfig:
        return web.json_response({"ok": False, "error": "Worker不存在"}, status=404)

    bot_config = load_json(BOT_CONFIG_FILE)
    api_id = bot_config.get("api_id")
    api_hash = bot_config.get("api_hash")

    if not api_id or not api_hash:
        return web.json_response({"ok": False, "error": "请先配置API ID和API Hash"}, status=400)

    # 分配代理
    proxy = proxy_pool.get_proxy_for_worker(worker_id)
    if proxy:
        wconfig["proxy"] = {
            "type": proxy["type"],
            "host": proxy["host"],
            "port": proxy["port"],
            "username": proxy.get("username", ""),
            "password": proxy.get("password", "")
        }

    worker = ShareWorker(wconfig, api_id, api_hash)
    worker._log_callback = log_step
    worker._on_dead_detected = _on_dead_detected
    worker._on_rate_limit_changed = _persist_worker_rate_limit
    success = await worker.connect()

    if success:
        workers[worker_id] = worker
        return web.json_response({"ok": True, "message": "连接成功"})
    else:
        return web.json_response({"ok": False, "error": worker.last_error}, status=400)



@routes.post("/api/workers/connect-all")
async def api_workers_connect_all(request):
    """一键连接所有水军号（受 MAX_CONCURRENT_CONNECTIONS 限制，防止内存爆死）"""
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    bot_config = load_json(BOT_CONFIG_FILE)
    api_id = bot_config.get("api_id")
    api_hash = bot_config.get("api_hash")
    if not api_id or not api_hash:
        return web.json_response({"ok": False, "error": "请先配置API ID和API Hash"}, status=400)

    results = []
    success_count = 0
    fail_count = 0
    skipped_count = 0

    for wconfig in worker_list:
        worker_id = wconfig["id"]
        if worker_id in workers and workers[worker_id]._connected:
            results.append({"id": worker_id, "phone": wconfig["phone"], "status": "already_connected"})
            success_count += 1
            continue

        connected_now = sum(1 for w in workers.values() if getattr(w, "_connected", False))
        if connected_now >= get_max_online():
            results.append({
                "id": worker_id,
                "phone": wconfig["phone"],
                "status": "skipped",
                "error": f"已达最大并发连接数 {MAX_CONCURRENT_CONNECTIONS}，请先断开部分后再连接"
            })
            skipped_count += 1
            continue

        mem = psutil.virtual_memory()
        if mem.percent > MEMORY_THRESHOLD:
            logger.warning(f"[connect-all] 内存 {mem.percent}% 过高，清理空闲连接后继续")
            await cleanup_connections()
            await asyncio.sleep(5)
            mem = psutil.virtual_memory()
            if mem.percent > MEMORY_THRESHOLD:
                results.append({
                    "id": worker_id,
                    "phone": wconfig["phone"],
                    "status": "skipped",
                    "error": f"内存使用过高({mem.percent}%)，已停止连接"
                })
                skipped_count += 1
                continue

        proxy = proxy_pool.get_proxy_for_worker(worker_id)
        if proxy:
            wconfig["proxy"] = {
                "type": proxy["type"],
                "host": proxy["host"],
                "port": proxy["port"],
                "username": proxy.get("username", ""),
                "password": proxy.get("password", "")
            }

        worker._log_callback = log_step
        worker = ShareWorker(wconfig, api_id, api_hash)
        worker._on_dead_detected = _on_dead_detected
        worker._on_rate_limit_changed = _persist_worker_rate_limit
        try:
            success = await worker.connect()
            if success:
                workers[worker_id] = worker
                results.append({"id": worker_id, "phone": wconfig["phone"], "status": "connected"})
                success_count += 1
                await asyncio.sleep(CONNECTION_INTERVAL)
            else:
                results.append({"id": worker_id, "phone": wconfig["phone"], "status": "failed", "error": worker.last_error})
                fail_count += 1
        except Exception as e:
            results.append({"id": worker_id, "phone": wconfig["phone"], "status": "failed", "error": str(e)})
            fail_count += 1

    return web.json_response({
        "ok": True,
        "message": f"连接完成: {success_count}成功, {fail_count}失败, {skipped_count}跳过(达上限/内存保护)",
        "success_count": success_count,
        "fail_count": fail_count,
        "skipped_count": skipped_count,
        "results": results
    })

@routes.post("/api/workers/{worker_id}/login")
async def api_worker_login(request):
    """水军号登录（发送验证码）"""
    worker_id = request.match_info["worker_id"]
    body = await request.json()
    phone = body.get("phone", "")
    code = body.get("code", "")
    password = body.get("password", "")

    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    wconfig = next((w for w in worker_list if w["id"] == worker_id), None)

    if not wconfig:
        return web.json_response({"ok": False, "error": "Worker不存在"}, status=404)

    bot_config = load_json(BOT_CONFIG_FILE)
    api_id = bot_config.get("api_id")
    api_hash = bot_config.get("api_hash")

    if not api_id or not api_hash:
        return web.json_response({"ok": False, "error": "请先配置API ID和API Hash"}, status=400)

    from telethon import TelegramClient
    session_path = os.path.join(str(SESSIONS_DIR), wconfig["session_name"])

    # 分配代理
    proxy_config = None
    proxy = proxy_pool.get_proxy_for_worker(worker_id)
    if proxy:
        try:
            import python_socks
            proxy_type_str = proxy.get("type", "socks5")
            if proxy_type_str == "socks5":
                p_type = python_socks.ProxyType.SOCKS5
            elif proxy_type_str == "socks4":
                p_type = python_socks.ProxyType.SOCKS4
            else:
                p_type = python_socks.ProxyType.HTTP
            proxy_config = {
                'proxy_type': p_type,
                'addr': proxy["host"],
                'port': int(proxy["port"]),
            }
            if proxy.get("username"):
                proxy_config['username'] = proxy["username"]
                proxy_config['password'] = proxy.get("password", "")
        except ImportError:
            import socks
            proxy_type = socks.SOCKS5
            proxy_config = (proxy_type, proxy["host"], int(proxy["port"]))
            if proxy.get("username"):
                proxy_config = (proxy_type, proxy["host"], int(proxy["port"]),
                              True, proxy["username"], proxy.get("password", ""))

    client = TelegramClient(session_path, api_id, api_hash, proxy=proxy_config)
    await client.connect()

    if not code:
        # 发送验证码
        try:
            result = await client.send_code_request(phone or wconfig["phone"])
            # 保存phone_code_hash
            wconfig["_phone_code_hash"] = result.phone_code_hash
            save_json(WORKERS_CONFIG_FILE, {"workers": worker_list})
            await client.disconnect()
            return web.json_response({"ok": True, "step": "code_sent", "message": "验证码已发送"})
        except Exception as e:
            await client.disconnect()
            return web.json_response({"ok": False, "error": str(e)}, status=400)
    else:
        # 验证码登录
        try:
            phone_code_hash = wconfig.get("_phone_code_hash", "")
            await client.sign_in(
                phone=phone or wconfig["phone"],
                code=code,
                phone_code_hash=phone_code_hash
            )
            wconfig["status"] = "active"
            if "_phone_code_hash" in wconfig:
                del wconfig["_phone_code_hash"]
            save_json(WORKERS_CONFIG_FILE, {"workers": worker_list})
            await client.disconnect()
            return web.json_response({"ok": True, "step": "logged_in", "message": "登录成功"})
        except Exception as e:
            err_str = str(e)
            if "password" in err_str.lower() or "2fa" in err_str.lower():
                if password:
                    try:
                        await client.sign_in(password=password)
                        wconfig["status"] = "active"
                        save_json(WORKERS_CONFIG_FILE, {"workers": worker_list})
                        await client.disconnect()
                        return web.json_response({"ok": True, "step": "logged_in", "message": "登录成功"})
                    except Exception as e2:
                        await client.disconnect()
                        return web.json_response({"ok": False, "error": str(e2)}, status=400)
                else:
                    await client.disconnect()
                    return web.json_response({"ok": True, "step": "need_2fa", "message": "需要两步验证密码"})
            await client.disconnect()
            return web.json_response({"ok": False, "error": err_str}, status=400)


# --- 广告管理 ---
@routes.get("/api/ads")
async def api_ads_list(request):
    ads = load_ads()
    return web.json_response(ads)


@routes.post("/api/ads")
async def api_ads_add(request):
    body = await request.json()
    ads = load_ads()
    ad = {
        "id": f"ad_{int(time.time())}",
        "name": body.get("name", f"广告{len(ads)+1}"),
        "message": body.get("message", ""),
        "image_url": body.get("image_url", ""),
        "image_file_id": body.get("image_file_id", ""),
        "url_buttons": body.get("url_buttons", []),
        "created_at": datetime.now().isoformat()
    }
    ads.append(ad)
    save_ads(ads)
    return web.json_response({"ok": True, "ad": ad})


@routes.delete("/api/ads/{ad_id}")
async def api_ads_delete(request):
    ad_id = request.match_info["ad_id"]
    ads = load_ads()
    ads = [a for a in ads if a.get("id") != ad_id]
    save_ads(ads)
    return web.json_response({"ok": True})


# --- 目标用户管理 ---
@routes.get("/api/targets")
async def api_targets_list(request):
    data = load_json(TARGETS_FILE)
    targets = data.get("targets", [])
    # 支持服务端分页
    page = int(request.query.get("page", "0"))
    per_page = int(request.query.get("per_page", "0"))
    status_filter = request.query.get("status", "")
    # 统计
    total = len(targets)
    pending = sum(1 for t in targets if t.get("status") == "pending")
    sent = sum(1 for t in targets if t.get("status") == "sent")
    failed = sum(1 for t in targets if t.get("status") == "failed")
    # 排序：pending在前，sent按时间倒序，failed按时间倒序
    pending_list = [t for t in targets if t.get("status") == "pending"]
    sent_list = sorted([t for t in targets if t.get("status") == "sent"], key=lambda t: t.get("sent_at") or "", reverse=True)
    failed_list = sorted([t for t in targets if t.get("status") == "failed"], key=lambda t: t.get("sent_at") or "", reverse=True)
    # 过滤
    if status_filter == "pending":
        sorted_targets = pending_list
    elif status_filter == "sent":
        sorted_targets = sent_list
    elif status_filter == "failed":
        sorted_targets = failed_list
    else:
        sorted_targets = pending_list + sent_list + failed_list
    filtered_total = len(sorted_targets)
    # 分页
    if page > 0 and per_page > 0:
        start = (page - 1) * per_page
        end = start + per_page
        page_targets = sorted_targets[start:end]
    else:
        page_targets = sorted_targets
    return web.json_response({
        "targets": page_targets,
        "total": total,
        "filtered_total": filtered_total,
        "pending": pending,
        "sent": sent,
        "failed": failed,
        "page": page,
        "per_page": per_page
    })

@routes.post("/api/targets")
async def api_targets_add(request):
    body = await request.json()
    data = load_json(TARGETS_FILE)
    targets = data.get("targets", [])
    new_targets = body.get("usernames", [])
    for username in new_targets:
        clean = username.strip().lstrip("@")
        if clean and clean not in [t["username"] for t in targets]:
            targets.append({
                "username": clean,
                "status": "pending",
                "sent_at": None,
                "result": None
            })
    save_json(TARGETS_FILE, {"targets": targets})
    return web.json_response({"ok": True, "count": len(targets)})


@routes.delete("/api/targets/all")
async def api_targets_delete_all(request):
    save_json(TARGETS_FILE, {"targets": []})
    return web.json_response({"ok": True, "message": "所有目标用户已删除"})

@routes.delete("/api/targets/{username}")
async def api_targets_delete(request):
    username = request.match_info["username"]
    data = load_json(TARGETS_FILE)
    targets = data.get("targets", [])
    targets = [t for t in targets if t["username"] != username]
    save_json(TARGETS_FILE, {"targets": targets})
    return web.json_response({"ok": True})


# --- IP代理池管理 ---
@routes.get("/api/proxies")
async def api_proxies_list(request):
    return web.json_response(proxy_pool.get_all())


@routes.post("/api/proxies")
async def api_proxies_add(request):
    body = await request.json()
    proxy = proxy_pool.add_proxy(
        host=body["host"],
        port=body["port"],
        proxy_type=body.get("type", "socks5"),
        username=body.get("username", ""),
        password=body.get("password", "")
    )
    return web.json_response({"ok": True, "proxy": proxy})


@routes.delete("/api/proxies/{proxy_id}")
async def api_proxies_delete(request):
    proxy_id = request.match_info["proxy_id"]
    proxy_pool.remove_proxy(proxy_id)
    return web.json_response({"ok": True})


@routes.post("/api/proxies/auto-assign")
async def api_proxies_auto_assign(request):
    """一键分配IP：每条IP分配5个水军 + 40个Bot"""
    body = await request.json()
    workers_per_proxy = body.get("workers_per_proxy", 5)
    bots_per_proxy = body.get("bots_per_proxy", 40)
    # 兼容旧参数
    if "per_proxy" in body:
        workers_per_proxy = body["per_proxy"]
    data = load_json(WORKERS_CONFIG_FILE)
    worker_ids = [w["id"] for w in data.get("workers", [])]
    # 获取Bot列表
    bots_data = load_json(BOTS_FILE)
    bot_ids = [b.get("username", b.get("name", "")) for b in bots_data.get("bots", [])]
    result = proxy_pool.auto_assign(worker_ids, bot_ids, workers_per_proxy, bots_per_proxy)
    return web.json_response({"ok": True, **result})


@routes.post("/api/proxies/batch")
async def api_proxies_batch_import(request):
    """批量导入代理 - 支持格式: host:port:username:password"""
    body = await request.json()
    lines = body.get("proxies", "").strip().split("\n")
    proxy_type = body.get("type", "http")
    added = 0
    errors = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) == 4:
            host, port, username, password = parts
        elif len(parts) == 2:
            host, port = parts
            username, password = "", ""
        else:
            errors.append(f"格式错误: {line}")
            continue
        try:
            proxy_pool.add_proxy(host=host, port=int(port), proxy_type=proxy_type, username=username, password=password)
            added += 1
        except Exception as e:
            errors.append(f"{line}: {str(e)}")
    return web.json_response({"ok": True, "added": added, "errors": errors})


# --- 系统设置 ---

# === Bot管理 ===
# 统一使用 config.BOTS_FILE
RESTRICTIONS_FILE = DATA_DIR / "restrictions.json"

def load_restrictions():
    """加载水军+Bot组合的限制记录"""
    data = load_json(RESTRICTIONS_FILE)
    if not data:
        data = {"records": {}}
    return data

def save_restrictions(data):
    """保存限制记录"""
    save_json(RESTRICTIONS_FILE, data)


def auto_delete_dead_worker(worker_phone):
    """限制10次：只标记受限，不删除，保持显示"""
    logger.warning(f"[标记受限] 水军号 {worker_phone} 被限制达到10次，标记为受限（不删除）")
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    for w in worker_list:
        if w.get("phone") == worker_phone:
            w["is_restricted"] = True
            w["restricted_reason"] = "平台限制达到10次"
            w["status"] = "restricted"
            break
    save_json(WORKERS_CONFIG_FILE, data)
    return  # 不删除，保持显示
    # 删除session文件
    session_name = worker_phone.replace("+", "")
    session_path = SESSIONS_DIR / f"{session_name}.session"
    journal_path = SESSIONS_DIR / f"{session_name}.session-journal"
    if session_path.exists():
        session_path.unlink()
        logger.info(f"[自动删除] 已删除session: {session_path}")
    if journal_path.exists():
        journal_path.unlink()
    logger.info(f"[自动删除] 水军号 {worker_phone} 已从系统中完全删除")


def _on_dead_detected(worker_id, phone, reason):
    """检测到死亡：标记受限，不删除，保持显示"""
    logger.warning(f"[标记死号] 水军号 {phone} 被TG平台标记死亡: {reason}")
    # 标记为死号，不删除
    if worker_id in workers:
        workers[worker_id].is_dead = True
        workers[worker_id].is_restricted = True
        workers[worker_id].restricted_reason = reason
    # 持久化标记
    data = load_json(WORKERS_CONFIG_FILE)
    for w in data.get("workers", []):
        if w.get("phone") == phone or w.get("id") == worker_id:
            w["is_dead"] = True
            w["is_restricted"] = True
            w["restricted_reason"] = reason
            break
    save_json(WORKERS_CONFIG_FILE, data)
    logger.info(f"[标记死号] 水军号 {phone} 已标记为死号（保持显示）")

def record_worker_bot_failure(worker_phone, bot_username, error_msg):
    """记录水军+Bot组合的失败，超过3次标记为禁止"""
    data = load_restrictions()
    records = data.get("records", {})
    key = f"{worker_phone}|{bot_username}"
    if key not in records:
        records[key] = {"worker_phone": worker_phone, "bot_username": bot_username, "fail_count": 0, "banned": False, "errors": [], "last_fail": ""}
    records[key]["fail_count"] += 1
    records[key]["last_fail"] = __import__("datetime").datetime.now().isoformat()
    records[key]["errors"].append(error_msg[:100])
    # 只保留最近5条错误
    records[key]["errors"] = records[key]["errors"][-5:]
    # 超过3次标记为禁止
    if records[key]["fail_count"] >= 3:
        records[key]["banned"] = True
    data["records"] = records
    save_restrictions(data)
    return records[key]["banned"]

def is_worker_bot_banned(worker_phone, bot_username):
    """检查水军+Bot组合是否被禁止"""
    data = load_restrictions()
    key = f"{worker_phone}|{bot_username}"
    record = data.get("records", {}).get(key, {})
    return record.get("banned", False)

def get_worker_restrictions(worker_phone):
    """获取某个水军号的所有限制记录"""
    data = load_restrictions()
    records = data.get("records", {})
    result = []
    for key, record in records.items():
        if record.get("worker_phone") == worker_phone:
            result.append(record)
    return result

@routes.get("/api/bots")
async def api_bots_list(request):
    data = load_json(BOTS_FILE)
    bot_list = data.get("bots", [])
    bot_list.sort(key=lambda b: b.get("number", 0))
    # Bot状态只显示平台级别限制（inline disabled/restricted）
    for bot in bot_list:
        if bot.get("is_restricted"):
            bot["restriction_status"] = "platform_banned"
            bot["restriction_reason"] = bot.get("restricted_reason", "被平台检测禁止使用")
        else:
            bot["restriction_status"] = "normal"
    return web.json_response(bot_list)

@routes.post("/api/bots/batch")
async def api_bots_batch_import(request):
    body = await request.json()
    tokens_text = body.get("tokens", "")
    data = load_json(BOTS_FILE)
    bot_list = data.get("bots", [])
    existing_tokens = {b["token"] for b in bot_list}
    added = 0
    errors = []
    lines = [l.strip() for l in tokens_text.strip().split("\n") if l.strip()]
    for line in lines:
        token = line
        if token in existing_tokens:
            continue
        if ":" not in token:
            errors.append("格式错误: " + line[:30])
            continue
        bot_id_str = token.split(":")[0]
        existing_numbers = [b.get("number", 0) for b in bot_list]
        next_number = max(existing_numbers) + 1 if existing_numbers else 1
        new_bot = {
            "id": "bot_" + bot_id_str,
            "token": token,
            "bot_id": bot_id_str,
            "username": "",
            "number": next_number,
            "status": "pending",
            "enabled": True,
            "total_sends": 0,
            "success_sends": 0,
            "fail_sends": 0
        }
        bot_list.append(new_bot)
        existing_tokens.add(token)
        added += 1
    save_json(BOTS_FILE, {"bots": bot_list})
    return web.json_response({"ok": True, "added": added, "total": len(bot_list), "errors": errors})

@routes.post("/api/bots/{bot_id}/verify")
async def api_bot_verify(request):
    bot_id = request.match_info["bot_id"]
    data = load_json(BOTS_FILE)
    bot_list = data.get("bots", [])
    bot = next((b for b in bot_list if b["id"] == bot_id), None)
    if not bot:
        return web.json_response({"ok": False, "error": "Bot不存在"}, status=404)
    import aiohttp
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"https://api.telegram.org/bot{bot['token']}/getMe", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                result = await resp.json()
                if result.get("ok"):
                    bot_info = result["result"]
                    bot["username"] = bot_info.get("username", "")
                    bot["status"] = "active"
                    bot["enabled"] = True
                    save_json(BOTS_FILE, {"bots": bot_list})
                    return web.json_response({"ok": True, "username": bot["username"]})
                else:
                    bot["status"] = "invalid"
                    save_json(BOTS_FILE, {"bots": bot_list})
                    return web.json_response({"ok": False, "error": result.get("description", "Token无效")})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)})

@routes.post("/api/bots/verify-all")
async def api_bots_verify_all(request):
    data = load_json(BOTS_FILE)
    bot_list = data.get("bots", [])
    import aiohttp
    results = {"active": 0, "invalid": 0, "error": 0}
    async with aiohttp.ClientSession() as session:
        for bot in bot_list:
            try:
                async with session.get(f"https://api.telegram.org/bot{bot['token']}/getMe", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    result = await resp.json()
                    if result.get("ok"):
                        bot["username"] = result["result"].get("username", "")
                        bot["status"] = "active"
                        bot["enabled"] = True
                        results["active"] += 1
                    else:
                        bot["status"] = "invalid"
                        bot["enabled"] = False
                        results["invalid"] += 1
            except:
                bot["status"] = "error"
                results["error"] += 1
    save_json(BOTS_FILE, {"bots": bot_list})
    return web.json_response({"ok": True, "results": results})

@routes.delete("/api/bots/{bot_id}")
async def api_bot_delete(request):
    bot_id = request.match_info["bot_id"]
    data = load_json(BOTS_FILE)
    bot_list = data.get("bots", [])
    bot_list = [b for b in bot_list if b["id"] != bot_id]
    save_json(BOTS_FILE, {"bots": bot_list})
    return web.json_response({"ok": True})

@routes.delete("/api/workers/{worker_id}")
async def api_worker_delete(request):
    """删除水军号"""
    worker_id = request.match_info["worker_id"]
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    original_len = len(worker_list)
    worker_list = [w for w in worker_list if w["id"] != worker_id]
    if len(worker_list) == original_len:
        return web.json_response({"ok": False, "error": "Worker不存在"}, status=404)
    save_json(WORKERS_CONFIG_FILE, {"workers": worker_list})
    # 删除session文件
    import glob
    for f in glob.glob(str(SESSIONS_DIR / "*")):
        if worker_id.split("_")[1] in f or worker_id.split("_")[2] in f:
            try:
                os.remove(f)
            except:
                pass
    # 从workers字典中移除
    if worker_id in workers:
        try:
            await workers[worker_id].disconnect()
        except:
            pass
        del workers[worker_id]
    return web.json_response({"ok": True, "message": "删除成功"})

@routes.get("/api/restrictions")
async def api_restrictions_list(request):
    """获取所有限制记录"""
    data = load_restrictions()
    records = list(data.get("records", {}).values())
    # 按fail_count降序排列
    records.sort(key=lambda r: r.get("fail_count", 0), reverse=True)
    return web.json_response(records)


@routes.post("/api/workers/clear-errors")
async def api_workers_clear_errors(request):
    """清除所有水军的last_error"""
    cleared = 0
    for wid, w in workers.items():
        if w.last_error:
            w.last_error = ""
            cleared += 1
    return web.json_response({"ok": True, "message": f"已清除 {cleared} 个水军的错误信息"})

@routes.post("/api/restrictions/reset")
async def api_restrictions_reset(request):
    """重置限制记录"""
    body = await request.json()
    worker_phone = body.get("worker_phone", "")
    bot_username = body.get("bot_username", "")
    data = load_restrictions()
    if worker_phone and bot_username:
        key = f"{worker_phone}|{bot_username}"
        if key in data.get("records", {}):
            del data["records"][key]
    elif worker_phone:
        # 重置某个水军号的所有记录
        data["records"] = {k: v for k, v in data.get("records", {}).items() if v.get("worker_phone") != worker_phone}
    else:
        # 重置所有
        data["records"] = {}
    save_restrictions(data)
    return web.json_response({"ok": True})

@routes.post("/api/workers/assign-bots")
async def api_workers_assign_bots(request):
    """[Deprecated] 现在使用全局Bot池轮换，不再固定分配"""
    return web.json_response({"ok": True, "message": "当前使用全局Bot池轮换机制，无需固定分配"})

@routes.post("/api/workers/assign-bot")
async def api_workers_assign_bot(request):
    """[Deprecated] 现在使用全局Bot池轮换，不再固定分配"""
    return web.json_response({"ok": True, "message": "当前使用全局Bot池轮换机制，无需固定分配"})

@routes.get("/api/config")
async def api_config_get(request):
    config = load_json(BOT_CONFIG_FILE)
    # 不返回敏感信息
    safe_config = {
        "api_id": config.get("api_id", ""),
        "api_hash": "***" if config.get("api_hash") else "",
        "bot_token": "***" if config.get("bot_token") else "",
        "bot_username": config.get("bot_username", ""),
        "daily_limit": config.get("daily_limit", 30),
        "send_interval_min": config.get("send_interval_min", 180),
        "send_interval_max": config.get("send_interval_max", 300),
    }
    return web.json_response(safe_config)


@routes.post("/api/config")
async def api_config_update(request):
    body = await request.json()
    config = load_json(BOT_CONFIG_FILE)
    for key in ["api_id", "api_hash", "bot_token", "bot_username",
                "daily_limit", "send_interval_min", "send_interval_max"]:
        if key in body:
            config[key] = body[key]
    save_json(BOT_CONFIG_FILE, config)
    return web.json_response({"ok": True})


# --- 发送任务 ---
@routes.post("/api/send/start")
async def api_send_start(request):
    """开始发送任务"""
    global scheduler_task, _manual_stopped
    if scheduler_task and not scheduler_task.done():
        return web.json_response({"ok": False, "error": "任务已在运行中"})
    _manual_stopped = False
    try:
        if os.path.exists("/tmp/tg_share_v2_sending.done"):
            os.remove("/tmp/tg_share_v2_sending.done")
    except Exception:
        pass
    scheduler_task = asyncio.create_task(run_send_scheduler())
    return web.json_response({"ok": True, "message": "发送任务已启动"})


@routes.post("/api/send/stop")
async def api_send_stop(request):
    """停止发送任务 — 任何情况都上报"""
    global scheduler_task, reconnect_task, _manual_stopped
    _manual_stopped = True
    disconnected = 0
    try:
        if scheduler_task and not scheduler_task.done():
            scheduler_task.cancel()
            try:
                await scheduler_task
            except Exception:
                pass
        scheduler_task = None
        if reconnect_task and not reconnect_task.done():
            reconnect_task.cancel()
            try:
                await reconnect_task
            except Exception:
                pass
        reconnect_task = None
        for wid in list(workers.keys()):
            try:
                if getattr(workers[wid], "_connected", False):
                    await workers[wid].disconnect()
                    disconnected += 1
            except Exception:
                pass
        logger.info(f"[调度器停止] 已断开 {disconnected} 个水军连接")
    finally:
        # 无论上面是否异常，都必须上报
        try:
            st = _today_send_stats()
            await send_panel_notify(
                f"【旧面板】发送已停止（手动）\n"
                f"已断开水军: {disconnected}\n"
                f"今日成功: {st['today']} 条\n"
                f"累计成功: {st['total']} 条\n"
                f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        except Exception as e:
            logger.warning(f"停止通知 finally 失败: {e}")
    
        try:
            open("/tmp/tg_share_v2_sending.done", "w").write("manual_stop")
            if os.path.exists("/tmp/tg_share_v2_sending"):
                os.remove("/tmp/tg_share_v2_sending")
        except Exception:
            pass

    return web.json_response({"ok": True, "message": f"发送任务已停止，已断开 {disconnected} 个水军连接"})


@routes.get("/api/send/status")
async def api_send_status(request):
    try:
        ensure_daily_reset()
    except Exception:
        pass
    """发送任务状态 - 包含工作流信息"""
    running = scheduler_task is not None and not scheduler_task.done()
    # 统计在线水军
    connected_workers = [(wid, w) for wid, w in workers.items() if w._connected]
    cooldown_workers = [(wid, w) for wid, w in workers.items() if w.is_in_cooldown()]
    # 加载统计
    stats = load_json(STATS_FILE)
    return web.json_response({
        "running": running,
        "connected_workers": len(connected_workers),
        "cooldown_workers": len(cooldown_workers),
        "today_sends": stats.get("today_sends", 0),
        "total_sends": stats.get("total_sends", 0),
        "current_activity": current_activity,
        "recent_logs": activity_log[-20:]  # 最近20条
    })

@routes.get("/api/send/activity")
async def api_send_activity(request):
    """获取完整活动日志"""
    return web.json_response({
        "current": current_activity,
        "logs": activity_log[-50:],
        "connected_workers": [(wid, workers[wid].phone if hasattr(workers[wid], 'phone') else wid) for wid, w in workers.items() if w._connected]
    })


# ============ 发送调度器 ============

async def _auto_reconnect_loop():
    """后台自动重连循环：每60秒检查所有水军连接状态，掉线自动重连"""
    config = load_json(BOT_CONFIG_FILE)
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    if not api_id or not api_hash:
        return
    while True:
        await asyncio.sleep(60)  # 每60秒检查一次
        workers_data = load_json(WORKERS_CONFIG_FILE)
        worker_configs = workers_data.get("workers", [])
        reconnected = 0
        for wc in worker_configs:
            wid = wc["id"]
            # 跳过已死的
            if wid in workers and getattr(workers[wid], 'is_dead', False):
                continue
            # 检查是否掉线
            if wid in workers and workers[wid]._connected:
                continue  # 在线，跳过
            # 需要重连
            proxy = proxy_pool.get_proxy_for_worker(wid)
            if proxy:
                wc["proxy"] = {
                    "type": proxy["type"],
                    "host": proxy["host"],
                    "port": proxy["port"],
                    "username": proxy.get("username", ""),
                    "password": proxy.get("password", "")
                }
            w = ShareWorker(wc, api_id, api_hash)
            w._on_dead_detected = _on_dead_detected
            w._log_callback = log_step
            w._on_rate_limit_changed = _persist_worker_rate_limit
            try:
                ok = await w.connect()
                if ok:
                    workers[wid] = w
                    reconnected += 1
            except Exception:
                pass
            await asyncio.sleep(0.5)
        if reconnected > 0:
            logger.info(f"[自动重连] 重连了 {reconnected} 个掉线水军")


async def stall_send_monitor():
    """2号面板：长时间无成功发送则自动重启；重启后仍无成功则上报"""
    global scheduler_task, _last_auto_restart_ts, _stall_alerted_after_restart
    global _last_success_send_ts
    await asyncio.sleep(60)
    while True:
        try:
            await asyncio.sleep(60)
            try:
                targets_data = load_json(TARGETS_FILE)
                pending = [t for t in targets_data.get("targets", []) if t.get("status") == "pending"]
            except Exception:
                pending = []
            if not pending:
                continue

            now = time.time()
            last_ok = _last_success_send_ts
            running = scheduler_task is not None and not scheduler_task.done()

            if not running:
                if _manual_stopped or (os.path.exists("/tmp/tg_share_v2_sending.done") and open("/tmp/tg_share_v2_sending.done").read().strip() == "manual_stop"):
                    continue
                logger.warning(f"[{PANEL_NAME}] 有待发目标但调度未运行，自动启动发送")
                try:
                    scheduler_task = asyncio.create_task(run_send_scheduler())
                    _last_auto_restart_ts = now
                    _stall_alerted_after_restart = False
                    if last_ok <= 0:
                        _last_success_send_ts = now
                except Exception as e:
                    logger.error(f"[{PANEL_NAME}] 自动启动失败: {e}")
                continue

            ref = last_ok if last_ok > 0 else (_last_auto_restart_ts or now)
            idle_min = (now - ref) / 60.0

            if idle_min >= STALL_MINUTES:
                if not (_last_auto_restart_ts and (now - _last_auto_restart_ts) < STALL_MINUTES * 60):
                    logger.warning(f"[{PANEL_NAME}] 已 {idle_min:.1f} 分钟无成功发送，自动重启调度")
                    try:
                        if scheduler_task and not scheduler_task.done():
                            scheduler_task.cancel()
                            try:
                                await scheduler_task
                            except Exception:
                                pass
                        scheduler_task = None
                        await asyncio.sleep(2)
                        scheduler_task = asyncio.create_task(run_send_scheduler())
                        _last_auto_restart_ts = time.time()
                        _stall_alerted_after_restart = False
                    except Exception as e:
                        logger.error(f"[{PANEL_NAME}] 自动重启调度失败: {e}")

            if (
                _last_auto_restart_ts > 0
                and not _stall_alerted_after_restart
                and (now - _last_auto_restart_ts) >= STALL_AFTER_RESTART_MINUTES * 60
                and last_ok < _last_auto_restart_ts
            ):
                _stall_alerted_after_restart = True
                try:
                    st = _today_send_stats()
                    msg = (
                        f"【2号面板】长时间无发送告警\n"
                        f"原因: 自动重启调度后 {STALL_AFTER_RESTART_MINUTES} 分钟仍无成功\n"
                        f"待发目标: {len(pending)}\n"
                        f"今日成功: {st.get('today', 0)} 条\n"
                        f"累计成功: {st.get('total', 0)} 条\n"
                        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    await send_panel_notify(msg)
                    logger.warning(f"[{PANEL_NAME}] 已上报：重启后仍无成功")
                except Exception as e:
                    logger.error(f"[{PANEL_NAME}] 上报失败: {e}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"[{PANEL_NAME}] stall_send_monitor 异常: {e}")
            await asyncio.sleep(30)



async def run_send_scheduler():
    """发送调度器 - 按需连接水军号，逐个发送（带异常保护）"""
    for _attempt in range(3):
        try:
            await _run_send_scheduler_inner()
            return
        except Exception as e:
            if "database is locked" in str(e).lower() and _attempt < 2:
                logger.warning(f"[调度] database is locked，第{_attempt+1}/3次重试，等待2秒")
                await asyncio.sleep(2)
                continue
            logger.error(f"=== 发送调度器异常退出: {e} ===")
            try:
                open("/tmp/tg_share_v2_sending.done", "w").write("error:"+str(e))
                if os.path.exists("/tmp/tg_share_v2_sending"):
                    os.remove("/tmp/tg_share_v2_sending")
            except Exception:
                pass
            import traceback
            logger.error(traceback.format_exc())
            break


async def _run_send_scheduler_inner():
    try:
        ensure_daily_reset()
    except Exception:
        pass
    """发送调度器内部实现"""
    global current_activity
    current_activity = {"status": "启动中", "worker": "", "target": "", "step": "初始化"}
    log_activity("调度器启动", "发送调度器开始运行", status="info")
    logger.info("=== 发送调度器启动 ===")
    # 为所有已存在的水军设置日志回调
    for w in workers.values():
        w._log_callback = log_step
    try:
        open("/tmp/tg_share_v2_sending", "w").write("1")
        if os.path.exists("/tmp/tg_share_v2_sending.done"):
            os.remove("/tmp/tg_share_v2_sending.done")
    except Exception:
        pass



    config = load_json(BOT_CONFIG_FILE)
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    daily_limit = config.get("daily_limit", 30)
    interval_min = config.get("send_interval_min", 180)
    interval_max = config.get("send_interval_max", 300)

    if not api_id or not api_hash:
        logger.error("未配置 API ID/Hash")
        return

    # 加载目标用户
    targets_data = load_json(TARGETS_FILE)
    targets = [t for t in targets_data.get("targets", []) if t["status"] == "pending"]

    if not targets:
        logger.info("没有待发送的目标用户")
        try:
            st = _today_send_stats()
            await send_panel_notify(f"【旧面板】发送停止\n原因: 没有待发送目标\n今日成功: {st['today']} 条\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception:
            pass
        return

    # 加载水军列表
    workers_data = load_json(WORKERS_CONFIG_FILE)
    worker_configs = [w for w in workers_data.get("workers", []) if w.get("status") == "active"]

    if not worker_configs:
        logger.info("没有可用的水军号")
        return

    # 统计
    stats = load_json(STATS_FILE)
    stats["start_time"] = stats.get("start_time", time.time())

    worker_idx = 0
    # 读取上轮停止位置，继续轮询，不从第1组重来
    try:
        _rr_saved = load_json(Path(__file__).parent / "data" / "rr_index.json") or {}
    except Exception:
        _rr_saved = {}
    group_rr_index = {"idx": int(_rr_saved.get("idx", 0) or 0), "pool": int(_rr_saved.get("pool", 0) or 0)}
    worker_use_count = {}
    worker_use_count = {}  # phone -> 使用次数
    sent_count = 0

    for target in targets:
        if sent_count >= daily_limit:
            logger.info(f"达到每日限制 {daily_limit}")
            break


        async def maintain_online_pool():
            """每组只连 1 个号，组间轮换，总在线约 max_online"""
            from collections import defaultdict
            target_online = get_max_online()
            # 按组归类
            by_group = defaultdict(list)
            for wc in worker_configs:
                gk = wc.get("group") or wc.get("group_no") or wc.get("api_config_id") or "default"
                by_group[gk].append(wc)
            # 统计每组已在线数，超 1 的断开多余
            for gk, wcs in by_group.items():
                online_in_group = []
                for wc in wcs:
                    w = workers.get(wc["id"])
                    if w and getattr(w, "_connected", False) and not w.is_in_cooldown():
                        online_in_group.append(w)
                # 保留限制次数最少的 1 个，断开其余
                if len(online_in_group) > 1:
                    online_in_group.sort(key=lambda x: getattr(x, "rate_limit_count", 0))
                    for extra in online_in_group[1:]:
                        try:
                            await extra.disconnect()
                            logger.info(f"[组限制] 断开多余号 {extra.phone}（组 {gk} 已有 1 个在线）")
                        except Exception:
                            pass
            # 硬限制：总数超过 target_online 时断开多余（保留限制次数少的）
            online_all = [w for w in workers.values() if getattr(w, "_connected", False) and not w.is_in_cooldown()]
            if len(online_all) > target_online:
                online_all.sort(key=lambda x: getattr(x, "rate_limit_count", 0))
                for extra in online_all[target_online:]:
                    try:
                        await extra.disconnect()
                        logger.info(f"[在线限制] 断开多余号 {extra.phone}（在线超过 {target_online}）")
                    except Exception:
                        pass
            # 补连接到每组 1 个，总数不超过 target_online
            online_now = sum(1 for w in workers.values() if getattr(w, "_connected", False) and not w.is_in_cooldown())
            logger.info(f"[预连接] 当前在线 {online_now}，目标 {target_online}，需补 {max(0, target_online-online_now)}")
            groups = sorted(by_group.keys(), key=lambda x: str(x))
            gi = group_rr_index.get("pool", 0)
            connected = 0
            for _ in range(len(groups)):
                if online_now + connected >= target_online:
                    break
                gk = groups[gi % len(groups)]
                gi += 1
                # 该组是否已有在线
                has_online = False
                for wc in by_group[gk]:
                    w = workers.get(wc["id"])
                    if w and getattr(w, "_connected", False) and not w.is_in_cooldown():
                        has_online = True
                        break
                if has_online:
                    continue
                # 选该组限制最少且可用的号连接
                cands = []
                for wc in by_group[gk]:
                    if wc.get("skip_auto_connect") or wc.get("marked_restricted_or_cooldown"):
                        continue
                    wid = wc["id"]
                    w = workers.get(wid)
                    if not w:
                        try:
                            api_id_w = int(wc.get("api_id") or 0)
                            api_hash_w = wc.get("api_hash") or ""
                            if api_id_w and api_hash_w:
                                w = ShareWorker(wc, api_id_w, api_hash_w)
                                w._on_dead_detected = _on_dead_detected
                                w._log_callback = log_step
                                w._on_rate_limit_changed = _persist_worker_rate_limit
                                workers[wid] = w
                            else:
                                continue
                        except Exception:
                            continue
                    if getattr(w, "is_restricted", False) or getattr(w, "is_dead", False) or getattr(w, "is_frozen", False):
                        continue
                    if w.is_in_cooldown():
                        continue
                    le = (getattr(w, "last_error", "") or "").lower()
                    if "floodwait" in le or "frozen" in le:
                        continue
                    if getattr(w, "_connected", False):
                        continue
                    cands.append((getattr(w, "rate_limit_count", 0), wc, w))
                if not cands:
                    continue
                cands.sort(key=lambda x: x[0])
                _, wc, w = cands[0]
                try:
                    ok = await w.connect()
                    if ok:
                        connected += 1
                        logger.info(f"[预连接] 组{gk} 连接 {wc.get('phone')}（每组1个）")
                except Exception as e:
                    logger.debug(f"[预连接] {wc.get('phone')} 失败: {e}")
            group_rr_index["pool"] = gi
            try:
                save_json(Path(__file__).parent / "data" / "rr_index.json", {"idx": group_rr_index.get("idx", 0), "pool": group_rr_index.get("pool", 0)})
            except Exception:
                pass
            if connected:
                logger.info(f"[预连接] 本轮按组预连接 {connected} 个")

        # === 辅助函数: 获取可用水军号 ===
        async def get_available_worker(skip_ids=None):
            """优先选限制次数最少的水军（按组轮换），跳过受限/冷却/死号/≥10次/冻号"""
            nonlocal worker_idx
            # 强制限制同时在线
            # 在线上限只限制新连接，不阻止选择已在线的号
            online_now = sum(1 for w in workers.values() if getattr(w, "_connected", False) and not w.is_in_cooldown())
            at_online_cap = online_now >= get_max_online()

            """优先选限制次数最少的水军（按组轮换），跳过受限/冷却/死号/≥10次/冻号"""
            nonlocal worker_idx
            if skip_ids is None:
                skip_ids = set()
            # 24h 重置高限制号
            for w in list(workers.values()):
                try:
                    if hasattr(w, "reset_rate_limit_if_expired"):
                        w.reset_rate_limit_if_expired()
                except Exception:
                    pass
            candidates = []
            for wc in worker_configs:
                wid = wc["id"]
                if wid in skip_ids:
                    continue
                # 跳过真正受限的号；冷却已结束的历史限制可调度
                if wc.get("skip_auto_connect"):
                    continue
                cd_until = float(wc.get("cooldown_until") or 0)
                still_cooling = cd_until > time.time()
                truly_blocked = bool(wc.get("is_restricted") or wc.get("is_dead") or wc.get("is_frozen") or int(wc.get("rate_limit_count") or 0) >= 10)
                if truly_blocked or still_cooling:
                    continue
                # 冷却已结束，清除过期标记（内存侧）
                if wc.get("marked_restricted_or_cooldown") and not still_cooling and not truly_blocked:
                    wc["marked_restricted_or_cooldown"] = False
                w = workers.get(wid)
                if not w:
                    # 按需创建（未连接也先建对象，后续 connect）
                    try:
                        api_id = int(wc.get("api_id") or 0)
                        api_hash = wc.get("api_hash") or ""
                        if api_id and api_hash:
                            w = ShareWorker(wc, api_id, api_hash)
                            w._on_dead_detected = _on_dead_detected
                            w._on_rate_limit_changed = _persist_worker_rate_limit
                            w._log_callback = log_step
                            workers[wid] = w
                        else:
                            continue
                    except Exception:
                        continue
                if getattr(w, "is_restricted", False):
                    if not w.is_in_cooldown():
                        try:
                            w.enter_cooldown(3600)  # 限制号至少冷却1小时
                        except Exception:
                            pass
                    continue
                if getattr(w, "is_dead", False):
                    continue
                if getattr(w, "is_frozen", False) or wc.get("status") == "frozen":
                    continue
                if w.is_in_cooldown():
                    continue
                # 跳过 last_error 仍为 FloodWait / frozen 的号
                le = (getattr(w, "last_error", "") or "").lower()
                if "floodwait" in le or "wait of" in le or "frozen" in le:
                    continue
                rate_count = getattr(w, "rate_limit_count", 0)
                if rate_count >= 10:
                    try:
                        auto_delete_dead_worker(w.phone)
                        if wid in workers:
                            del workers[wid]
                    except Exception:
                        pass
                    continue
                try:
                    w_limit = int(config.get("worker_daily_limit", 50) or 50)
                except Exception:
                    w_limit = 50
                if getattr(w, "daily_sends", 0) >= w_limit:
                    continue
                # 标准方案：每组取限制最少一个 + 组间轮换
                phone = wc.get("phone", "")
                use_cnt = worker_use_count.get(phone, 0)
                group_key = wc.get("group") or wc.get("api_config_id") or "default"
                candidates.append((rate_count, use_cnt, group_key, wid, wc, w))
            if not candidates:
                return None, None
            from collections import defaultdict
            by_group = defaultdict(list)
            for item in candidates:
                by_group[item[2]].append(item)
            groups = sorted(by_group.keys(), key=lambda x: str(x))
            if not groups:
                return None, None
            gi = group_rr_index.get("idx", 0) % len(groups)
            chosen = groups[gi]
            group_rr_index["idx"] = gi + 1
            try:
                save_json(Path(__file__).parent / "data" / "rr_index.json", {"idx": group_rr_index.get("idx", 0), "pool": group_rr_index.get("pool", 0)})
            except Exception:
                pass
            items = by_group[chosen]
            # 规则：限制次数最少优先（降低再次被限），次数相同则使用次数最少
            items.sort(key=lambda x: (x[0], x[1]))
            rate_count, use_cnt, _, wid, wc, w = items[0]
            phone = wc.get("phone", "")
            worker_use_count[phone] = use_cnt + 1
            return wc, w
        # === 检查是否所有水军号都不可用 ===
        all_unavailable = True
        for wc in worker_configs:
            wid_check = wc["id"]
            if wid_check in workers:
                if not workers[wid_check].is_restricted and not workers[wid_check].is_in_cooldown():
                    all_unavailable = False
                    break
            else:
                all_unavailable = False
                break
        if all_unavailable and workers:
            min_remaining = 600
            for w in workers.values():
                if w.is_in_cooldown():
                    remaining = w._cooldown_until - time.time()
                    if remaining < min_remaining:
                        min_remaining = remaining
            wait_cd = max(int(min_remaining) + 5, 30)
            current_activity = {"status": "等待冷却", "worker": "", "target": target['username'], "step": f"所有水军号冷却中，等待 {wait_cd} 秒"}
            log_activity("等待冷却", f"所有水军号冷却中，等待 {wait_cd}s", target=target['username'], status="warning")
            logger.info(f"[调度] 所有水军号冷却中，等待 {wait_cd} 秒...")
            await asyncio.sleep(wait_cd)

        # 检查内存
        mem = psutil.virtual_memory()
        if mem.percent > MEMORY_THRESHOLD:
            logger.warning(f"内存使用 {mem.percent}%，暂停并清理连接")
            await cleanup_connections()
            await asyncio.sleep(30)

        try:
            await check_workers_running_low(worker_configs, interval_min)
        except Exception as _e:
            logger.warning(f"水军不足检查失败: {_e}")
        # 每目标最多 2 个水军尝试，失败则不再推送
        target_try_count = 0
        max_tries_per_target = 2
        # === Step 1: 先用一个水军号测试目标用户是否可达 ===
        target_try_count += 1
        if target_try_count > max_tries_per_target:
            target["status"] = "failed"
            target["sent_at"] = datetime.now().isoformat()
            target["result"] = f"已尝试{max_tries_per_target}个水军均失败，停止推送该目标"
            target["bot_username"] = ""
            save_json(TARGETS_FILE, {"targets": targets_data["targets"]})
            logger.info(f"[调度] 目标 @{target['username']} 已达{max_tries_per_target}次尝试上限，跳过")
            await asyncio.sleep(1)
            continue
        try:
            await maintain_online_pool()
        except Exception:
            pass
        test_wconfig, test_worker = await get_available_worker()
        if not test_worker:
            logger.warning(f"[调度] 没有可用水军号，等待30秒...")
            await asyncio.sleep(30)
            continue

        current_activity = {"status": "测试目标", "worker": test_wconfig['phone'], "target": target['username'], "step": "验证用户是否存在"}
        log_step(2, "检查目标用户", worker_phone=test_wconfig["phone"], target=target["username"])
        logger.info(f"[调度] 测试目标 @{target['username']}（使用 {test_wconfig['phone']}）")
        try:
            user_entity, matched = await asyncio.wait_for(
                test_worker.search_user(target["username"]),
                timeout=45
            )
        except asyncio.TimeoutError:
            logger.error(f"[调度] ⏰ 测试搜索超时(45s): @{target['username']}，跳过该水军号")
            test_worker._connected = False
            user_entity, matched = None, False
        except Exception as e:
            err = str(e)
            logger.warning(f"[调度] 测试搜索异常: {err}")
            # FloodWait → 冷却并断开
            if "wait of" in err.lower() and "seconds" in err.lower():
                try:
                    import re as _re
                    m = _re.search(r"wait of (\d+) seconds", err, _re.I)
                    secs = int(m.group(1)) if m else 3600
                    if test_worker:
                        test_worker.enter_cooldown(max(secs, 60))
                        test_worker._connected = False
                        test_worker.status = "cooldown"
                        test_worker.last_error = f"FloodWait {secs}s"
                        try:
                            await test_worker.disconnect()
                        except Exception:
                            pass
                        logger.warning(f"[调度] 水军 {test_wconfig['phone']} FloodWait {secs}s，已冷却并断开")
                        log_activity("FloodWait", f"{test_wconfig['phone']} 冷却 {secs}s", worker_phone=test_wconfig['phone'], status="warning")
                except Exception:
                    pass
            # frozen → 直接自动删除
            elif "frozen" in err.lower() or "not available for frozen" in err.lower():
                try:
                    phone = test_wconfig.get("phone") if test_wconfig else ""
                    if test_worker:
                        test_worker.is_frozen = True
                        test_worker._connected = False
                        try:
                            await test_worker.disconnect()
                        except Exception:
                            pass
                    if phone:
                        logger.warning(f"[自动删除] 水军 {phone} 被平台冻结，立即删除")
                        log_activity("号被冻结-已删除", f"{phone} frozen，已自动删除", worker_phone=phone, status="error")
                        auto_delete_dead_worker(phone)
                        # 从内存移除
                        if test_wconfig and test_wconfig.get("id") in workers:
                            del workers[test_wconfig["id"]]
                except Exception as _e:
                    logger.error(f"[自动删除] frozen 删除失败: {_e}")
            user_entity, matched = None, False

        # 区分：目标真不存在 vs 水军搜索失败
        if not user_entity:
            worker_bad = False
            if test_worker:
                le = (getattr(test_worker, "last_error", "") or "").lower()
                if (getattr(test_worker, "is_restricted", False)
                    or getattr(test_worker, "is_frozen", False)
                    or test_worker.is_in_cooldown()
                    or "floodwait" in le or "wait of" in le or "frozen" in le
                    or "搜索超时" in le or "搜索异常" in le):
                    worker_bad = True
            if worker_bad:
                # 水军有问题，不标记目标失败，换号继续
                logger.info(f"[调度] 水军 {test_wconfig['phone']} 搜索失败，目标 @{target['username']} 保留，换号")
                log_activity("换号", f"{test_wconfig['phone']} 搜索失败，换号重试 @{target['username']}", worker_phone=test_wconfig['phone'], target=target['username'], status="warning")
                try:
                    if getattr(test_worker, "_connected", False):
                        await test_worker.disconnect()
                except Exception:
                    pass
                await asyncio.sleep(2)
                continue
            else:
                # 确认目标不存在
                target["status"] = "failed"
                target["sent_at"] = datetime.now().isoformat()
                target["result"] = f"目标用户 @{target['username']} 不存在"
                target["bot_username"] = ""
                save_json(TARGETS_FILE, {"targets": targets_data["targets"]})
                logger.info(f"[调度] ❌ 目标 @{target['username']} 不存在，跳过")
                # 验证失败也进入冷却
                if test_worker:
                    test_worker.cooldown_until = time.time() + 300
                    test_worker.status = "cooldown"
                    logger.info(f"[轮换] {test_wconfig['phone']} 验证失败后进入冷却池5分钟")
                    log_activity("换号冷却", f"{test_wconfig['phone']} 验证失败进入冷却池5分钟", worker_phone=test_wconfig['phone'], target=target.get("username",""), status="info")
                await asyncio.sleep(2)
                continue
        if not matched:
            target["status"] = "failed"
            target["sent_at"] = datetime.now().isoformat()
            target["result"] = "用户名不匹配"
            target["bot_username"] = ""
            save_json(TARGETS_FILE, {"targets": targets_data["targets"]})
            logger.info(f"[调度] ❌ 目标 @{target['username']} 用户名不匹配，跳过")
            await asyncio.sleep(2)
            continue

        current_activity = {"status": "准备发送", "worker": test_wconfig['phone'], "target": target['username'], "step": "目标确认存在，准备分享"}
        log_step(2, "目标确认存在", worker_phone=test_wconfig["phone"], target=target["username"])
        logger.info(f"[调度] ✅ 目标 @{target['username']} 确认存在，开始发送...")

        # === Step 2: 目标可达，正式发送（最多尝试3个不同水军号）===
        max_retry_workers = 1  # 标准: 同一目标最多2个水军
        tried_worker_ids = set()
        tried_worker_ids.add(test_wconfig["id"])  # 测试用的水军号也可以用来发送
        tried_worker_ids.discard(test_wconfig["id"])  # 先不排除测试号，让它也参与发送
        send_success = False
        send_msg = ""
        send_worker = None

        for attempt in range(max_retry_workers):
            wconfig_send, worker_send = await get_available_worker(skip_ids=tried_worker_ids)
            if not worker_send:
                logger.warning(f"[调度] 没有更多可用水军号")
                break
            tried_worker_ids.add(wconfig_send["id"])
            # 强制设置日志回调
            worker_send._log_callback = log_step
            send_worker = worker_send


            global _last_attempt_ts
            log_step(1, "按组轮询取限制最少水军", worker_phone=wconfig_send.get("phone"), target=target.get("username"))
            logger.info(f"[调度] 水军 {wconfig_send['phone']} → @{target['username']}（第{attempt+1}次）")
            try:
                success, msg = await asyncio.wait_for(
                    worker_send.execute_share_task(target["username"], ad_index=0),
                    timeout=90
                )
            except asyncio.TimeoutError:
                logger.error(f"[调度] ⏰ 发送任务超时(90s): @{target['username']}，标记水军号断开")
                success, msg = False, "发送任务超时(90s)"
                worker_send._connected = False
            except Exception as task_err:
                logger.error(f"[调度] 执行任务异常: {task_err}")
                success, msg = False, f"任务异常: {task_err}"
                worker_send._connected = False

            if success:
                send_success = True
                send_msg = msg
                break
            else:
                send_msg = msg
                # 判断失败是否可重试
                is_rate_limit = ("too many" in msg.lower() or "flood" in msg.lower()
                                or "冷却" in msg.lower() or "cooldown" in msg.lower())
                is_target_issue = ("不存在" in msg or "不匹配" in msg or "blocked" in msg.lower()
                                  or "隐私" in msg or "privacy" in msg.lower()
                                  or "forbidden" in msg.lower() or "无法向该用户" in msg)
                if is_target_issue:
                    logger.info(f"[调度] 目标问题({msg})，不再重试")
                    break
                elif is_rate_limit:
                    # 记录水军+Bot组合的失败
                    bot_name = worker_send.current_bot_username if worker_send else ""
                    if bot_name:
                        banned = record_worker_bot_failure(wconfig_send["phone"], bot_name, send_msg)
                        if banned:
                            logger.warning(f"[调度] ⚠️ 水军 {wconfig_send['phone']} + Bot @{bot_name} 已被多次限制，标记为禁止")
                    logger.info(f"[调度] 频率限制，全局暂停30-60秒"); pause = random.randint(30, 60); await asyncio.sleep(pause); logger.info(f"[调度] 暂停{pause}秒后换号...")
                    await asyncio.sleep(3)
                    continue
                else:
                    logger.info(f"[调度] 发送失败({msg})，尝试换水军号...")
                    await asyncio.sleep(3)
                    continue

        # === Step 3: 更新目标状态 ===
        # 记录失败到restrictions（Too many requests等）
        if not send_success and send_worker and send_worker.current_bot_username:
            if "too many" in send_msg.lower() or "flood" in send_msg.lower():
                record_worker_bot_failure(
                    wconfig_send["phone"] if wconfig_send else "",
                    send_worker.current_bot_username,
                    send_msg
                )
        if not send_success and send_worker and send_worker.current_bot_username:
            if "Bot" in send_msg or "bot" in send_msg or "inline" in send_msg.lower():
                if "限制" in send_msg or "禁用" in send_msg or "restricted" in send_msg.lower() or "disabled" in send_msg.lower():
                    bot_name = send_worker.current_bot_username
                    bots_data = load_json(BOTS_FILE)
                    for bot in bots_data.get("bots", []):
                        if bot.get("username", "").lstrip("@") == bot_name:
                            bot["is_restricted"] = True
                            bot["restricted_at"] = datetime.now().isoformat()
                            bot["restricted_reason"] = send_msg
                            break
                    save_json(BOTS_FILE, bots_data)
                    logger.warning(f"[调度] Bot @{bot_name} 被标记为限制")

        target["status"] = "sent" if send_success else "failed"
        target["sent_at"] = datetime.now().isoformat()
        target["result"] = send_msg
        target["bot_username"] = send_worker.current_bot_username if send_worker and send_worker.current_bot_username else ""
        save_json(TARGETS_FILE, {"targets": targets_data["targets"]})

        if send_success:
            sent_count += 1
            global _last_success_send_ts, _stall_alerted_after_restart
            _last_success_send_ts = time.time()
            _stall_alerted_after_restart = False
            stats["total_sends"] = stats.get("total_sends", 0) + 1
            stats["today_sends"] = stats.get("today_sends", 0) + 1
            save_json(STATS_FILE, stats)

        # 发送间隔（标准方案：成功后休息 5-10 秒，强制换组换号）
        if send_success:
            wait_time = 300  # 强制冷却10分钟
            current_activity = {"status": "等待中", "worker": wconfig_send['phone'] if wconfig_send else "", "target": target['username'], "step": f"发送成功，进入冷却池5分钟"}
            log_activity("发送成功", f"成功发送给 @{target['username']}，进入冷却池5分钟", worker_phone=wconfig_send['phone'] if wconfig_send else "", target=target['username'], status="success")
            logger.info(f"[调度] ✅ 发送成功，休息 {wait_time} 秒后换组换号继续...")
            try:
                if send_worker:
                    if hasattr(send_worker, "rest_after_success"):
                        send_worker.rest_after_success(wait_time)
                    else:
                        send_worker.cooldown_until = time.time() + wait_time
                        send_worker.needs_disconnect = True
                        send_worker.status = "cooldown"
                    logger.info(f"[轮换] {send_worker.phone} 进入冷却池5分钟")
                    log_activity("换号冷却", f"{send_worker.phone} 进入冷却池5分钟", worker_phone=send_worker.phone, target=target.get("username",""), status="info")
                    # 立即断开，释放在线名额，由下一组连接
                    try:
                        if getattr(send_worker, "_connected", False):
                            await send_worker.disconnect()
                            logger.info(f"[轮换] 已断开 {send_worker.phone}")
                    except Exception:
                        pass
            except Exception:
                pass
            # 不再等待，直接换号
        else:
            # 失败也冷却5分钟
            if send_worker:
                send_worker.cooldown_until = time.time() + 300
                send_worker.status = "cooldown"
                logger.info(f"[轮换] {send_worker.phone} 失败后进入冷却池5分钟")
            await asyncio.sleep(5)
    current_activity = {"status": "已完成", "worker": "", "target": "", "step": f"共发送 {sent_count} 条"}
    log_activity("调度器完成", f"共发送 {sent_count} 条", status="info")
    try:
        st = _today_send_stats()
        await send_panel_notify(f"【旧面板】发送任务结束\n原因: 目标用完或达每日上限\n本轮: {sent_count} 条\n今日成功: {st['today']} 条\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:
        pass
    try:
        await send_panel_notify(f"旧面板发送任务完成\n本次发送: {sent_count} 条\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:
        pass
    try:
        await send_panel_notify(f"旧面板发送任务完成\n本次发送: {sent_count} 条\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:
        pass
    logger.info(f"=== 发送调度器完成，共发送 {sent_count} 条 ===")
    try:
        open("/tmp/tg_share_v2_sending.done", "w").write("done:"+str(sent_count))
        if os.path.exists("/tmp/tg_share_v2_sending"):
            os.remove("/tmp/tg_share_v2_sending")
    except Exception:
        pass

    # 调度器完成后断开所有水军连接
    disconnected = 0
    for wid in list(workers.keys()):
        try:
            if workers[wid]._connected:
                await workers[wid].disconnect()
                disconnected += 1
        except Exception:
            pass
    logger.info(f"[调度] 调度器结束，已断开 {disconnected} 个水军连接")


async def cleanup_connections():
    """清理空闲连接"""
    for wid in list(workers.keys()):
        if workers[wid].status == "idle" and workers[wid]._connected:
            await workers[wid].disconnect()
            logger.info(f"清理连接: {wid}")



async def release_banned_worker(wid):
    """释放被禁止/已死水军号的资源（断开连接、释放代理IP）"""
    if wid in workers:
        w = workers[wid]
        if w._connected:
            await w.disconnect()
            logger.info(f"已断开被禁止/已死水军号 {wid}，释放IP和资源")
        # 从proxy_pool中移除该worker的分配（释放IP占用）
        for proxy in proxy_pool.proxies:
            if wid in proxy.get("assigned_bots", []):
                proxy["assigned_bots"].remove(wid)
                logger.info(f"已释放水军号 {wid} 的代理IP {proxy['host']}:{proxy['port']}")
        proxy_pool.save()

async def disconnect_oldest_worker():
    """断开最久未使用的worker"""
    idle_workers = [(wid, w) for wid, w in workers.items() if w._connected and w.status == "idle"]
    if idle_workers:
        wid, worker = idle_workers[0]
        await worker.disconnect()
        logger.info(f"断开最久未使用: {wid}")


# ============ 启动 ============
async def start_bot():
    """启动 Telegram Bot（polling模式）"""
    global bot_app
    config = load_json(BOT_CONFIG_FILE)
    bot_token = config.get("bot_token", "")

    if not bot_token:
        logger.warning("未配置 Bot Token，Bot 功能不可用")
        return

    bot_app = Application.builder().token(bot_token).build()
    bot_app.add_handler(CommandHandler("start", bot_start_command))
    bot_app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r".*预览.*"),
        bot_preview_text
    ))
    bot_app.add_handler(CallbackQueryHandler(bot_callback))
    bot_app.add_handler(InlineQueryHandler(bot_inline_query))

    # 使用非阻塞方式启动
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram Bot 已启动 (polling)")


async def stop_bot():
    """停止Bot"""
    global bot_app
    if bot_app:
        await bot_app.updater.stop()
        await bot_app.stop()
        await bot_app.shutdown()



async def hourly_purge_frozen():
    """每小时扫描内存+配置，强制删除冻结/死号/限流达到10次的号，不进冷却"""
    while True:
        try:
            await asyncio.sleep(3600)
            phones = set()
            for wid, w in list(workers.items()):
                le = str(getattr(w, "last_error", "") or "")
                rr = str(getattr(w, "restricted_reason", "") or "")
                blob = (le + " " + rr).lower()
                rate_count = getattr(w, "rate_limit_count", 0) or 0
                if getattr(w, "is_frozen", False) or getattr(w, "is_dead", False) or "frozen" in blob or "冻结" in le or "方法不可用" in le or rate_count >= 10:
                    phones.add(getattr(w, "phone", ""))
            data = load_json(WORKERS_CONFIG_FILE)
            for wc in data.get("workers", []):
                le = str(wc.get("last_error") or "")
                rr = str(wc.get("restricted_reason") or "")
                rate_count = wc.get("rate_limit_count", 0) or 0
                if wc.get("is_frozen") or wc.get("is_dead") or "frozen" in le.lower() or "冻结" in le or "冻结" in rr or rate_count >= 10:
                    phones.add(wc.get("phone"))
            phones.discard("")
            phones.discard(None)
            for ph in phones:
                try:
                    auto_delete_dead_worker(ph)
                    for wid, w in list(workers.items()):
                        if getattr(w, "phone", "") == ph:
                            del workers[wid]
                    logger.warning(f"[每小时清理] 已删除冻结/死号/限流号 {ph}")
                except Exception as e:
                    logger.warning(f"[每小时清理] 删除 {ph} 失败: {e}")
            if phones:
                logger.info(f"[每小时清理] 本轮删除冻结/死号/限流号")
        except Exception as e:
            logger.warning(f"[每小时清理] 异常: {e}")


async def on_startup(app):
    """Web应用启动时"""
    # 初始化数据目录
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # 初始化鉴权 (首次启动生成随机管理员密码, 仅显示一次)
    _cfg, _plain = auth.load_or_init_auth()
    if _plain:
        logger.warning("=" * 56)
        logger.warning("  首次启动 - 已生成 Web 管理面板管理员账号")
        logger.warning("  用户名: admin")
        logger.warning(f"  密码:   {_plain}")
        logger.warning("  请妥善保存! 此密码仅在本次启动显示一次。")
        logger.warning("=" * 56)

    # 初始化统计
    stats = load_json(STATS_FILE)
    stats["start_time"] = time.time()
    save_json(STATS_FILE, stats)

    # Bot polling 已禁用 - 避免与旧服务器冲突
    # 如需启用，取消下面注释:
    # try:
    #     await start_bot()
    # except Exception as e:
    #     logger.error(f"Bot启动失败: {e}")
    logger.info("分享工作模式 - Bot polling 已禁用，由其他服务器处理")
    register_exit_signals()

    global _stall_monitor_task
    if _stall_monitor_task is None or _stall_monitor_task.done():
        _stall_monitor_task = asyncio.create_task(stall_send_monitor())
    asyncio.create_task(hourly_purge_frozen())
    logger.info("[每小时清理] 冻结/死号扫描已启动")
    logger.info(f"[{PANEL_NAME}] 长时间无发送监控已启动 (stall={STALL_MINUTES}min, alert={STALL_AFTER_RESTART_MINUTES}min)")
    # 自动启动调度器
    global scheduler_task
    if scheduler_task is None or scheduler_task.done():
        scheduler_task = asyncio.create_task(run_send_scheduler())
        logger.info("[强制] 已启动发送调度器")



async def on_cleanup(app):
    """Web应用关闭时"""
    # 断开所有worker
    for wid, worker in workers.items():
        try:
            await worker.disconnect()
        except:
            pass

    # 停止Bot
    try:
        await stop_bot()
    except:
        pass



# --- 2号面板：进程退出/重启时尽量上报（SIGTERM，pkill 默认）---
_exit_notify_sent = False

def _sync_panel_notify(text_msg: str):
    """同步发通知（signal/退出路径不能依赖 running loop）"""
    try:
        import json as _json
        from pathlib import Path as _P
        import urllib.request
        cfg_path = _P(__file__).resolve().parent / "data" / "notify_config.json"
        if not cfg_path.exists():
            return False
        cfg = _json.loads(cfg_path.read_text(encoding="utf-8"))
        token = (cfg.get("bot_token") or "").strip()
        chat_id = str(cfg.get("chat_id") or "").strip()
        if not token or not chat_id:
            return False
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = _json.dumps({"chat_id": chat_id, "text": str(text_msg)[:3500]}).encode()
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            return resp.status == 200
    except Exception as e:
        try:
            logger.warning(f"[通知] 退出上报失败: {e}")
        except Exception:
            pass
        return False


def on_process_signal(signum, frame):
    """SIGTERM/SIGINT：上报后退出（pkill 默认 SIGTERM）"""
    global _exit_notify_sent
    if _exit_notify_sent:
        return
    _exit_notify_sent = True
    try:
        name = globals().get("PANEL_NAME", "2号面板")
    except Exception:
        name = "2号面板"
    sig_name = "SIGTERM" if signum == getattr(__import__("signal"), "SIGTERM", 15) else f"signal:{signum}"
    msg = (
        f"【{name}】进程退出上报\n"
        f"原因: 收到 {sig_name}（服务重启/停止）\n"
        f"时间: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    try:
        logger.warning(f"[{name}] 收到 {sig_name}，准备退出并上报")
    except Exception:
        pass
    _sync_panel_notify(msg)
    import os as _os
    _os._exit(0)


def register_exit_signals():
    try:
        signal.signal(signal.SIGTERM, on_process_signal)
        signal.signal(signal.SIGINT, on_process_signal)
        logger.info(f"[{globals().get('PANEL_NAME', '2号面板')}] 已注册 SIGTERM/SIGINT 退出上报")
    except Exception as e:
        try:
            logger.warning(f"注册退出信号失败: {e}")
        except Exception:
            pass


PROFILE_TEMPLATE_FILE = Path(__file__).resolve().parent / "data" / "profile_template.json"

@routes.get("/api/profile-template")
async def api_get_profile_template(request):
    if PROFILE_TEMPLATE_FILE.exists():
        try:
            return web.json_response(json.loads(PROFILE_TEMPLATE_FILE.read_text(encoding="utf-8")))
        except Exception:
            pass
    return web.json_response({})

@routes.post("/api/profile-template")
async def api_save_profile_template(request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    tpl = {
        "first_name": (body.get("first_name") or "").strip(),
        "last_name": (body.get("last_name") or "").strip(),
        "username_prefix": (body.get("username_prefix") or "").strip(),
        "bio": (body.get("bio") or "").strip(),
    }
    PROFILE_TEMPLATE_FILE.write_text(json.dumps(tpl, ensure_ascii=False, indent=2), encoding="utf-8")
    return web.json_response({"ok": True})


def create_app():
    """创建Web应用"""
    app = web.Application(middlewares=[auth_middleware])
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    
    register_batch_import_routes(routes)
    app.add_routes(routes)

    # 静态文件
    frontend_dir = BASE_DIR / "frontend"
    if frontend_dir.exists():
        app.router.add_static("/static/", frontend_dir, show_index=False)
    avatars_dir = DATA_DIR / "avatars"
    if avatars_dir.exists():
        app.router.add_static("/static/avatars/", avatars_dir, show_index=False)

    # CORS
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=False,  # 使用 X-Auth-Token 请求头鉴权, 不依赖 cookie
            expose_headers="*",
            allow_headers="*",
            allow_methods="*"
        )
    })
    for route in list(app.router.routes()):
        try:
            cors.add(route)
        except:
            pass

    return app


if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=8001)
