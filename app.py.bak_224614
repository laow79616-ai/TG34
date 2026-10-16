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


# API_GROUP_ASSIGN_V1
def _api_cfg_path():
    from pathlib import Path as _P
    cand = [
        _P("/root/tg_share_panel_a/data/api_configs.json"),
        _P("/root/tg_share_panel_a/api_configs.json"),
    ]
    try:
        from config import DATA_DIR
        cand.insert(0, DATA_DIR / "api_configs.json")
    except Exception:
        pass
    for x in cand:
        if x.exists():
            return x
    return cand[0]

def _load_api_cfgs():
    path = _api_cfg_path()
    if not path.exists():
        return []
    raw = load_json(path)
    if isinstance(raw, list):
        return raw
    return raw.get("configs") or raw.get("pool") or []

def _save_api_cfgs(arr):
    save_json(_api_cfg_path(), {"configs": arr})

def _load_proxies_list():
    pdata = load_json(PROXY_POOL_FILE)
    if isinstance(pdata, list):
        return pdata, None
    return pdata.get("proxies") or [], pdata

def _save_proxies_list(proxies, raw):
    if raw is None:
        save_json(PROXY_POOL_FILE, proxies)
    else:
        raw["proxies"] = proxies
        save_json(PROXY_POOL_FILE, raw)

def _assigned_api_ids(proxies):
    s = set()
    for px in proxies:
        a = px.get("assigned_api") or {}
        if a.get("api_id"):
            s.add(str(a.get("api_id")))
    return s

def api_auto_assign_by_group():
    """前 20 条 IP 各配 1 条 API，剩余进待配池（不写 assigned_api）。"""
    cfgs = _load_api_cfgs()
    proxies, raw = _load_proxies_list()
    used = set()
    assigned = 0
    idx = 0
    for i, px in enumerate(proxies[:20]):
        cur = px.get("assigned_api") or {}
        if cur.get("api_id") and any(str(x.get("api_id")) == str(cur.get("api_id")) for x in cfgs):
            used.add(str(cur.get("api_id")))
            assigned += 1
            continue
        while idx < len(cfgs) and str(cfgs[idx].get("api_id")) in used:
            idx += 1
        if idx >= len(cfgs):
            px["assigned_api"] = None
            continue
        item = cfgs[idx]
        px["assigned_api"] = {
            "api_id": item.get("api_id"),
            "api_hash": item.get("api_hash"),
            "id": item.get("id"),
            "note": item.get("note") or f"线路{i+1}",
        }
        used.add(str(item.get("api_id")))
        assigned += 1
        idx += 1
    _save_proxies_list(proxies, raw)
    standby = [x for x in cfgs if str(x.get("api_id")) not in used]
    return {"ok": True, "assigned": assigned, "standby": len(standby), "message": f"已按组分配 {assigned} 条，待配池 {len(standby)} 条"}

def api_replace_group(group_index):
    """该组 API 有问题：从待配池任意抽 1 条换上，旧的回待配池。"""
    cfgs = _load_api_cfgs()
    proxies, raw = _load_proxies_list()
    if group_index < 0 or group_index >= len(proxies):
        return {"ok": False, "error": "组不存在"}
    used = _assigned_api_ids(proxies)
    old = (proxies[group_index].get("assigned_api") or {}).get("api_id")
    if old:
        used.discard(str(old))
    standby = [x for x in cfgs if str(x.get("api_id")) not in used]
    if not standby:
        return {"ok": False, "error": "待配池为空"}
    pick = random.choice(standby)
    proxies[group_index]["assigned_api"] = {
        "api_id": pick.get("api_id"),
        "api_hash": pick.get("api_hash"),
        "id": pick.get("id"),
        "note": pick.get("note") or "",
    }
    _save_proxies_list(proxies, raw)
    return {"ok": True, "message": f"线路{group_index+1} 已换 API {pick.get('api_id')}"}



def resolve_api_for_worker(wc, config):
    """优先：水军所在 IP 组的 assigned_api；没有则待配池随机 1 条；再没有才用系统默认。"""
    try:
        proxies, _raw = _load_proxies_list()
        wid = str((wc or {}).get("id") or "")
        phone = str((wc or {}).get("phone") or "")
        px = None
        for item in proxies:
            ids = [str(x) for x in (item.get("assigned_bots") or [])]
            phones = [str(x) for x in (item.get("assigned_worker_phones") or [])]
            if wid in ids or phone in phones:
                px = item
                break
        if px and (px.get("assigned_api") or {}).get("api_id"):
            aa = px["assigned_api"]
            return aa.get("api_id"), aa.get("api_hash")
        used = _assigned_api_ids(proxies)
        standby = [x for x in _load_api_cfgs() if str(x.get("api_id")) not in used]
        if standby:
            pick = random.choice(standby)
            return pick.get("api_id"), pick.get("api_hash")
    except Exception:
        pass
    return config.get("api_id"), config.get("api_hash")


# GROUP_RUNTIME_V2
_group_rr_index = 0

def _load_api_list():
    from pathlib import Path as _P
    import json as _json
    for path in (_P("/root/tg_share_panel_a/data/api_configs.json"), _P("/root/tg_share_panel_a/api_configs.json")):
        if path.exists():
            raw = _json.loads(path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, list) else (raw.get("configs") or raw.get("pool") or []), path, raw
    return [], _P("/root/tg_share_panel_a/data/api_configs.json"), {"configs": []}

def _save_api_list(arr, path, raw):
    import json as _json
    if isinstance(raw, list):
        path.write_text(_json.dumps(arr, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        raw["configs"] = arr
        path.write_text(_json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")

def _load_px():
    pdata = load_json(PROXY_POOL_FILE)
    if isinstance(pdata, list):
        return pdata, None
    return pdata.get("proxies") or [], pdata

def _save_px(proxies, raw):
    if raw is None:
        save_json(PROXY_POOL_FILE, proxies)
    else:
        raw["proxies"] = proxies
        save_json(PROXY_POOL_FILE, raw)

def resolve_api_for_group(group_index, config=None):
    """只用该组已配 API。没有才回退系统默认。不在这里动待配池。"""
    config = config or {}
    apis, _path, _raw = _load_api_list()
    for a in apis:
        if a.get("group") == group_index:
            return a.get("api_id"), a.get("api_hash"), "group"
    proxies, _ = _load_px()
    if 0 <= group_index < len(proxies):
        aa = (proxies[group_index].get("assigned_api") or {})
        if aa.get("api_id"):
            return aa.get("api_id"), aa.get("api_hash"), "proxy"
    return config.get("api_id"), config.get("api_hash"), "global_fallback"

def replace_group_api_from_standby(group_index, reason="api_invalid"):
    """仅 API 失效/发码失败时调用。号限流禁止走这里。"""
    apis, path, raw = _load_api_list()
    standby = [a for a in apis if a.get("group") == "standby"]
    if not standby:
        return False, "待配池为空"
    pick = standby[0]
    old = None
    for a in apis:
        if a.get("group") == group_index:
            old = a
            a["group"] = "standby"
            a["status"] = "failed"
            a["fail_reason"] = reason
    pick["group"] = group_index
    pick["status"] = "assigned"
    _save_api_list(apis, path, raw)
    proxies, praw = _load_px()
    if 0 <= group_index < len(proxies):
        proxies[group_index]["assigned_api"] = {
            "api_id": pick.get("api_id"),
            "api_hash": pick.get("api_hash"),
            "id": pick.get("id"),
            "note": pick.get("note") or "",
        }
        _save_px(proxies, praw)
    return True, f"组{group_index+1} API已从待配池替换为 {pick.get('api_id')} (old={None if not old else old.get('api_id')})"

def is_api_failure(err):
    s = str(err or "")
    keys = ("ApiIdInvalid", "api_id/api_hash", "API_ID_INVALID", "api_hash combination is invalid", "SendCodeRequest")
    return any(k.lower() in s.lower() for k in keys)

def is_flood_err(err):
    s = str(err or "")
    return "FloodWait" in s or "FLOOD" in s or "flood" in s.lower()

def pick_worker_group_rr(worker_configs, live_workers, skip_ids=None):
    """
    1-20 组轮询。
    当前组若有空闲可用号，只抽 1 个；该组某号在冷却，不影响同组其它空闲号。
    整组都不可用才切下一组。
    """
    global _group_rr_index
    skip_ids = skip_ids or set()
    n = 30
    cfg_by = {str(w.get("id")): w for w in worker_configs}

    def usable(wc):
        wid = str(wc.get("id"))
        if wid in skip_ids:
            return False
        w = live_workers.get(wid)
        if w is None:
            return True
        if getattr(w, "is_restricted", False) or getattr(w, "is_dead", False):
            return False
        if hasattr(w, "is_in_cooldown") and w.is_in_cooldown():
            return False
        if getattr(w, "rate_limit_count", 0) >= 5:
            return False
        return True

    by_group = {i: [] for i in range(n)}
    for wc in worker_configs:
        g = wc.get("group")
        if not isinstance(g, int):
            # 用已绑定 proxy 推断组
            host = ((wc.get("proxy") or {}).get("host"))
            proxies, _ = _load_px()
            g = 0
            for i, px in enumerate(proxies[:20]):
                if px.get("host") == host or str(wc.get("id")) in [str(x) for x in (px.get("assigned_bots") or [])]:
                    g = i
                    break
        if isinstance(g, int) and 0 <= g < 20:
            by_group[g].append(wc)

    for step in range(n):
        gi = (_group_rr_index + step) % n
        cands = [wc for wc in by_group.get(gi, []) if usable(wc)]
        if not cands:
            continue
        _group_rr_index = (gi + 1) % n
        return cands[0], gi
    return None, None

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
manual_send_stopped = False
SEND_STOP_FLAG = False
# --- 2号面板：长时间无发送自动续跑 / 上报 ---
PANEL_NAME = "2号面板"
_STALL_MONITOR_STARTED = False
_EXIT_SIGNALS_REGISTERED = False
STALL_MINUTES = 15
STALL_AFTER_RESTART_MINUTES = 10
_stall_monitor_task = None
_last_success_send_ts = 0.0
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
    if False and (usable <= 15 or capacity < need_for_5h):
        st = _today_send_stats() if "_today_send_stats" in globals() or True else {"today": 0, "total": 0}
        try:
            st = _today_send_stats()
        except Exception:
            st = {"today": 0, "total": 0}
        msg = (
            f"【2号面板】水军预计5小时内可能用完\\n"
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







def log_activity(action, details="", worker_phone="", target="", status="info"):
    """记录工作流活动"""
    global activity_log
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
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
MAX_CONCURRENT_CONNECTIONS = 20
CONNECTION_INTERVAL = 10  # 秒
MEMORY_THRESHOLD = 80  # %


# ============ IP线路固定绑定 / 轮询调度 ============
IP_LINE_MAX_WORKERS = 10
GROUP_PER_IP = 10
MAX_IP_GROUPS = 30
IP_LINE_MAX_LINES = 30
_ip_line_cursor = 0


def _norm_phone(ph):
    return str(ph or "").replace(" ", "").replace("+", "").strip()


def _proxy_tuple_from_obj(proxy):
    if not proxy:
        return None
    return {
        "id": proxy.get("id"),
        "host": proxy.get("host"),
        "port": proxy.get("port"),
        "type": proxy.get("type") or "http",
        "username": proxy.get("username") or "",
        "password": proxy.get("password") or "",
    }


def _find_proxy(proxies, proxy_id=None, host=None, port=None):
    if proxy_id:
        for p in proxies:
            if p.get("id") == proxy_id:
                return p
    if host is not None and port is not None:
        for p in proxies:
            if str(p.get("host")) == str(host) and int(p.get("port") or 0) == int(port):
                return p
    return None


def _worker_bound_proxy_id(w, proxies):
    pid = w.get("proxy_id") or w.get("bound_proxy_id")
    if pid:
        return pid
    px = w.get("proxy") or {}
    p = _find_proxy(proxies, host=px.get("host"), port=px.get("port"))
    return p.get("id") if p else None


def _active_ip_lines(proxies):
    lines = [p for p in proxies if p.get("status", "active") != "disabled"]
    return lines[:IP_LINE_MAX_LINES]


def bind_worker_to_proxy_locked(worker, proxy, proxies, workers, *, allow_rebind=False):
    """把水军锁到指定IP。已绑其他IP则拒绝。每条最多10个。"""
    if not proxy:
        return False, "IP线路不存在"
    pid = proxy.get("id")
    assigned = list(proxy.get("assigned_bots") or [])
    wid = worker.get("id")
    cur = _worker_bound_proxy_id(worker, proxies)
    if cur and cur != pid and not allow_rebind:
        return False, "该水军已绑定其他IP线路，禁止串换"
    if wid not in assigned and len(assigned) >= IP_LINE_MAX_WORKERS:
        return False, f"该IP线路已满（最多{IP_LINE_MAX_WORKERS}个水军）"
    if wid not in assigned:
        assigned.append(wid)
    proxy["assigned_bots"] = assigned
    worker["proxy_id"] = pid
    worker["bound_proxy_id"] = pid
    worker["proxy"] = _proxy_tuple_from_obj(proxy)
    # 从其他IP的名单里摘掉（防历史脏数据）
    for p in proxies:
        if p.get("id") != pid:
            p["assigned_bots"] = [x for x in (p.get("assigned_bots") or []) if x != wid]
    return True, "ok"


def worker_in_cooldown(w):
    until = float(w.get("cooldown_until") or 0)
    return until > time.time()


def mark_worker_cooldown(w, seconds=900, reason="flood"):
    w["cooldown_until"] = time.time() + int(seconds)
    w["cooldown_reason"] = reason
    w["status"] = "cooling"
    w["pool"] = "cooldown"
    # 归属IP不变
    return w


def clear_worker_cooldown_if_ready(w):
    if not worker_in_cooldown(w):
        if w.get("pool") == "cooldown" or w.get("status") == "cooling":
            w["pool"] = "ip_line"
            if w.get("status") == "cooling":
                w["status"] = "idle"
            w["cooldown_until"] = 0
        return True
    return False


def pick_one_idle_worker_on_ip(proxy, workers, workers_runtime=None):
    """当前IP只挑1个可工作号。冷却中的跳过，冷却结束仍只在本IP里复活。"""
    ids = list(proxy.get("assigned_bots") or [])
    by_id = {w.get("id"): w for w in workers}
    for wid in ids:
        w = by_id.get(wid)
        if not w:
            continue
        clear_worker_cooldown_if_ready(w)
        if worker_in_cooldown(w):
            continue
        if w.get("status") in ("banned", "deleted", "pending_login"):
            continue
        rt = None
        if workers_runtime is not None:
            rt = workers_runtime.get(wid)
        if rt is not None and not getattr(rt, "_connected", False):
            continue
        return w
    return None



def attach_group_fields(proxy_list, worker_list=None, bot_list=None):
    """给 IP/水军/Bot 打组号：第 N 条 IP = 组N，每组最多10号。"""
    for i, p in enumerate(proxy_list[:20]):
        p["group_no"] = i + 1
        p["group_name"] = f"组{i+1}"
    for p in proxy_list[20:]:
        p["group_no"] = 0
        p["group_name"] = "未编组"
    pmap = {p.get("id"): p for p in proxy_list}
    if worker_list:
        for w in worker_list:
            pid = w.get("bound_proxy_id") or w.get("proxy_id") or ""
            g = (pmap.get(pid) or {}).get("group_no") or 0
            w["group_no"] = g
            w["group_name"] = f"组{g}" if g else "未编组"
    if bot_list:
        name_g = {}
        for p in proxy_list:
            g = p.get("group_no") or 0
            for n in (p.get("assigned_bot_accounts") or []) + (p.get("assigned_bots") or []):
                name_g[str(n).lower()] = g
        for b in bot_list:
            key = str(b.get("username") or b.get("bot_username") or b.get("name") or "").lower()
            g = name_g.get(key) or 0
            b["group_no"] = g
            b["group_name"] = f"组{g}" if g else "未编组"
    return proxy_list

def pick_next_ip_worker(workers_cfg, runtime_workers):
    """按IP线路轮询：当前线路只取1个空闲号，然后切下一条。"""
    global _ip_line_cursor
    pdata = load_json(PROXY_POOL_FILE)
    proxies = _active_ip_lines(pdata.get("proxies", []))
    if not proxies:
        return None, None
    n = len(proxies)
    for step in range(n):
        idx = (_ip_line_cursor + step) % n
        proxy = proxies[idx]
        w = pick_one_idle_worker_on_ip(proxy, workers_cfg, runtime_workers)
        if w:
            _ip_line_cursor = (idx + 1) % n
            return w, proxy
    return None, None

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
    if path == "/api/login" or path.startswith("/static/") or not path.startswith("/api/") or path in ("/api/workers/export", "/api/workers/export-deleted"):
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



@routes.get("/api/group_notes")
async def api_group_notes_get(request):
    path = DATA_DIR / "group_notes.json"
    if not path.exists():
        return web.json_response({"notes": {}})
    try:
        return web.json_response({"notes": json.loads(path.read_text(encoding="utf-8"))})
    except Exception:
        return web.json_response({"notes": {}})

@routes.post("/api/group_notes")
async def api_group_notes_save(request):
    body = await request.json()
    no = str(body.get("group_no") or "")
    note = str(body.get("note") or "").strip()[:80]
    path = DATA_DIR / "group_notes.json"
    notes = {}
    if path.exists():
        try:
            notes = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            notes = {}
    if no:
        notes[no] = note
        path.write_text(json.dumps(notes, ensure_ascii=False, indent=2), encoding="utf-8")
    return web.json_response({"ok": True, "notes": notes})


@routes.post("/api/bots/regroup")
async def api_bots_regroup(request):
    body = await request.json()
    want = int(body.get("group_no") or 0)
    data = load_json(BOTS_FILE) if "BOTS_FILE" in globals() else None
    path = None
    for cand in ["BOTS_FILE", "BOT_FILE"]:
        if cand in globals():
            path = globals()[cand]
            break
    from pathlib import Path as P
    if path is None:
        path = DATA_DIR / "bots.json"
    try:
        data = load_json(path)
    except Exception:
        import json
        data = json.loads(Path(path).read_text() or "{}")
    bots = data.get("bots") or data if isinstance(data, list) else data.get("bots") or []
    # 重新按每组10个填，或把未分组/最后导入的填到指定组
    if want and 1 <= want <= 20:
        # 指定组现有数量
        cur = [b for b in bots if int(b.get("group_no") or 0)==want]
        room = max(0, 10-len(cur))
        for b in bots:
            g=int(b.get("group_no") or 0)
            if g==0 and room>0:
                b["group_no"]=want
                b["group_name"]=f"组{want}"
                room -= 1
    else:
        # 自动补齐
        buckets={i:0 for i in range(1,21)}
        for b in bots:
            g=int(b.get("group_no") or 0)
            if 1<=g<=20: buckets[g]+=1
        for b in bots:
            g=int(b.get("group_no") or 0)
            if g: continue
            for i in range(1,21):
                if buckets[i]<10:
                    b["group_no"]=i
                    b["group_name"]=f"组{i}"
                    buckets[i]+=1
                    break
    if isinstance(data, dict):
        data["bots"]=bots
        save_json(path, data)
    else:
        save_json(path, {"bots": bots})
    return web.json_response({"ok": True, "count": len(bots)})

@routes.get("/api/status")
async def api_status(request):
    """系统状态"""
    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.5)
    stats = load_json(STATS_FILE)
    # 总数从配置文件读，在线数从内存连接状态读
    cfg_workers = load_json(WORKERS_CONFIG_FILE).get("workers", [])
    workers_total = len(cfg_workers)
    workers_connected = sum(1 for w in workers.values() if getattr(w, "_connected", False))
    return web.json_response({
        "status": "running",
        "memory_percent": mem.percent,
        "memory_used_mb": round(mem.used / 1024 / 1024, 1),
        "memory_total_mb": round(mem.total / 1024 / 1024, 1),
        "cpu_percent": cpu,
        "workers_total": workers_total,
        "workers_connected": workers_connected,
        "total_sends": stats.get("total_sends", 0),
        "today_sends": stats.get("today_sends", 0),
        "uptime": time.time() - stats.get("start_time", time.time())
    })


# --- 水军管理 ---
@routes.get("/api/workers")
async def api_workers_list(request):
    """获取水军列表"""
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    # 附加运行时状态
    for w in worker_list:
        wid = w["id"]
        if wid in workers:
            w["runtime_status"] = workers[wid].status
            w["connected"] = workers[wid]._connected
            w["daily_sends"] = workers[wid].daily_sends
            w["last_error"] = workers[wid].last_error
            w["is_restricted"] = workers[wid].is_restricted
            w["is_dead"] = getattr(workers[wid], 'is_dead', False)
            w["restricted_at"] = workers[wid].restricted_at or ""
            w["restricted_reason"] = workers[wid].restricted_reason
            w["rate_limit_count"] = getattr(workers[wid], 'rate_limit_count', 0)
            w["has_been_banned_24h"] = getattr(workers[wid], 'has_been_banned_24h', False)
            w["in_cooldown"] = workers[wid].is_in_cooldown()
            w["cooldown_remaining"] = max(0, int(workers[wid].cooldown_until - time.time())) if workers[wid].is_in_cooldown() else 0
        else:
            w["runtime_status"] = "offline"
            w["connected"] = False
            w["is_restricted"] = False
            w["is_dead"] = False
            w["rate_limit_count"] = 0
            w["has_been_banned_24h"] = False
            w["in_cooldown"] = False
            w["cooldown_remaining"] = 0
    # 附加限制记录
    for w in worker_list:
        # 只显示水军自身的限制次数（rate_limit_count）
        w["restriction_count"] = w.get("rate_limit_count", 0)
    return web.json_response(worker_list)


@routes.post("/api/workers/add")
async def api_worker_add(request):
    """添加水军号"""
    body = await request.json()
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    group_no = int(body.get("group_no") or 1)
    proxy_id = body.get("proxy_id") or ""
    bot_id = body.get("bot_id") or ""
    bot_username = body.get("bot_username") or ""
    if not bot_id:
        try:
            bots = load_json(BOTS_FILE).get("bots", [])
        except Exception:
            bots = []
        start = (group_no-1)*10
        chunk = bots[start:start+10]
        if chunk:
            bot_id = chunk[0].get("id","")
            bot_username = (chunk[0].get("username") or chunk[0].get("number") or "").lstrip("@")


    new_worker = {
        "id": f"w_{int(time.time())}_{random.randint(100,999)}",
        "phone": body["phone"],
        "bot_token": body.get("bot_token", ""),
        "bot_username": body.get("bot_username", ""),
        "session_name": body["phone"].replace("+", ""),
        "proxy": None,
        "status": "pending_login",
        "created_at": datetime.now().isoformat()
    }
    new_worker["group_no"]=int(body.get("group_no") or 1)
    new_worker["proxy_id"]=body.get("proxy_id") or new_worker.get("proxy_id","")
    if bot_id: new_worker["bot_id"]=bot_id
    if bot_username: new_worker["bot_username"]=bot_username
    
    # bind to selected proxy / group
    try:
        group_no = int(body.get("group_no") or new_worker.get("group_no") or 1)
    except Exception:
        group_no = 1
    proxy_id = str(body.get("proxy_id") or new_worker.get("proxy_id") or "")
    new_worker["group_no"] = group_no
    pdata = load_json(PROXY_POOL_FILE)
    proxies = pdata.get("proxies", [])
    target = None
    if proxy_id:
        target = next((x for x in proxies if str(x.get("id"))==str(proxy_id)), None)
    if target is None and 1<=group_no<=len(proxies):
        target = proxies[group_no-1]
    if target is not None:
        new_worker["proxy_id"] = target.get("id")
        new_worker["proxy"] = {
            "host": target.get("host"),
            "port": target.get("port"),
            "type": target.get("type") or "http",
            "username": target.get("username") or "",
            "password": target.get("password") or "",
        }
        assigned = list(target.get("assigned_bots") or [])
        phone = new_worker.get("phone")
        if phone and phone not in assigned:
            assigned.append(phone)
        target["assigned_bots"] = assigned
        save_json(PROXY_POOL_FILE, {"proxies": proxies})

    worker_list.append(new_worker)
    save_json(WORKERS_CONFIG_FILE, {"workers": worker_list})
    return web.json_response({"ok": True, "worker": new_worker})


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
        if connected_now >= MAX_CONCURRENT_CONNECTIONS:
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

        worker = ShareWorker(wconfig, api_id, api_hash)
        worker._on_rate_limit_changed = _persist_worker_rate_limit
        worker._on_dead_detected = _on_dead_detected
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
    """代理列表，附带已分配水军手机号"""
    data = load_json(PROXY_POOL_FILE)
    proxies = data.get("proxies", [])
    workers = load_json(WORKERS_CONFIG_FILE).get("workers", [])
    id2w = {w.get("id"): w for w in workers}
    out = []
    for p in proxies:
        item = dict(p)
        assigned = []
        for wid in (p.get("assigned_bots") or []):
            w = id2w.get(wid) or {}
            assigned.append({
                "id": wid,
                "phone": w.get("phone") or "",
                "status": w.get("status") or "",
            })
        item["assigned_workers"] = assigned
        item["assigned_worker_phones"] = [x["phone"] for x in assigned if x.get("phone")]
        out.append(item)
    return web.json_response({"ok": True, "proxies": out})

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



@routes.post("/api/proxies/{proxy_id}/assign-workers")
async def api_proxies_assign_workers(request):
    """绑定水军到指定IP：每条最多10个，已绑其他IP禁止串换。"""
    proxy_id = request.match_info["proxy_id"]
    body = await request.json()
    phones = body.get("phones") or []
    phones = [str(x).strip().replace(" ", "") for x in phones if str(x).strip()]
    pdata = load_json(PROXY_POOL_FILE)
    proxies = pdata.get("proxies", [])
    proxy = next((p for p in proxies if p.get("id") == proxy_id), None)
    if not proxy:
        return web.json_response({"ok": False, "error": "代理不存在"}, status=404)
    wdata = load_json(WORKERS_CONFIG_FILE)
    workers = wdata.get("workers", [])
    added, missing, rejected = [], [], []
    for ph in phones:
        w = next((x for x in workers if _norm_phone(x.get("phone")) == _norm_phone(ph)), None)
        if not w:
            missing.append(ph)
            continue
        ok, msg = bind_worker_to_proxy_locked(w, proxy, proxies, workers, allow_rebind=False)
        if ok:
            w["pool"] = "ip_line"
            added.append(w.get("phone"))
        else:
            rejected.append({"phone": ph, "error": msg})
    save_json(PROXY_POOL_FILE, {"proxies": proxies})
    save_json(WORKERS_CONFIG_FILE, {"workers": workers})
    return web.json_response({
        "ok": True,
        "added": added,
        "missing": missing,
        "rejected": rejected,
        "count": len(proxy.get("assigned_bots") or []),
        "cap": IP_LINE_MAX_WORKERS,
    })


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
    """当水军被限制达到10次时，自动从系统中删除"""
    import shutil
    logger.warning(f"[自动删除] 水军号 {worker_phone} 被限制达到10次，自动删除")
    # 从workers_config中删除
    data = load_json(WORKERS_CONFIG_FILE)
    worker_list = data.get("workers", [])
    data["workers"] = [w for w in worker_list if w.get("phone") != worker_phone]
    save_json(WORKERS_CONFIG_FILE, data)
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
    """当检测到水军号死亡时自动删除"""
    logger.warning(f"[自动删除-死亡检测] 水军号 {phone} 被TG平台标记死亡: {reason}")
    auto_delete_dead_worker(phone)
    # 从内存中的workers字典删除
    if worker_id in workers:
        del workers[worker_id]
    logger.info(f"[自动删除-死亡检测] 水军号 {phone} 已从系统完全移除")

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


@routes.post("/api/bots/redistribute")
async def api_bots_redistribute(request):
    """20组均匀分配：按用户名数字排序后写入 group_no + 组内编号"""
    GROUPS = 20
    data = load_json(BOTS_FILE)
    bots = data.get("bots", [])

    def bot_num(b):
        raw = str(b.get("username") or b.get("number") or "")
        digits = "".join(ch for ch in raw if ch.isdigit())
        return int(digits) if digits else 10**9

    bots.sort(key=bot_num)
    n = len(bots)
    if n == 0:
        save_json(BOTS_FILE, {"bots": bots})
        return web.json_response({"ok": True, "total": 0, "groups": GROUPS, "per_group": []})

    base, rem = divmod(n, GROUPS)
    sizes = [(base + 1) if i < rem else base for i in range(GROUPS)]
    idx = 0
    per_group = []
    for g in range(GROUPS):
        chunk = bots[idx: idx + sizes[g]]
        idx += sizes[g]
        for seq, b in enumerate(chunk, 1):
            b["group_no"] = g + 1
            b["group_seq"] = seq
            b["number"] = seq
        per_group.append({"group": g + 1, "count": len(chunk)})
    save_json(BOTS_FILE, {"bots": bots})
    return web.json_response({"ok": True, "total": n, "groups": GROUPS, "per_group": per_group})


@routes.get("/api/bots")
async def api_bots_list(request):
    data = load_json(BOTS_FILE)
    bot_list = data.get("bots", [])
    def _bn(b):
        raw = str(b.get("username") or b.get("number") or "")
        digits = "".join(ch for ch in raw if ch.isdigit())
        return (int(b.get("group_no") or 0), int(digits) if digits else 10**9)
    bot_list.sort(key=_bn)
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
    # 删除前先备份
    to_delete = [w for w in worker_list if w["id"] == worker_id]
    if to_delete:
        try:
            _backup_deleted_worker(to_delete[0])
        except Exception as e:
            logger.warning(f"备份删除水军失败: {e}")
    worker_list = [w for w in worker_list if w["id"] != worker_id]
    if len(worker_list) == original_len:
        return web.json_response({"ok": False, "error": "Worker不存在"}, status=404)
    save_json(WORKERS_CONFIG_FILE, {"workers": worker_list})
    # 不再删除 session 文件，保留以便后续导出
    # （如需彻底删除可手动清理 data/sessions/）
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
    global scheduler_task, SEND_STOP_FLAG, manual_send_stopped
    SEND_STOP_FLAG = False
    manual_send_stopped = False
    if scheduler_task and not scheduler_task.done():
        return web.json_response({"ok": False, "error": "任务已在运行中"})
    scheduler_task = asyncio.create_task(run_7step_scheduler())
    return web.json_response({"ok": True, "message": "发送任务已启动"})



@routes.post("/api/send/stop")
async def api_send_stop(request):
    """停止发送任务 — 任何情况都上报"""
    global scheduler_task, reconnect_task, SEND_STOP_FLAG, manual_send_stopped
    SEND_STOP_FLAG = True
    manual_send_stopped = True
    disconnected = 0
    try:
        if scheduler_task and not scheduler_task.done():
            scheduler_task.cancel()
            try:
                await scheduler_task
            except (asyncio.CancelledError, Exception):
                pass
        scheduler_task = None
        for wid, w in list(workers.items()):
            try:
                if getattr(w, "_connected", False):
                    await w.disconnect()
                    disconnected += 1
            except Exception:
                pass
        logger.info("[调度器停止] 已断开 %s 个水军连接" % disconnected)
    except Exception as e:
        logger.error("停止发送异常: %s" % e)
    finally:
        try:
            st = _today_send_stats()
            msg = "停止发送 已断开水军:%s 今日:%s 累计:%s" % (disconnected, st["today"], st["total"])
            await send_panel_notify(msg)
        except Exception as e:
            logger.warning("停止通知失败: %s" % e)
        try:
            open("/tmp/tg_share_v2_sending.done", "w").write("manual_stop")
            if os.path.exists("/tmp/tg_share_v2_sending"):
                os.remove("/tmp/tg_share_v2_sending")
        except Exception:
            pass
    return web.json_response({"ok": True, "message": "发送任务已停止，已断开 %s 个水军连接" % disconnected})


@routes.get("/api/send/status")
async def api_send_status(request):
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
            _gi = int(wc.get('group') or 0)
            _aid, _ah, _asrc = resolve_api_for_group(_gi, {'api_id': api_id, 'api_hash': api_hash})
            if _aid and _ah:
                api_id, api_hash = _aid, _ah
            w = ShareWorker(wc, api_id, api_hash)
            w._on_rate_limit_changed = _persist_worker_rate_limit
            w._on_dead_detected = _on_dead_detected
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
                if not manual_send_stopped:
                    logger.warning(f"[{PANEL_NAME}] 有待发目标但调度未运行，自动启动发送")
                try:
                    scheduler_task = asyncio.create_task(run_7step_scheduler())
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
                        scheduler_task = asyncio.create_task(run_7step_scheduler())
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



async def run_7step_scheduler():
    """发送调度器 - 按需连接水军号，逐个发送（带异常保护）"""
    try:
        await _run_send_scheduler_inner()
    except Exception as e:
        logger.error(f"=== 发送调度器异常退出: {e} ===")
        try:
            open("/tmp/tg_share_v2_sending.done", "w").write("error:"+str(e))
            if os.path.exists("/tmp/tg_share_v2_sending"):
                os.remove("/tmp/tg_share_v2_sending")
        except Exception:
            pass
        import traceback
        logger.error(traceback.format_exc())




# GROUP_RR_V1
_group_rr_index = 0

def _proxy_groups_ordered():
    try:
        pdata = proxy_pool.proxies if hasattr(proxy_pool, "proxies") else []
    except Exception:
        pdata = []
    return list(pdata)[:20]

def _worker_ids_of_proxy(px):
    ids = [str(x) for x in (px.get("assigned_bots") or [])]
    return ids

async def pick_worker_by_group_rr(worker_configs, workers, skip_ids=None):
    """1-20 组轮询：当前组只抽 1 个空闲号，用完切下一组。"""
    global _group_rr_index
    if skip_ids is None:
        skip_ids = set()
    groups = _proxy_groups_ordered()
    if not groups:
        return None, None
    cfg_by_id = {str(wc.get("id")): wc for wc in worker_configs}
    n = len(groups)
    for step in range(n):
        gi = (_group_rr_index + step) % n
        px = groups[gi]
        cands = []
        for wid in _worker_ids_of_proxy(px):
            if wid in skip_ids:
                continue
            wc = cfg_by_id.get(wid)
            if not wc:
                continue
            if wid in workers:
                w = workers[wid]
                if getattr(w, "is_restricted", False) or (hasattr(w, "is_in_cooldown") and w.is_in_cooldown()) or getattr(w, "is_dead", False):
                    continue
                if getattr(w, "rate_limit_count", 0) >= 5:
                    continue
            cands.append(wc)
        if not cands:
            continue
        wc = cands[0]
        _group_rr_index = (gi + 1) % n
        wc["_group_proxy"] = px
        return wc, px
    return None, None

async def _run_send_scheduler_inner():
    """发送调度器内部实现"""
    global current_activity
    current_activity = {"status": "启动中", "worker": "", "target": "", "step": "初始化"}
    log_activity("调度器启动", "发送调度器开始运行", status="info")
    logger.info("=== 发送调度器启动 ===")
    global SEND_STOP_FLAG
    SEND_STOP_FLAG = False
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
            await send_panel_notify(f"【2号面板】发送停止\n原因: 没有待发送目标\n今日成功: {st['today']} 条\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    sent_count = 0

    for target in targets:
        if sent_count >= daily_limit:
            logger.info(f"达到每日限制 {daily_limit}")
            break

        # === 辅助函数: 获取可用水军号 ===
        async def get_available_worker(skip_ids=None):
            """按组抽 1 个空闲号；同组其它号冷却不影响。"""
            if skip_ids is None:
                skip_ids = set()
            if len(skip_ids) >= max(1, len(worker_configs)):
                return None, None
            wc, gi = pick_worker_group_rr(worker_configs, workers, skip_ids)
            if wc is None:
                return None, None
            wid_try = wc["id"]
            _aid, _ah, _src = resolve_api_for_group(int(gi or 0), {"api_id": api_id, "api_hash": api_hash})
            use_id = _aid or api_id
            use_hash = _ah or api_hash
            if wid_try in workers:
                w = workers[wid_try]
                if w.is_restricted or w.is_in_cooldown() or getattr(w, "is_dead", False) or getattr(w, "rate_limit_count", 0) >= 5:
                    skip_ids.add(wid_try)
                    return await get_available_worker(skip_ids)
            if wid_try not in workers or not getattr(workers.get(wid_try), "_connected", False):
                connected_n = sum(1 for x in workers.values() if getattr(x, "_connected", False))
                if connected_n >= MAX_CONCURRENT_CONNECTIONS:
                    await disconnect_oldest_worker()
                try:
                    proxy = proxy_pool.get_proxy_for_worker(wid_try)
                except Exception:
                    proxy = wc.get("proxy")
                if isinstance(proxy, dict) and proxy.get("host"):
                    wc["proxy"] = proxy
                w = ShareWorker(wc, use_id, use_hash)
                try:
                    w._on_rate_limit_changed = _persist_worker_rate_limit
                except Exception:
                    pass
                ok = await w.connect()
                if not ok:
                    skip_ids.add(wid_try)
                    err = getattr(w, "last_error", "") or ""
                    if is_api_failure(err):
                        replace_group_api_from_standby(int(gi or 0), reason=str(err)[:120])
                    return await get_available_worker(skip_ids)
                workers[wid_try] = w
                await asyncio.sleep(CONNECTION_INTERVAL)
            w = workers[wid_try]
            try:
                if not await w.ensure_connected():
                    skip_ids.add(wid_try)
                    return await get_available_worker(skip_ids)
            except Exception:
                skip_ids.add(wid_try)
                return await get_available_worker(skip_ids)
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
        if SEND_STOP_FLAG:
            logger.info("[调度] 收到停止指令，立即退出循环")
            break
        # === Step 1: 换号验证目标，冻号跳过不标失败 ===
        skip_ids = set()
        user_entity, matched = None, False
        test_wconfig = test_worker = None
        frozen_only = False
        for _try in range(10):
            test_wconfig, test_worker = await get_available_worker(skip_ids)
            if not test_worker:
                logger.warning("[调度] 无可用水军做验证")
                break
            current_activity = {"status": "测试目标", "worker": test_wconfig['phone'], "target": target['username'], "step": "验证用户是否存在"}
            log_activity("测试目标", f"验证 @{target['username']} 是否存在", worker_phone=test_wconfig['phone'], target=target['username'])
            logger.info(f"[调度] 测试目标 @{target['username']}（使用 {test_wconfig['phone']}）")
            try:
                user_entity, matched = await asyncio.wait_for(
                    test_worker.search_user(target['username']),
                    timeout=35
                )
                if matched and user_entity:
                    break
                skip_ids.add(test_wconfig.get("id"))
                logger.warning(f"[调度] 未命中，换号 @{target['username']}")
            except Exception as e:
                es = str(e)
                logger.warning(f"[调度] 测试搜索异常: {e}")
                skip_ids.add(test_wconfig.get("id"))
                if "frozen" in es.lower() or "not available" in es.lower():
                    frozen_only = True
                    logger.warning(f"[调度] 水军冻结，换号重试 @{target['username']}")
                    await asyncio.sleep(1)
                    continue
                if "UsernameNotOccupied" in es or "UsernameInvalid" in es:
                    continue
                await asyncio.sleep(1)
                continue
        if not (matched and user_entity):
            if frozen_only or skip_ids:
                logger.warning(f"[调度] @{target['username']} 验证失败但可能是冻号，保持 pending")
                await asyncio.sleep(2)
                continue
            target["status"] = "failed"
            target["sent_at"] = datetime.now().isoformat()
            target["result"] = f"目标用户 @{target['username']} 不存在"
            target["bot_username"] = ""
            save_json(TARGETS_FILE, {"targets": targets_data["targets"]})
            logger.info(f"[调度] ❌ 目标 @{target['username']} 不存在，跳过")
            await asyncio.sleep(2)
            continue

        # === Step 2: 目标可达，正式发送（最多尝试3个不同水军号）===
        max_retry_workers = 2  # TG34: 同一目标最多2个水军
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
            send_worker = worker_send

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
                es = str(task_err)
                if "frozen" in es.lower() or "not available" in es.lower():
                    logger.warning("[调度] 冻号导致任务异常，目标保持 pending，换号")
                    success, msg = False, "worker_frozen"
                else:
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
                    logger.info(f"[调度] 频率限制，换下一个水军号...")
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

        if (not send_success) and str(msg) == "worker_frozen":
            logger.warning(f"[调度] 冻号，目标 @{target.get('username')} 保持 pending")
            target["status"] = "pending"
            target["result"] = "水军冻结，等待可用号"
        else:
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

        # 发送间隔
        if send_success:
            wait_time = random.randint(interval_min, interval_max)
            current_activity = {"status": "等待中", "worker": wconfig_send['phone'] if wconfig_send else "", "target": target['username'], "step": f"发送成功，等待 {wait_time} 秒"}
            log_activity("发送成功", f"成功发送给 @{target['username']}，等待 {wait_time}s", worker_phone=wconfig_send['phone'] if wconfig_send else "", target=target['username'], status="success")
            logger.info(f"[调度] ✅ 发送成功，等待 {wait_time} 秒后继续...")
            await asyncio.sleep(wait_time)
        else:
            await asyncio.sleep(5)
    current_activity = {"status": "已完成", "worker": "", "target": "", "step": f"共发送 {sent_count} 条"}
    log_activity("调度器完成", f"共发送 {sent_count} 条", status="info")
    try:
        st = _today_send_stats()
        await send_panel_notify(f"【2号面板】发送任务结束\n原因: 目标用完或达每日上限\n本轮: {sent_count} 条\n今日成功: {st['today']} 条\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:
        pass
    try:
        await send_panel_notify(f"1号面板发送任务完成\n本次发送: {sent_count} 条\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:
        pass
    try:
        await send_panel_notify(f"1号面板发送任务完成\n本次发送: {sent_count} 条\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    _cfg_workers = load_json(WORKERS_CONFIG_FILE).get("workers", [])
    wconfig_send, _cur_ip = pick_next_ip_worker(_cfg_workers, workers)
    idle_workers = []
    if wconfig_send:
        wid = wconfig_send.get("id")
        worker = workers.get(wid)
        if worker is None:
            for _k, _w in workers.items():
                if getattr(_w, "phone", "") == wconfig_send.get("phone"):
                    wid, worker = _k, _w
                    break
        if worker is not None and getattr(worker, "_connected", False):
            idle_workers = [(wid, worker)]
            logger.info(f"[调度] IP线路 {_cur_ip.get('host')}:{_cur_ip.get('port')} 选用 {wconfig_send.get('phone')}")
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
        if not globals().get("_STALL_MONITOR_STARTED"):
            globals()["_stall_monitor_task"] = asyncio.create_task(stall_send_monitor())
            globals()["_STALL_MONITOR_STARTED"] = True
            logger.info(f"[{PANEL_NAME}] 长时间无发送监控已启动 (stall 仅一次)")



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
        name = "1号面板"
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
    global _EXIT_SIGNALS_REGISTERED
    if globals().get("_EXIT_SIGNALS_REGISTERED"):
        return
    try:
        signal.signal(signal.SIGTERM, on_process_signal)
        signal.signal(signal.SIGINT, on_process_signal)
        globals()['_EXIT_SIGNALS_REGISTERED'] = True
        logger.info(f"[{globals().get('PANEL_NAME', '2号面板')}] 已注册 SIGTERM/SIGINT 退出上报")
    except Exception as e:
        try:
            logger.warning(f"注册退出信号失败: {e}")
        except Exception:
            pass


@routes.post("/api/workers/export-deleted")
async def api_workers_export_deleted(request):
    """导出已删除（从 data/returned/ 打包）"""
    import zipfile, io
    returned = Path("data/returned")
    if not returned.exists():
        return web.json_response({"ok": False, "error": "没有已删除记录"}, status=400)
    buf = io.BytesIO()
    count = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for folder in returned.iterdir():
            if not folder.is_dir():
                continue
            for f in folder.iterdir():
                if f.is_file():
                    zf.write(f, f"{folder.name}/{f.name}")
                    count += 1
    if count == 0:
        return web.json_response({"ok": False, "error": "没有可导出的文件"}, status=400)
    buf.seek(0)
    return web.Response(
        body=buf.read(),
        content_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=deleted_workers.zip"}
    )

@routes.post("/api/profile-template")
async def api_save_profile_template(request):
    try:
        data = await request.json()
        save_json(PROFILE_TEMPLATE_FILE, data)
        return web.json_response({"ok": True})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)


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


@routes.get("/api/api-configs")
async def api_api_configs_get(request):
    return web.json_response({"configs": _load_api_cfgs()})

@routes.post("/api/api-configs")
async def api_api_configs_add(request):
    body = await request.json()
    arr = _load_api_cfgs()
    item = {
        "id": f"api_{int(time.time())}_{random.randint(100,999)}",
        "api_id": body.get("api_id"),
        "api_hash": body.get("api_hash"),
        "note": body.get("note") or "",
    }
    if not item["api_id"] or not item["api_hash"]:
        return web.json_response({"ok": False, "error": "缺少 api_id/api_hash"}, status=400)
    arr.append(item)
    _save_api_cfgs(arr)
    return web.json_response({"ok": True, "total": len(arr)})

@routes.post("/api/api-configs/batch")
async def api_api_configs_batch(request):
    body = await request.json()
    text = body.get("text") or ""
    arr = _load_api_cfgs()
    added = 0
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [x.strip() for x in line.replace(",", "|").split("|")]
        if len(parts) < 2:
            continue
        arr.append({"id": f"api_{int(time.time())}_{added}", "api_id": parts[0], "api_hash": parts[1], "note": parts[2] if len(parts)>2 else ""})
        added += 1
    _save_api_cfgs(arr)
    return web.json_response({"ok": True, "added": added, "total": len(arr)})

@routes.delete("/api/api-configs/{config_id}")
async def api_api_configs_del(request):
    cid = request.match_info["config_id"]
    arr = [x for x in _load_api_cfgs() if str(x.get("id")) != str(cid) and str(x.get("api_id")) != str(cid)]
    _save_api_cfgs(arr)
    return web.json_response({"ok": True, "total": len(arr)})

@routes.post("/api/api-configs/auto-assign")
async def api_api_configs_auto(request):
    return web.json_response(api_auto_assign_by_group())

@routes.post("/api/api-configs/replace-group")
async def api_api_configs_replace(request):
    body = await request.json()
    return web.json_response(api_replace_group(int(body.get("group", -1))))

# ============ 7步工作流调度器 ============
_group_rr_index = 0
_bot_rr_index = {}

def _get_groups_count():
    try:
        pdata = load_json(PROXY_POOL_FILE)
        proxies = pdata.get("proxies", pdata if isinstance(pdata, list) else [])
        return min(30, len(proxies) or 30)
    except Exception:
        return 30

def pick_worker_least_limit(worker_configs, live_workers, skip_ids=None):
    global _group_rr_index
    skip_ids = skip_ids or set()
    n = _get_groups_count()
    by_group = {i: [] for i in range(n)}
    for wc in worker_configs:
        g = wc.get("group")
        if not isinstance(g, int):
            g = 0
        if 0 <= g < n:
            by_group[g].append(wc)

    def usable(wc):
        wid = str(wc.get("id"))
        if wid in skip_ids:
            return False
        w = live_workers.get(wid)
        if w is None:
            return True
        if getattr(w, "is_dead", False) or getattr(w, "is_restricted", False):
            return False
        if hasattr(w, "is_in_cooldown") and w.is_in_cooldown():
            return False
        if getattr(w, "rate_limit_count", 0) >= 10:
            return False
        return True

    for step in range(n):
        gi = (_group_rr_index + step) % n
        cands = [wc for wc in by_group.get(gi, []) if usable(wc)]
        if not cands:
            continue
        cands.sort(key=lambda x: int(x.get("rate_limit_count", 0) or 0))
        wc = cands[0]
        _group_rr_index = (gi + 1) % n
        return wc, gi
    return None, None

def pick_bot_for_group(group_index, bots_list):
    global _bot_rr_index
    if not bots_list:
        return None
    idx = _bot_rr_index.get(group_index, 0)
    bot = bots_list[idx % len(bots_list)]
    _bot_rr_index[group_index] = (idx + 1) % len(bots_list)
    return bot

async def log_step(step_num, step_name, worker_phone="", target="", details="", status="info"):
    global current_activity
    msg = f"第{step_num}步: {step_name}"
    if details:
        msg += f" - {details}"
    current_activity = {"status": f"第{step_num}步", "worker": worker_phone, "target": target, "step": step_name, "details": details}
    log_activity(msg, details, worker_phone=worker_phone, target=target, status=status)
    logger.info(f"[7步] {msg} | 水军:{worker_phone} 目标:{target}")

async def run_7step_scheduler():
    global current_activity, SEND_STOP_FLAG
    SEND_STOP_FLAG = False
    current_activity = {"status": "启动中", "worker": "", "target": "", "step": "初始化"}
    log_activity("调度器启动", "7步工作流开始", status="info")
    logger.info("=== 7步工作流调度器启动 ===")

    config = load_json(BOT_CONFIG_FILE)
    api_id = config.get("api_id")
    api_hash = config.get("api_hash")
    daily_limit = config.get("daily_limit", 30)
    rest_after_success = config.get("rest_after_success", 300)
    step_wait = config.get("step_wait", 2)

    if not api_id or not api_hash:
        logger.error("未配置 API ID/Hash")
        return

    targets_data = load_json(TARGETS_FILE)
    targets = [t for t in targets_data.get("targets", []) if t.get("status") == "pending"]
    if not targets:
        logger.info("没有待发送目标")
        return

    workers_data = load_json(WORKERS_CONFIG_FILE)
    worker_configs = [w for w in workers_data.get("workers", []) if w.get("status") == "active"]
    if not worker_configs:
        logger.info("没有可用水军")
        return

    bots_data = load_json(BOTS_FILE)
    all_bots = bots_data.get("bots", [])

    sent_count = 0
    for target in targets:
        if SEND_STOP_FLAG or sent_count >= daily_limit:
            break

        target_username = target.get("username", "")
        wc, gi = pick_worker_least_limit(worker_configs, workers)
        if not wc:
            await log_step(1, "选水军失败", details="无可用水军，跳过此目标", status="warning")
            continue

        phone = wc.get("phone", "")
        wid = wc.get("id")
        await log_step(1, "从水军池拿水军", worker_phone=phone, target=target_username, details=f"组{gi+1} 限流最少号 {phone}")

        worker = workers.get(wid)
        if not worker:
            # 自动创建并连接
            try:
                from worker import ShareWorker
                api_id = config.get("api_id")
                api_hash = config.get("api_hash")
                worker = ShareWorker(wc, api_id, api_hash)
                workers[wid] = worker
                await worker.ensure_connected()
                await log_step(1, "自动连接水军", worker_phone=phone, details="新建并连接", status="info")
            except Exception as e:
                await log_step(1, "水军连接失败", worker_phone=phone, details=str(e), status="error")
                continue
        elif not getattr(worker, "_connected", False):
            try:
                await worker.ensure_connected()
                await log_step(1, "重新连接水军", worker_phone=phone, status="info")
            except Exception as e:
                await log_step(1, "重连失败", worker_phone=phone, details=str(e), status="error")
                continue

        await log_step(2, "检查目标用户", worker_phone=phone, target=target_username, details="搜索用户是否存在")
        try:
            user_entity, matched = await asyncio.wait_for(worker.search_user(target_username), timeout=30)
            if not (matched and user_entity):
                target["status"] = "failed"
                target["result"] = "首次验证失败，不再使用"
                save_json(TARGETS_FILE, {"targets": targets_data["targets"]})
                await log_step(2, "验证失败", worker_phone=phone, target=target_username, details="首次失败，此用户不再使用", status="error")
                continue
            await log_step(2, "验证成功", worker_phone=phone, target=target_username, status="success")
        except Exception as e:
            await log_step(2, "验证异常", worker_phone=phone, target=target_username, details=str(e), status="error")
            continue

        group_bots = [b for b in all_bots if b.get("group") == gi]
        if not group_bots:
            group_bots = all_bots
        bot = pick_bot_for_group(gi, group_bots)
        if not bot:
            await log_step(3, "无可用Bot", worker_phone=phone, status="error")
            continue
        bot_username = bot.get("username", "").lstrip("@")
        await log_step(3, "给Bot发送/start", worker_phone=phone, target=target_username, details=f"Bot: @{bot_username}")

        bot_replied = await worker.send_start_and_wait(bot_username, timeout=15)
        if not bot_replied:
            await log_step(3, "Bot无回复，换下一个", worker_phone=phone, details="马上换Bot", status="warning")
            bot2 = pick_bot_for_group(gi, group_bots)
            if bot2 and bot2 != bot:
                bot_username = bot2.get("username", "").lstrip("@")
                bot_replied = await worker.send_start_and_wait(bot_username, timeout=15)
        if not bot_replied:
            await log_step(3, "Bot无有效回复", status="error")
            continue
        await log_step(3, "Bot已回复", worker_phone=phone, details=f"@{bot_username}", status="success")

        await log_step(4, "读取Bot消息并点击转发", worker_phone=phone, target=target_username)
        await worker.read_bot_and_prepare_share(bot_username)

        await log_step(5, "搜索指定用户", worker_phone=phone, target=target_username)
        await log_step(6, "确认用户并点击发送", worker_phone=phone, target=target_username)

        await log_step(7, "等待验证发送结果", worker_phone=phone, target=target_username)
        try:
            success, msg = await asyncio.wait_for(worker.execute_share_task(target_username), timeout=60)
            if success:
                sent_count += 1
                target["status"] = "sent"
                target["sent_at"] = datetime.now().isoformat()
                target["result"] = "成功"
                save_json(TARGETS_FILE, {"targets": targets_data["targets"]})
                await log_step(7, "发送成功", worker_phone=phone, target=target_username, details="已记录仪表", status="success")
                # 更新统计
                try:
                    stats = load_json(STATS_FILE)
                    from datetime import date
                    today = str(date.today())
                    if stats.get("today_date") != today:
                        stats["today_sends"] = 0
                        stats["today_date"] = today
                    stats["total_sends"] = stats.get("total_sends", 0) + 1
                    stats["today_sends"] = stats.get("today_sends", 0) + 1
                    save_json(STATS_FILE, stats)
                except Exception as e:
                    logger.warning(f"更新统计失败: {e}")
                await log_step(7, "成功后休息", details=f"休息 {rest_after_success} 秒后换组换号", status="info")
                await asyncio.sleep(rest_after_success)
            else:
                target["status"] = "failed"
                target["result"] = msg or "发送失败，不再使用"
                save_json(TARGETS_FILE, {"targets": targets_data["targets"]})
                await log_step(7, "发送失败", worker_phone=phone, target=target_username, details="失败不记录仪表，此用户不再使用", status="error")
        except Exception as e:
            await log_step(7, "验证异常", details=str(e), status="error")

        await asyncio.sleep(step_wait)

    log_activity("调度器结束", f"本轮完成，成功 {sent_count} 条", status="info")
    logger.info("=== 7步工作流调度器结束 ===")

@routes.post("/api/workers/export")
async def api_workers_export(request):
    """一键批量导出：扫描 sessions 目录，已删除的也能导出"""
    import zipfile, io
    from pathlib import Path as _P

    sessions_dir = _P("data/sessions")
    if not sessions_dir.exists():
        sessions_dir = _P("sessions")
    if not sessions_dir.exists():
        return web.json_response({"ok": False, "error": "sessions 目录不存在"}, status=400)

    session_files = list(sessions_dir.glob("*.session"))
    if not session_files:
        return web.json_response({"ok": False, "error": "没有可导出的 session 文件"}, status=400)

    # 尝试读取已删除前的备份，补充 json 信息
    bak = _P("data/workers_config.json.bak_before_delete")
    bak_workers = {}
    if bak.exists():
        try:
            bak_data = json.loads(bak.read_text(encoding="utf-8"))
            for w in bak_data.get("workers", []):
                phone = str(w.get("phone", "")).replace("+", "")
                bak_workers[phone] = w
        except Exception:
            pass

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for sess in session_files:
            phone = sess.stem  # 文件名即手机号
            zf.write(sess, f"{phone}.session")
            # 尽量带上 json 信息
            info = bak_workers.get(phone, {"phone": f"+{phone}"})
            zf.writestr(f"{phone}.json", json.dumps(info, ensure_ascii=False, indent=2))

    buf.seek(0)
    return web.Response(
        body=buf.read(),
        content_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=workers_export.zip"}
    )


def _backup_deleted_worker(worker_info):
    """删除前归档 session+json 到 data/returned/{phone}/"""
    import shutil
    phone = str(worker_info.get("phone", "")).replace("+", "")
    if not phone:
        return
    dest = Path("data/returned") / phone
    dest.mkdir(parents=True, exist_ok=True)
    # 拷贝 session
    sess = Path("data/sessions") / f"{phone}.session"
    if sess.exists():
        shutil.copy2(sess, dest / f"{phone}.session")
    # 写 json
    (dest / f"{phone}.json").write_text(json.dumps(worker_info, ensure_ascii=False, indent=2), encoding="utf-8")
    # 记录清单
    rec_file = Path("data/returned_workers.json")
    rec = {"items": []}
    if rec_file.exists():
        try:
            rec = json.loads(rec_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    items = [x for x in rec.get("items", []) if x.get("phone") != phone]
    items.append({"phone": phone, "deleted_at": datetime.now().isoformat()})
    rec["items"] = items
    rec_file.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"已归档删除的水军: {phone}")


PROFILE_TEMPLATE_FILE = DATA_DIR / "profile_template.json"

@routes.get("/api/profile-template")
async def api_get_profile_template(request):
    data = load_json(PROFILE_TEMPLATE_FILE)
    return web.json_response(data or {})
