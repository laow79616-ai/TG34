#!/usr/bin/env python3
"""
长时间未发送 → 自动重启调度；
重启后 10 分钟仍无成功 → 上报 Bot
"""
import json, time, os, urllib.request, urllib.parse, subprocess, socket
from pathlib import Path
from datetime import datetime

CONFIG = json.loads(Path("/root/tg_status_bot/config.json").read_text())
BOT_TOKEN = CONFIG["bot_token"]
CHAT_ID = str(CONFIG["chat_id"])
INTERVAL = 15

# 超过该秒数无「发送成功」→ 自动刷新启动
NO_SUCCESS_AUTO_RESTART_SEC = 300   # 5 分钟无成功就自动拉起
# 自动刷新后，再等该秒数仍无成功 → 上报
AFTER_RESTART_WARN_SEC = 600        # 10 分钟

PANELS = {
    "a": {
        "title": "面板A (3号) a.tg34.pw",
        "tag": "A3",
        "port": 8001,
        "flag": "/tmp/tg_panel_a_sending",
        "data": "/root/tg_share_panel_a/data",
        "svc": "tg-panel-a",
        "unit": "tg-panel-a",
        "start_url": "http://127.0.0.1:8001/api/send/start",
    },
    "b": {
        "title": "面板B (4号) b.tg34.pw",
        "tag": "B4",
        "port": 8002,
        "flag": "/tmp/tg_panel_b_sending",
        "data": "/root/tg_share_panel_b/data",
        "svc": "tg-panel-b",
        "unit": "tg-panel-b",
        "start_url": "http://127.0.0.1:8002/api/send/start",
    },
}

state = {
    k: {
        "svc_ok": True,
        "port_ok": True,
        "last_success_ts": time.time(),
        "last_auto_restart_ts": 0,
        "waiting_after_restart": False,
        "restart_ts": 0,
        "last_event": "",
        "reported_after_restart": False,
    }
    for k in PANELS
}

def send(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": CHAT_ID, "text": text, "parse_mode": "HTML"
    }).encode()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=15) as r:
            return r.status == 200
    except Exception as e:
        print("send fail:", e)
        return False

def summary(data_dir):
    lines = []
    try:
        workers = json.loads(Path(f"{data_dir}/workers_config.json").read_text()).get("workers", [])
        targets = json.loads(Path(f"{data_dir}/targets.json").read_text()).get("targets", [])
        pending = sum(1 for t in targets if t.get("status") == "pending")
        sent = sum(1 for t in targets if t.get("status") == "sent")
        lines.append(f"水军:{len(workers)} 待发:{pending} 已发:{sent}")
    except Exception:
        lines.append("数据读取失败")
    try:
        stats = json.loads(Path(f"{data_dir}/stats.json").read_text())
        lines.append(f"今日:{stats.get('today_sends',0)} 累计:{stats.get('total_sends',0)}")
    except Exception:
        pass
    return "\n".join(lines)

def notify(title, data_dir, reason, level="⚠️", panel_tag=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tag = panel_tag or title
    # 第一行就标明哪个面板
    msg = (
        f"{level} <b>[{tag}]</b> {reason}\n"
        f"时间: {now}\n\n"
        f"<b>{title}</b>\n"
        f"{summary(data_dir)}"
    )
    ok = send(msg)
    print(now, tag, reason, "OK" if ok else "FAIL")
    return ok

def svc_active(name):
    try:
        return subprocess.run(
            ["systemctl", "is-active", name], capture_output=True, text=True
        ).stdout.strip() == "active"
    except Exception:
        return False

def port_open(port):
    try:
        s = socket.create_connection(("127.0.0.1", port), timeout=2)
        s.close()
        return True
    except Exception:
        return False

def journal_text(unit, n=60):
    try:
        r = subprocess.run(
            ["journalctl", "-u", unit, "-n", str(n), "--no-pager", "-o", "cat"],
            capture_output=True, text=True, errors="ignore",
        )
        return r.stdout
    except Exception:
        return ""

def try_start_send(p):
    """调用本地 start 接口；若需登录则仅写标记+依赖面板侧逻辑"""
    # 1) 确保服务在
    if not svc_active(p["svc"]):
        subprocess.run(["systemctl", "restart", p["svc"]], capture_output=True)
        time.sleep(3)
    # 2) POST /api/send/start（无 token 时可能 401，仍尝试）
    try:
        req = urllib.request.Request(
            p["start_url"],
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            body = r.read().decode("utf-8", errors="ignore")
            print("start resp", p["port"], r.status, body[:120])
            return True
    except Exception as e:
        print("start api fail", p["port"], e)
        # 401 时：重启服务让用户配置的常驻逻辑，或仅重启进程
        subprocess.run(["systemctl", "restart", p["svc"]], capture_output=True)
        time.sleep(2)
        return False

def main():
    print("监控: 久未发送自动刷新; 刷新后10分钟仍无成功才上报")
    send("✅ 监控策略已更新：久未发送将自动重启调度；重启后10分钟仍无成功再通知")

    for k, p in PANELS.items():
        state[k]["svc_ok"] = svc_active(p["svc"])
        state[k]["port_ok"] = port_open(p["port"])
        state[k]["last_success_ts"] = time.time()

    while True:
        time.sleep(INTERVAL)
        now = time.time()
        for k, p in PANELS.items():
            try:
                ok_svc = svc_active(p["svc"])
                ok_port = port_open(p["port"])

                # 服务/端口挂掉：先自动拉起服务，再视情况上报
                if not ok_svc:
                    subprocess.run(["systemctl", "restart", p["svc"]], capture_output=True)
                    time.sleep(3)
                    ok_svc = svc_active(p["svc"])
                    if not ok_svc:
                        notify(p["title"], p["data"], "服务无法启动(502)", "🔴", panel_tag=p.get("tag", "A/B"))
                    else:
                        # 服务拉起后尝试继续发送
                        try_start_send(p)
                        state[k]["waiting_after_restart"] = True
                        state[k]["restart_ts"] = now
                        state[k]["reported_after_restart"] = False
                        print(k, "svc restarted + try start")

                if state[k]["port_ok"] and not ok_port and ok_svc:
                    # 进程在但端口异常，重启一次
                    subprocess.run(["systemctl", "restart", p["svc"]], capture_output=True)
                    time.sleep(3)
                    try_start_send(p)
                    state[k]["waiting_after_restart"] = True
                    state[k]["restart_ts"] = now
                    state[k]["reported_after_restart"] = False

                # 扫日志：成功 / 异常
                log = journal_text(p["unit"], 80)
                for line in log.splitlines()[-40:]:
                    if "发送成功" in line:
                        state[k]["last_success_ts"] = now
                        state[k]["waiting_after_restart"] = False
                        state[k]["reported_after_restart"] = False
                        try:
                            open(p["flag"], "w").write("1")
                        except Exception:
                            pass
                    if line != state[k]["last_event"] and "发送调度器异常退出" in line:
                        state[k]["last_event"] = line
                        # 异常：自动再拉起，不立刻刷屏；进入等待成功窗口
                        try_start_send(p)
                        state[k]["waiting_after_restart"] = True
                        state[k]["restart_ts"] = now
                        state[k]["reported_after_restart"] = False
                        print(k, "exception → auto restart send")
                    if line != state[k]["last_event"] and "发送调度器完成" in line:
                        state[k]["last_event"] = line
                        # 正常完成也自动再开一轮（持续跑）
                        try_start_send(p)
                        state[k]["waiting_after_restart"] = True
                        state[k]["restart_ts"] = now
                        state[k]["reported_after_restart"] = False
                        print(k, "complete → auto restart send")

                # 长时间无成功 → 自动刷新（不先上报）
                idle = now - state[k]["last_success_ts"]
                since_restart = now - state[k]["last_auto_restart_ts"]
                if ok_svc and idle >= NO_SUCCESS_AUTO_RESTART_SEC and since_restart >= 120:
                    print(k, f"idle {int(idle)}s → auto restart send")
                    try_start_send(p)
                    state[k]["last_auto_restart_ts"] = now
                    state[k]["waiting_after_restart"] = True
                    state[k]["restart_ts"] = now
                    state[k]["reported_after_restart"] = False
                    try:
                        open(p["flag"], "w").write("1")
                    except Exception:
                        pass

                # 自动刷新后 10 分钟仍无成功 → 上报
                if (
                    state[k]["waiting_after_restart"]
                    and not state[k]["reported_after_restart"]
                    and state[k]["restart_ts"]
                    and (now - state[k]["restart_ts"]) >= AFTER_RESTART_WARN_SEC
                    and (now - state[k]["last_success_ts"]) >= AFTER_RESTART_WARN_SEC
                ):
                    notify(
                        p["title"],
                        p["data"],
                        f"已自动刷新，但{AFTER_RESTART_WARN_SEC//60}分钟仍无发送成功",
                        "🔴",
                    )
                    state[k]["reported_after_restart"] = True

                state[k]["svc_ok"] = ok_svc
                state[k]["port_ok"] = ok_port
            except Exception as e:
                print("loop", k, e)

if __name__ == "__main__":
    main()
