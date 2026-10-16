"""
配置管理模块
- 管理 API 配置、Worker 配置、用户名列表、发送日志、IP池
"""
import os
import json
import tempfile
import logging
from pathlib import Path

logger = logging.getLogger("Config")

# 基础路径（支持环境变量覆盖）
BASE_DIR = Path(os.environ.get("TG_SHARE_BASE_DIR", "/root/tg_share_panel_a"))
DATA_DIR = Path(os.environ.get("TG_SHARE_DATA_DIR", str(BASE_DIR / "data")))
SESSIONS_DIR = DATA_DIR / "sessions"
LOGS_DIR = BASE_DIR / "logs"

# 配置文件
BOT_CONFIG_FILE = DATA_DIR / "bot_config.json"
WORKERS_CONFIG_FILE = DATA_DIR / "workers_config.json"
TARGETS_FILE = DATA_DIR / "targets.json"
BOTS_FILE = DATA_DIR / "bots.json"
PROXY_POOL_FILE = DATA_DIR / "proxy_pool.json"
AD_CONFIG_FILE = DATA_DIR / "ad_config.json"
SCHEDULE_FILE = DATA_DIR / "schedule.json"
STATS_FILE = DATA_DIR / "stats.json"

# 确保目录存在
DATA_DIR.mkdir(parents=True, exist_ok=True)
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)


def load_json(filepath):
    """安全加载JSON文件"""
    filepath = Path(filepath)
    if filepath.exists():
        try:
            return json.loads(filepath.read_text(encoding='utf-8'))
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"加载文件失败 {filepath}: {e}")
    return {}


def save_json(filepath, data):
    """安全保存JSON文件(原子写入)

    先写入同目录下的临时文件并 fsync 落盘, 再用 os.replace 原子替换目标文件。
    避免写入过程中进程崩溃/磁盘满导致目标文件被截断损坏。
    """
    filepath = Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(data, ensure_ascii=False, indent=2)
    # 临时文件必须与目标同目录, 确保 os.replace 是同一文件系统上的原子操作
    fd, tmp_path = tempfile.mkstemp(
        dir=str(filepath.parent), prefix=f".{filepath.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())  # 确保数据真正落盘, 防止崩溃后内容丢失
        os.replace(tmp_path, filepath)  # 原子替换: 读取方要么看到旧文件, 要么看到完整新文件
    except Exception:
        try:
            os.unlink(tmp_path)  # 失败时清理残留临时文件
        except OSError:
            pass
        raise
