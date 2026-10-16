"""
Web 管理面板鉴权模块
- 首次启动自动生成随机管理员密码(仅显示一次), 密码以 pbkdf2-sha256 hash 持久化, 不存明文
- 无状态 HMAC-SHA256 签名 token, 默认 7 天有效期(secret_key 首次随机生成并持久化)
"""
import hmac
import json
import time
import base64
import hashlib
import secrets
import logging

logger = logging.getLogger("Auth")

TOKEN_TTL = 7 * 24 * 3600  # token 有效期: 7 天
_PBKDF2_ITERATIONS = 100_000
# 去除易混字符(0/O, 1/l/I)的强密码字母表
_PASSWORD_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789"

# 进程内缓存的鉴权配置
_auth_cfg = None


def _hash_password(password, salt_hex):
    dk = hashlib.pbkdf2_hmac(
        "sha256", (password or "").encode("utf-8"),
        bytes.fromhex(salt_hex), _PBKDF2_ITERATIONS
    )
    return dk.hex()


def _generate_password(length=16):
    return "".join(secrets.choice(_PASSWORD_ALPHABET) for _ in range(length))


def _b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s):
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def load_or_init_auth():
    """加载鉴权配置; 若不存在则首次生成随机密码并持久化。

    返回 (config, plaintext_password_or_None)。plaintext 仅在本次生成时非空,
    供调用方打印一次。延迟 import config 以避免模块导入副作用。
    """
    global _auth_cfg
    from config import DATA_DIR, save_json, load_json
    auth_file = DATA_DIR / "auth.json"
    existing = load_json(auth_file)
    if existing and existing.get("password_hash") and existing.get("secret_key"):
        _auth_cfg = existing
        return existing, None
    # 首次生成
    password = _generate_password()
    salt = secrets.token_hex(16)
    cfg = {
        "admin_username": "admin",
        "salt": salt,
        "password_hash": _hash_password(password, salt),
        "secret_key": secrets.token_hex(32),
    }
    save_json(auth_file, cfg)
    _auth_cfg = cfg
    return cfg, password


def _get_cfg():
    if _auth_cfg is None:
        load_or_init_auth()
    return _auth_cfg


def verify_credentials(username, password):
    """校验用户名/密码(恒定时间比较)"""
    cfg = _get_cfg()
    if username != cfg.get("admin_username"):
        return False
    expected = cfg.get("password_hash", "")
    actual = _hash_password(password, cfg.get("salt", ""))
    return hmac.compare_digest(expected, actual)


def create_token(username):
    """生成无状态 HMAC 签名 token: base64url(payload).base64url(sig)"""
    cfg = _get_cfg()
    payload = {"u": username, "exp": int(time.time()) + TOKEN_TTL}
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(
        cfg["secret_key"].encode("utf-8"), payload_b64.encode("ascii"), hashlib.sha256
    ).digest()
    return payload_b64 + "." + _b64url(sig)


def verify_token(token):
    """校验 token 签名与过期时间, 有效返回 True"""
    if not token or "." not in token:
        return False
    cfg = _get_cfg()
    try:
        payload_b64, sig_b64 = token.rsplit(".", 1)
        expected_sig = hmac.new(
            cfg["secret_key"].encode("utf-8"), payload_b64.encode("ascii"), hashlib.sha256
        ).digest()
        if not hmac.compare_digest(_b64url(expected_sig), sig_b64):
            return False
        payload = json.loads(_b64url_decode(payload_b64))
        return int(payload.get("exp", 0)) >= int(time.time())
    except Exception:
        return False
