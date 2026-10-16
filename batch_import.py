"""
批量导入水军号模块
- API配置管理（多组api_id/api_hash）
- ZIP批量导入session文件
- 手动输入资料 + 一键设置（名字/用户名前缀/简介/头像）
- 头像库管理
"""
import os
import json
import zipfile
import shutil
import random
import asyncio
import logging
import time
from pathlib import Path
from aiohttp import web

from config import save_json  # 原子写入, 避免并发/崩溃损坏配置文件

logger = logging.getLogger("BatchImport")

# 配置文件路径
DATA_DIR = Path(os.environ.get("TG_SHARE_DATA_DIR", Path(__file__).resolve().parent / "data"))
API_CONFIGS_FILE = DATA_DIR / "api_configs.json"
AVATARS_DIR = DATA_DIR / "avatars"
IMPORT_TEMP_DIR = DATA_DIR / "import_temp"
SESSIONS_DIR = DATA_DIR / "sessions"

# 确保目录存在
AVATARS_DIR.mkdir(parents=True, exist_ok=True)
IMPORT_TEMP_DIR.mkdir(parents=True, exist_ok=True)

# 女性名字库（两个字）
FEMALE_NAMES = [
    "婉儿", "雪儿", "小雅", "诗涵", "梦琪", "雨薇", "紫萱", "欣怡",
    "思琪", "雅琪", "梦洁", "语嫣", "佳琪", "雨晴", "梦瑶", "诗雨",
    "芷若", "若曦", "沐晴", "清雅", "芊芊", "雨桐", "思雨", "梦蝶",
    "紫嫣", "冰冰", "甜甜", "蜜儿", "小鱼", "糖糖", "果果", "朵朵",
    "莉莉", "薇薇", "琳琳", "萱萱", "悠悠", "柔柔", "暖暖", "安安",
    "静静", "盈盈", "楚楚", "依依", "晴晴", "茜茜", "曼曼", "灵灵",
    "珊珊", "露露", "菲菲", "蓉蓉", "娜娜", "媛媛", "婷婷", "倩倩",
    "丽丽", "颖颖", "慧慧", "敏敏", "佳佳", "欢欢", "乐乐", "圆圆",
    "可可", "豆豆", "妮妮", "兰兰", "芳芳", "燕燕", "凤凤", "玲玲",
    "青青", "翠翠", "红红", "彤彤", "紫紫", "碧碧", "瑶瑶", "琪琪",
    "梦梦", "云云", "雪雪", "霜霜", "月月", "星星", "花花", "叶叶",
    "竹子", "梅子", "桃子", "杏儿", "樱子", "莲儿", "荷儿", "菊儿",
    "兰儿", "芝芝", "蕊蕊", "苗苗", "芽芽", "蔓蔓"
]

# 统一简介
DEFAULT_BIO = """https://kuaiyue.vip
https://t.me/kuaiyue9
咨询热线：@kuaiyue123456789"""


def load_api_configs():
    """加载API配置"""
    if API_CONFIGS_FILE.exists():
        try:
            return json.loads(API_CONFIGS_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {"configs": [], "next_username_number": 210}


def save_api_configs(data):
    """保存API配置"""
    save_json(API_CONFIGS_FILE, data)


def get_available_name():
    """获取一个未使用的女性名字"""
    used_names = set()
    workers_file = DATA_DIR / "workers_config.json"
    if workers_file.exists():
        try:
            workers = json.loads(workers_file.read_text(encoding='utf-8'))
            for w in workers.get("workers", []):
                name = w.get("display_name", "")
                if "-" in name:
                    used_names.add(name.split("-")[0])
        except:
            pass
    available = [n for n in FEMALE_NAMES if n not in used_names]
    if not available:
        # 如果名字用完了，加数字后缀
        return random.choice(FEMALE_NAMES) + str(random.randint(1, 99))
    return random.choice(available)


def get_next_username_number():
    """获取下一个可用的用户名编号"""
    config = load_api_configs()
    num = config.get("next_username_number", 210)
    config["next_username_number"] = num + 1
    save_api_configs(config)
    return num


def get_unused_avatar():
    """获取一个未使用的头像文件路径"""
    if not AVATARS_DIR.exists():
        return None
    used_avatars = set()
    workers_file = DATA_DIR / "workers_config.json"
    if workers_file.exists():
        try:
            workers = json.loads(workers_file.read_text(encoding='utf-8'))
            for w in workers.get("workers", []):
                avatar = w.get("avatar_file", "")
                if avatar:
                    used_avatars.add(avatar)
        except:
            pass
    all_avatars = [f.name for f in AVATARS_DIR.iterdir() if f.suffix.lower() in ('.jpg', '.jpeg', '.png', '.webp')]
    available = [a for a in all_avatars if a not in used_avatars]
    if not available:
        # 如果头像用完了，随机选一个
        if all_avatars:
            return random.choice(all_avatars)
        return None
    return random.choice(available)


# ============ API路由 ============

# --- API配置管理 ---
async def api_get_api_configs(request):
    """获取所有API配置"""
    config = load_api_configs()
    return web.json_response(config)


async def api_add_api_config(request):
    """添加单个API配置"""
    data = await request.json()
    api_id = data.get("api_id", "").strip()
    api_hash = data.get("api_hash", "").strip()
    note = data.get("note", "").strip()
    
    if not api_id or not api_hash:
        return web.json_response({"error": "api_id和api_hash不能为空"}, status=400)
    
    config = load_api_configs()
    # 检查重复
    for c in config["configs"]:
        if c["api_id"] == api_id:
            return web.json_response({"error": f"api_id {api_id} 已存在"}, status=400)
    
    config["configs"].append({
        "id": f"api_{int(time.time())}_{random.randint(100,999)}",
        "api_id": api_id,
        "api_hash": api_hash,
        "note": note or f"API-{len(config['configs'])+1}",
        "assigned_workers": 0,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
    })
    save_api_configs(config)
    return web.json_response({"success": True, "total": len(config["configs"])})


async def api_batch_add_api_configs(request):
    """批量添加API配置（一键填充）"""
    data = await request.json()
    items = data.get("items", [])  # [{api_id, api_hash, note}]
    
    if not items:
        # 尝试解析文本格式: 每行 api_id|api_hash 或 api_id,api_hash
        text = data.get("text", "")
        if text:
            for line in text.strip().split("\n"):
                line = line.strip()
                if not line:
                    continue
                # 支持 api_id|api_hash 、逗号、以及 api_id-api_hash（横线，hash 为十六进制）
                if "|" in line or "," in line or "\t" in line:
                    parts = line.replace("|", ",").replace("\t", ",").split(",")
                elif "-" in line:
                    a, b = line.split("-", 1)
                    parts = [a, b]
                else:
                    parts = [line]
                if len(parts) >= 2 and parts[0].strip().isdigit() and len(parts[1].strip()) >= 16:
                    items.append({
                        "api_id": parts[0].strip(),
                        "api_hash": parts[1].strip(),
                        "note": parts[2].strip() if len(parts) > 2 else ""
                    })
    
    if not items:
        return web.json_response({"error": "没有有效的API配置"}, status=400)
    
    config = load_api_configs()
    existing_ids = {c["api_id"] for c in config["configs"]}
    added = 0
    
    for item in items:
        api_id = str(item.get("api_id", "")).strip()
        api_hash = str(item.get("api_hash", "")).strip()
        if api_id and api_hash and api_id not in existing_ids:
            config["configs"].append({
                "id": f"api_{int(time.time())}_{random.randint(100,999)}",
                "api_id": api_id,
                "api_hash": api_hash,
                "note": item.get("note", "") or f"API-{len(config['configs'])+1}",
                "assigned_workers": 0,
                "created_at": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            existing_ids.add(api_id)
            added += 1
    
    save_api_configs(config)
    return web.json_response({"success": True, "added": added, "total": len(config["configs"])})


async def api_delete_api_config(request):
    """删除API配置"""
    config_id = request.match_info["config_id"]
    config = load_api_configs()
    config["configs"] = [c for c in config["configs"] if c["id"] != config_id]
    save_api_configs(config)
    return web.json_response({"success": True})


async def api_auto_assign_apis(request):
    """一键自动分配API到水军号"""
    config = load_api_configs()
    api_configs = config.get("configs", [])
    
    if not api_configs:
        return web.json_response({"error": "没有可用的API配置"}, status=400)
    
    workers_file = DATA_DIR / "workers_config.json"
    if not workers_file.exists():
        return web.json_response({"error": "没有水军号配置"}, status=400)
    
    workers_data = json.loads(workers_file.read_text(encoding='utf-8'))
    workers = workers_data.get("workers", [])
    
    # 均匀分配
    for i, worker in enumerate(workers):
        api_cfg = api_configs[i % len(api_configs)]
        worker["api_id"] = api_cfg["api_id"]
        worker["api_hash"] = api_cfg["api_hash"]
        worker["api_config_id"] = api_cfg["id"]
    
    workers_data["workers"] = workers
    save_json(workers_file, workers_data)
    # 按每组10个均匀分配水军到IP线路（固定每组最多10个，不串换）
    try:
        from app import proxy_pool, IP_LINE_MAX_WORKERS
        proxies = proxy_pool.get_all()
        workers = workers_data.get("workers", [])
        unbound = [w for w in workers if not w.get("proxy_id") and not w.get("bound_proxy_id")]
        if unbound and proxies:
            active_proxies = [px for px in proxies if px.get("status", "active") != "disabled"][:30]
            for i, w in enumerate(unbound):
                if not active_proxies:
                    break
                px = active_proxies[i % len(active_proxies)]
                assigned = list(px.get("assigned_bots") or [])
                if len(assigned) >= IP_LINE_MAX_WORKERS:
                    continue
                assigned.append(w["id"])
                px["assigned_bots"] = assigned
                w["proxy_id"] = px.get("id")
                w["bound_proxy_id"] = px.get("id")
                w["group_no"] = (i % len(active_proxies)) + 1
            proxy_pool.save()
            save_json(workers_file, workers_data)
    except Exception as e:
        print(f"[导入] 分组分配失败: {e}")

    
    # 更新统计
    for cfg in api_configs:
        cfg["assigned_workers"] = sum(1 for w in workers if w.get("api_config_id") == cfg["id"])
    save_api_configs(config)
    
    return web.json_response({
        "success": True,
        "total_workers": len(workers),
        "total_apis": len(api_configs),
        "per_api": len(workers) // len(api_configs)
    })


# --- 头像库管理 ---
async def api_get_avatars(request):
    """获取头像库列表"""
    if not AVATARS_DIR.exists():
        return web.json_response({"avatars": [], "total": 0})
    
    avatars = []
    for f in sorted(AVATARS_DIR.iterdir()):
        if f.suffix.lower() in ('.jpg', '.jpeg', '.png', '.webp'):
            avatars.append({
                "filename": f.name,
                "size": f.stat().st_size,
                "url": f"/static/avatars/{f.name}"
            })
    
    return web.json_response({"avatars": avatars, "total": len(avatars)})


async def api_upload_avatars(request):
    """上传头像（支持ZIP包或单个图片）"""
    reader = await request.multipart()
    uploaded = 0
    
    while True:
        field = await reader.next()
        if field is None:
            break
        
        filename = field.filename
        if not filename:
            continue
        
        file_data = await field.read()
        
        if filename.lower().endswith('.zip'):
            # 解压ZIP包
            zip_path = IMPORT_TEMP_DIR / filename
            zip_path.write_bytes(file_data)
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')) and not name.startswith('__MACOSX'):
                            # 提取文件名（去掉目录路径）
                            base_name = os.path.basename(name)
                            if base_name:
                                target = AVATARS_DIR / base_name
                                with zf.open(name) as src:
                                    target.write_bytes(src.read())
                                uploaded += 1
            finally:
                zip_path.unlink(missing_ok=True)
        elif filename.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
            target = AVATARS_DIR / filename
            target.write_bytes(file_data)
            uploaded += 1
    
    return web.json_response({"success": True, "uploaded": uploaded})


async def api_delete_avatar(request):
    """删除头像"""
    filename = request.match_info["filename"]
    avatar_path = AVATARS_DIR / filename
    if avatar_path.exists():
        avatar_path.unlink()
    return web.json_response({"success": True})


# --- 批量导入水军号 ---
async def api_batch_import_workers(request):
    """批量导入水军号（ZIP包含session+json文件）"""
    reader = await request.multipart()
    
    field = await reader.next()
    if not field or not field.filename:
        return web.json_response({"error": "请上传ZIP文件"}, status=400)
    
    file_data = await field.read()
    
    # 保存并解压ZIP
    zip_path = IMPORT_TEMP_DIR / "import.zip"
    zip_path.write_bytes(file_data)
    
    extract_dir = IMPORT_TEMP_DIR / "extracted"
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir()
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_dir)
    except Exception as e:
        return web.json_response({"error": f"ZIP解压失败: {e}"}, status=400)
    finally:
        zip_path.unlink(missing_ok=True)
    
    # 查找所有session和json文件
    session_files = {}
    json_files = {}
    
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            full_path = os.path.join(root, f)
            name_without_ext = os.path.splitext(f)[0]
            if f.endswith('.session'):
                session_files[name_without_ext] = full_path
            elif f.endswith('.json'):
                json_files[name_without_ext] = full_path
    
    # 配对
    pairs = []
    for name, session_path in session_files.items():
        json_path = json_files.get(name)
        if json_path:
            try:
                json_data = json.loads(Path(json_path).read_text(encoding='utf-8'))
                pairs.append({
                    "name": name,
                    "session_path": session_path,
                    "json_data": json_data,
                    "phone": json_data.get("phone", name),
                    "api_id": str(json_data.get("app_id", "")),
                    "api_hash": json_data.get("app_hash", ""),
                    "twoFA": json_data.get("twoFA", ""),
                })
            except Exception as e:
                logger.warning(f"解析JSON失败 {name}: {e}")
    
    # 复制session文件到sessions目录
    imported = []
    workers_file = DATA_DIR / "workers_config.json"
    workers_data = json.loads(workers_file.read_text(encoding='utf-8')) if workers_file.exists() else {"workers": []}
    existing_phones = {w.get("phone", "").replace("+", "").replace(" ", "") for w in workers_data.get("workers", [])}
    
    config = load_api_configs()
    api_configs = config.get("configs", [])
    
    for pair in pairs:
        phone_clean = pair["phone"].replace("+", "").replace(" ", "")
        if phone_clean in existing_phones:
            continue  # 跳过已存在的
        
        # 复制session文件
        session_dest = SESSIONS_DIR / f"{phone_clean}.session"
        shutil.copy2(pair["session_path"], session_dest)
        
        # 分配API
        if api_configs:
            # 均匀分配到API组
            api_idx = len(workers_data["workers"]) % len(api_configs)
            api_cfg = api_configs[api_idx]
            use_api_id = api_cfg["api_id"]
            use_api_hash = api_cfg["api_hash"]
        else:
            # 使用session自带的api_id
            use_api_id = pair["api_id"]
            use_api_hash = pair["api_hash"]
        
        # 生成名字和用户名：优先用手动保存的资料模板
        tpl_file = DATA_DIR / "profile_template.json"
        tpl = {}
        if tpl_file.exists():
            try:
                tpl = json.loads(tpl_file.read_text(encoding="utf-8"))
            except Exception:
                tpl = {}
        fn = (tpl.get("first_name") or "").strip() or get_available_name()
        ln = (tpl.get("last_name") or "").strip()
        display_name = fn + ((" " + ln) if ln else "")
        username_num = get_next_username_number()
        prefix = (tpl.get("username_prefix") or "").strip() or "kuaiyue"
        target_username = f"{prefix}{username_num}"
        use_bio = (tpl.get("bio") or "").strip() or DEFAULT_BIO
        
        # 分配头像
        avatar_file = get_unused_avatar()
        
        # 创建worker配置
        worker_id = f"w_{int(time.time())}_{random.randint(100,999)}"
        worker_config = {
            "id": worker_id,
            "phone": f"+{phone_clean}",
            "session_name": phone_clean,
            "api_id": use_api_id,
            "api_hash": use_api_hash,
            "twoFA": pair.get("twoFA", ""),
            "proxy": None,
            "status": "active",  # 直接可用
            "bot_username": "",
            "bot_token": "",
            "display_name": display_name,
            "first_name": fn,
            "last_name": ln,
            "target_username": target_username,
            "target_bio": use_bio,
            "bio": use_bio,
            "avatar_file": avatar_file or "",
            "profile_set": False,  # 资料是否已设置
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "imported_from": pair["name"],
        }
        
        workers_data["workers"].append(worker_config)
        existing_phones.add(phone_clean)
        imported.append({
            "phone": f"+{phone_clean}",
            "display_name": display_name,
            "target_username": target_username,
        })
    
    # 保存
    save_json(workers_file, workers_data)

    # 新导入的号均匀分配到各代理（round-robin）
    try:
        import json as _json
        proxy_file = DATA_DIR / "proxy_pool.json"
        if proxy_file.exists() and imported:
            pd = _json.loads(proxy_file.read_text(encoding="utf-8"))
            proxies = [p for p in pd.get("proxies", []) if p.get("status", "active") != "disabled"]
            new_phones = {x["phone"] for x in imported}
            unbound = [w for w in workers_data.get("workers", []) if w.get("phone") in new_phones and not w.get("proxy_id")]
            if proxies and unbound:
                # 补到当前人最少的线路，保持均匀
                def line_count(px):
                    return len(px.get("assigned_worker_phones") or [])
                for w in unbound:
                    px = min(proxies, key=line_count)
                    gi = proxies.index(px)
                    bots = list(px.get("assigned_bots") or [])
                    phones = list(px.get("assigned_worker_phones") or [])
                    if w["id"] not in bots:
                        bots.append(w["id"])
                    if w.get("phone") and w["phone"] not in phones:
                        phones.append(w["phone"])
                    px["assigned_bots"] = bots
                    px["assigned_worker_phones"] = phones
                    w["proxy_id"] = px.get("id")
                    w["bound_proxy_id"] = px.get("id")
                    w["group_no"] = gi + 1
                proxy_file.write_text(_json.dumps(pd, ensure_ascii=False, indent=2), encoding="utf-8")
                save_json(workers_file, workers_data)
    except Exception as e:
        logger.warning(f"[导入] 自动分配代理失败: {e}")

    # 清理临时文件
    shutil.rmtree(extract_dir, ignore_errors=True)
    
    return web.json_response({
        "success": True,
        "imported": len(imported),
        "skipped": len(pairs) - len(imported),
        "total_pairs": len(pairs),
        "details": imported[:20],  # 只返回前20个详情
    })


# --- 一键设置资料 ---
async def api_setup_profiles(request):
    """手动输入资料后，一键设置所有未设置资料的水军号"""
    from telethon import TelegramClient
    from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
    from telethon.tl.functions.photos import UploadProfilePhotoRequest

    try:
        body = await request.json()
    except Exception:
        body = {}

    # 用户手动输入的资料
    first_name = (body.get("first_name") or "").strip()
    last_name = (body.get("last_name") or "").strip()
    username_prefix = (body.get("username_prefix") or "kuaiyue").strip()
    bio = (body.get("bio") or "").strip() or DEFAULT_BIO
    use_random_avatar = body.get("use_random_avatar", True)

    if not first_name:
        return web.json_response({"error": "名字不能为空", "success": False}, status=400)

    workers_file = DATA_DIR / "workers_config.json"
    if not workers_file.exists():
        return web.json_response({"error": "没有水军号配置", "success": False}, status=400)

    workers_data = json.loads(workers_file.read_text(encoding='utf-8'))
    workers = workers_data.get("workers", [])

    # 只处理未设置资料的
    to_setup = [w for w in workers if not w.get("profile_set", False) and w.get("status") in ("imported", "active")]

    if not to_setup:
        return web.json_response({
            "message": "所有水军号资料已设置",
            "total": 0,
            "success": True,
            "success_count": 0,
            "failed_count": 0
        })

    results = []

    for worker in to_setup:
        phone_clean = worker["phone"].replace("+", "").replace(" ", "")
        session_path = str(SESSIONS_DIR / phone_clean)
        try:
            api_id = int(worker.get("api_id", "2040") or 2040)
        except Exception:
            api_id = 2040
        api_hash = worker.get("api_hash", "")

        # 代理
        proxy = None
        try:
            from app import proxy_pool
            proxy_data = proxy_pool.get_proxy_for_worker(worker["id"])
            if proxy_data:
                import python_socks
                proxy = {
                    'proxy_type': python_socks.ProxyType.HTTP,
                    'addr': proxy_data["host"],
                    'port': int(proxy_data["port"]),
                }
                if proxy_data.get("username"):
                    proxy['username'] = proxy_data["username"]
                    proxy['password'] = proxy_data.get("password", "")
        except Exception:
            pass

        try:
            client = TelegramClient(session_path, api_id, api_hash, proxy=proxy)
            await client.connect()

            if not await client.is_user_authorized():
                results.append({"phone": worker["phone"], "status": "未授权", "success": False})
                await client.disconnect()
                continue

            # ========== 使用用户手动输入的资料 ==========
            await client(UpdateProfileRequest(
                first_name=first_name,
                last_name=last_name,
                about=bio
            ))

            # 用户名：前缀 + 自动数字，冲突时自动换号
            success_username = None
            for retry in range(8):
                num = get_next_username_number()
                try_username = f"{username_prefix}{num}"
                try:
                    await client(UpdateUsernameRequest(username=try_username))
                    success_username = try_username
                    break
                except Exception as e:
                    logger.warning(f"用户名 {try_username} 不可用: {e}")
                    continue

            if not success_username:
                results.append({"phone": worker["phone"], "status": "用户名全部冲突", "success": False})
                await client.disconnect()
                continue

            # 头像
            if use_random_avatar:
                avatar_file = get_unused_avatar()
                if avatar_file:
                    avatar_path = AVATARS_DIR / avatar_file
                    if avatar_path.exists():
                        try:
                            photo = await client.upload_file(str(avatar_path))
                            await client(UploadProfilePhotoRequest(file=photo))
                            worker["avatar_file"] = avatar_file
                        except Exception as e:
                            logger.warning(f"设置头像失败 {worker['phone']}: {e}")

            # 更新本地记录
            worker["display_name"] = f"{first_name}-{last_name}" if last_name else first_name
            worker["target_username"] = success_username
            worker["target_bio"] = bio
            worker["profile_set"] = True
            worker["status"] = "active"

            results.append({
                "phone": worker["phone"],
                "status": "成功",
                "success": True,
                "username": success_username
            })

            await client.disconnect()
            await asyncio.sleep(3)  # 防限流

        except Exception as e:
            results.append({"phone": worker["phone"], "status": f"失败: {str(e)[:60]}", "success": False})

    # 保存
    workers_data["workers"] = workers
    save_json(workers_file, workers_data)

    success_count = sum(1 for r in results if r["success"])
    return web.json_response({
        "success": True,
        "total": len(results),
        "success_count": success_count,
        "failed_count": len(results) - success_count,
        "results": results
    })


# --- 获取导入状态 ---
async def api_get_import_status(request):
    """获取批量导入的水军号状态"""
    workers_file = DATA_DIR / "workers_config.json"
    if not workers_file.exists():
        return web.json_response({"workers": [], "stats": {}})
    
    workers_data = json.loads(workers_file.read_text(encoding='utf-8'))
    workers = workers_data.get("workers", [])
    
    imported = [w for w in workers if w.get("imported_from")]
    
    stats = {
        "total_imported": len(imported),
        "profile_set": sum(1 for w in imported if w.get("profile_set")),
        "pending_profile": sum(1 for w in imported if not w.get("profile_set")),
        "active": sum(1 for w in imported if w.get("status") == "active"),
    }
    
    return web.json_response({
        "workers": [{
            "phone": w.get("phone"),
            "display_name": w.get("display_name"),
            "target_username": w.get("target_username"),
            "avatar_file": w.get("avatar_file"),
            "profile_set": w.get("profile_set", False),
            "status": w.get("status"),
            "api_id": w.get("api_id"),
        } for w in imported],
        "stats": stats
    })


def register_batch_import_routes(routes):
    """注册所有批量导入相关的路由"""
    # API配置管理
    routes.get("/api/api-configs")(api_get_api_configs)
    routes.post("/api/api-configs")(api_add_api_config)
    routes.post("/api/api-configs/batch")(api_batch_add_api_configs)
    routes.delete("/api/api-configs/{config_id}")(api_delete_api_config)
    routes.post("/api/api-configs/auto-assign")(api_auto_assign_apis)
    
    # 头像库
    routes.get("/api/avatars")(api_get_avatars)
    routes.post("/api/avatars/upload")(api_upload_avatars)
    routes.delete("/api/avatars/{filename}")(api_delete_avatar)
    
    # 批量导入
    routes.post("/api/workers/batch-import")(api_batch_import_workers)
    routes.get("/api/workers/import-status")(api_get_import_status)
    
    # 一键设置资料
    routes.post("/api/workers/setup-profiles")(api_setup_profiles)
