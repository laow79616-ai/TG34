"""
Bot 端程序
功能：
1. 处理 /start 命令
2. 处理 "👁 预览消息" - 返回广告列表
3. 处理 preview_X 回调 - 返回带分享按钮的广告
4. 处理 inline query - 返回广告供分享
"""
import os
import json
import asyncio
import logging
from pathlib import Path
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaPhoto, InlineQueryResultCachedPhoto,
    InlineQueryResultArticle, InputTextMessageContent,
    InlineQueryResultPhoto
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, InlineQueryHandler,
    filters, ContextTypes
)

logger = logging.getLogger("Bot")

# 数据目录（与 config.py 保持一致）
try:
    from config import DATA_DIR, AD_CONFIG_FILE, BOT_CONFIG_FILE
except ImportError:
    DATA_DIR = Path(os.environ.get("TG_SHARE_DATA_DIR", Path(__file__).resolve().parent / "data"))
    AD_CONFIG_FILE = DATA_DIR / "ad_config.json"
    BOT_CONFIG_FILE = DATA_DIR / "bot_config.json"


def load_ads():
    """加载广告列表"""
    if AD_CONFIG_FILE.exists():
        try:
            data = json.loads(AD_CONFIG_FILE.read_text())
            return data.get("ads", [])
        except:
            pass
    return []


def load_bot_config():
    """加载Bot配置"""
    if BOT_CONFIG_FILE.exists():
        try:
            return json.loads(BOT_CONFIG_FILE.read_text())
        except:
            pass
    return {}


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理 /start 命令"""
    keyboard = [
        [InlineKeyboardButton("👁 预览消息", callback_data="preview_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "欢迎使用快约到家推广系统！\n\n请点击下方按钮预览广告消息：",
        reply_markup=reply_markup
    )


async def preview_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理文字消息 '👁 预览消息'"""
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


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理回调按钮"""
    query = update.callback_query
    await query.answer()

    data = query.data

    if data == "preview_menu":
        # 显示广告列表
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
        # 显示具体广告 + 分享按钮
        idx = int(data.split("_")[1])
        ads = load_ads()
        if idx >= len(ads):
            await query.edit_message_text("广告不存在")
            return

        ad = ads[idx]
        caption = ad.get("message", "")
        image_url = ad.get("image_url", "")
        image_file_id = ad.get("image_file_id", "")

        # 构建按钮：URL按钮 + 分享按钮
        keyboard = []

        # URL 按钮行
        url_buttons = ad.get("url_buttons", [])
        for btn in url_buttons:
            keyboard.append([InlineKeyboardButton(btn["text"], url=btn["url"])])

        # 分享按钮 - 使用 switch_inline_query_chosen_chat
        keyboard.append([InlineKeyboardButton(
            "📤 分享给用户",
            switch_inline_query=str(idx)
        )])

        reply_markup = InlineKeyboardMarkup(keyboard)

        # 发送广告（带图片或纯文字）
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


async def inline_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    处理 inline query - 当水军点击"分享给用户"按钮后
    在目标用户的聊天框中输入 @botUsername 时触发
    返回广告内容供选择发送
    """
    query = update.inline_query
    query_text = query.query.strip()

    ads = load_ads()
    results = []

    # 如果 query_text 是数字，只返回对应广告
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
        image_url = ad.get("image_url", "")
        image_file_id = ad.get("image_file_id", "")

        # 构建 URL 按钮
        keyboard = []
        url_buttons = ad.get("url_buttons", [])
        for btn in url_buttons:
            keyboard.append([InlineKeyboardButton(btn["text"], url=btn["url"])])

        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None

        if image_file_id or image_url:
            # 带图片的广告
            photo_source = image_file_id if image_file_id else image_url
            result = InlineQueryResultPhoto(
                id=str(i),
                photo_url=photo_source if not image_file_id else f"https://placeholder.com/{i}",
                thumbnail_url=photo_source if not image_file_id else f"https://placeholder.com/{i}",
                title=ad.get("name", f"广告{i+1}"),
                caption=caption,
                parse_mode="HTML",
                reply_markup=reply_markup
            )
            # 如果有 file_id，用 cached photo
            if image_file_id:
                result = InlineQueryResultCachedPhoto(
                    id=str(i),
                    photo_file_id=image_file_id,
                    title=ad.get("name", f"广告{i+1}"),
                    caption=caption,
                    parse_mode="HTML",
                    reply_markup=reply_markup
                )
            results.append(result)
        else:
            # 纯文字广告
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


def run_bot(token):
    """启动Bot"""
    app = Application.builder().token(token).build()

    # 注册处理器
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r".*预览消息.*"),
        preview_menu_handler
    ))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(InlineQueryHandler(inline_query_handler))

    logger.info(f"Bot 已启动")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    token = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("BOT_TOKEN", "")
    if not token:
        print("请提供 Bot Token")
        sys.exit(1)

    run_bot(token)
