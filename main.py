"""
IELTS Speaking Mock Test — Telegram Bot
========================================
Stack:
  - aiogram 3.x        (Telegram bot framework, async)
  - groq               (Llama 3.3 70B — savol generatsiya + baholash,
                         Whisper-large-v3-turbo — Speech-to-Text)
  - gTTS               (Text-to-Speech — Google Text-to-Speech, tekin)
  - pydub + ffmpeg     (audio konvertatsiya: mp3 -> ogg/opus voice)

Render.com Free Tier'da ishlashga moslashtirilgan (WEBHOOK rejimida):
  - Bot Telegram'dan WEBHOOK orqali yangilanishlarni oladi (polling emas) —
    bu bir nechta instance bir-biriga to'qnashib "TelegramConflictError"
    berish xavfini deyarli yo'qotadi.
  - Render "Web Service" allaqachon ochiq HTTPS URL (RENDER_EXTERNAL_URL)
    beradi, shu URL avtomatik webhook manzili sifatida ishlatiladi.
  - Har bir ishga tushishda tasodifiy secret_token bilan set_webhook chaqiriladi.

MUHIM (Render sozlamalari uchun):
  - Environment Variables:
        BOT_TOKEN   = <BotFather bergan token>
        GROQ_API_KEY= <Groq Cloud API key>
        PORT        = 10000   (Render avtomatik beradi, kod o'zi ham oladi)
        (WEBHOOK_BASE_URL kerak emas — Render'da RENDER_EXTERNAL_URL avtomatik keladi;
         boshqa platformada ishlatsangiz shuni qo'lda kiritish kerak bo'ladi)
  - Build Command misolida ffmpeg ham o'rnatilishi kerak, chunki pydub
    ffmpeg binarini talab qiladi. Masalan Render "Native Environment"da:
        apt-get update && apt-get install -y ffmpeg
    yoki Dockerfile ishlatilsa:
        RUN apt-get update && apt-get install -y ffmpeg
  - WEB_CONCURRENCY=1 (Render env var) — bitta worker process bo'lishini
    ta'minlaydi.
"""

import os
import re
import json
import asyncio
import logging
import tempfile
import shutil
import secrets
import base64
import html
import contextvars
import hmac
import hashlib
import random
import urllib.parse
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    FSInputFile,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    WebAppInfo,
    Update,
    LabeledPrice,
    PreCheckoutQuery,
    BufferedInputFile,
    InputMediaPhoto,
    MenuButtonWebApp,
)
from aiogram.client.session.middlewares.base import BaseRequestMiddleware
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from gtts import gTTS
from pydub import AudioSegment
from groq import Groq
from upstash_redis.asyncio import Redis as UpstashRedis
from PIL import Image, ImageDraw, ImageFont

from aiohttp import web

# --------------------------------------------------------------------------
# 1) SOZLAMALAR (ENV VARIABLES)
# --------------------------------------------------------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable topilmadi!")
if not GROQ_API_KEY:
    raise RuntimeError("GROQ_API_KEY environment variable topilmadi!")

LLM_MODEL = "openai/gpt-oss-120b"
STT_MODEL = "whisper-large-v3-turbo"

PART1_QUESTIONS_COUNT = 4
PART3_QUESTIONS_COUNT = 4

TEMP_DIR = os.path.join(tempfile.gettempdir(), "ielts_bot_files")
os.makedirs(TEMP_DIR, exist_ok=True)

# --- Webhook sozlamalari -------------------------------------------------
# Render "Web Service"lar uchun RENDER_EXTERNAL_URL o'zgaruvchisini avtomatik
# beradi (masalan https://my-firs-best-bot-dude-bro.onrender.com). Boshqa
# platformada ishlatsangiz, buni qo'lda WEBHOOK_BASE_URL orqali bering.
WEBHOOK_PATH = "/webhook"
WEBHOOK_BASE_URL = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("WEBHOOK_BASE_URL")
if not WEBHOOK_BASE_URL:
    raise RuntimeError(
        "WEBHOOK_BASE_URL (yoki Render'ning RENDER_EXTERNAL_URL) topilmadi! "
        "Render'da bu avtomatik beriladi; boshqa platformada WEBHOOK_BASE_URL "
        "environment variable orqali to'liq https:// manzilni kiriting."
    )
WEBHOOK_URL = WEBHOOK_BASE_URL.rstrip("/") + WEBHOOK_PATH
# Har bir ishga tushishda yangi tasodifiy secret token generatsiya qilinadi va
# set_webhook orqali darhol qayta ro'yxatdan o'tkaziladi, shuning uchun uni
# alohida saqlash yoki env orqali berish shart emas.
WEBHOOK_SECRET = secrets.token_urlsafe(32)

# --- Foydalanuvchilar statistikasi (Upstash Redis) ----------------------
# Render'ning fayl tizimi vaqtinchalik bo'lgani uchun (har qayta ishga
# tushishda yo'qoladi), foydalanuvchilar sonini Upstash Redis'da (bepul,
# doimiy saqlanadigan HTTP-based Redis) saqlaymiz.
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
BOT_USERS_SET_KEY = "ielts_bot:unique_users"
BOT_STARTS_COUNTER_KEY = "ielts_bot:total_starts"
WRITING_PRICE_STARS = 5   # IELTS Writing tahlili narxi (⭐ Telegram Stars) — faqat shu raqamni o'zgartirish kifoya
SPEAKING_PRICE_STARS = 3   # IELTS Speaking to'liq mock narxi (⭐ Telegram Stars)
PRO_PRICE_STARS = 10   # PRO obuna narxi (⭐ Telegram Stars)
GATE_CHAT_ENGLISH = True   # True: ingliz tiliga oid chat buyruqlari (grammar, test, reading/listening) faqat PRO uchun
PRO_DAYS = 30
BOT_USER_INFO_KEY = "ielts_bot:user_info"      # hash: user_id -> JSON {n, u, d}
BOT_ACTIVE_DAY_PREFIX = "ielts_bot:active:"   # + YYYY-MM-DD (Toshkent vaqti)
_active_seen: set = set()  # (kun, user_id) — Redis'ga kuniga bir marta yozish uchun
LEADERBOARD_QUIZ_KEY = "ielts_bot:leaderboard:quiz"
LEADERBOARD_CERT_KEY = "ielts_bot:leaderboard:sertifikat"
LEADERBOARD_READING_KEY = "ielts_bot:leaderboard:reading"
LEADERBOARD_LISTENING_KEY = "ielts_bot:leaderboard:listening"
LEADERBOARD_TOP_N = 10
# Ixtiyoriy: /stats buyrug'ini faqat shu Telegram user_id egasiga cheklash uchun.
# Agar bo'sh qoldirilsa, /stats hamma uchun ochiq bo'ladi.
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

# --- Majburiy obuna (kanal) ----------------------------------------------
# REQUIRED_CHANNEL — kanal username'i (masalan "@mening_kanalim") yoki
# raqamli ID (masalan "-1001234567890"). Bo'sh qoldirilsa, tekshiruv o'chirilgan.
# MUHIM: bot shu kanalda ADMIN bo'lishi shart, aks holda a'zolikni tekshira olmaydi.
REQUIRED_CHANNEL = (os.getenv("REQUIRED_CHANNEL") or "").strip()
if REQUIRED_CHANNEL and not REQUIRED_CHANNEL.startswith(("@", "-")):
    REQUIRED_CHANNEL = "@" + REQUIRED_CHANNEL
# Kanalga o'tish tugmasi uchun havola. Username bo'lsa avtomatik yasaladi;
# yopiq kanal bo'lsa, taklif havolasini REQUIRED_CHANNEL_LINK orqali bering.
REQUIRED_CHANNEL_LINK = (os.getenv("REQUIRED_CHANNEL_LINK") or "").strip()
if not REQUIRED_CHANNEL_LINK and REQUIRED_CHANNEL.startswith("@"):
    REQUIRED_CHANNEL_LINK = f"https://t.me/{REQUIRED_CHANNEL[1:]}"

# --------------------------------------------------------------------------
# 2) LOGGING
# --------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("ielts_bot")

# --------------------------------------------------------------------------
# 3) BOT / DISPATCHER / GROQ CLIENT / REDIS CLIENT
# --------------------------------------------------------------------------

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
groq_client = Groq(api_key=GROQ_API_KEY)

if UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
    redis_client = UpstashRedis(url=UPSTASH_REDIS_REST_URL, token=UPSTASH_REDIS_REST_TOKEN)
    logger.info("Upstash Redis ulandi — foydalanuvchilar statistikasi saqlanadi.")
else:
    redis_client = None
    logger.warning(
        "UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN topilmadi — "
        "foydalanuvchilar statistikasi (/stats) o'chirilgan holda ishlaydi."
    )


async def track_user(user_id: int, name: str = "", username: str = ""):
    """Foydalanuvchini Upstash Redis'dagi noyob foydalanuvchilar to'plamiga
    qo'shadi va umumiy murojaatlar sonini oshiradi. Xatolik bo'lsa botni
    to'xtatmaydi, faqat log yozadi."""
    if redis_client is None:
        return
    try:
        await redis_client.sadd(BOT_USERS_SET_KEY, str(user_id))
        await redis_client.incr(BOT_STARTS_COUNTER_KEY)

        # Kunlik faol foydalanuvchilar: har kim uchun kuniga faqat bir marta yoziladi
        today = datetime.now(timezone(timedelta(hours=5))).strftime("%Y-%m-%d")
        if (today, user_id) not in _active_seen:
            if len(_active_seen) > 50000:
                _active_seen.clear()
            _active_seen.add((today, user_id))
            day_key = f"{BOT_ACTIVE_DAY_PREFIX}{today}"
            await redis_client.sadd(day_key, str(user_id))
            # Egasi uchun /users ro'yxati: ism, username va oxirgi faol kun (kuniga bir marta)
            await redis_client.hset(BOT_USER_INFO_KEY, str(user_id), json.dumps(
                {"n": (name or "")[:60], "u": (username or "")[:40], "d": today}, ensure_ascii=False))
            await redis_client.expire(day_key, 8 * 24 * 3600)
    except Exception as e:
        logger.warning("Redis orqali statistika yozishda xatolik: %s", e)


# ---- Faoliyat jurnali (faqat bot egasi /activity orqali ko'radi) ---------------
ACTIVITY_KEY = "ielts_bot:activity"
ACTIVITY_MAX = 300
_activity_mem: list = []


async def log_activity(uid, name: str, username: str, what: str):
    """Foydalanuvchi nima qilganini qisqa yozuv sifatida jurnalga qo'shadi (oxirgi ACTIVITY_MAX ta)."""
    try:
        rec = json.dumps({"t": int(datetime.now(timezone.utc).timestamp()), "id": int(uid),
                          "n": (name or "")[:40], "u": (username or "")[:32], "w": what[:80]}, ensure_ascii=False)
        _activity_mem.insert(0, rec)
        del _activity_mem[ACTIVITY_MAX:]
        if redis_client is not None:
            await redis_client.lpush(ACTIVITY_KEY, rec)
            await redis_client.ltrim(ACTIVITY_KEY, 0, ACTIVITY_MAX - 1)
    except Exception as e:
        logger.warning("Faoliyat jurnaliga yozishda xatolik: %s", e)


async def read_activity() -> list:
    raw = []
    try:
        if redis_client is not None:
            raw = await redis_client.lrange(ACTIVITY_KEY, 0, ACTIVITY_MAX - 1) or []
    except Exception as e:
        logger.warning("Faoliyat jurnalini o'qishda xatolik: %s", e)
    if not raw:
        raw = list(_activity_mem)
    out = []
    for r in raw:
        try:
            out.append(json.loads(r.decode() if isinstance(r, bytes) else r))
        except Exception:
            pass
    return out



async def update_leaderboard(leaderboard_key: str, user_id: int, percentage: int):
    """Foydalanuvchining shu leaderboard'dagi eng yaxshi natijasini saqlaydi
    (faqat oldingi natijasidan yuqori bo'lsagina yangilanadi)."""
    if redis_client is None:
        return
    try:
        current = await redis_client.zscore(leaderboard_key, str(user_id))
        current_val = float(current) if current is not None else -1.0
        if percentage > current_val:
            await redis_client.zadd(leaderboard_key, {str(user_id): float(percentage)})
    except Exception as e:
        logger.warning("Reyting jadvalini yangilashda xatolik: %s", e)


async def get_leaderboard_entries(leaderboard_key: str, top_n: int = LEADERBOARD_TOP_N):
    """Leaderboard'dagi barcha yozuvlarni o'qib, eng yuqori foizdan pastga
    saralab, TOP N tasini qaytaradi: [(user_id_str, percentage_float), ...]."""
    if redis_client is None:
        return []
    try:
        raw = await redis_client.zrange(leaderboard_key, 0, -1, withscores=True)
    except Exception as e:
        logger.warning("Reyting jadvalini o'qishda xatolik: %s", e)
        return []

    pairs = []
    if raw:
        # Upstash Redis python klienti (member, score) juftliklari yoki tekis
        # ro'yxat ([member, score, member, score, ...]) qaytarishi mumkin —
        # ikkalasini ham to'g'ri qayta ishlaymiz.
        if isinstance(raw[0], (list, tuple)):
            pairs = [(m, float(s)) for m, s in raw]
        else:
            it = iter(raw)
            pairs = [(m, float(s)) for m, s in zip(it, it)]

    pairs.sort(key=lambda item: item[1], reverse=True)
    return pairs[:top_n]


async def format_leaderboard_text(title: str, leaderboard_key: str) -> str:
    """Leaderboard uchun tayyor, foydalanuvchi ismlari bilan formatlangan
    matnni qaytaradi. Ismlarni Telegram'dan (bot.get_chat) real vaqtda oladi."""
    entries = await get_leaderboard_entries(leaderboard_key)
    if not entries:
        return f"{title}\n\nHozircha natijalar yo'q. Birinchi bo'lib test yeching!"

    medals = ["🥇", "🥈", "🥉"]
    lines = [title, ""]
    for i, (user_id_str, score) in enumerate(entries):
        try:
            chat = await bot.get_chat(int(user_id_str))
            name = chat.full_name or (f"@{chat.username}" if chat.username else f"Foydalanuvchi {user_id_str}")
        except Exception:
            name = f"Foydalanuvchi {user_id_str}"
        prefix = medals[i] if i < len(medals) else f"{i + 1}."
        lines.append(f"{prefix} {name} — <b>{int(score)}%</b>")

    return "\n".join(lines)


# --- Dashboard kontenti (Reading / Listening / Grammar) -------------------
# Barcha kontent Upstash Redis'da saqlanadi (Render fayl tizimi vaqtinchalik
# bo'lgani uchun). Har bir tur uchun alohida Redis Hash ishlatiladi:
#   ielts_bot:content:<type>            -> {id: json_string}
#   ielts_bot:content:<type>:counter    -> keyingi id uchun hisoblagich
# Listening uchun audio fayl Telegram'ning o'zida saqlanadi (file_id orqali) —
# alohida fayl xosting kerak emas, chunki Telegram file_id'lar doimiy amal qiladi.

CONTENT_TYPES = ("reading", "listening", "grammar")


def _content_hash_key(content_type: str) -> str:
    return f"ielts_bot:content:{content_type}"


def _content_counter_key(content_type: str) -> str:
    return f"ielts_bot:content:{content_type}:counter"


async def get_next_content_id(content_type: str) -> int:
    new_id = await redis_client.incr(_content_counter_key(content_type))
    return int(new_id)


async def save_content_item(content_type: str, item_id: int, item: dict):
    await redis_client.hset(_content_hash_key(content_type), str(item_id), json.dumps(item))


async def delete_content_item(content_type: str, item_id: int) -> bool:
    if redis_client is None:
        return False
    try:
        removed = await redis_client.hdel(_content_hash_key(content_type), str(item_id))
        return bool(removed)
    except Exception as e:
        logger.warning("Kontentni o'chirishda xatolik: %s", e)
        return False


async def list_content_items(content_type: str) -> list:
    """Berilgan turdagi barcha kontentni [{"id": int, ...item fields...}, ...]
    ko'rinishida qaytaradi, id bo'yicha o'sish tartibida saralangan."""
    if redis_client is None:
        return []
    try:
        raw = await redis_client.hgetall(_content_hash_key(content_type))
    except Exception as e:
        logger.warning("Kontent ro'yxatini o'qishda xatolik: %s", e)
        return []

    if not raw:
        return []

    items = []
    for field_id, value in raw.items():
        try:
            item = json.loads(value)
            item["id"] = int(field_id)
            items.append(item)
        except (ValueError, TypeError, json.JSONDecodeError):
            continue

    items.sort(key=lambda x: x["id"])
    return items


async def get_content_item(content_type: str, item_id: int) -> dict | None:
    if redis_client is None:
        return None
    try:
        raw = await redis_client.hget(_content_hash_key(content_type), str(item_id))
    except Exception as e:
        logger.warning("Kontentni o'qishda xatolik: %s", e)
        return None
    if raw is None:
        return None
    try:
        item = json.loads(raw)
        item["id"] = item_id
        return item
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def _parse_admin_questions_text(text: str) -> list | None:
    """Admin yuborgan savollar matnini tahlil qiladi. Har bir qator:
    'Savol | Variant A | Variant B | Variant C | Variant D | to'g'ri raqam(1-4)'
    formatida bo'lishi kerak. Agar admin savol qo'shmoqchi bo'lmasa,
    'yo'q' / 'yoq' / 'no' / '-' deb yozishi mumkin — bo'sh ro'yxat qaytadi.
    Format xato bo'lsa None qaytadi (chaqiruvchi xatolik haqida xabar beradi)."""
    stripped = text.strip()
    if stripped.lower() in ("yo'q", "yoq", "yo`q", "no", "-", "yo'q."):
        return []

    questions = []
    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) != 6:
            return None
        question_text, opt_a, opt_b, opt_c, opt_d, correct_str = parts
        try:
            correct_num = int(correct_str)
        except ValueError:
            return None
        if not (1 <= correct_num <= 4) or not question_text or not all([opt_a, opt_b, opt_c, opt_d]):
            return None
        questions.append({
            "question": question_text,
            "options": [opt_a, opt_b, opt_c, opt_d],
            "correct_index": correct_num - 1,
        })
    return questions


class UserTrackingMiddleware(BaseMiddleware):
    """Har bir kiruvchi xabar/tugma bosilishida foydalanuvchini fon rejimida
    (asosiy jarayonni kutdirmasdan) statistikaga qo'shib boradi."""

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if user is not None:
            asyncio.create_task(track_user(user.id, user.full_name or "", user.username or ""))
            what = None
            if isinstance(event, Message):
                t = (event.text or "").strip()
                if t.startswith("/"):
                    what = "💬 " + t.split()[0].split("@")[0]
                elif event.voice or event.audio:
                    what = "🎙 ovozli xabar"
                elif t:
                    what = "✉️ matn yubordi"
            elif isinstance(event, CallbackQuery) and event.data:
                what = "🔘 " + event.data[:40]
            if what:
                asyncio.create_task(log_activity(user.id, user.full_name or "", user.username or "", what))
        return await handler(event, data)


dp.message.outer_middleware(UserTrackingMiddleware())
dp.callback_query.outer_middleware(UserTrackingMiddleware())


# --- 🌐 Til tanlash (🇺🇿 / 🇷🇺 / 🇬🇧) ---------------------------------------
# Foydalanuvchi tilini tanlaydi (Redis'da saqlanadi). Til AI yaratadigan barcha
# kontentga (savollar, izohlar, hikoyalar, tahlillar) va asosiy menyuga ta'sir qiladi.

LANGS = {
    "en": ("🇬🇧", "English", "English"),
    "uz": ("🇺🇿", "O'zbekcha", "Uzbek"),
    "ru": ("🇷🇺", "Русский", "Russian"),
}
DEFAULT_LANG = "uz"
UNSET_LANG = "en"   # til hali tanlanmagan foydalanuvchilar uchun (birinchi /start gacha)
BOT_USER_LANG_KEY = "ielts_bot:user_lang"   # hash: user_id -> uz|ru|en
current_lang: contextvars.ContextVar = contextvars.ContextVar("current_lang", default=DEFAULT_LANG)
_user_lang: dict = {}   # user_id -> "uz"|"ru"|"en" ("" = hali tanlanmagan)


def cur_lang() -> str:
    return current_lang.get()


async def get_lang(user_id: int):
    """Foydalanuvchi tilini qaytaradi; hali tanlamagan bo'lsa None."""
    if user_id in _user_lang:
        return _user_lang[user_id] or None
    lang = ""
    if redis_client is not None:
        try:
            val = await redis_client.hget(BOT_USER_LANG_KEY, str(user_id))
            if val in LANGS:
                lang = val
        except Exception as e:
            logger.warning("Tilni o'qishda xatolik: %s", e)
    _user_lang[user_id] = lang
    return lang or None


async def set_lang(user_id: int, lang: str):
    _user_lang[user_id] = lang
    if redis_client is not None:
        try:
            await redis_client.hset(BOT_USER_LANG_KEY, str(user_id), lang)
        except Exception as e:
            logger.warning("Tilni saqlashda xatolik: %s", e)


# AI chaqiruvlariga til ko'rsatmasi qo'shiladi (barcha promptlarga alohida tegmasdan).
_orig_groq_create = groq_client.chat.completions.create


def _groq_create_with_lang(*args, **kwargs):
    lang = current_lang.get()
    msgs = kwargs.get("messages")
    if lang != "uz" and msgs is not None:
        name = LANGS[lang][2]
        note = (
            f"The user's interface language is {name}. Wherever the instructions below ask you to write "
            f"or answer in UZBEK (O'zbek tilida / o'zbek tilida / Uzbek), write that text in {name} instead. "
            "Text that must stay in English (English sentences, answer options, examples, quoted learner text, "
            "IELTS terms) stays in English. Keep the requested output format (e.g. JSON keys) unchanged."
        )
        kwargs["messages"] = [{"role": "system", "content": note}] + list(msgs)
    return _orig_groq_create(*args, **kwargs)


groq_client.chat.completions.create = _groq_create_with_lang


class LanguageMiddleware(BaseMiddleware):
    """Har bir yangilanishda foydalanuvchi tilini aniqlab, contextvar'ga qo'yadi."""

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        lang = None
        if user is not None and not user.is_bot:
            lang = await get_lang(user.id)
        current_lang.set(lang or UNSET_LANG)
        data["lang"] = lang
        return await handler(event, data)


dp.message.outer_middleware(LanguageMiddleware())
dp.callback_query.outer_middleware(LanguageMiddleware())


I18N = {
    "pick": {
        "_": "🌐 <b>Choose your language / Выберите язык / Tilni tanlang</b>",
    },
    "intro": {
        "uz": "👋 <b>Salom! Men universal yordamchi botman.</b>\n\n💬 Menga istalgan narsani yozing yoki ovozli xabar yuboring — savol bering, gaplashing. Yoki pastdan yo'nalishni tanlang 👇",
        "ru": "👋 <b>Привет! Я универсальный помощник.</b>\n\n💬 Пишите мне что угодно или отправляйте голосовые — задавайте вопросы, болтайте. Или выберите раздел ниже 👇",
        "en": "👋 <b>Hi! I'm a universal assistant bot.</b>\n\n💬 Write or send a voice message about anything — ask questions, just chat. Or pick a section below 👇",
    },
    "main_menu": {"uz": "📋 <b>Asosiy menyu</b> 👇", "ru": "📋 <b>Главное меню</b> 👇", "en": "📋 <b>Main menu</b> 👇"},
    "btn_sci": {"uz": "🔬 Ilm yo'nalishi", "ru": "🔬 Наука и игры", "en": "🔬 Science & games"},
    "btn_eng": {"uz": "🇬🇧 Ingliz tili", "ru": "🇬🇧 Английский язык", "en": "🇬🇧 English learning"},
    "btn_all": {"uz": "ℹ️ Barcha buyruqlar", "ru": "ℹ️ Все команды", "en": "ℹ️ All commands"},
    "btn_lang": {"uz": "🌐 Til", "ru": "🌐 Язык", "en": "🌐 Language"},
    "btn_back": {"uz": "⬅️ Orqaga", "ru": "⬅️ Назад", "en": "⬅️ Back"},
    "sci_title": {
        "uz": "🔬 <b>Ilm yo'nalishi</b>\n\nViktorinalar, jumboqlar va o'yinlar — tanlang 👇",
        "ru": "🔬 <b>Наука и игры</b>\n\nВикторины, головоломки и игры — выбирайте 👇",
        "en": "🔬 <b>Science & games</b>\n\nQuizzes, puzzles and games — pick one 👇",
    },
    "eng_title": {
        "uz": "🇬🇧 <b>Ingliz tili</b>\n\nSuhbat, IELTS testlari va materiallar — tanlang 👇",
        "ru": "🇬🇧 <b>Английский язык</b>\n\nРазговор, тесты IELTS и материалы — выбирайте 👇",
        "en": "🇬🇧 <b>English learning</b>\n\nConversation, IELTS tests and materials — pick one 👇",
    },
    "lang_set": {
        "uz": "✅ Til tanlandi: <b>O'zbekcha</b> 🇺🇿",
        "ru": "✅ Язык выбран: <b>Русский</b> 🇷🇺",
        "en": "✅ Language set: <b>English</b> 🇬🇧",
    },
    "sub_text": {
        "uz": "📢 <b>Botdan foydalanish uchun kanalimizga obuna bo'ling!</b>\n\n1️⃣ Pastdagi tugma orqali kanalga o'ting va obuna bo'ling.\n2️⃣ Keyin <b>✅ Tekshirish</b> tugmasini bosing.",
        "ru": "📢 <b>Чтобы пользоваться ботом, подпишитесь на наш канал!</b>\n\n1️⃣ Перейдите в канал по кнопке ниже и подпишитесь.\n2️⃣ Затем нажмите <b>✅ Проверить</b>.",
        "en": "📢 <b>Please subscribe to our channel to use the bot!</b>\n\n1️⃣ Open the channel with the button below and subscribe.\n2️⃣ Then tap <b>✅ Check</b>.",
    },
    "sub_btn": {"uz": "📢 Kanalga obuna bo'lish", "ru": "📢 Подписаться на канал", "en": "📢 Subscribe to the channel"},
    "sub_check": {"uz": "✅ Tekshirish", "ru": "✅ Проверить", "en": "✅ Check"},
    "sub_alert": {
        "uz": "📢 Avval kanalga obuna bo'ling!",
        "ru": "📢 Сначала подпишитесь на канал!",
        "en": "📢 Please subscribe to the channel first!",
    },
}

MENU_LABELS = {
    "quiz": {"uz": "Viktorina", "ru": "Викторина", "en": "Quiz"},
    "teaser": {"uz": "Miya jumboqlari", "ru": "Головоломки", "en": "Brain teasers"},
    "whatif": {"uz": "Nima bo'ladi?", "ru": "Что будет?", "en": "What happens?"},
    "lie": {"uz": "Yolg'onni top", "ru": "Найди ложь", "en": "Spot the lie"},
    "guess": {"uz": "Men topaman", "ru": "Я угадаю", "en": "I'll guess it"},
    "story": {"uz": "Interaktiv hikoya", "ru": "Интерактивная история", "en": "Interactive story"},
    "daily": {"uz": "Kunlik fan va fakt", "ru": "Факт и тест дня", "en": "Daily fact & quiz"},
    "certificate": {"uz": "Sertifikat testi", "ru": "Тест с сертификатом", "en": "Certificate test"},
    "leaderboard": {"uz": "Reyting", "ru": "Рейтинг", "en": "Leaderboard"},
    "english": {"uz": "Ingliz tilida suhbat", "ru": "Разговор на английском", "en": "English chat"},
    "grammar": {"uz": "Grammatika", "ru": "Грамматика", "en": "Grammar"},
    "writing": {"uz": "Writing tahlili", "ru": "Разбор Writing", "en": "Writing review"},
    "pro": {"uz": "PRO obuna", "ru": "PRO-подписка", "en": "PRO subscription"},
    "speaking": {"uz": "Speaking to'liq mock", "ru": "Полный Speaking мок", "en": "Full Speaking mock"},
    "test": {"uz": "IELTS Speaking", "ru": "IELTS Speaking", "en": "IELTS Speaking"},
    "reading_test": {"uz": "IELTS Reading", "ru": "IELTS Reading", "en": "IELTS Reading"},
    "listening_test": {"uz": "IELTS Listening", "ru": "IELTS Listening", "en": "IELTS Listening"},
    "dashboard": {"uz": "Dashboard va materiallar", "ru": "Дашборд и материалы", "en": "Dashboard & materials"},
}


def tr(key: str, lang: str = None, **kw) -> str:
    lang = lang or cur_lang()
    d = I18N.get(key, {})
    text = d.get(lang) or d.get(DEFAULT_LANG) or d.get("_", "")
    return text.format(**kw) if kw else text


def _lang_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"{e} {name}", callback_data=f"lg:{code}") for code, (e, name, _en) in LANGS.items()
    ]])


# --- Statik matnlarni tarjima qilish qatlami ----------------------------------
# Botning tayyor (qo'lda yozilgan, o'zbekcha) xabarlari Telegram'ga yuborilishidan
# oldin tanlangan tilga o'giriladi. Bitta joyda ishlaydi, shuning uchun har bir
# handlerga tegish shart emas. AI yaratgan kontent esa allaqachon kerakli tilda keladi.
# Kalit — kodda yozilgan o'zbekcha matn bo'lagi, qiymat — (ruscha, inglizcha).

I18N_FRAGMENTS = {
    '⭐ <i>PRO — tekin.</i>\n\n': ('⭐ <i>PRO — бесплатно.</i>\n\n', '⭐ <i>PRO — included.</i>\n\n'),
    '✅ <b>Test tayyor!</b>\n\n🎤 <b>PART 1</b> — Introduction & Interview (qisqa test)\n🎙 Har bir savolga ovozli xabar bilan javob bering.': ('✅ <b>Тест готов!</b>\n\n🎤 <b>PART 1</b> — Introduction & Interview (короткий тест)\n🎙 Отвечайте на каждый вопрос голосовым сообщением.', '✅ <b>Test ready!</b>\n\n🎤 <b>PART 1</b> — Introduction & Interview (short test)\n🎙 Answer every question with a voice message.'),
    '✅ <b>Part 1 tugadi!</b>\n\n📊 Yakuniy natija tayyorlanmoqda, biroz kuting...': ('✅ <b>Part 1 завершена!</b>\n\n📊 Готовлю итоговый результат, подождите...', '✅ <b>Part 1 complete!</b>\n\n📊 Preparing your final result, please wait...'),
    '🏁 <b>IELTS SPEAKING PART 1 — YAKUNIY NATIJA</b>\n\n🗣 Fluency & Coherence: <b>': ('🏁 <b>IELTS SPEAKING PART 1 — ИТОГОВЫЙ РЕЗУЛЬТАТ</b>\n\n🗣 Fluency & Coherence: <b>', '🏁 <b>IELTS SPEAKING PART 1 — FINAL RESULT</b>\n\n🗣 Fluency & Coherence: <b>'),
    "🔁 Yangi test uchun /test, to'liq 3 qismli mock uchun /speaking yuboring.": ('🔁 Для нового теста — /test, для полного мока из 3 частей — /speaking.', '🔁 Send /test for a new test, or /speaking for the full 3-part mock.'),
    "\n\nHozircha natijalar yo'q. Birinchi bo'lib test yeching!": ('\n\nПока нет результатов. Пройдите тест первым!', '\n\nNo results yet. Be the first to take a test!'),
    'Foydalanuvchi ': ('Пользователь ', 'User '),
    '✅ Rahmat! Obuna tasdiqlandi.': ('✅ Спасибо! Подписка подтверждена.', '✅ Thank you! Subscription confirmed.'),
    "✅ <b>Obuna tasdiqlandi!</b>\n\nEndi botdan to'liq foydalanishingiz mumkin. Boshlash uchun /start yuboring.": ('✅ <b>Подписка подтверждена!</b>\n\nТеперь вы можете пользоваться ботом в полном объёме. Отправьте /start, чтобы начать.', '✅ <b>Subscription confirmed!</b>\n\nYou can now use the bot in full. Send /start to begin.'),
    "❌ Siz hali obuna bo'lmagansiz. Avval kanalga obuna bo'ling.": ('❌ Вы ещё не подписаны. Сначала подпишитесь на канал.', "❌ You haven't subscribed yet. Please subscribe to the channel first."),
    'Geografiya': ('География', 'Geography'),
    'Fizika': ('Физика', 'Physics'),
    'Kimyo': ('Химия', 'Chemistry'),
    'Biologiya': ('Биология', 'Biology'),
    'Matematika': ('Математика', 'Mathematics'),
    'Texnologiya va IT': ('Технологии и IT', 'Technology & IT'),
    'Adabiyot va madaniyat': ('Литература и культура', 'Literature & culture'),
    "San'at va musiqa": ('Искусство и музыка', 'Art & music'),
    'Koinot': ('Космос', 'Space'),
    'Iqtisodiyot va moliya': ('Экономика и финансы', 'Economics & finance'),
    'Psixologiya': ('Психология', 'Psychology'),
    'Sport': ('Спорт', None),
    'Salomatlik va ovqatlanish': ('Здоровье и питание', 'Health & nutrition'),
    'Ingliz tili': ('Английский язык', 'English language'),
    '🎓 Sertifikat testi': ('🎓 Тест с сертификатом', '🎓 Certificate test'),
    '🧩 Viktorina': ('🧩 Викторина', '🧩 Quiz'),
    '📅 Kunlik test': ('📅 Тест дня', '📅 Daily quiz'),
    '📖 Reading testi yakunlandi!</b>\n\n🎯 Natijangiz: <b>': ('📖 Тест Reading завершён!</b>\n\n🎯 Ваш результат: <b>', '📖 Reading test complete!</b>\n\n🎯 Your score: <b>'),
    '🎧 Listening testi yakunlandi!</b>\n\n🎯 Natijangiz: <b>': ('🎧 Тест Listening завершён!</b>\n\n🎯 Ваш результат: <b>', '🎧 Listening test complete!</b>\n\n🎯 Your score: <b>'),
    '📖 Reading testi': ('📖 Тест Reading', '📖 Reading test'),
    '🎧 Listening testi': ('🎧 Тест Listening', '🎧 Listening test'),
    '🧩 Savol': ('🧩 Вопрос', '🧩 Question'),
    "🛑 Viktorina to'xtatildi. Yangisini boshlash uchun /quiz yuboring.": ('🛑 Викторина остановлена. Чтобы начать новую, отправьте /quiz.', '🛑 Quiz stopped. Send /quiz to start a new one.'),
    '🔄 Suhbat xotirasi tozalandi. Yangidan yozishingiz mumkin, yoki /quiz bilan viktorina boshlang.': ('🔄 Память разговора очищена. Можете писать заново или начать викторину командой /quiz.', '🔄 Chat memory cleared. You can start fresh, or begin a quiz with /quiz.'),
    "🛑 Test to'xtatildi. Qayta boshlash uchun /test yuboring.": ('🛑 Тест остановлен. Чтобы начать заново, отправьте /test.', '🛑 Test stopped. Send /test to start over.'),
    '⛔ Bu buyruq faqat bot egasi uchun.': ('⛔ Эта команда доступна только владельцу бота.', '⛔ This command is for the bot owner only.'),
    "⚠️ Statistika xizmati sozlanmagan.\n\nUPSTASH_REDIS_REST_URL va UPSTASH_REDIS_REST_TOKEN environment variable'larini qo'shing.": ('⚠️ Сервис статистики не настроен.\n\nДобавьте переменные окружения UPSTASH_REDIS_REST_URL и UPSTASH_REDIS_REST_TOKEN.', "⚠️ The statistics service isn't set up.\n\nAdd the UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN environment variables."),
    '❌ Statistikani olishda xatolik yuz berdi.': ('❌ Не удалось получить статистику.', "❌ Couldn't load the statistics."),
    '📊 <b>Bot statistikasi</b>\n\n👥 Jami noyob foydalanuvchilar: <b>': ('📊 <b>Статистика бота</b>\n\n👥 Всего уникальных пользователей: <b>', '📊 <b>Bot statistics</b>\n\n👥 Total unique users: <b>'),
    '</b>\n🟢 Bugun faol: <b>': ('</b>\n🟢 Активны сегодня: <b>', '</b>\n🟢 Active today: <b>'),
    '</b>\n📅 Oxirgi 7 kunda faol: <b>': ('</b>\n📅 Активны за последние 7 дней: <b>', '</b>\n📅 Active in the last 7 days: <b>'),
    '</b>\n📨 Jami murojaatlar (xabar/tugma): <b>': ('</b>\n📨 Всего обращений (сообщения/кнопки): <b>', '</b>\n📨 Total interactions (messages/buttons): <b>'),
    '⚠️ Redis sozlanmagan.': ('⚠️ Redis не настроен.', "⚠️ Redis isn't set up."),
    "❌ Ro'yxatni olishda xatolik yuz berdi.": ('❌ Не удалось получить список.', "❌ Couldn't load the list."),
    "📭 Hozircha ro'yxat bo'sh. Foydalanuvchilar faol bo'lgach shu yerda ko'rinadi.": ('📭 Список пока пуст. Пользователи появятся здесь, когда станут активны.', "📭 The list is empty for now. Users will show up here once they're active."),
    "👥 <b>Foydalanuvchilar</b> (ro'yxatda: ": ('👥 <b>Пользователи</b> (в списке: ', '👥 <b>Users</b> (in the list: '),
    ', oxirgi ': (', последние ', ', latest '),
    ' ta faol)\n': (' активных)\n', ' active)\n'),
    "\n<i>Eslatma: ro'yxat yangilanish kunidan boshlab to'planadi; eski foydalanuvchilar faol bo'lgach qo'shiladi.</i>": ('\n<i>Примечание: список накапливается с момента обновления; прежние пользователи добавятся после первой активности.</i>', "\n<i>Note: the list builds up from the update onward; earlier users are added once they're active.</i>"),
    "⚠️ Reyting xizmati sozlanmagan.\n\nUPSTASH_REDIS_REST_URL va UPSTASH_REDIS_REST_TOKEN environment variable'larini qo'shing.": ('⚠️ Сервис рейтинга не настроен.\n\nДобавьте переменные окружения UPSTASH_REDIS_REST_URL и UPSTASH_REDIS_REST_TOKEN.', "⚠️ The leaderboard service isn't set up.\n\nAdd the UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN environment variables."),
    '🏆 <b>Viktorina reytingi</b> (TOP 10)': ('🏆 <b>Рейтинг викторины</b> (ТОП 10)', '🏆 <b>Quiz leaderboard</b> (TOP 10)'),
    '🎓 <b>Sertifikat testi reytingi</b> (TOP 10)': ('🎓 <b>Рейтинг теста с сертификатом</b> (ТОП 10)', '🎓 <b>Certificate test leaderboard</b> (TOP 10)'),
    '📖 <b>Reading testi reytingi</b> (TOP 10)': ('📖 <b>Рейтинг теста Reading</b> (ТОП 10)', '📖 <b>Reading test leaderboard</b> (TOP 10)'),
    '🎧 <b>Listening testi reytingi</b> (TOP 10)': ('🎧 <b>Рейтинг теста Listening</b> (ТОП 10)', '🎧 <b>Listening test leaderboard</b> (TOP 10)'),
    "🎓 Dashboard'ni ochish": ('🎓 Открыть дашборд', '🎓 Open the dashboard'),
    "🎓 <b>IELTS Dashboard</b>\n\nProfilingiz, Reading, Listening va Grammar materiallarini shu yerdan ko'rishingiz mumkin:": ('🎓 <b>IELTS Dashboard</b>\n\nЗдесь вы найдёте свой профиль и материалы по Reading, Listening и Grammar:', '🎓 <b>IELTS Dashboard</b>\n\nYour profile and the Reading, Listening and Grammar materials are available here:'),
    "❌ Xatolik yuz berdi. Iltimos, qayta urinib ko'ring.": ('❌ Произошла ошибка. Пожалуйста, попробуйте ещё раз.', '❌ Something went wrong. Please try again.'),
    "❌ Ovozli xabarni tushunishda xatolik yuz berdi. Iltimos, qayta urinib ko'ring.": ('❌ Не удалось обработать голосовое сообщение. Пожалуйста, попробуйте ещё раз.', "❌ Couldn't process your voice message. Please try again."),
    '⚠️ Ovozingizda nutq aniqlanmadi. Please try speaking again.': ('⚠️ Речь в записи не распознана. Please try speaking again.', '⚠️ No speech was detected in your recording. Please try speaking again.'),
    '⛔ Bu buyruq faqat bot egasi (admin) uchun.': ('⛔ Эта команда доступна только владельцу бота (админу).', '⛔ This command is for the bot owner (admin) only.'),
    '🎲 Tasodifiy (aralash)': ('🎲 Случайно (микс)', '🎲 Random (mixed)'),
    "🧩 <b>Qaysi fandan viktorina o'ynaymiz?</b>\n\nFan tanlang — men ": ('🧩 <b>По какой теме сыграем в викторину?</b>\n\nВыберите тему — я задам ', "🧩 <b>Which subject shall we quiz on?</b>\n\nPick a subject — I'll ask you "),
    ' ta qiziqarli savol beraman:': (' интересных вопросов:', ' fun questions:'),
    '🎲 Aralash viktorina': ('🎲 Микс-викторина', '🎲 Mixed quiz'),
    ' ta savol tayyorlanmoqda...': (' вопросов готовятся...', ' questions are being prepared...'),
    "❌ Savollarni tayyorlashda xatolik yuz berdi. Iltimos /quiz bilan qayta urinib ko'ring.": ('❌ Не удалось подготовить вопросы. Попробуйте ещё раз: /quiz', "❌ Couldn't prepare the questions. Please try again with /quiz."),
    '🔄 Yangi jumboq': ('🔄 Новая головоломка', '🔄 New puzzle'),
    '💡 Maslahat': ('💡 Подсказка', '💡 Hint'),
    "✅ Javobni ko'rish": ('✅ Показать ответ', '✅ Show the answer'),
    '🔄 Boshqa jumboq': ('🔄 Другая головоломка', '🔄 Another puzzle'),
    "❌ Jumboq tayyorlashda xatolik yuz berdi. /teaser bilan qayta urinib ko'ring.": ('❌ Не удалось подготовить головоломку. Попробуйте ещё раз: /teaser', "❌ Couldn't prepare the puzzle. Please try again with /teaser."),
    "\n\n<i>O'ylab ko'ring, so'ng tugmalardan foydalaning 👇</i>": ('\n\n<i>Подумайте, а затем воспользуйтесь кнопками 👇</i>', '\n\n<i>Give it some thought, then use the buttons 👇</i>'),
    'Bu jumboq eskirgan. /teaser bilan yangisini oling.': ('Эта головоломка устарела. Получите новую: /teaser.', 'This puzzle has expired. Get a new one with /teaser.'),
    "Maslahat yo'q": ('Подсказки нет', 'No hint available'),
    '🧠 <b>Miya jumboqi</b>\n\n': ('🧠 <b>Головоломка</b>\n\n', '🧠 <b>Brain teaser</b>\n\n'),
    '\n\n✅ <b>Javob:</b> ': ('\n\n✅ <b>Ответ:</b> ', '\n\n✅ <b>Answer:</b> '),
    "🤷 Ba'zan / bilmayman": ('🤷 Иногда / не знаю', "🤷 Sometimes / I don't know"),
    "🙅 Yo'q, xato": ('🙅 Нет, ошибка', '🙅 No, wrong'),
    "🔁 Yana o'ynash": ('🔁 Сыграть ещё', '🔁 Play again'),
    '⚠️ Kechirasiz, xatolik yuz berdi. Qayta boshlash: /guess': ('⚠️ Извините, произошла ошибка. Начать заново: /guess', '⚠️ Sorry, something went wrong. Start over with /guess'),
    "🔮 <b>Menimcha, siz o'ylagan narsa:</b>\n\n✨ <b>": ('🔮 <b>Думаю, вы загадали:</b>\n\n✨ <b>', '🔮 <b>I think you were thinking of:</b>\n\n✨ <b>'),
    '</b>\n\nTopdimmi?': ('</b>\n\nУгадал?', '</b>\n\nAm I right?'),
    'Bu tirik mavjudotmi?': ('Это живое существо?', 'Is it a living thing?'),
    '🔮 <b>Savol ': ('🔮 <b>Вопрос ', '🔮 <b>Question '),
    "🔮 <b>Men topaman!</b>\n\nBiror narsani <b>o'ylang</b> — mashhur inson, hayvon, narsa, ovqat, multfilm qahramoni... Hech qayerga yozmang, faqat xayolingizda tuting 🤫\n\nMen ": ('🔮 <b>Я угадаю!</b>\n\nЗагадайте что-нибудь — известного человека, животное, предмет, блюдо, героя мультфильма... Никому не говорите, держите в уме 🤫\n\nЯ задам до ', "🔮 <b>I'll guess it!</b>\n\nThink of something — a famous person, an animal, an object, a food, a cartoon character... Don't tell anyone, just keep it in your head 🤫\n\nI'll ask up to "),
    ' tagacha savol berib, topishga harakat qilaman. Siz faqat tugmalarni bosasiz 👇': (' вопросов и постараюсь угадать. Вам нужно только нажимать кнопки 👇', ' questions and try to guess it. You only need to tap the buttons 👇'),
    "Bu o'yin tugagan. Yangisi: /guess": ('Эта игра закончена. Новая: /guess', 'This game is over. Start a new one: /guess'),
    "⏳ Bir soniya, o'ylayapman...": ('⏳ Секунду, думаю...', "⏳ One moment, I'm thinking..."),
    '🎉 <b>Yutdim!</b> Men ': ('🎉 <b>Угадал!</b> Вопросов задано: ', '🎉 <b>I won!</b> It took me '),
    ' ta savolda <b>': ('. Это — <b>', ' questions to find <b>'),
    "</b> ni topdim 😎\n\nYana o'ynaymizmi?": ('</b> 😎\n\nСыграем ещё?', '</b> 😎\n\nShall we play again?'),
    "Sizning o'ylaganingiz ": ('Вы загадали ', 'Were you thinking of '),
    "🏳️ <b>Yutqazdim!</b> Siz meni chalg'itdingiz 👏\n\nYana bir marta urinib ko'raymi?": ('🏳️ <b>Сдаюсь!</b> Вы меня запутали 👏\n\nПопробуем ещё раз?', "🏳️ <b>I give up!</b> You've outsmarted me 👏\n\nShall I try once more?"),
    "🤔 Hmm, xato qildim. Yana bir oz o'ylab ko'raman...": ('🤔 Хм, ошибся. Подумаю ещё немного...', '🤔 Hmm, I was wrong. Let me think a bit more...'),
    'Detektiv': ('Детектив', 'Detective'),
    'Kosmos': ('Космос', 'Space'),
    'Fantaziya': ('Фэнтези', 'Fantasy'),
    'Omon qolish': ('Выживание', 'Survival'),
    'Qaroqchilar': ('Пираты', 'Pirates'),
    'Vaqt sayohati': ('Путешествие во времени', 'Time travel'),
    "❌ Hikoyani davom ettirib bo'lmadi. Qayta urinish uchun /story yuboring.": ('❌ Не удалось продолжить историю. Чтобы начать заново, отправьте /story.', "❌ Couldn't continue the story. Send /story to start again."),
    "\n\n🎭 <b>Hikoya yakunlandi!</b> Boshqa tanlov — boshqa yakun. Yana o'ynaymizmi? 👇": ('\n\n🎭 <b>История завершена!</b> Другой выбор — другой финал. Сыграем ещё? 👇', '\n\n🎭 <b>The story is over!</b> A different choice means a different ending. Play again? 👇'),
    '🔁 Yangi hikoya': ('🔁 Новая история', '🔁 New story'),
    '\n\n<i>Nima qilasiz? 👇</i>': ('\n\n<i>Что будете делать? 👇</i>', '\n\n<i>What will you do? 👇</i>'),
    '⏳ Avval joriy testni tugating yoki /stop yuboring.': ('⏳ Сначала завершите текущий тест или отправьте /stop.', '⏳ Please finish the current test first, or send /stop.'),
    "📖 <b>Interaktiv hikoya</b>\n\nMen hikoya boshlayman, siz esa har qadamda yo'lni tanlaysiz — ": ('📖 <b>Интерактивная история</b>\n\nЯ начну историю, а вы на каждом шаге выбираете путь — финал зависит от ваших решений после ', "📖 <b>Interactive story</b>\n\nI'll start a story and you choose the path at every step — the ending depends on your decisions after "),
    " qadamdan keyin yakun sizning tanlovlaringizga bog'liq bo'ladi 🎲\n\nJanrni tanlang 👇": (' шагов 🎲\n\nВыберите жанр 👇', ' steps 🎲\n\nChoose a genre 👇'),
    '📖 Janrni tanlang 👇': ('📖 Выберите жанр 👇', '📖 Choose a genre 👇'),
    'Xatolik': ('Ошибка', 'Error'),
    'Bu hikoya tugagan. /story bilan yangisini boshlang.': ('Эта история закончена. Начните новую: /story', 'This story is over. Start a new one: /story'),
    '👉 <b>Siz tanladingiz:</b> ': ('👉 <b>Вы выбрали:</b> ', '👉 <b>You chose:</b> '),
    "1️⃣ yolg'on": ('1️⃣ ложь', '1️⃣ is the lie'),
    "2️⃣ yolg'on": ('2️⃣ ложь', '2️⃣ is the lie'),
    "3️⃣ yolg'on": ('3️⃣ ложь', '3️⃣ is the lie'),
    "❌ Raund tayyorlanmadi. /lie bilan qayta urinib ko'ring.": ('❌ Раунд не подготовлен. Попробуйте ещё раз: /lie', "❌ The round couldn't be prepared. Please try again with /lie."),
    '\n🔥 Ketma-ket: <b>': ('\n🔥 Подряд: <b>', '\n🔥 In a row: <b>'),
    "🕵️ <b>Yolg'onni top!</b>": ('🕵️ <b>Найди ложь!</b>', '🕵️ <b>Spot the lie!</b>'),
    "\n\nUlardan <b>bittasi yolg'on</b>. Qaysi biri? 👇": ('\n\nИз них <b>одно — ложь</b>. Какое? 👇', '\n\nOne of them is <b>a lie</b>. Which one? 👇'),
    'Bu raund tugagan. /lie bilan yangisini boshlang.': ('Этот раунд закончен. Начните новый: /lie', 'This round is over. Start a new one: /lie'),
    '🎯 <b>Topdingiz!</b> 🔥 Ketma-ket: <b>': ('🎯 <b>Угадали!</b> 🔥 Подряд: <b>', '🎯 <b>You got it!</b> 🔥 In a row: <b>'),
    "😅 <b>Yolg'on tuzog'iga tushdingiz!</b>": ('😅 <b>Попались на ложь!</b>', '😅 <b>You fell for the lie!</b>'),
    ' (seriya ': (' (серия из ', ' (your streak of '),
    " da to'xtadi)": (' прервалась)', ' ended)'),
    'Xato 😅': ('Мимо 😅', 'Missed 😅'),
    "❌ yolg'on": ('❌ ложь', '❌ lie'),
    '✅ rost': ('✅ правда', '✅ true'),
    '\n\n🏆 Eng yaxshi seriya: ': ('\n\n🏆 Лучшая серия: ', '\n\n🏆 Best streak: '),
    '➡️ Keyingisi': ('➡️ Дальше', '➡️ Next'),
    "❌ Savol tayyorlanmadi. /whatif bilan qayta urinib ko'ring.": ('❌ Вопрос не подготовлен. Попробуйте ещё раз: /whatif', "❌ The question couldn't be prepared. Please try again with /whatif."),
    "🔬 <b>Nima bo'ladi?</b>": ('🔬 <b>Что будет?</b>', '🔬 <b>What happens?</b>'),
    '\n\n<i>Sizningcha-chi? 👇</i>': ('\n\n<i>Как вы думаете? 👇</i>', '\n\n<i>What do you think? 👇</i>'),
    'Bu savol tugagan. /whatif bilan yangisini oling.': ('Этот вопрос закончен. Получите новый: /whatif', 'This question is over. Get a new one with /whatif'),
    "🎉 <b>To'g'ri taxmin!</b>": ('🎉 <b>Верный прогноз!</b>', '🎉 <b>Great guess!</b>'),
    "🤯 <b>Aksincha bo'ldi!</b>": ('🤯 <b>Вышло наоборот!</b>', '🤯 <b>It turned out differently!</b>'),
    'Kutilmagan 🤯': ('Неожиданно 🤯', 'Unexpected 🤯'),
    '\n\n🔎 <b>Ilm nima deydi:</b> ': ('\n\n🔎 <b>Что говорит наука:</b> ', '\n\n🔎 <b>What science says:</b> '),
    '\n\n🎯 Natijangiz: <b>': ('\n\n🎯 Ваш результат: <b>', '\n\n🎯 Your score: <b>'),
    '➡️ Yana bitta': ('➡️ Ещё один', '➡️ One more'),
    "A1–A2 (boshlang'ich)": ('A1–A2 (начальный)', 'A1–A2 (beginner)'),
    "B1–B2 (o'rta)": ('B1–B2 (средний)', 'B1–B2 (intermediate)'),
    'C1 (yuqori)': ('C1 (продвинутый)', 'C1 (advanced)'),
    'Plurals va this/that': ('Plurals и this/that', 'Plurals & this/that'),
    '🎲 Tasodifiy mavzu': ('🎲 Случайная тема', '🎲 Random topic'),
    '⬅️ Darajalar': ('⬅️ Уровни', '⬅️ Levels'),
    '📝 <b>Mashq ': ('📝 <b>Упражнение ', '📝 <b>Exercise '),
    '📝 <b>Ingliz tili grammatikasi</b>\n\nDarajangizni tanlang — qisqa dars va 5 ta mashq tayyorlayman 👇': ('📝 <b>Грамматика английского</b>\n\nВыберите уровень — я подготовлю короткий урок и 5 упражнений 👇', "📝 <b>English grammar</b>\n\nChoose your level — I'll prepare a short lesson and 5 exercises 👇"),
    '📝 <b>Grammatika</b>\n\nDarajani tanlang 👇': ('📝 <b>Грамматика</b>\n\nВыберите уровень 👇', '📝 <b>Grammar</b>\n\nChoose a level 👇'),
    '</b>\n\nMavzuni tanlang 👇': ('</b>\n\nВыберите тему 👇', '</b>\n\nChoose a topic 👇'),
    '⏳ Dars tayyorlanmoqda...': ('⏳ Готовлю урок...', '⏳ Preparing the lesson...'),
    "❌ Dars tayyorlanmadi. /grammar bilan qayta urinib ko'ring.": ('❌ Не удалось подготовить урок. Попробуйте ещё раз: /grammar', "❌ Couldn't prepare the lesson. Please try again with /grammar."),
    '\n\n✨ <b>Misollar:</b>\n': ('\n\n✨ <b>Примеры:</b>\n', '\n\n✨ <b>Examples:</b>\n'),
    "\n\n⚠️ <b>Ko'p uchraydigan xato:</b> ": ('\n\n⚠️ <b>Частая ошибка:</b> ', '\n\n⚠️ <b>A common mistake:</b> '),
    '📝 Mashqni boshlash': ('📝 Начать упражнения', '📝 Start the exercises'),
    'Bu sessiya tugagan. /grammar bilan qayta boshlang.': ('Эта сессия закончена. Начните заново: /grammar', 'This session is over. Start again with /grammar'),
    "❌ Noto'g'ri": ('❌ Неверно', '❌ Incorrect'),
    "🏆 Ajoyib! Bu mavzuni yaxshi o'zlashtirdingiz.": ('🏆 Отлично! Вы хорошо усвоили эту тему.', "🏆 Excellent! You've got a good grip on this topic."),
    "👍 Yomon emas! Yana bir bor mashq qilsangiz, mukammal bo'ladi.": ('👍 Неплохо! Ещё немного практики — и будет идеально.', "👍 Not bad! A little more practice and it'll be perfect."),
    "📚 Dars matnini qayta o'qib, yana urinib ko'ring — albatta chiqadi!": ('📚 Перечитайте урок и попробуйте ещё раз — обязательно получится!', "📚 Re-read the lesson and try again — you'll get it!"),
    '</b> — natija\n\n🎯 <b>': ('</b> — результат\n\n🎯 <b>', '</b> — results\n\n🎯 <b>'),
    '🔁 Shu darajadan boshqa mavzu': ('🔁 Другая тема этого уровня', '🔁 Another topic at this level'),
    '✍️ <b>IELTS WRITING — Task 2</b>\n\n📋 <b>Topshiriq:</b>\n<i>': ('✍️ <b>IELTS WRITING — Task 2</b>\n\n📋 <b>Задание:</b>\n<i>', '✍️ <b>IELTS WRITING — Task 2</b>\n\n📋 <b>Task:</b>\n<i>'),
    "</i>\n\n⏱ Vaqt: 40 daqiqa (mock uchun cheklov yo'q)\n📝 Kamida <b>": ('</i>\n\n⏱ Время: 40 минут (в тренировке без ограничения)\n📝 Не менее <b>', '</i>\n\n⏱ Time: 40 minutes (no limit in this practice)\n📝 At least <b>'),
    " so'z</b> (rasmiy imtihonda 250) — inshoni bitta xabar qilib yuboring.\n📌 Reja: kirish → 2 ta asosiy paragraf → xulosa.\n\n<i>Bekor qilish: /stop</i>": (' слов</b> (на экзамене — 250). Отправьте эссе одним сообщением.\n📌 План: введение → 2 основных абзаца → вывод.\n\n<i>Отмена: /stop</i>', ' words</b> (250 in the real exam). Send your essay as a single message.\n📌 Plan: introduction → 2 body paragraphs → conclusion.\n\n<i>Cancel: /stop</i>'),
    '🔄 Boshqa mavzu': ('🔄 Другая тема', '🔄 Another topic'),
    '📝 <b>IELTS WRITING — TAHLIL</b>\n': ('📝 <b>IELTS WRITING — РАЗБОР</b>\n', '📝 <b>IELTS WRITING — FEEDBACK</b>\n'),
    '\n💪 <b>Kuchli tomonlar:</b>\n': ('\n💪 <b>Сильные стороны:</b>\n', '\n💪 <b>Strengths:</b>\n'),
    '\n🔧 <b>Xatolar va tuzatishlar:</b>': ('\n🔧 <b>Ошибки и исправления:</b>', '\n🔧 <b>Mistakes and corrections:</b>'),
    "\n📚 <b>Boy lug'at:</b>\n": ('\n📚 <b>Расширьте словарь:</b>\n', '\n📚 <b>Vocabulary boosters:</b>\n'),
    '\n✨ <b>Yaxshilangan paragraf:</b>\n<i>': ('\n✨ <b>Улучшенный абзац:</b>\n<i>', '\n✨ <b>Improved paragraph:</b>\n<i>'),
    '\n🚀 <b>Keyingi qadamlar:</b>\n': ('\n🚀 <b>Следующие шаги:</b>\n', '\n🚀 <b>Next steps:</b>\n'),
    '🎁 <i>Siz uchun bepul.</i>\n\n': ('🎁 <i>Для вас бесплатно.</i>\n\n', '🎁 <i>Free for you.</i>\n\n'),
    "✅ <i>To'lov qabul qilingan.</i>\n\n": ('✅ <i>Оплата получена.</i>\n\n', '✅ <i>Payment received.</i>\n\n'),
    "💎 <b>IELTS Writing tahlili (Premium)</b>\n\nXuddi mock testdagidek: men IELTS <b>Writing Task 2</b> topshirig'ini beraman, siz insho yozasiz — men esa IELTS mezonlari bo'yicha tekshiraman:\n🏆 band ballni (4 ta mezon bo'yicha)\n🔧 xatolar va ularning tuzatilishini\n📚 boy lug'at takliflarini\n✨ yaxshilangan paragrafni beraman.\n\nNarxi: <b>": ('💎 <b>Разбор IELTS Writing (Premium)</b>\n\nКак на пробном экзамене: я даю задание IELTS <b>Writing Task 2</b>, вы пишете эссе — а я проверяю его по критериям IELTS:\n🏆 общий балл (band) по 4 критериям\n🔧 ошибки и их исправления\n📚 советы по расширению словаря\n✨ улучшенный абзац.\n\nСтоимость: <b>', '💎 <b>IELTS Writing review (Premium)</b>\n\nJust like a mock exam: I give you an IELTS <b>Writing Task 2</b>, you write an essay — and I assess it against the IELTS criteria:\n🏆 band score across 4 criteria\n🔧 mistakes and their corrections\n📚 vocabulary suggestions\n✨ an improved paragraph.\n\nPrice: <b>'),
    ' ⭐ Telegram Stars</b>': (' ⭐ Telegram Stars</b>', None),
    'IELTS Writing tahlili': ('Разбор IELTS Writing', 'IELTS Writing review'),
    'Inshongiz uchun band ball, xatolar tahlili va yaxshilangan variant.': ('Балл band, разбор ошибок и улучшенный вариант вашего эссе.', 'A band score, a mistake analysis and an improved version of your essay.'),
    "❌ To'lov oynasini ochib bo'lmadi. Birozdan so'ng qayta urinib ko'ring.": ('❌ Не удалось открыть окно оплаты. Попробуйте чуть позже.', "❌ Couldn't open the payment window. Please try again a little later."),
    "To'lov ma'lumotlari mos kelmadi. /writing orqali qayta urinib ko'ring.": ('Данные платежа не совпали. Попробуйте ещё раз через /writing.', "The payment details didn't match. Please try again via /writing."),
    "✅ <b>To'lov qabul qilindi, rahmat!</b> ⭐\n\n": ('✅ <b>Оплата получена, спасибо!</b> ⭐\n\n', '✅ <b>Payment received, thank you!</b> ⭐\n\n'),
    'Bu sessiya tugagan. /writing bilan qayta boshlang.': ('Эта сессия закончена. Начните заново: /writing', 'This session is over. Start again with /writing'),
    '✍️ Insho juda qisqa (': ('✍️ Эссе слишком короткое (', '✍️ Your essay is too short ('),
    " so'z). Kamida ": (' слов). Нужно не менее ', ' words). You need at least '),
    " so'z yozing va qayta yuboring.": (' слов. Допишите и отправьте снова.', ' words. Add more and send it again.'),
    '✂️ Insho juda uzun. Iltimos, ': ('✂️ Эссе слишком длинное. Пожалуйста, не более ', '✂️ Your essay is too long. Please keep it under '),
    ' belgidan oshmasin.': (' символов.', ' characters.'),
    '🔎 Inshoyingiz tahlil qilinmoqda, 20–40 soniya kuting...': ('🔎 Ваше эссе анализируется, подождите 20–40 секунд...', '🔎 Analysing your essay — please wait 20–40 seconds...'),
    "❌ Tahlilda xatolik yuz berdi. To'lovingiz saqlandi — inshongizni qayta yuboring.": ('❌ При анализе произошла ошибка. Оплата сохранена — отправьте эссе ещё раз.', '❌ Something went wrong during the analysis. Your payment is safe — please send your essay again.'),
    "</i>\n📏 So'zlar soni: <b>": ('</i>\n📏 Слов: <b>', '</i>\n📏 Word count: <b>'),
    '🔁 Yana tahlil qildirish uchun /writing yuboring.': ('🔁 Чтобы разобрать ещё одно эссе, отправьте /writing.', '🔁 To get another essay reviewed, send /writing.'),
    '📅 <b>Bugungi fan: ': ('📅 <b>Тема дня: ', "📅 <b>Today's subject: "),
    '💡 <b>Bilasizmi?</b>\n': ('💡 <b>А вы знали?</b>\n', '💡 <b>Did you know?</b>\n'),
    '🇬🇧 <b>Kunlik ingliz tili: ': ('🇬🇧 <b>Английский на каждый день: ', '🇬🇧 <b>Daily English: '),
    '🎯 Kunlik testni boshlash (': ('🎯 Начать тест дня (', "🎯 Start today's quiz ("),
    '⏳ Bugungi kontent tayyorlanmoqda...': ('⏳ Готовлю сегодняшний материал...', "⏳ Preparing today's content..."),
    "❌ Bugungi kontentni tayyorlab bo'lmadi. Birozdan keyin /daily bilan qayta urinib ko'ring.": ('❌ Не удалось подготовить сегодняшний материал. Попробуйте чуть позже: /daily', "❌ Couldn't prepare today's content. Please try again shortly with /daily."),
    '⚠️ Bugungi test hozir mavjud emas. Boshqa viktorina uchun /quiz yuboring.': ('⚠️ Сегодняшний тест сейчас недоступен. Другую викторину можно запустить командой /quiz.', "⚠️ Today's quiz isn't available right now. Try another quiz with /quiz."),
    '⚠️ Bu xizmat hozir sozlanmagan.': ('⚠️ Этот сервис сейчас не настроен.', "⚠️ This service isn't set up right now."),
    '🔔 Kunlik xabarlar <b>yoqildi</b>. Har kuni yangi fan va ingliz tili maslahatini olasiz!': ('🔔 Ежедневные сообщения <b>включены</b>. Каждый день вы будете получать новую тему и совет по английскому!', "🔔 Daily messages are <b>on</b>. Every day you'll get a new subject and an English tip!"),
    "🔕 Kunlik xabarlar <b>o'chirildi</b>. Qayta yoqish uchun yana /notify yuboring.\n\nKunlik kontentni /daily orqali o'zingiz ham ochishingiz mumkin.": ('🔕 Ежедневные сообщения <b>отключены</b>. Чтобы включить снова, отправьте /notify.\n\nЕжедневный материал можно открыть и самому командой /daily.', '🔕 Daily messages are <b>off</b>. To turn them back on, send /notify.\n\nYou can also open the daily content yourself with /daily.'),
    "❌ Xatolik yuz berdi. Keyinroq qayta urinib ko'ring.": ('❌ Произошла ошибка. Попробуйте позже.', '❌ Something went wrong. Please try again later.'),
    "\n\n<i>Kunlik xabarni o'chirish: /notify</i>": ('\n\n<i>Отключить ежедневные сообщения: /notify</i>', '\n\n<i>Turn off daily messages: /notify</i>'),
    '⏳ Sertifikat testi uchun ': ('⏳ Готовлю вопросы для сертификатного теста: ', '⏳ Preparing questions for the certificate test: '),
    ' ta savol tayyorlanmoqda... (bu biroz vaqt olishi mumkin, iltimos kuting)': (' шт. (это может занять немного времени, подождите)', ' (this may take a little while, please wait)'),
    "❌ Savollarni tayyorlashda xatolik yuz berdi. Iltimos /certificate bilan qayta urinib ko'ring.": ('❌ Не удалось подготовить вопросы. Попробуйте ещё раз: /certificate', "❌ Couldn't prepare the questions. Please try again with /certificate."),
    '🎓 <b>Sertifikat testi boshlandi!</b>\n\n📝 Jami ': ('🎓 <b>Сертификатный тест начался!</b>\n\n📝 Всего вопросов: ', '🎓 <b>The certificate test has started!</b>\n\n📝 Total questions: '),
    ' ta savol. Agar natijangiz <b>': ('. Если ваш результат будет <b>', '. If your score is <b>'),
    "%</b> yoki undan yuqori bo'lsa, test yakunida shaxsiy sertifikat rasmini olasiz 🏅\n\nOmad! 🍀": ('%</b> или выше, в конце вы получите именной сертификат 🏅\n\nУдачи! 🍀', "%</b> or higher, you'll receive a personal certificate at the end 🏅\n\nGood luck! 🍀"),
    '⏳ Reading testi tayyorlanmoqda... (bu biroz vaqt olishi mumkin)': ('⏳ Готовлю тест Reading... (это может занять немного времени)', '⏳ Preparing the Reading test... (this may take a little while)'),
    "❌ Testni tayyorlashda xatolik yuz berdi. Iltimos /reading_test bilan qayta urinib ko'ring.": ('❌ Не удалось подготовить тест. Попробуйте ещё раз: /reading_test', "❌ Couldn't prepare the test. Please try again with /reading_test."),
    '📝 Endi quyidagi ': ('📝 Теперь ответьте на ', '📝 Now answer the following '),
    ' ta savolga javob bering 👇': (' вопросов 👇', ' questions 👇'),
    '⏳ Listening testi tayyorlanmoqda... (bu biroz vaqt olishi mumkin)': ('⏳ Готовлю тест Listening... (это может занять немного времени)', '⏳ Preparing the Listening test... (this may take a little while)'),
    "❌ Testni tayyorlashda xatolik yuz berdi. Iltimos /listening_test bilan qayta urinib ko'ring.": ('❌ Не удалось подготовить тест. Попробуйте ещё раз: /listening_test', "❌ Couldn't prepare the test. Please try again with /listening_test."),
    '🎧 <b>Listening — tinglang va savollarga javob bering:</b>': ('🎧 <b>Listening — послушайте и ответьте на вопросы:</b>', '🎧 <b>Listening — listen and answer the questions:</b>'),
    "⚠️ Audio yaratib bo'lmadi, lekin matn asosida davom etamiz:\n\n": ('⚠️ Не удалось создать аудио, продолжим по тексту:\n\n', "⚠️ Couldn't create the audio, so we'll continue with the text:\n\n"),
    "⏱ Bu sessiya tugagan yoki muddati o'tgan. Yangi test uchun /quiz yuboring.": ('⏱ Эта сессия закончилась или устарела. Для нового теста отправьте /quiz.', '⏱ This session has ended or expired. Send /quiz to start a new test.'),
    'Xatolik yuz berdi.': ('Произошла ошибка.', 'Something went wrong.'),
    "✅ <b>To'g'ri javob!</b>": ('✅ <b>Правильно!</b>', '✅ <b>Correct!</b>'),
    "❌ <b>Noto'g'ri.</b> To'g'ri javob: <b>": ('❌ <b>Неверно.</b> Правильный ответ: <b>', '❌ <b>Incorrect.</b> The correct answer: <b>'),
    ' <b>Savol ': (' <b>Вопрос ', ' <b>Question '),
    "Noto'g'ri 😔": ('Неверно 😔', 'Incorrect 😔'),
    'Foydalanuvchi': ('Пользователь', 'User'),
    '🏁 <b>Sertifikat testi yakunlandi!</b>\n\n🎯 Natijangiz: <b>': ('🏁 <b>Сертификатный тест завершён!</b>\n\n🎯 Ваш результат: <b>', '🏁 <b>Certificate test complete!</b>\n\n🎯 Your score: <b>'),
    '%)\n\n🎉 Tabriklaymiz! Sertifikatingiz tayyorlanmoqda...': ('%)\n\n🎉 Поздравляем! Ваш сертификат готовится...', '%)\n\n🎉 Congratulations! Your certificate is being prepared...'),
    '🎓 <b>Tabriklaymiz, ': ('🎓 <b>Поздравляем, ', '🎓 <b>Congratulations, '),
    '!</b>\nSiz ': ('!</b>\nИз ', '!</b>\nOut of '),
    ' savoldan ': (' вопросов вы ответили правильно на ', ' questions you answered '),
    " tasiga to'g'ri javob berib, (": (' и получили сертификат (', ' correctly and earned a certificate ('),
    "%) sertifikatga sazovor bo'ldingiz!\n\n🏆 Reytingni /leaderboard orqali ko'rishingiz mumkin.": ('%)!\n\n🏆 Рейтинг можно посмотреть командой /leaderboard.', '%)!\n\n🏆 You can see the leaderboard with /leaderboard.'),
    '⚠️ Sertifikat rasmini yaratishda xatolik yuz berdi, lekin natijangiz hisobga olindi.': ('⚠️ Не удалось создать изображение сертификата, но ваш результат засчитан.', "⚠️ Couldn't create the certificate image, but your result has been recorded."),
    '%)\n\n😔 Afsuski, sertifikat olish uchun kamida ': ('%)\n\n😔 К сожалению, для получения сертификата нужно набрать не менее ', '%)\n\n😔 Unfortunately, you need at least '),
    "% kerak. Ko'proq mashq qilib, yana urinib ko'ring!\n\n🔁 Qayta urinish uchun /certificate yuboring.": ('%. Потренируйтесь ещё и попробуйте снова!\n\n🔁 Чтобы попробовать снова, отправьте /certificate.', '% to earn a certificate. Practise some more and try again!\n\n🔁 To retry, send /certificate.'),
    '🏆 Ajoyib natija!': ('🏆 Отличный результат!', '🏆 Excellent result!'),
    '👍 Yomon emas, yana mashq qiling!': ('👍 Неплохо, продолжайте тренироваться!', '👍 Not bad — keep practising!'),
    "📚 Ko'proq mashq qilish kerak.": ('📚 Нужно больше практики.', '📚 More practice is needed.'),
    ' testi yakunlandi!</b>\n\n🎯 Natijangiz: <b>': (' — тест завершён!</b>\n\n🎯 Ваш результат: <b>', ' test complete!</b>\n\n🎯 Your score: <b>'),
    '\n\n🔁 Yangi test uchun ': ('\n\n🔁 Для нового теста — ', '\n\n🔁 For a new test, send '),
    ", reytingni ko'rish uchun /leaderboard yuboring.": (', чтобы посмотреть рейтинг — /leaderboard.', ' — to see the leaderboard, send /leaderboard.'),
    "🏆 Zo'r! Bugungi fanni yaxshi bilar ekansiz.": ('🏆 Отлично! Вы хорошо знаете сегодняшнюю тему.', "🏆 Great! You know today's subject well."),
    '👍 Yaxshi, ertaga yangi fan sizni kutadi!': ('👍 Хорошо, завтра вас ждёт новая тема!', '👍 Good — a new subject awaits you tomorrow!'),
    "📚 Hechqisi yo'q — yangi narsa o'rganish ham yutuq!": ('📚 Ничего страшного — узнать новое тоже достижение!', '📚 No worries — learning something new is an achievement too!'),
    '🏁 <b>Kunlik test yakunlandi!</b>\n\n🎯 Natijangiz: <b>': ('🏁 <b>Тест дня завершён!</b>\n\n🎯 Ваш результат: <b>', '🏁 <b>Daily quiz complete!</b>\n\n🎯 Your score: <b>'),
    "\n\n🧩 Yana o'ynash uchun /quiz, jumboq uchun /teaser yuboring.": ('\n\n🧩 Чтобы сыграть ещё — /quiz, за головоломкой — /teaser.', '\n\n🧩 To play more, send /quiz — for a puzzle, send /teaser.'),
    '🏆 Ajoyib natija! Bilimingiz juda yaxshi.': ('🏆 Отличный результат! У вас очень хорошие знания.', '🏆 Excellent result! Your knowledge is really solid.'),
    "👍 Yomon emas! Yana mashq qilsangiz yanada yaxshi bo'ladi.": ('👍 Неплохо! С небольшой практикой будет ещё лучше.', "👍 Not bad! A bit more practice and you'll do even better."),
    "📚 Ko'proq o'rganishga arziydi, lekin harakat qildingiz!": ('📚 Стоит узнать больше, но вы старались!', "📚 There's more to learn, but you gave it a good try!"),
    '🏁 <b>Viktorina yakunlandi!</b>\n\n🎯 Natijangiz: <b>': ('🏁 <b>Викторина завершена!</b>\n\n🎯 Ваш результат: <b>', '🏁 <b>Quiz complete!</b>\n\n🎯 Your score: <b>'),
    "\n\n🔁 Yana o'ynash uchun /quiz, sertifikat testi uchun /certificate, reytingni ko'rish uchun /leaderboard yuboring.": ('\n\n🔁 Чтобы сыграть ещё — /quiz, тест с сертификатом — /certificate, рейтинг — /leaderboard.', '\n\n🔁 To play again send /quiz, for the certificate test /certificate, for the leaderboard /leaderboard.'),
    '⏳ IELTS Speaking testi tayyorlanmoqda, biroz kuting...': ('⏳ Готовлю тест IELTS Speaking, немного подождите...', '⏳ Preparing your IELTS Speaking test, please wait a moment...'),
    "❌ Savollarni generatsiya qilishda xatolik yuz berdi. Iltimos /test bilan qayta urinib ko'ring.": ('❌ Не удалось сгенерировать вопросы. Попробуйте ещё раз: /test', "❌ Couldn't generate the questions. Please try again with /test."),
    '✅ <b>Test tayyor!</b>\n\n🎤 <b>PART 1</b> — Introduction & Interview\n🎙 Har bir savolga ovozli xabar bilan javob bering.': ('✅ <b>Тест готов!</b>\n\n🎤 <b>PART 1</b> — Introduction & Interview\n🎙 Отвечайте на каждый вопрос голосовым сообщением.', '✅ <b>Your test is ready!</b>\n\n🎤 <b>PART 1</b> — Introduction & Interview\n🎙 Answer each question with a voice message.'),
    '🗣 <b>Savol 1/': ('🗣 <b>Вопрос 1/', '🗣 <b>Question 1/'),
    '❌ Ovozli xabarni yuklab olishda xatolik. Iltimos qayta yuboring.': ('❌ Не удалось загрузить голосовое сообщение. Отправьте ещё раз.', "❌ Couldn't download your voice message. Please send it again."),
    '❌ Ovozni matnga aylantirishda xatolik. Iltimos qayta yuboring.': ('❌ Не удалось преобразовать голос в текст. Отправьте ещё раз.', "❌ Couldn't convert your speech to text. Please send it again."),
    '⚠️ Ovozingizda nutq aniqlanmadi. Iltimos, aniqroq gapirib qayta yuboring.': ('⚠️ В записи не распознана речь. Скажите чётче и отправьте снова.', '⚠️ No speech was detected. Please speak more clearly and send it again.'),
    "Baholab bo'lmadi (texnik xatolik).": ('Не удалось оценить (техническая ошибка).', "Couldn't be assessed (technical error)."),
    '📝 Javob qabul qilindi.': ('📝 Ответ принят.', '📝 Answer received.'),
    '🗣 <b>Savol ': ('🗣 <b>Вопрос ', '🗣 <b>Question '),
    "✅ <b>Part 1 tugadi!</b>\n\n🎬 <b>PART 2</b> — Cue Card\n⏱ O'zingizni tayyorlang va so'ng 1-2 daqiqa gapirib, ovozli xabar sifatida yuboring.": ('✅ <b>Part 1 завершена!</b>\n\n🎬 <b>PART 2</b> — Cue Card\n⏱ Подготовьтесь, затем говорите 1–2 минуты и отправьте голосовое сообщение.', '✅ <b>Part 1 complete!</b>\n\n🎬 <b>PART 2</b> — Cue Card\n⏱ Take a moment to prepare, then speak for 1–2 minutes and send a voice message.'),
    '🎴 <b>Cue Card:</b>\n': ('🎴 <b>Cue Card:</b>\n', None),
    '✅ <b>Part 2 tugadi!</b>\n\n💭 <b>PART 3</b> — Discussion': ('✅ <b>Part 2 завершена!</b>\n\n💭 <b>PART 3</b> — Discussion', '✅ <b>Part 2 complete!</b>\n\n💭 <b>PART 3</b> — Discussion'),
    '✅ <b>Part 3 tugadi!</b>\n\n📊 Yakuniy natija tayyorlanmoqda, biroz kuting...': ('✅ <b>Part 3 завершена!</b>\n\n📊 Готовлю итоговый результат, подождите...', '✅ <b>Part 3 complete!</b>\n\n📊 Preparing your final result, please wait...'),
    '❌ Yakuniy hisobotni tayyorlashda xatolik yuz berdi.': ('❌ Не удалось подготовить итоговый отчёт.', "❌ Couldn't prepare the final report."),
    '🏁 <b>IELTS SPEAKING — YAKUNIY NATIJA</b>\n\n🗣 Fluency & Coherence: <b>': ('🏁 <b>IELTS SPEAKING — ИТОГОВЫЙ РЕЗУЛЬТАТ</b>\n\n🗣 Fluency & Coherence: <b>', '🏁 <b>IELTS SPEAKING — FINAL RESULT</b>\n\n🗣 Fluency & Coherence: <b>'),
    '</b>\n\n💬 <b>Examiner fikri:</b>\n': ('</b>\n\n💬 <b>Мнение экзаменатора:</b>\n', "</b>\n\n💬 <b>Examiner's feedback:</b>\n"),
    "🔁 Yangi test uchun /test buyrug'ini yuboring.": ('🔁 Для нового теста отправьте команду /test.', '🔁 Send /test to start a new test.'),
    '🎙 Iltimos, javobingizni faqat <b>ovozli xabar (voice)</b> shaklida yuboring.': ('🎙 Пожалуйста, отправьте ответ именно <b>голосовым сообщением (voice)</b>.', '🎙 Please send your answer as a <b>voice message</b>.'),
    '❓ Bunday buyruqni tanimadim. Lekin menga oddiygina yozsangiz — suhbatlashishga tayyorman!': ('❓ Такую команду я не знаю. Но если просто напишете мне — с радостью пообщаюсь!', "❓ I don't recognise that command. But if you simply write to me, I'm happy to chat!"),
    "❌ Javob berishda xatolik yuz berdi. Iltimos, qayta urinib ko'ring.": ('❌ Не удалось ответить. Пожалуйста, попробуйте ещё раз.', "❌ Couldn't reply. Please try again."),
    '🎙 <i>Siz aytdingiz:</i> ': ('🎙 <i>Вы сказали:</i> ', None),
    "ℹ️ Menga oddiy matn yoki ovozli xabar yuborishingiz mumkin — suhbatlashamiz. IELTS testi uchun /test, to'xtatish uchun /stop buyrug'ini yuboring.": ('ℹ️ Можете прислать мне обычный текст или голосовое сообщение — поболтаем. Для теста IELTS — /test, чтобы остановить — /stop.', "ℹ️ You can send me plain text or a voice message — let's chat. For an IELTS test use /test, to stop use /stop."),
    'Asosiy menyu (Ilm / Ingliz tili)': ('Главное меню (Наука / Английский)', 'Main menu (Science / English)'),
    'Boshlash va barcha imkoniyatlar': ('Начать и все возможности', 'Start and all features'),
    'Istalgan fandan viktorina': ('Викторина по любой науке', 'A quiz on any subject'),
    'Miya jumboqlari va topishmoqlar': ('Головоломки и загадки', 'Brain teasers and riddles'),
    "Men o'ylagan narsani top (o'yin)": ('Я угадаю, что вы загадали', "I'll guess what you're thinking of"),
    "Interaktiv hikoya — yo'lni o'zingiz tanlang": ('Интерактивная история — выбирайте путь сами', 'Interactive story — choose your own path'),
    "3 ta gapdan yolg'onini toping": ('Найдите ложь среди 3 фактов', 'Spot the lie among 3 statements'),
    "Nima bo'ladi? Fan jumboqlari": ('Что будет? Научные загадки', 'What happens? Science riddles'),
    'Bugungi fan, fakt va ingliz tili maslahati': ('Тема дня, факт и совет по английскому', "Today's subject, fact and English tip"),
    'IELTS Writing tahlili (': ('Разбор IELTS Writing (', 'IELTS Writing review ('),
    'Ingliz grammatikasi: dars va mashqlar': ('Грамматика английского: уроки и упражнения', 'English grammar: lessons and exercises'),
    'Ingliz tilida suhbat (xatolarni tuzatadi)': ('Разговор на английском (исправляю ошибки)', 'Chat in English (I correct your mistakes)'),
    '25 savolli sertifikat testi': ('Тест из 25 вопросов с сертификатом', '25-question certificate test'),
    'TOP 10 reyting jadvallari': ('ТОП 10: таблицы лидеров', 'TOP 10 leaderboards'),
    'IELTS Speaking mock testi': ('Пробный тест IELTS Speaking', 'IELTS Speaking mock test'),
    'IELTS Reading testi': ('Тест IELTS Reading', 'IELTS Reading test'),
    'IELTS Listening testi': ('Тест IELTS Listening', 'IELTS Listening test'),
    "Profil va o'quv materiallari": ('Профиль и учебные материалы', 'Profile and study materials'),
    "Kunlik xabarni yoqish/o'chirish": ('Включить/выключить ежедневные сообщения', 'Turn daily messages on/off'),
    "Foydalanuvchilar ro'yxati (faqat egasi)": ('Список пользователей (только владелец)', 'User list (owner only)'),
    'Bot statistikasi': ('Статистика бота', 'Bot statistics'),
    "Joriy jarayonni to'xtatish": ('Остановить текущий процесс', 'Stop the current process'),
    '✅ Ha': ('✅ Да', '✅ Yes'),
    "❌ Yo'q": ('❌ Нет', '❌ No'),
    "🎯 Ha, to'g'ri!": ('🎯 Да, верно!', "🎯 Yes, that's right!"),
    "✅ <b>To'g'ri!</b>": ('✅ <b>Верно!</b>', '✅ <b>Correct!</b>'),
    "✅ To'g'ri!": ('✅ Верно!', '✅ Correct!'),
    "🎉 To'g'ri!": ('🎉 Верно!', '🎉 Correct!'),
    "To'g'ri! 🎉": ('Верно! 🎉', 'Correct! 🎉'),
    ' savol)': (' вопросов)', ' questions)'),
    "  — bo'sh —": ('  — пусто —', '  — empty —'),
    'Writing tahlili': ('Разбор Writing', 'Writing review'),
}

_frag_rx: dict = {}


def _compile_fragments(lang: str):
    idx = 0 if lang == "ru" else 1
    items = []
    for key, vals in I18N_FRAGMENTS.items():
        val = vals[idx]
        if val is not None:
            items.append((key, val))
    items.sort(key=lambda kv: len(kv[0]), reverse=True)
    parts, mapping = [], {}
    for key, val in items:
        pat = re.escape(key)
        if key[0].isalnum() or key[0] == "_":
            pat = r"(?<!\w)" + pat
        if key[-1].isalnum() or key[-1] == "_":
            pat = pat + r"(?!\w)"
        parts.append(pat)
        mapping[key] = val
    return re.compile("|".join(parts)), mapping


def translate_text(text: str, lang: str) -> str:
    """O'zbekcha statik bo'laklarni lang (ru/en) tiliga almashtiradi."""
    if not text or lang not in ("ru", "en"):
        return text
    if lang not in _frag_rx:
        _frag_rx[lang] = _compile_fragments(lang)
    rx, mapping = _frag_rx[lang]
    return rx.sub(lambda m: mapping.get(m.group(0), m.group(0)), text)


def _translate_markup(markup, lang: str):
    """InlineKeyboardMarkup ichidagi tugma yozuvlarini o'giradi."""
    if not isinstance(markup, InlineKeyboardMarkup):
        return markup
    new_rows = []
    changed = False
    for row in markup.inline_keyboard:
        new_row = []
        for btn in row:
            t = translate_text(btn.text, lang)
            if t != btn.text:
                btn = btn.model_copy(update={"text": t})
                changed = True
            new_row.append(btn)
        new_rows.append(new_row)
    return markup.model_copy(update={"inline_keyboard": new_rows}) if changed else markup


async def _outgoing_lang(method) -> str:
    chat_id = getattr(method, "chat_id", None)
    if isinstance(chat_id, int) and chat_id > 0:
        return (await get_lang(chat_id)) or UNSET_LANG
    return current_lang.get()


class TranslateOutgoingMiddleware(BaseRequestMiddleware):
    """Telegram'ga ketayotgan har bir so'rovdagi matn, sarlavha va tugmalarni
    foydalanuvchi tiliga o'giradi (o'zbek tili uchun hech narsa o'zgarmaydi)."""

    async def __call__(self, make_request, bot, method):
        try:
            lang = await _outgoing_lang(method)
            if lang in ("ru", "en"):
                for field in ("text", "caption", "title", "description", "error_message"):
                    val = getattr(method, field, None)
                    if isinstance(val, str):
                        setattr(method, field, translate_text(val, lang))
                markup = getattr(method, "reply_markup", None)
                if markup is not None:
                    setattr(method, "reply_markup", _translate_markup(markup, lang))
                prices = getattr(method, "prices", None)
                if prices:
                    setattr(method, "prices", [p.model_copy(update={"label": translate_text(p.label, lang)}) for p in prices])
        except Exception as e:
            logger.warning("Tarjima qatlamida xatolik: %s", e)
        return await make_request(bot, method)


bot.session.middleware(TranslateOutgoingMiddleware())


# --- Majburiy obuna tekshiruvi -------------------------------------------

CHECK_SUB_CALLBACK = "checksub"
_SUBSCRIBED_STATUSES = {"creator", "administrator", "member"}


async def is_user_subscribed(user_id: int) -> bool:
    """Foydalanuvchi REQUIRED_CHANNEL a'zosimi, tekshiradi. Agar tekshirib bo'lmasa
    (masalan bot kanalda admin emas), botni butunlay bloklab qo'ymaslik uchun
    True qaytaradi va xatolikni logga yozadi."""
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL, user_id=user_id)
        status = getattr(member.status, "value", member.status)
        return status in _SUBSCRIBED_STATUSES
    except Exception as e:
        logger.warning(
            "Obunani tekshirib bo'lmadi (bot kanalda admin ekanini va "
            "REQUIRED_CHANNEL to'g'riligini tekshiring): %s", e
        )
        return True


def _subscribe_keyboard() -> InlineKeyboardMarkup:
    rows = []
    if REQUIRED_CHANNEL_LINK:
        rows.append([InlineKeyboardButton(text=tr("sub_btn"), url=REQUIRED_CHANNEL_LINK)])
    rows.append([InlineKeyboardButton(text=tr("sub_check"), callback_data=CHECK_SUB_CALLBACK)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _subscribe_text() -> str:
    return tr("sub_text")


class SubscriptionMiddleware(BaseMiddleware):
    """Foydalanuvchi kanalga obuna bo'lmaguncha botning hech bir buyrug'iga
    ruxsat bermaydi — faqat obuna bo'lish va tekshirish tugmalarini ko'rsatadi."""

    async def __call__(self, handler, event, data):
        if not REQUIRED_CHANNEL:
            return await handler(event, data)

        user = getattr(event, "from_user", None)
        if user is None or user.is_bot:
            return await handler(event, data)

        # "Tekshirish" tugmasi o'zining handleriga o'tishi kerak
        if isinstance(event, CallbackQuery) and event.data == CHECK_SUB_CALLBACK:
            return await handler(event, data)

        if await is_user_subscribed(user.id):
            return await handler(event, data)

        if isinstance(event, CallbackQuery):
            await event.answer(tr("sub_alert"), show_alert=True)
            if event.message:
                try:
                    await event.message.answer(_subscribe_text(), reply_markup=_subscribe_keyboard())
                except Exception:
                    pass
        elif isinstance(event, Message):
            await event.answer(_subscribe_text(), reply_markup=_subscribe_keyboard())
        return None


# Har bir xabar va tugma bosilishida (handlerlardan oldin) ishlaydi.
dp.message.outer_middleware(SubscriptionMiddleware())
dp.callback_query.outer_middleware(SubscriptionMiddleware())



@dp.callback_query(F.data == CHECK_SUB_CALLBACK)
async def handle_check_subscription(callback: CallbackQuery):
    if not REQUIRED_CHANNEL or await is_user_subscribed(callback.from_user.id):
        await callback.answer("✅ Rahmat! Obuna tasdiqlandi.", show_alert=False)
        try:
            await callback.message.edit_text(
                "✅ <b>Obuna tasdiqlandi!</b>\n\n"
                "Endi botdan to'liq foydalanishingiz mumkin. Boshlash uchun /start yuboring."
            )
        except Exception:
            pass
    else:
        await callback.answer("❌ Siz hali obuna bo'lmagansiz. Avval kanalga obuna bo'ling.", show_alert=True)


# --------------------------------------------------------------------------
# 4) FSM HOLATLARI
# --------------------------------------------------------------------------

class ExamStates(StatesGroup):
    part1 = State()
    part2 = State()
    part3 = State()


class EnglishPracticeStates(StatesGroup):
    practicing = State()


class AdminContentStates(StatesGroup):
    reading_title = State()
    reading_body = State()
    reading_questions = State()
    listening_title = State()
    listening_audio = State()
    listening_questions = State()
    grammar_title = State()
    grammar_body = State()


# --------------------------------------------------------------------------
# 5) YORDAMCHI FUNKSIYALAR (Groq / gTTS / pydub bilan ishlash — sync,
#    shuning uchun asyncio.to_thread orqali chaqiriladi)
# --------------------------------------------------------------------------

def _user_dir(user_id: int) -> str:
    path = os.path.join(TEMP_DIR, str(user_id))
    os.makedirs(path, exist_ok=True)
    return path


def _generate_questions_sync() -> dict:
    """Groq (Llama 3.3 70B) orqali to'liq IELTS Speaking savollar to'plamini
    JSON formatida generatsiya qiladi."""
    prompt = (
        "You are an official IELTS Speaking examiner. Generate a fresh, realistic "
        "IELTS Speaking Mock Test. Respond ONLY with a valid JSON object, no extra text, "
        "in exactly this structure:\n"
        "{\n"
        f'  "part1": ["question1", "question2", "question3", "question4"],\n'
        '  "part2": {\n'
        '    "topic": "short topic name",\n'
        '    "cue_card": "Describe a ... You should say:\\n- point 1\\n- point 2\\n- point 3\\nand explain ..."\n'
        "  },\n"
        f'  "part3": ["question1", "question2", "question3", "question4"]\n'
        "}\n"
        "Part 1 must be simple everyday questions (introduction/familiar topics). "
        "Part 2 must be a classic IELTS cue card format. "
        "Part 3 questions must be more abstract/analytical and thematically connected to the Part 2 topic. "
        f"Part 1 must have exactly {PART1_QUESTIONS_COUNT} questions, Part 3 exactly {PART3_QUESTIONS_COUNT} questions."
    )
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.9,
        response_format={"type": "json_object"},
    )
    content = completion.choices[0].message.content
    return json.loads(content)


def _transcribe_sync(file_path: str) -> str:
    """Groq Whisper (whisper-large-v3-turbo) orqali ovozni matnga aylantiradi."""
    with open(file_path, "rb") as f:
        transcription = groq_client.audio.transcriptions.create(
            file=(os.path.basename(file_path), f.read()),
            model=STT_MODEL,
            language="en",
            response_format="json",
        )
    return (transcription.text or "").strip()


def _evaluate_answer_sync(question: str, transcript: str) -> dict:
    """Bitta javobni IELTS mezonlari bo'yicha (1-9 band) tezkor baholaydi."""
    prompt = (
        "You are an official IELTS Speaking examiner. Evaluate the candidate's spoken "
        "answer below strictly according to IELTS band descriptors (1-9 scale, half "
        "bands allowed, e.g. 6.5). Respond ONLY with a valid JSON object exactly like:\n"
        "{\n"
        '  "fluency_coherence": 0.0,\n'
        '  "lexical_resource": 0.0,\n'
        '  "grammar_range_accuracy": 0.0,\n'
        '  "pronunciation_estimate": 0.0,\n'
        '  "short_comment": "one short sentence of feedback in English"\n'
        "}\n\n"
        f"Question: {question}\n"
        f"Candidate's transcribed answer: {transcript if transcript else '(no speech detected / empty answer)'}"
    )
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        response_format={"type": "json_object"},
    )
    return json.loads(completion.choices[0].message.content)


def _generate_final_report_sync(answers: list) -> dict:
    """Barcha javoblar va ballar asosida yakuniy Examiner hisobotini yozadi."""
    summary_lines = []
    for i, a in enumerate(answers, start=1):
        summary_lines.append(
            f"{i}. [{a['part']}] Q: {a['question']}\n"
            f"   Answer: {a['transcript'][:300]}\n"
            f"   Scores: {a['scores']}"
        )
    joined = "\n".join(summary_lines)

    prompt = (
        "You are an official IELTS Speaking examiner writing the candidate's final "
        "report after a full mock test (Part 1, Part 2, Part 3). Below is the full "
        "transcript of the test with per-question band scores already calculated. "
        "Write a warm but honest, professional final feedback as plain text (not JSON), "
        "addressed directly to the candidate, covering:\n"
        "- Fluency & Coherence\n"
        "- Lexical Resource\n"
        "- Grammatical Range & Accuracy\n"
        "- Pronunciation\n"
        "- Overall Band Score (state a single number, e.g. 6.5)\n"
        "- 2-3 concrete tips for improvement\n"
        "Keep it concise (150-220 words), speak as a real examiner would, in English.\n\n"
        f"TEST TRANSCRIPT AND SCORES:\n{joined}"
    )
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5,
    )
    feedback_text = completion.choices[0].message.content.strip()

    # Lokal average band hisoblash (raqamli ko'rsatkich sifatida ham beramiz)
    criteria = ["fluency_coherence", "lexical_resource", "grammar_range_accuracy", "pronunciation_estimate"]
    totals = {c: [] for c in criteria}
    for a in answers:
        for c in criteria:
            val = a["scores"].get(c)
            if isinstance(val, (int, float)):
                totals[c].append(val)
    averages = {c: (round(sum(v) / len(v) * 2) / 2 if v else 0.0) for c, v in totals.items()}
    overall = round(sum(averages.values()) / len(averages) * 2) / 2 if averages else 0.0

    return {"text": feedback_text, "averages": averages, "overall_band": overall}


# --- Universal viktorina (istalgan fan bo'yicha) -------------------------

# Har bir foydalanuvchining hozirgi faol viktorina sessiyasini saqlab turadi.
# user_id -> {"questions": [...], "idx": int, "score": int, "mode": str, "subject": str}
active_quizzes: dict[int, dict] = {}

QUIZ_LENGTH = 10
CERTIFICATE_QUIZ_LENGTH = 25
CERTIFICATE_PASS_PERCENT = 70  # sertifikat olish uchun kerakli minimal foiz
MAX_QUESTIONS_PER_LLM_CALL = 10  # bitta so'rovda ishonchli generatsiya qilinadigan maksimal savol soni

# (emoji, nomi, LLM uchun mavzu tavsifi)
SUBJECTS = [
    ("🌍", "Geografiya", "geografiya: mamlakatlar, shaharlar, tabiat hodisalari, rekordlar"),
    ("🔬", "Fizika", "fizika: kundalik hayotdagi hodisalar, mashhur olimlar, kashfiyotlar"),
    ("🧪", "Kimyo", "kimyo: elementlar, moddalar, qiziqarli reaksiyalar"),
    ("🧬", "Biologiya", "biologiya: hayvonlar, o'simliklar, inson tanasi"),
    ("➗", "Matematika", "matematika: mantiqiy masalalar, qiziqarli faktlar, oddiy hisoblar"),
    ("💻", "Texnologiya va IT", "texnologiya: internet, dasturlash, sun'iy intellekt, gadjetlar"),
    ("📚", "Adabiyot va madaniyat", "adabiyot, mashhur yozuvchilar va asarlar, madaniyat, an'analar"),
    ("🎨", "San'at va musiqa", "san'at, rassomlar, musiqa, kino"),
    ("🚀", "Koinot", "astronomiya va koinot: sayyoralar, yulduzlar, kosmik missiyalar"),
    ("💰", "Iqtisodiyot va moliya", "iqtisodiyot, pul, biznes, shaxsiy moliya asoslari"),
    ("🧠", "Psixologiya", "psixologiya: xotira, e'tibor, odatlar, qiziqarli tajribalar"),
    ("⚽", "Sport", "sport: tarmoqlar, rekordlar, mashhur sportchilar, qoidalar"),
    ("🥗", "Salomatlik va ovqatlanish", "salomatlik, ovqatlanish, vitaminlar, uyqu va sog'lom odatlar"),
    ("🌐", "Ingliz tili", "ingliz tili: grammatika, lug'at, frazal fe'llar, idiomalar"),
]
SUBJECT_BY_NAME = {name: (emoji, desc) for emoji, name, desc in SUBJECTS}


def _generate_quiz_batch_sync(count: int, subject_name: str | None = None) -> list:
    """Groq (LLM) orqali `count` ta ko'p variantli (ABCD) savolni bitta so'rovda
    JSON ko'rinishida generatsiya qiladi. `subject_name` berilsa shu fandan,
    berilmasa aralash (har safar tasodifiy tanlangan bir nechta fandan) bo'ladi.
    `count` MAX_QUESTIONS_PER_LLM_CALL dan oshmasligi kerak (ishonchlilik uchun)."""
    import random

    if subject_name and subject_name in SUBJECT_BY_NAME:
        scope = f"Fan: {SUBJECT_BY_NAME[subject_name][1]}."
        is_english = subject_name == "Ingliz tili"
    else:
        picked = random.sample(SUBJECTS, k=min(4, len(SUBJECTS)))
        scope = "Quyidagi fanlardan aralash savollar tuzing: " + "; ".join(p[2] for p in picked) + "."
        is_english = False

    language_rule = (
        "Savollar va variantlar INGLIZ tilida bo'lsin, izoh (explanation) esa o'zbek tilida."
        if is_english
        else "Savol, variantlar va izoh o'zbek tilida bo'lsin."
    )

    prompt = (
        "Siz qiziqarli, zeriktirmaydigan viktorina (trivia) savollari tuzuvchi mutaxassissiz. "
        f"{scope} Bir-biriga o'xshamaydigan, xilma-xil {count} ta savol tuzing. "
        "Savollar qiziqarli, hayotiy va aniq bir to'g'ri javobli bo'lsin; juda quruq yoki "
        "bahsli savollardan qoching. Tarix fanidan savol bermang. "
        f"{language_rule} "
        "Javobni FAQAT quyidagi JSON formatida qaytaring, boshqa hech qanday matn qo'shmang:\n"
        "{\n"
        '  "questions": [\n'
        "    {\n"
        '      "question": "savol matni",\n'
        '      "options": ["variant A", "variant B", "variant C", "variant D"],\n'
        '      "correct_index": 0,\n'
        '      "explanation": "to\'g\'ri javobning qisqa (1-2 gapli) izohi"\n'
        "    }\n"
        f"    // ... jami {count} ta shunday obyekt\n"
        "  ]\n"
        "}\n"
        "Har bir savolning options ro'yxatida aniq 4 ta variant, correct_index esa 0-3 "
        "oralig'idagi (options ro'yxatidagi to'g'ri variant indeksi) butun son bo'lsin."
    )
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=1.0,
        response_format={"type": "json_object"},
    )
    data = json.loads(completion.choices[0].message.content)
    return _filter_valid_questions(data.get("questions", []))[:count]


def _filter_valid_questions(questions: list) -> list:
    """Faqat to'g'ri formatdagi savollarni qoldiradi (LLM xato format bersa ham bot yiqilmasin)."""
    valid = []
    for q in questions or []:
        if not isinstance(q, dict):
            continue
        options = q.get("options")
        correct_index = q.get("correct_index")
        if (
            isinstance(options, list)
            and len(options) >= 2
            and isinstance(correct_index, int)
            and 0 <= correct_index < len(options)
            and q.get("question")
        ):
            valid.append(q)
    return valid


def _generate_quiz_set_sync(count: int, subject_name: str | None = None) -> list:
    """Kerakli umumiy savol sonini MAX_QUESTIONS_PER_LLM_CALL dan oshmaydigan
    bo'laklarga bo'lib, bir necha marta LLM'ga murojaat qilib yig'adi. Bu
    katta (masalan 25 ta) savol to'plamini so'rasak ham JSON kesilib qolish
    yoki formatning buzilish xavfini kamaytiradi."""
    all_questions = []
    remaining = count
    attempts = 0
    max_attempts = (count // MAX_QUESTIONS_PER_LLM_CALL) + 3
    while remaining > 0 and attempts < max_attempts:
        attempts += 1
        batch_size = min(remaining, MAX_QUESTIONS_PER_LLM_CALL)
        try:
            batch = _generate_quiz_batch_sync(batch_size, subject_name)
        except Exception as e:
            logger.exception("Savol partiyasini generatsiya qilishda xatolik: %s", e)
            batch = []
        all_questions.extend(batch)
        remaining = count - len(all_questions)
    return all_questions[:count]


# --- Authentik IELTS Reading/Listening testlari (AI generatsiya) --------

READING_TEST_QUESTIONS_COUNT = 6
LISTENING_TEST_QUESTIONS_COUNT = 5

READING_TOPICS = [
    "climate change and the environment",
    "the history of a scientific discovery",
    "urbanization and city planning",
    "animal behavior and wildlife conservation",
    "the impact of technology on society",
    "ancient civilizations and archaeology",
    "renewable energy sources",
    "psychology and human behavior",
    "space exploration",
    "economic globalization",
]

LISTENING_TOPICS = [
    "a conversation about booking accommodation",
    "a university lecture about a scientific topic",
    "a conversation between a student and a course advisor",
    "a guided tour description of a museum or landmark",
    "a phone conversation about arranging an event",
    "a workplace conversation about a project",
    "a conversation about renting an apartment",
    "an informational talk about a community service",
]


def _generate_reading_test_sync() -> dict:
    """Groq (LLM) orqali IELTS Academic Reading uslubidagi original passage
    va unga oid ko'p variantli (ABCD) savollarni generatsiya qiladi."""
    import random
    topic = random.choice(READING_TOPICS)
    prompt = (
        "You are an IELTS test material writer. Write an original IELTS Academic Reading "
        f"passage (300-400 words) about {topic}, in a formal academic style similar to real "
        f"IELTS Reading passages. Then write {READING_TEST_QUESTIONS_COUNT} multiple-choice "
        "comprehension questions about the passage.\n\n"
        "Respond ONLY with valid JSON in this exact format, no extra text:\n"
        "{\n"
        '  "passage": "the full passage text",\n'
        '  "questions": [\n'
        "    {\n"
        '      "question": "question text",\n'
        '      "options": ["A", "B", "C", "D"],\n'
        '      "correct_index": 0,\n'
        '      "explanation": "brief explanation of the correct answer"\n'
        "    }\n"
        f"    // ... total {READING_TEST_QUESTIONS_COUNT} questions\n"
        "  ]\n"
        "}"
    )
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.9,
        response_format={"type": "json_object"},
    )
    data = json.loads(completion.choices[0].message.content)
    questions = [
        q for q in data.get("questions", [])
        if isinstance(q.get("options"), list) and len(q["options"]) >= 2
        and isinstance(q.get("correct_index"), int) and 0 <= q["correct_index"] < len(q["options"])
        and q.get("question")
    ]
    return {"passage": data.get("passage", ""), "questions": questions[:READING_TEST_QUESTIONS_COUNT]}


def _generate_listening_test_sync() -> dict:
    """Groq (LLM) orqali IELTS Listening uslubidagi original skript (dialog/monolog)
    va unga oid ko'p variantli (ABCD) savollarni generatsiya qiladi."""
    import random
    topic = random.choice(LISTENING_TOPICS)
    prompt = (
        "You are an IELTS test material writer. Write an original IELTS Listening script "
        f"(150-250 words) about {topic}, written as natural spoken dialogue or narration "
        f"suitable to be read aloud. Then write {LISTENING_TEST_QUESTIONS_COUNT} multiple-choice "
        "comprehension questions about the script content.\n\n"
        "Respond ONLY with valid JSON in this exact format, no extra text:\n"
        "{\n"
        '  "script": "the full spoken script text, plain narration without speaker labels",\n'
        '  "questions": [\n'
        "    {\n"
        '      "question": "question text",\n'
        '      "options": ["A", "B", "C", "D"],\n'
        '      "correct_index": 0,\n'
        '      "explanation": "brief explanation of the correct answer"\n'
        "    }\n"
        f"    // ... total {LISTENING_TEST_QUESTIONS_COUNT} questions\n"
        "  ]\n"
        "}"
    )
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.9,
        response_format={"type": "json_object"},
    )
    data = json.loads(completion.choices[0].message.content)
    questions = [
        q for q in data.get("questions", [])
        if isinstance(q.get("options"), list) and len(q["options"]) >= 2
        and isinstance(q.get("correct_index"), int) and 0 <= q["correct_index"] < len(q["options"])
        and q.get("question")
    ]
    return {"script": data.get("script", ""), "questions": questions[:LISTENING_TEST_QUESTIONS_COUNT]}


def _build_quiz_question_view(quiz: dict, idx: int):
    """Berilgan indeksdagi savol uchun matn va inline tugmalarni tayyorlaydi."""
    q = quiz["questions"][idx]
    total = len(quiz["questions"])
    letters = ["A", "B", "C", "D", "E", "F"]
    buttons = [
        [InlineKeyboardButton(text=f"{letters[i]}) {opt}", callback_data=f"qans:{i}")]
        for i, opt in enumerate(q["options"])
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    mode_labels = {
        "certificate": "🎓 Sertifikat testi",
        "quiz": quiz.get("subject") or "🧩 Viktorina",
        "daily": "📅 Kunlik test",
        "reading": "📖 Reading testi",
        "listening": "🎧 Listening testi",
    }
    mode_label = mode_labels.get(quiz.get("mode"), "🧩 Savol")
    text = f"{mode_label} — <b>{idx + 1}/{total}:</b>\n\n{q['question']}"
    return text, keyboard


# --- Sertifikat rasmi (Pillow orqali) -----------------------------------

# Dockerfile'da o'rnatilgan "fonts-dejavu-core" paketi shu manzilga shrift qo'yadi.
FONT_BOLD_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
FONT_REGULAR_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"


def _load_font(path: str, size: int):
    """Shrift faylini yuklaydi; agar topilmasa (masalan lokal Windows/Mac muhitida
    ishga tushirilsa), PIL'ning standart shriftiga o'tadi — bot yiqilib qolmaydi."""
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        return ImageFont.load_default()


CERT_TEXTS = {
    "uz": {
        "title": "SERTIFIKAT",
        "subtitle": "Bilim va yutuqlar sertifikati",
        "awarded": "Ushbu sertifikat quyidagi shaxsga topshiriladi:",
        "line1": "IELTS Speaking AI Examiner Bot tomonidan tashkil etilgan {total} savolli",
        "line2": "umumiy bilim sertifikat testidan muvaffaqiyatli o'tgani uchun.",
        "result": "Natija: {score}/{total} ({pct}%)",
        "date": "Sana: {date}",
    },
    "ru": {
        "title": "СЕРТИФИКАТ",
        "subtitle": "Сертификат знаний и достижений",
        "awarded": "Настоящий сертификат вручается:",
        "line1": "За успешное прохождение теста общих знаний из {total} вопросов,",
        "line2": "организованного ботом IELTS Speaking AI Examiner.",
        "result": "Результат: {score}/{total} ({pct}%)",
        "date": "Дата: {date}",
    },
    "en": {
        "title": "CERTIFICATE",
        "subtitle": "Certificate of Knowledge and Achievement",
        "awarded": "This certificate is proudly awarded to:",
        "line1": "For successfully completing the {total}-question general knowledge test",
        "line2": "organised by the IELTS Speaking AI Examiner Bot.",
        "result": "Score: {score}/{total} ({pct}%)",
        "date": "Date: {date}",
    },
}


def _generate_certificate_image_sync(full_name: str, score: int, total: int, percentage: int) -> str:
    """Pillow yordamida chiroyli sertifikat rasmini (PNG) yaratadi va fayl
    yo'lini qaytaradi."""
    width, height = 1400, 990
    bg_color = (253, 249, 240)
    border_gold = (191, 155, 48)
    text_dark = (40, 40, 60)
    text_gray = (110, 110, 120)

    img = Image.new("RGB", (width, height), bg_color)
    draw = ImageDraw.Draw(img)

    # Tashqi va ichki dekorativ ramka
    draw.rectangle([30, 30, width - 30, height - 30], outline=border_gold, width=8)
    draw.rectangle([55, 55, width - 55, height - 55], outline=border_gold, width=2)

    font_title = _load_font(FONT_BOLD_PATH, 74)
    font_subtitle = _load_font(FONT_REGULAR_PATH, 30)
    font_name = _load_font(FONT_BOLD_PATH, 52)
    font_body = _load_font(FONT_REGULAR_PATH, 30)
    font_score = _load_font(FONT_BOLD_PATH, 40)
    font_footer = _load_font(FONT_REGULAR_PATH, 22)

    def center_text(y, text, font, fill):
        bbox = draw.textbbox((0, 0), text, font=font)
        text_width = bbox[2] - bbox[0]
        draw.text(((width - text_width) / 2, y), text, font=font, fill=fill)

    lang = current_lang.get()
    ct = CERT_TEXTS.get(lang, CERT_TEXTS["uz"])

    center_text(120, ct["title"], font_title, border_gold)
    center_text(215, ct["subtitle"], font_subtitle, text_gray)

    draw.line([(300, 290), (width - 300, 290)], fill=border_gold, width=2)

    center_text(350, ct["awarded"], font_body, text_dark)
    center_text(410, full_name, font_name, text_dark)

    draw.line([(400, 500), (width - 400, 500)], fill=border_gold, width=1)

    center_text(560, ct["line1"].format(total=total), font_body, text_dark)
    center_text(605, ct["line2"], font_body, text_dark)

    center_text(690, ct["result"].format(score=score, total=total, pct=percentage), font_score, border_gold)

    date_str = datetime.now().strftime("%d.%m.%Y")
    center_text(770, ct["date"].format(date=date_str), font_body, text_gray)

    center_text(height - 90, "IELTS Speaking AI Examiner Bot", font_footer, text_gray)

    out_path = os.path.join(TEMP_DIR, f"certificate_{secrets.token_hex(6)}.png")
    img.save(out_path, "PNG")
    return out_path


# --- Oddiy erkin suhbat (Free Chat) -------------------------------------

CHAT_HISTORY_LIMIT = 20  # oxirgi N ta xabar (foydalanuvchi+bot) kontekstda saqlanadi

CHAT_SYSTEM_PROMPT = (
    "Siz aqlli, samimiy va quvnoq universal yordamchisiz: istalgan mavzuda (fan, hayot, "
    "o'qish, ish, texnologiya, ingliz tili va boshqalar) odam bilan gaplashasiz. "
    "Foydalanuvchi qaysi tilda yozsa, o'sha tilda javob bering (odatda o'zbek tilida). "
    "Javoblar qisqa, aniq va tabiiy bo'lsin, zerikarli yoki rasmiy dars o'qiyotgandek emas — "
    "do'stona, ba'zan yengil hazil va mos emoji bilan. Keraksiz cho'zmang, lekin savolga "
    "to'liq va foydali javob bering. Aniq bilmagan narsangizni to'qimang, shuni ochiq ayting."
)


def _generate_chat_reply_sync(history: list) -> str:
    """Groq (LLM) orqali erkin suhbat uchun tabiiy javob generatsiya qiladi.
    `history` — {"role": "user"/"assistant", "content": str} lar ro'yxati."""
    messages = [{"role": "system", "content": CHAT_SYSTEM_PROMPT}] + history
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=messages,
        temperature=0.8,
    )
    return completion.choices[0].message.content.strip()


# --- Ingliz tili mashq rejimi (English Practice) ------------------------

ENGLISH_PRACTICE_HISTORY_LIMIT = 20

ENGLISH_PRACTICE_SYSTEM_PROMPT = (
    "You are a warm, encouraging English conversation tutor helping an Uzbek-speaking "
    "student practice English. ALWAYS reply in English only, regardless of what language "
    "the student writes in.\n\n"
    "For every message the student sends, follow this structure:\n"
    "1. If their message contains any grammar, vocabulary, spelling, or word-choice "
    "mistakes, start your reply with a line '📝 Correction: <the corrected sentence>', "
    "then one short line explaining the fix in simple terms, e.g. "
    "'💡 Tip: we use \"have been\" for actions that started in the past and continue now.'\n"
    "2. If their message has no mistakes, start with a short, genuine compliment instead "
    "(e.g. 'Great sentence! 👍').\n"
    "3. After the correction/compliment, continue the conversation naturally in English — "
    "react to what they said and ask a friendly follow-up question to keep them talking.\n\n"
    "Keep your whole reply concise (3-5 sentences total), friendly, and never condescending. "
    "Use simple, encouraging language appropriate for an intermediate English learner."
)


def _generate_english_practice_reply_sync(history: list) -> str:
    """Groq (LLM) orqali ingliz tili mashq rejimi uchun javob generatsiya qiladi:
    xatolarni tuzatadi va suhbatni ingliz tilida davom ettiradi."""
    messages = [{"role": "system", "content": ENGLISH_PRACTICE_SYSTEM_PROMPT}] + history
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=messages,
        temperature=0.7,
    )
    return completion.choices[0].message.content.strip()


def _text_to_speech_ogg_sync(text: str, out_path_ogg: str) -> str:
    """gTTS bilan matnni ovozga aylantiradi va Telegram voice uchun
    ogg/opus formatiga konvertatsiya qiladi."""
    mp3_path = out_path_ogg.replace(".ogg", ".mp3")
    tts = gTTS(text=text, lang="en")
    tts.save(mp3_path)
    audio = AudioSegment.from_mp3(mp3_path)
    audio.export(out_path_ogg, format="ogg", codec="libopus", bitrate="64k")
    try:
        os.remove(mp3_path)
    except OSError:
        pass
    return out_path_ogg


async def send_text_and_voice(message: Message, text: str, tts_text: str | None = None):
    """Berilgan matnni yuboradi va uni ovozga aylantirib voice sifatida ham jo'natadi."""
    await message.answer(text)
    speak_text = tts_text if tts_text is not None else text
    # gTTS uchun juda uzun matnni qisqartiramiz (limitlarga tushib qolmaslik uchun)
    speak_text = speak_text[:1500]
    user_dir = _user_dir(message.from_user.id)
    ogg_path = os.path.join(user_dir, f"tts_{datetime.now().timestamp()}.ogg")
    try:
        await asyncio.to_thread(_text_to_speech_ogg_sync, speak_text, ogg_path)
        await message.answer_voice(FSInputFile(ogg_path))
    except Exception as e:
        logger.exception("TTS yuborishda xatolik: %s", e)
    finally:
        if os.path.exists(ogg_path):
            try:
                os.remove(ogg_path)
            except OSError:
                pass


def cleanup_user_dir(user_id: int):
    path = os.path.join(TEMP_DIR, str(user_id))
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)


# --------------------------------------------------------------------------
# 6) HANDLERLAR
# --------------------------------------------------------------------------

WELCOME_TEXT = (
    "👋 <b>Salom! Men universal yordamchi botman.</b>\n\n"
    "Menga istalgan narsani yozing yoki ovozli xabar yuboring — savol bering, "
    "maslahat so'rang, shunchaki gaplashing. Buyruq shart emas 💬\n\n"
    "🧩 <b>Bilim va o'yin</b>\n"
    "/quiz — istalgan fandan viktorina (fanni o'zingiz tanlaysiz)\n"
    "/teaser — miya jumboqlari va topishmoqlar\n"
    "🔮 /guess — siz o'ylagan narsani men topaman!\n"
    "/daily — bugungi fan, qiziqarli fakt va test\n"
    "/story — interaktiv hikoya: tanlovingiz yakunni o'zgartiradi 🎭\n"
    "/lie — 3 ta gapdan yolg'onini toping 🕵️\n"
    "/whatif — \"Nima bo'ladi?\" fan jumboqlari: taxmin qiling 🔬\n"
    "/certificate — 25 savolli test, natija yaxshi bo'lsa sertifikat\n"
    "/leaderboard — eng yaxshilar jadvali\n\n"
    "🇬🇧 <b>Ingliz tili va IELTS</b>\n"
    "/english — ingliz tilida suhbat, xatolaringizni tuzatib boraman\n"
    "/grammar — grammatika: dars + mashqlar 📝\n"
    f"/writing — 💎 IELTS Writing tahlili ({WRITING_PRICE_STARS} ⭐)\n"
    f"/speaking — 🎙 IELTS Speaking to'liq mock ({SPEAKING_PRICE_STARS} ⭐)\n"
    f"/pro — ⭐ PRO obuna: Writing va Speaking tekin ({PRO_PRICE_STARS} ⭐ / {PRO_DAYS} kun)\n"
    "/test — IELTS Speaking mock testi (ovozli javoblar)\n"
    "/reading_test, /listening_test — IELTS uslubidagi testlar\n"
    "/dashboard — profil va o'quv materiallari\n\n"
    "🔔 Har kuni ertalab yangi fan va ingliz tili maslahati yuboraman — o'chirish: /notify\n"
    "🛑 Jarayonni to'xtatish: /stop"
)


# --- Asosiy menyu: 🔬 Ilm / 🇬🇧 Ingliz tili -------------------------------

# (emoji, buyruq) — matnlar MENU_LABELS orqali tanlangan tilda olinadi
MENU_SCIENCE = [
    ("🧩", "quiz"), ("🧠", "teaser"), ("🔬", "whatif"), ("🕵️", "lie"), ("🔮", "guess"),
    ("📖", "story"), ("📅", "daily"), ("🎓", "certificate"), ("🏆", "leaderboard"),
]
MENU_ENGLISH = [
    ("💬", "english"), ("📝", "grammar"), ("⭐", "pro"), ("💎", "writing"), ("🎙", "speaking"), ("🎤", "test"),
    ("📖", "reading_test"), ("🎧", "listening_test"), ("📚", "dashboard"),
]

ALL_COMMANDS = {
    "uz": WELCOME_TEXT,
    "ru": (
        "👋 <b>Привет! Я универсальный помощник.</b>\n\n"
        "Пишите что угодно или отправляйте голосовые — задавайте вопросы, просите совета, просто общайтесь 💬\n\n"
        "🧩 <b>Знания и игры</b>\n"
        "/quiz — викторина по любой науке\n"
        "/teaser — головоломки и загадки\n"
        "🔮 /guess — я угадаю, что вы загадали!\n"
        "/daily — факт, тест и совет дня\n"
        "/story — интерактивная история 🎭\n"
        "/lie — найдите ложь среди 3 фактов 🕵️\n"
        "/whatif — «Что будет?» научные загадки 🔬\n"
        "/certificate — тест из 25 вопросов с сертификатом\n"
        "/leaderboard — таблица лидеров\n\n"
        "🇬🇧 <b>Английский и IELTS</b>\n"
        "/english — разговор на английском с исправлением ошибок\n"
        "/grammar — грамматика: урок + упражнения 📝\n"
        f"/writing — 💎 разбор IELTS Writing ({WRITING_PRICE_STARS} ⭐)\n"
        f"/speaking — 🎙 полный мок IELTS Speaking ({SPEAKING_PRICE_STARS} ⭐)\n"
        f"/pro — ⭐ PRO-подписка: Writing и Speaking бесплатно ({PRO_PRICE_STARS} ⭐ / {PRO_DAYS} дней)\n"
        "/test — IELTS Speaking (голосовые ответы)\n"
        "/reading_test, /listening_test — тесты в стиле IELTS\n"
        "/dashboard — профиль и материалы\n\n"
        "🌐 /language — сменить язык\n"
        "🔔 Каждое утро присылаю новый факт и совет по английскому — отключить: /notify\n"
        "🛑 Остановить процесс: /stop"
    ),
    "en": (
        "👋 <b>Hi! I'm a universal assistant bot.</b>\n\n"
        "Write or send a voice message about anything — ask questions, get advice, just chat 💬\n\n"
        "🧩 <b>Knowledge & games</b>\n"
        "/quiz — a quiz on any subject\n"
        "/teaser — brain teasers and riddles\n"
        "🔮 /guess — I'll guess what you're thinking of!\n"
        "/daily — daily fact, quiz and tip\n"
        "/story — interactive story 🎭\n"
        "/lie — spot the lie among 3 facts 🕵️\n"
        "/whatif — \"What happens?\" science riddles 🔬\n"
        "/certificate — 25-question test with a certificate\n"
        "/leaderboard — leaderboards\n\n"
        "🇬🇧 <b>English & IELTS</b>\n"
        "/english — chat in English, I'll fix your mistakes\n"
        "/grammar — grammar: lesson + exercises 📝\n"
        f"/writing — 💎 IELTS Writing review ({WRITING_PRICE_STARS} ⭐)\n"
        f"/speaking — 🎙 IELTS Speaking full mock ({SPEAKING_PRICE_STARS} ⭐)\n"
        f"/pro — ⭐ PRO: Writing and Speaking included ({PRO_PRICE_STARS} ⭐ / {PRO_DAYS} days)\n"
        "/test — IELTS Speaking mock (voice answers)\n"
        "/reading_test, /listening_test — IELTS-style tests\n"
        "/dashboard — profile and study materials\n\n"
        "🌐 /language — change language\n"
        "🔔 Every morning I send a new fact and an English tip — turn off: /notify\n"
        "🛑 Stop the current process: /stop"
    ),
}


I18N.update({
    "btn_app": {"uz": "🎓 Ilovani ochish", "ru": "🎓 Открыть приложение", "en": "🎓 Open the app"},
    "btn_pro_go": {"uz": "⭐ PRO bilan davom etish · {p} ⭐", "ru": "⭐ Продолжить с PRO · {p} ⭐", "en": "⭐ Continue with PRO · {p} ⭐"},
    "btn_free": {"uz": "🆓 Bepul versiya", "ru": "🆓 Бесплатная версия", "en": "🆓 Free version"},
    "pitch": {
        "uz": "👋 <b>Salom! Talk botiga xush kelibsiz.</b>\n\n⭐ <b>PRO bilan hammasi ochiladi</b> — atigi {p} ⭐ ga {d} kun:\n💎 IELTS Writing tahlili\n🎙 To'liq Speaking mock (mikrofon bilan)\n📝 Grammar darslari va mashqlar\n📖 Reading va 🎧 Listening testlari\n🌐 Ingliz tili viktorinalari va kutubxona\n\nHar birini alohida olsangiz ancha qimmatroq — PRO eng foydali yo'l 🚀\n\n🆓 <b>Bepul versiyada:</b> fan viktorinalari, jumboqlar, hikoyalar, o'yinlar, kunlik fakt, sertifikat testi va ingliz tilida erkin suhbat.\n\nQaysi birini tanlaysiz? 👇",
        "ru": "👋 <b>Привет! Добро пожаловать в Talk.</b>\n\n⭐ <b>С PRO открывается всё</b> — всего {p} ⭐ на {d} дней:\n💎 Разбор IELTS Writing\n🎙 Полный Speaking мок (с микрофоном)\n📝 Уроки и упражнения по Grammar\n📖 Тесты Reading и 🎧 Listening\n🌐 Английские викторины и библиотека\n\nПо отдельности каждое стоит заметно дороже — PRO самый выгодный путь 🚀\n\n🆓 <b>В бесплатной версии:</b> викторины по наукам, головоломки, истории, игры, факт дня, тест с сертификатом и свободный разговор на английском.\n\nЧто выбираете? 👇",
        "en": "👋 <b>Hi! Welcome to Talk.</b>\n\n⭐ <b>PRO unlocks everything</b> — just {p} ⭐ for {d} days:\n💎 IELTS Writing review\n🎙 Full Speaking mock (with your microphone)\n📝 Grammar lessons and practice\n📖 Reading and 🎧 Listening tests\n🌐 English quizzes and the library\n\nBought one by one it costs much more — PRO is the best deal 🚀\n\n🆓 <b>The free version has:</b> science quizzes, brain teasers, stories, games, the daily fact, the certificate test and free chat in English.\n\nWhich do you choose? 👇",
    },
    "free_go": {
        "uz": "🆓 <b>Bepul versiya yoqildi!</b>\n\nFan viktorinalari, o'yinlar, hikoyalar va ingliz tilida suhbat — hammasi bepul. Ingliz tiliga oid vositalar (Writing, Speaking, Grammar, Reading, Listening) PRO'da. Xohlagan payt /pro buyrug'i bilan ulanishingiz mumkin.\n\nIlovani oching yoki menyudan tanlang 👇",
        "ru": "🆓 <b>Бесплатная версия включена!</b>\n\nВикторины, игры, истории и разговор на английском — всё бесплатно. Инструменты английского (Writing, Speaking, Grammar, Reading, Listening) — в PRO. Подключить можно в любой момент командой /pro.\n\nОткройте приложение или выберите в меню 👇",
        "en": "🆓 <b>Free version on!</b>\n\nScience quizzes, games, stories and chatting in English are all free. The English tools (Writing, Speaking, Grammar, Reading, Listening) are in PRO — you can switch it on any time with /pro.\n\nOpen the app or pick from the menu 👇",
    },
    "welcome_back": {
        "uz": "👋 <b>Xush kelibsiz!</b> ⭐ PRO faol.\n\nIlovani oching yoki menyudan tanlang 👇",
        "ru": "👋 <b>С возвращением!</b> ⭐ PRO активна.\n\nОткройте приложение или выберите в меню 👇",
        "en": "👋 <b>Welcome back!</b> ⭐ PRO is active.\n\nOpen the app or pick from the menu 👇",
    },
    "gate": {
        "uz": "🔒 <b>Bu PRO funksiya</b>\n\nIngliz tiliga oid barcha vositalar (Grammar, Reading, Listening, Speaking, Writing) PRO'da. Ingliz tilida erkin suhbat (/english) esa hamma uchun bepul.\n\n⭐ PRO — {p} ⭐ / {d} kun.",
        "ru": "🔒 <b>Это функция PRO</b>\n\nВсе инструменты английского (Grammar, Reading, Listening, Speaking, Writing) — в PRO. Свободный разговор на английском (/english) бесплатен для всех.\n\n⭐ PRO — {p} ⭐ / {d} дней.",
        "en": "🔒 <b>This is a PRO feature</b>\n\nAll English tools (Grammar, Reading, Listening, Speaking, Writing) are in PRO. Free chat in English (/english) stays free for everyone.\n\n⭐ PRO — {p} ⭐ / {d} days.",
    },
    "paid_app": {
        "uz": "✅ <b>To'lov qabul qilindi!</b> ⭐ Ilovaga qaytib, davom eting.",
        "ru": "✅ <b>Оплата получена!</b> ⭐ Вернитесь в приложение и продолжайте.",
        "en": "✅ <b>Payment received!</b> ⭐ Go back to the app and continue.",
    },
})


def _app_url(route: str = "") -> str:
    base = f"{WEBHOOK_BASE_URL.rstrip('/')}/miniapp/dashboard"
    return f"{base}?r={route}" if route else base


def _app_button(route: str = "") -> InlineKeyboardButton:
    return InlineKeyboardButton(text=tr("btn_app"), web_app=WebAppInfo(url=_app_url(route)))


def _menu_label(cmd: str) -> str:
    base = MENU_LABELS.get(cmd, {}).get(cur_lang()) or MENU_LABELS.get(cmd, {}).get("uz") or cmd
    if cmd == "writing":
        base += f" (⭐{WRITING_PRICE_STARS})"
    if cmd == "speaking":
        base += f" (⭐{SPEAKING_PRICE_STARS})"
    if cmd == "pro":
        base += f" (⭐{PRO_PRICE_STARS})"
    return base


def _main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [_app_button()],
        [InlineKeyboardButton(text=tr("btn_sci"), callback_data="mn:sci")],
        [InlineKeyboardButton(text=tr("btn_eng"), callback_data="mn:eng")],
        [InlineKeyboardButton(text=tr("btn_all"), callback_data="mn:all"),
         InlineKeyboardButton(text=tr("btn_lang"), callback_data="mn:lang")],
    ])


def _submenu_keyboard(items: list) -> InlineKeyboardMarkup:
    rows, row = [], []
    for emoji, cmd in items:
        row.append(InlineKeyboardButton(text=f"{emoji} {_menu_label(cmd)}", callback_data=f"mn:run:{cmd}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text=tr("btn_back"), callback_data="mn:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _pitch_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=tr("btn_pro_go", p=PRO_PRICE_STARS), callback_data="pw:buy")],
        [InlineKeyboardButton(text=tr("btn_free"), callback_data="pw:free")],
    ])


async def _start_screen(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    """/start dan keyin ko'rsatiladigan ekran: PRO foydalanuvchiga menyu, qolganlarga PRO taklifi."""
    if await _is_premium(uid):
        return tr("welcome_back"), _main_menu_keyboard()
    return tr("pitch", p=PRO_PRICE_STARS, d=PRO_DAYS), _pitch_keyboard()


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, command: CommandObject):
    await state.clear()
    if await get_lang(message.from_user.id) is None:
        await message.answer(tr("pick"), reply_markup=_lang_keyboard())
        return
    # Mini App'dan kelgan chuqur havola: t.me/<bot>?start=run_<buyruq>
    payload = (command.args or "").strip()
    if payload.startswith("run_"):
        cmd = payload[4:]
        if cmd in {c for _e, c in MENU_SCIENCE + MENU_ENGLISH}:
            fake = message.model_copy(update={"text": f"/{cmd}", "entities": None})
            await dp.feed_update(bot, Update(update_id=0, message=fake))
            return
    text, kb = await _start_screen(message.from_user.id)
    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "pw:buy")
async def handle_pitch_buy(callback: CallbackQuery):
    await callback.answer()
    await _send_pro_invoice(callback.message)


@dp.callback_query(F.data == "pw:free")
async def handle_pitch_free(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.edit_text(tr("free_go"), reply_markup=_main_menu_keyboard())
    except Exception:
        await callback.message.answer(tr("free_go"), reply_markup=_main_menu_keyboard())


@dp.message(Command("language", "til"))
async def cmd_til(message: Message):
    await message.answer(tr("pick"), reply_markup=_lang_keyboard())


@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    await message.answer(tr("main_menu"), reply_markup=_main_menu_keyboard())


@dp.callback_query(F.data.startswith("lg:"))
async def handle_language(callback: CallbackQuery):
    code = callback.data.split(":", 1)[1]
    if code not in LANGS:
        await callback.answer()
        return
    await set_lang(callback.from_user.id, code)
    current_lang.set(code)
    await callback.answer(LANGS[code][0])
    body, kb = await _start_screen(callback.from_user.id)
    text = f"{tr('lang_set')}\n\n{body}"
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("mn:"))
async def handle_menu(callback: CallbackQuery):
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    msg = callback.message

    async def show(text: str, kb: InlineKeyboardMarkup):
        try:
            await msg.edit_text(text, reply_markup=kb)
        except Exception:
            await msg.answer(text, reply_markup=kb)

    if action == "home":
        await callback.answer()
        await show(tr("main_menu"), _main_menu_keyboard())
    elif action == "sci":
        await callback.answer()
        await show(tr("sci_title"), _submenu_keyboard(MENU_SCIENCE))
    elif action == "eng":
        await callback.answer()
        await show(tr("eng_title"), _submenu_keyboard(MENU_ENGLISH))
    elif action == "lang":
        await callback.answer()
        await show(tr("pick"), _lang_keyboard())
    elif action == "all":
        await callback.answer()
        await show(ALL_COMMANDS.get(cur_lang(), WELCOME_TEXT), InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=tr("btn_back"), callback_data="mn:home")]]))
    elif action == "run" and len(parts) > 2:
        cmd = parts[2]
        allowed = {c for _e, c in MENU_SCIENCE + MENU_ENGLISH}
        if cmd not in allowed:
            await callback.answer()
            return
        await callback.answer()
        # Tugmani xuddi foydalanuvchi "/buyruq" yozgandek ishga tushiramiz
        fake = msg.model_copy(update={"text": f"/{cmd}", "from_user": callback.from_user, "entities": None})
        await dp.feed_update(bot, Update(update_id=0, message=fake))
    else:
        await callback.answer()


@dp.message(Command("stop"))
async def cmd_stop(message: Message, state: FSMContext):
    current = await state.get_state()

    # Davom etayotgan viktorina/test (Quiz, Kunlik, Reading, Listening, Sertifikat) va jumboqni ham to'xtatamiz
    quiz_stopped = active_quizzes.pop(message.from_user.id, None) is not None
    active_teasers.pop(message.from_user.id, None)
    active_stories.pop(message.from_user.id, None)
    active_lies.pop(message.from_user.id, None)
    active_what.pop(message.from_user.id, None)
    active_grammar.pop(message.from_user.id, None)
    awaiting_essay.pop(message.from_user.id, None)
    active_guess_games.pop(message.from_user.id, None)

    if current is None:
        # Faol imtihon yo'q, lekin suhbat xotirasi bo'lishi mumkin — uni tozalaymiz.
        await state.set_data({})
        if quiz_stopped:
            await message.answer("🛑 Viktorina to'xtatildi. Yangisini boshlash uchun /quiz yuboring.")
        else:
            await message.answer("🔄 Suhbat xotirasi tozalandi. Yangidan yozishingiz mumkin, yoki /quiz bilan viktorina boshlang.")
        return

    if current == EnglishPracticeStates.practicing.state:
        await state.clear()
        await message.answer("🛑 English Practice Mode stopped. Send /english to start again.")
        return

    await state.clear()
    cleanup_user_dir(message.from_user.id)
    await message.answer("🛑 Test to'xtatildi. Qayta boshlash uchun /test yuboring.")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Botga jami nechta noyob foydalanuvchi kirganini va umumiy murojaatlar
    sonini ko'rsatadi. Agar ADMIN_USER_ID sozlangan bo'lsa, faqat o'sha
    foydalanuvchiga ruxsat beriladi."""
    if ADMIN_USER_ID and str(message.from_user.id) != str(ADMIN_USER_ID):
        await message.answer("⛔ Bu buyruq faqat bot egasi uchun.")
        return

    if redis_client is None:
        await message.answer(
            "⚠️ Statistika xizmati sozlanmagan.\n\n"
            "UPSTASH_REDIS_REST_URL va UPSTASH_REDIS_REST_TOKEN "
            "environment variable'larini qo'shing."
        )
        return

    try:
        total_users = await redis_client.scard(BOT_USERS_SET_KEY)
        total_starts = await redis_client.get(BOT_STARTS_COUNTER_KEY)
        now = datetime.now(timezone(timedelta(hours=5)))
        day_keys = [f"{BOT_ACTIVE_DAY_PREFIX}{(now - timedelta(days=d)).strftime('%Y-%m-%d')}" for d in range(7)]
        active_today = await redis_client.scard(day_keys[0])
        week_members = await redis_client.sunion(*day_keys)
        active_week = len(week_members or [])
    except Exception as e:
        logger.exception("Statistikani o'qishda xatolik: %s", e)
        await message.answer("❌ Statistikani olishda xatolik yuz berdi.")
        return

    total_starts = total_starts or 0

    await message.answer(
        "📊 <b>Bot statistikasi</b>\n\n"
        f"👥 Jami noyob foydalanuvchilar: <b>{total_users}</b>\n"
        f"🟢 Bugun faol: <b>{active_today}</b>\n"
        f"📅 Oxirgi 7 kunda faol: <b>{active_week}</b>\n"
        f"📨 Jami murojaatlar (xabar/tugma): <b>{total_starts}</b>"
    )


@dp.message(Command("activity"))
async def cmd_activity(message: Message):
    """Oxirgi 10 ta foydalanuvchi va ularning so'nggi harakatlari. Faqat bot egasi uchun."""
    if not _is_admin(message.from_user.id):
        return  # boshqalarga hech narsa ko'rsatilmaydi (buyruq mavjudligi ham sezilmaydi)
    rows = await read_activity()
    if not rows:
        await message.answer("Hozircha faoliyat yozuvlari yo'q.")
        return
    tz = timezone(timedelta(hours=5))
    users: dict = {}
    for r in rows:  # yangidan eskiga
        u = users.setdefault(r["id"], {"n": r.get("n"), "u": r.get("u"), "acts": []})
        if len(u["acts"]) < 4:
            u["acts"].append(r)
        if len(users) >= 10 and r["id"] not in users:
            break
    top = list(users.items())[:10]
    lines = ["🕵️ <b>Oxirgi 10 ta foydalanuvchi</b>\n"]
    for uid, u in top:
        who = html.escape(u["n"] or str(uid))
        if u["u"]:
            who += f" (@{html.escape(u['u'])})"
        lines.append(f"👤 <b>{who}</b> · <code>{uid}</code>")
        for a in u["acts"]:
            hm = datetime.fromtimestamp(a["t"], tz).strftime("%d.%m %H:%M")
            lines.append(f"   {hm} — {html.escape(a['w'])}")
        lines.append("")
    await message.answer("\n".join(lines)[:4000])


@dp.message(Command("users"))
async def cmd_users(message: Message):
    """Faqat bot egasi uchun: oxirgi faol foydalanuvchilar ro'yxati (ism, username, ID)."""
    if not ADMIN_USER_ID or str(message.from_user.id) != str(ADMIN_USER_ID):
        await message.answer("⛔ Bu buyruq faqat bot egasi uchun.")
        return
    if redis_client is None:
        await message.answer("⚠️ Redis sozlanmagan.")
        return
    try:
        raw = await redis_client.hgetall(BOT_USER_INFO_KEY) or {}
    except Exception as e:
        logger.exception("Foydalanuvchilar ro'yxatini o'qishda xatolik: %s", e)
        await message.answer("❌ Ro'yxatni olishda xatolik yuz berdi.")
        return
    if not raw:
        await message.answer("📭 Hozircha ro'yxat bo'sh. Foydalanuvchilar faol bo'lgach shu yerda ko'rinadi.")
        return
    rows = []
    for uid, val in raw.items():
        try:
            d = json.loads(val)
        except (TypeError, ValueError):
            d = {}
        rows.append((str(d.get("d", "")), str(uid), str(d.get("n", "")), str(d.get("u", ""))))
    rows.sort(reverse=True)
    limit = 40
    lines = [f"👥 <b>Foydalanuvchilar</b> (ro'yxatda: {len(rows)}, oxirgi {min(limit, len(rows))} ta faol)\n"]
    for day, uid, name, uname in rows[:limit]:
        nm = html.escape(name) or "—"
        un = f" @{html.escape(uname)}" if uname else ""
        lines.append(f"• <a href=\"tg://user?id={uid}\">{nm}</a>{un} — <code>{uid}</code> — {day}")
    lines.append("\n<i>Eslatma: ro'yxat yangilanish kunidan boshlab to'planadi; eski foydalanuvchilar faol bo'lgach qo'shiladi.</i>")
    await message.answer("\n".join(lines))


@dp.message(Command("leaderboard", "reyting"))
async def cmd_reyting(message: Message):
    """Viktorina (/quiz), sertifikat (/certificate), Reading va Listening testlari uchun
    alohida TOP 10 reyting jadvallarini ko'rsatadi."""
    if redis_client is None:
        await message.answer(
            "⚠️ Reyting xizmati sozlanmagan.\n\n"
            "UPSTASH_REDIS_REST_URL va UPSTASH_REDIS_REST_TOKEN "
            "environment variable'larini qo'shing."
        )
        return

    await bot.send_chat_action(message.chat.id, "typing")

    quiz_text = await format_leaderboard_text(
        "🏆 <b>Viktorina reytingi</b> (TOP 10)", LEADERBOARD_QUIZ_KEY
    )
    cert_text = await format_leaderboard_text(
        "🎓 <b>Sertifikat testi reytingi</b> (TOP 10)", LEADERBOARD_CERT_KEY
    )
    reading_text = await format_leaderboard_text(
        "📖 <b>Reading testi reytingi</b> (TOP 10)", LEADERBOARD_READING_KEY
    )
    listening_text = await format_leaderboard_text(
        "🎧 <b>Listening testi reytingi</b> (TOP 10)", LEADERBOARD_LISTENING_KEY
    )

    await message.answer(quiz_text)
    await message.answer(cert_text)
    await message.answer(reading_text)
    await message.answer(listening_text)


_DASH_T = {
    "btn": {"uz": "🎓 Ilovani ochish", "ru": "🎓 Открыть приложение", "en": "🎓 Open the app"},
    "txt": {
        "uz": "🎓 <b>Talk ilovasi</b>\n\nBarcha o'yinlar, testlar, Writing va Speaking, reyting va ⭐ PRO — bitta joyda:",
        "ru": "🎓 <b>Приложение Talk</b>\n\nВсе игры, тесты, Writing и Speaking, рейтинг и ⭐ PRO — в одном месте:",
        "en": "🎓 <b>The Talk app</b>\n\nAll games, tests, Writing and Speaking, the leaderboard and ⭐ PRO — in one place:",
    },
}


@dp.message(Command("dashboard"))
async def cmd_dashboard(message: Message):
    """Yangi Mini App dashboard: bosh sahifa, mashqlar, kutubxona, PRO."""
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/miniapp/dashboard"
    lang = cur_lang()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=_DASH_T["btn"][lang], web_app=WebAppInfo(url=url))]]
    )
    await message.answer(_DASH_T["txt"][lang], reply_markup=keyboard)


@dp.message(Command("english"))
async def cmd_english(message: Message, state: FSMContext):
    """Ingliz tili mashq rejimini yoqadi — bot ingliz tilida yozadi,
    foydalanuvchining xatolarini muloyimlik bilan tuzatib, suhbatni davom ettiradi."""
    await state.set_data({"english_history": []})
    await state.set_state(EnglishPracticeStates.practicing)
    await message.answer(
        "🇬🇧 <b>English Practice Mode ON!</b>\n\n"
        "Write to me in English (text or voice) and I'll gently correct your mistakes "
        "while we chat. Don't worry about being perfect — let's just talk!\n\n"
        "To exit, send /stop.\n\n"
        "So, tell me — how has your day been so far? 😊"
    )


@dp.message(EnglishPracticeStates.practicing, F.text)
async def handle_english_practice_text(message: Message, state: FSMContext):
    data = await state.get_data()
    history = data.get("english_history", [])
    history.append({"role": "user", "content": message.text})

    await bot.send_chat_action(message.chat.id, "typing")

    try:
        reply = await asyncio.to_thread(_generate_english_practice_reply_sync, history)
    except Exception as e:
        logger.exception("Ingliz tili mashqida javob generatsiya qilishda xatolik: %s", e)
        await message.answer("❌ Xatolik yuz berdi. Iltimos, qayta urinib ko'ring.")
        return

    history.append({"role": "assistant", "content": reply})
    await state.update_data(english_history=history[-ENGLISH_PRACTICE_HISTORY_LIMIT:])
    await message.answer(reply)


@dp.message(EnglishPracticeStates.practicing, F.voice)
async def handle_english_practice_voice(message: Message, state: FSMContext):
    """Ingliz tili mashqida ovozli xabar yuborilsa, uni matnga aylantirib
    xuddi yozma xabardek tuzatish va javob beradi."""
    user_dir = _user_dir(message.from_user.id)
    ogg_path = os.path.join(user_dir, f"english_voice_{message.voice.file_unique_id}.ogg")

    try:
        file = await bot.get_file(message.voice.file_id)
        await bot.download_file(file.file_path, destination=ogg_path)
        transcript = await asyncio.to_thread(_transcribe_sync, ogg_path)
    except Exception as e:
        logger.exception("Ingliz tili mashqida ovozni tanib olishda xatolik: %s", e)
        await message.answer("❌ Ovozli xabarni tushunishda xatolik yuz berdi. Iltimos, qayta urinib ko'ring.")
        return
    finally:
        if os.path.exists(ogg_path):
            try:
                os.remove(ogg_path)
            except OSError:
                pass

    if not transcript:
        await message.answer("⚠️ Ovozingizda nutq aniqlanmadi. Please try speaking again.")
        return

    data = await state.get_data()
    history = data.get("english_history", [])
    history.append({"role": "user", "content": transcript})

    await bot.send_chat_action(message.chat.id, "typing")

    try:
        reply = await asyncio.to_thread(_generate_english_practice_reply_sync, history)
    except Exception as e:
        logger.exception("Ingliz tili mashqida javob generatsiya qilishda xatolik: %s", e)
        await message.answer("❌ Xatolik yuz berdi. Iltimos, qayta urinib ko'ring.")
        return

    history.append({"role": "assistant", "content": reply})
    await state.update_data(english_history=history[-ENGLISH_PRACTICE_HISTORY_LIMIT:])
    await message.answer(f"🎙 <i>You said:</i> {transcript}\n\n{reply}")


# --- Admin: Dashboard kontentini boshqarish (Reading/Listening/Grammar) --

def _is_admin(user_id: int) -> bool:
    return bool(ADMIN_USER_ID) and str(user_id) == str(ADMIN_USER_ID)


async def _require_admin_and_redis(message: Message) -> bool:
    """Agar foydalanuvchi admin bo'lmasa yoki Redis ulanmagan bo'lsa, mos
    xabar yuborib False qaytaradi (chaqiruvchi funksiya to'xtashi kerak)."""
    if not ADMIN_USER_ID:
        await message.answer(
            "⚠️ ADMIN_USER_ID sozlanmagan. Kontent qo'shish uchun avval "
            "Render'da ADMIN_USER_ID environment variable'ini sozlang."
        )
        return False
    if not _is_admin(message.from_user.id):
        await message.answer("⛔ Bu buyruq faqat bot egasi (admin) uchun.")
        return False
    if redis_client is None:
        await message.answer("⚠️ Kontent xizmati sozlanmagan (Upstash Redis ulanmagan).")
        return False
    return True


QUESTIONS_FORMAT_HELP = (
    "Endi savollarni yuboring (ixtiyoriy). Har bir savolni alohida qatorga, "
    "quyidagi formatda yozing:\n\n"
    "<code>Savol matni | Variant A | Variant B | Variant C | Variant D | to'g'ri raqam</code>\n\n"
    "Masalan:\n"
    "<code>What is the capital of France? | London | Paris | Rome | Berlin | 2</code>\n\n"
    "Bir nechta savolni bir nechta qatorda yuborishingiz mumkin. "
    "Agar savol qo'shmoqchi bo'lmasangiz, shunchaki <b>yo'q</b> deb yozing."
)


@dp.message(Command("add_reading"))
async def cmd_add_reading(message: Message, state: FSMContext):
    if not await _require_admin_and_redis(message):
        return
    await state.set_state(AdminContentStates.reading_title)
    await message.answer("📖 <b>Yangi Reading materiali</b>\n\nSarlavhasini yuboring:")


@dp.message(AdminContentStates.reading_title, F.text)
async def admin_reading_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(AdminContentStates.reading_body)
    await message.answer("✍️ Endi o'qish matnini (passage) to'liq yuboring:")


@dp.message(AdminContentStates.reading_body, F.text)
async def admin_reading_body(message: Message, state: FSMContext):
    await state.update_data(body=message.text.strip())
    await state.set_state(AdminContentStates.reading_questions)
    await message.answer(QUESTIONS_FORMAT_HELP)


@dp.message(AdminContentStates.reading_questions, F.text)
async def admin_reading_questions(message: Message, state: FSMContext):
    questions = _parse_admin_questions_text(message.text)
    if questions is None:
        await message.answer("❌ Format noto'g'ri. Iltimos, yuqoridagi formatga qayta amal qilib yuboring, yoki 'yo'q' deb yozing.")
        return

    data = await state.get_data()
    item_id = await get_next_content_id("reading")
    await save_content_item("reading", item_id, {
        "title": data["title"],
        "body": data["body"],
        "questions": questions,
    })
    await state.clear()
    await message.answer(
        f"✅ Reading materiali qo'shildi! (ID: {item_id})\n"
        f"📝 Savollar soni: {len(questions)}"
    )


@dp.message(Command("add_grammar"))
async def cmd_add_grammar(message: Message, state: FSMContext):
    if not await _require_admin_and_redis(message):
        return
    await state.set_state(AdminContentStates.grammar_title)
    await message.answer("📚 <b>Yangi Grammar/Article materiali</b>\n\nSarlavhasini yuboring:")


@dp.message(AdminContentStates.grammar_title, F.text)
async def admin_grammar_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(AdminContentStates.grammar_body)
    await message.answer("✍️ Endi maqola/qoidalar matnini to'liq yuboring:")


@dp.message(AdminContentStates.grammar_body, F.text)
async def admin_grammar_body(message: Message, state: FSMContext):
    data = await state.get_data()
    item_id = await get_next_content_id("grammar")
    await save_content_item("grammar", item_id, {
        "title": data["title"],
        "body": message.text.strip(),
    })
    await state.clear()
    await message.answer(f"✅ Grammar/Article materiali qo'shildi! (ID: {item_id})")


@dp.message(Command("add_listening"))
async def cmd_add_listening(message: Message, state: FSMContext):
    if not await _require_admin_and_redis(message):
        return
    await state.set_state(AdminContentStates.listening_title)
    await message.answer("🎧 <b>Yangi Listening materiali</b>\n\nSarlavhasini yuboring:")


@dp.message(AdminContentStates.listening_title, F.text)
async def admin_listening_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(AdminContentStates.listening_audio)
    await message.answer("🎧 Endi audio faylni yuboring (voice yoki audio fayl sifatida):")


@dp.message(AdminContentStates.listening_audio, F.voice)
async def admin_listening_audio_voice(message: Message, state: FSMContext):
    await state.update_data(file_id=message.voice.file_id, file_kind="voice")
    await state.set_state(AdminContentStates.listening_questions)
    await message.answer(QUESTIONS_FORMAT_HELP)


@dp.message(AdminContentStates.listening_audio, F.audio)
async def admin_listening_audio_file(message: Message, state: FSMContext):
    await state.update_data(file_id=message.audio.file_id, file_kind="audio")
    await state.set_state(AdminContentStates.listening_questions)
    await message.answer(QUESTIONS_FORMAT_HELP)


@dp.message(AdminContentStates.listening_audio)
async def admin_listening_audio_invalid(message: Message):
    await message.answer("🎧 Iltimos, audio faylni voice yoki audio ko'rinishida yuboring.")


@dp.message(AdminContentStates.listening_questions, F.text)
async def admin_listening_questions(message: Message, state: FSMContext):
    questions = _parse_admin_questions_text(message.text)
    if questions is None:
        await message.answer("❌ Format noto'g'ri. Iltimos, yuqoridagi formatga qayta amal qilib yuboring, yoki 'yo'q' deb yozing.")
        return

    data = await state.get_data()
    item_id = await get_next_content_id("listening")
    await save_content_item("listening", item_id, {
        "title": data["title"],
        "file_id": data["file_id"],
        "file_kind": data["file_kind"],
        "questions": questions,
    })
    await state.clear()
    await message.answer(
        f"✅ Listening materiali qo'shildi! (ID: {item_id})\n"
        f"📝 Savollar soni: {len(questions)}"
    )


@dp.message(Command("content_list"))
async def cmd_content_list(message: Message):
    if not await _require_admin_and_redis(message):
        return

    lines = ["📂 <b>Barcha dashboard kontenti</b>\n"]
    for content_type, label in (("reading", "📖 Reading"), ("listening", "🎧 Listening"), ("grammar", "📚 Grammar")):
        items = await list_content_items(content_type)
        lines.append(f"{label} ({len(items)} ta):")
        if not items:
            lines.append("  — bo'sh —")
        else:
            for item in items:
                q_count = len(item.get("questions", [])) if "questions" in item else None
                suffix = f" ({q_count} savol)" if q_count else ""
                lines.append(f"  #{item['id']} — {item.get('title', 'nomsiz')}{suffix}")
        lines.append("")

    lines.append("O'chirish uchun: /delete_content &lt;tur&gt; &lt;id&gt; (masalan: /delete_content reading 3)")
    await message.answer("\n".join(lines))


@dp.message(Command("delete_content"))
async def cmd_delete_content(message: Message, command: CommandObject):
    if not await _require_admin_and_redis(message):
        return

    args = (command.args or "").split()
    if len(args) != 2 or args[0] not in CONTENT_TYPES or not args[1].isdigit():
        await message.answer(
            "ℹ️ Foydalanish: <code>/delete_content &lt;reading|listening|grammar&gt; &lt;id&gt;</code>\n"
            "Masalan: <code>/delete_content reading 3</code>"
        )
        return

    content_type, item_id_str = args[0], args[1]
    deleted = await delete_content_item(content_type, int(item_id_str))
    if deleted:
        await message.answer(f"🗑 {content_type} #{item_id_str} o'chirildi.")
    else:
        await message.answer(f"⚠️ {content_type} #{item_id_str} topilmadi.")



def _quiz_subject_keyboard() -> InlineKeyboardMarkup:
    """Fan tanlash tugmalari (2 ustunli) + 'Tasodifiy' tugmasi."""
    rows, row = [], []
    for i, (emoji, name, _desc) in enumerate(SUBJECTS):
        row.append(InlineKeyboardButton(text=f"{emoji} {name}", callback_data=f"qzsub:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🎲 Tasodifiy (aralash)", callback_data="qzsub:r")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("quiz"))
async def cmd_quiz(message: Message):
    f"""Istalgan fandan {QUIZ_LENGTH} ta savollik viktorina. Foydalanuvchi fanni tugma bilan
    tanlaydi. IELTS imtihon holatidan (FSM) mustaqil, alohida (active_quizzes) xotirada ishlaydi."""
    await message.answer(
        "🧩 <b>Qaysi fandan viktorina o'ynaymiz?</b>\n\n"
        f"Fan tanlang — men {QUIZ_LENGTH} ta qiziqarli savol beraman:",
        reply_markup=_quiz_subject_keyboard(),
    )


@dp.callback_query(F.data.startswith("qzsub:"))
async def handle_quiz_subject(callback: CallbackQuery):
    choice = callback.data.split(":", 1)[1]
    if choice == "r":
        subject_name, label = None, "🎲 Aralash viktorina"
    else:
        try:
            emoji, name, _desc = SUBJECTS[int(choice)]
        except (ValueError, IndexError):
            await callback.answer("Xatolik yuz berdi.", show_alert=True)
            return
        subject_name, label = name, f"{emoji} {name}"

    await callback.answer()
    try:
        await callback.message.edit_text(f"⏳ {label}: {QUIZ_LENGTH} ta savol tayyorlanmoqda...")
    except Exception:
        pass

    try:
        questions = await asyncio.to_thread(_generate_quiz_set_sync, QUIZ_LENGTH, subject_name)
        if not questions:
            raise ValueError("Yaroqli savollar generatsiya qilinmadi")
    except Exception as e:
        logger.exception("Viktorinani generatsiya qilishda xatolik: %s", e)
        try:
            await callback.message.edit_text("❌ Savollarni tayyorlashda xatolik yuz berdi. Iltimos /quiz bilan qayta urinib ko'ring.")
        except Exception:
            pass
        return

    user_id = callback.from_user.id
    active_quizzes[user_id] = {
        "questions": questions,
        "idx": 0,
        "score": 0,
        "mode": "quiz",
        "subject": label,
    }
    text, keyboard = _build_quiz_question_view(active_quizzes[user_id], 0)
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except Exception:
        await callback.message.answer(text, reply_markup=keyboard)



# --- Miya jumboqlari (Brain Teasers) -------------------------------------

# user_id -> {"puzzle": str, "hint": str, "answer": str, "explanation": str, "kind": str}
active_teasers: dict[int, dict] = {}

TEASER_KINDS = [
    "topishmoq (an'anaviy o'zbek topishmoqlari uslubida)",
    "mantiqiy masala (matematik hisoblash talab qilmaydigan)",
    "so'z o'yini yoki harflar bilan bog'liq jumboq",
    "raqamlar ketma-ketligi yoki oddiy matematik jumboq",
    "kutilmagan javobli (lateral thinking) qisqa hikoya-jumboq",
    "kundalik hayotdagi 'aldamchi' savol (javobi sodda, lekin birdan o'ylab bo'lmaydi)",
]


def _generate_teaser_sync() -> dict:
    """Groq (LLM) orqali bitta original miya jumboqini (jumboq, maslahat, javob, izoh)
    JSON ko'rinishida generatsiya qiladi. Turi har safar tasodifiy tanlanadi."""
    import random

    kind = random.choice(TEASER_KINDS)
    prompt = (
        "Siz qiziqarli, zerikarsiz miya jumboqlari (brain teasers) ixtirochisisiz. "
        f"Turi: {kind}. Bitta original, aniq javobli, o'ylantiradigan jumboq tuzing. "
        "Jumboq 1-4 gapdan oshmasin. Javob qisqa va bir ma'noli bo'lsin. "
        "Hammasi o'zbek tilida bo'lsin. "
        "Javobni FAQAT quyidagi JSON formatida qaytaring, boshqa hech qanday matn qo'shmang:\n"
        "{\n"
        '  "puzzle": "jumboq matni",\n'
        '  "hint": "javobni oshkor qilmaydigan qisqa maslahat",\n'
        '  "answer": "qisqa javob",\n'
        '  "explanation": "nega aynan shu javob ekanini 1-2 gapda tushuntiring"\n'
        "}"
    )
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=1.0,
        response_format={"type": "json_object"},
    )
    data = json.loads(completion.choices[0].message.content)
    if not (data.get("puzzle") and data.get("answer")):
        raise ValueError("Jumboq to'liq generatsiya qilinmadi")
    data["kind"] = kind
    return data


def _teaser_keyboard(show_answer: bool = False) -> InlineKeyboardMarkup:
    if show_answer:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Yangi jumboq", callback_data="tz:new")],
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💡 Maslahat", callback_data="tz:hint"),
            InlineKeyboardButton(text="✅ Javobni ko'rish", callback_data="tz:ans"),
        ],
        [InlineKeyboardButton(text="🔄 Boshqa jumboq", callback_data="tz:new")],
    ])


async def _send_new_teaser(chat_message: Message, user_id: int):
    """Yangi jumboq generatsiya qilib, shu chatga yuboradi."""
    await bot.send_chat_action(chat_message.chat.id, "typing")
    try:
        teaser = await asyncio.to_thread(_generate_teaser_sync)
    except Exception as e:
        logger.exception("Jumboq generatsiya qilishda xatolik: %s", e)
        await chat_message.answer("❌ Jumboq tayyorlashda xatolik yuz berdi. /teaser bilan qayta urinib ko'ring.")
        return
    active_teasers[user_id] = teaser
    await chat_message.answer(
        f"🧠 <b>Miya jumboqi</b>\n\n{html.escape(str(teaser['puzzle']))}\n\n<i>O'ylab ko'ring, so'ng tugmalardan foydalaning 👇</i>",
        reply_markup=_teaser_keyboard(),
    )


@dp.message(Command("teaser"))
async def cmd_teaser(message: Message):
    await _send_new_teaser(message, message.from_user.id)


@dp.callback_query(F.data.startswith("tz:"))
async def handle_teaser_action(callback: CallbackQuery):
    action = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id

    if action == "new":
        await callback.answer()
        await _send_new_teaser(callback.message, user_id)
        return

    teaser = active_teasers.get(user_id)
    if not teaser:
        await callback.answer("Bu jumboq eskirgan. /teaser bilan yangisini oling.", show_alert=True)
        return

    if action == "hint":
        await callback.answer("💡 " + teaser.get("hint", "Maslahat yo'q"), show_alert=True)
        return

    if action == "ans":
        await callback.answer()
        text = (
            f"🧠 <b>Miya jumboqi</b>\n\n{html.escape(str(teaser['puzzle']))}\n\n"
            f"✅ <b>Javob:</b> {html.escape(str(teaser['answer']))}\n"
            f"💬 <i>{html.escape(str(teaser.get('explanation', '')))}</i>"
        )
        try:
            await callback.message.edit_text(text, reply_markup=_teaser_keyboard(show_answer=True))
        except Exception:
            await callback.message.answer(text, reply_markup=_teaser_keyboard(show_answer=True))
        return

    await callback.answer()



# --- 🔮 "Men topaman" o'yini (Akinator uslubi) ----------------------------
# Foydalanuvchi biror narsani o'ylaydi (shaxs, hayvon, narsa, qahramon...),
# bot faqat Ha/Yo'q savollar berib, uni topishga harakat qiladi. Faqat tugmalar.

GUESS_MAX_QUESTIONS = 20
active_guess_games: dict[int, dict] = {}

_GUESS_ANSWERS = {"y": "Ha", "n": "Yo'q", "m": "Ba'zan / bilmayman"}


def _guess_step_sync(history: list, force_guess: bool, wrong_guesses: list) -> dict:
    lines = "\n".join(f"{i + 1}. {q} -> {a}" for i, (q, a) in enumerate(history)) or "(hali savol berilmagan)"
    wrong = ", ".join(wrong_guesses) if wrong_guesses else "yo'q"
    prompt = (
        "You are playing a 20-questions guessing game (like Akinator) in UZBEK (Latin script). "
        "The player secretly thinks of something well-known: a famous person (real or fictional), "
        "an animal, an object, a food, a place or a movie/cartoon character. "
        "You ask ONE yes/no question at a time to narrow it down, then guess.\n\n"
        f"Questions so far:\n{lines}\n\n"
        f"Wrong guesses already made (never repeat them): {wrong}\n\n"
        "Rules: start with broad questions (person/animal/object, real/fictional, alive/dead...) and get "
        "more specific. Never repeat a question. Each question must be answerable with Yes/No. "
        + ("You MUST make your final guess now. " if force_guess else
           "Guess only when you are fairly confident (usually after 8+ questions). ")
        + 'Reply ONLY with JSON: {"action": "ask", "question": "..."} or {"action": "guess", "guess": "..."} '
        "(question and guess in Uzbek, short, friendly, may include one emoji)."
    )
    return _llm_json_sync(prompt, temperature=0.7)


def _guess_answer_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Ha", callback_data="gs:y"),
            InlineKeyboardButton(text="❌ Yo'q", callback_data="gs:n"),
        ],
        [InlineKeyboardButton(text="🤷 Ba'zan / bilmayman", callback_data="gs:m")],
    ])


def _guess_verdict_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎯 Ha, to'g'ri!", callback_data="gs:win"),
            InlineKeyboardButton(text="🙅 Yo'q, xato", callback_data="gs:lose"),
        ],
    ])


def _guess_end_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Yana o'ynash", callback_data="gs:new")],
    ])


async def _guess_next_step(target: Message, user_id: int):
    """Keyingi savol yoki taxminni LLM'dan olib, foydalanuvchiga yuboradi."""
    game = active_guess_games.get(user_id)
    if not game:
        return
    game["busy"] = True
    try:
        n = len(game["history"])
        force = n >= GUESS_MAX_QUESTIONS - 1
        try:
            step = await asyncio.to_thread(_guess_step_sync, game["history"], force, game["wrong"])
        except Exception as e:
            logger.exception("Guess o'yini LLM xatosi: %s", e)
            active_guess_games.pop(user_id, None)
            await target.answer("⚠️ Kechirasiz, xatolik yuz berdi. Qayta boshlash: /guess")
            return

        if step.get("action") == "guess" and step.get("guess"):
            game["last_guess"] = str(step["guess"])
            await target.answer(
                f"🔮 <b>Menimcha, siz o'ylagan narsa:</b>\n\n✨ <b>{html.escape(game['last_guess'])}</b>\n\nTopdimmi?",
                reply_markup=_guess_verdict_keyboard(),
            )
        else:
            question = str(step.get("question") or "Bu tirik mavjudotmi?")
            game["last_question"] = question
            await target.answer(
                f"🔮 <b>Savol {n + 1}/{GUESS_MAX_QUESTIONS}</b>\n\n{html.escape(question)}",
                reply_markup=_guess_answer_keyboard(),
            )
    finally:
        game["busy"] = False


async def _guess_start(target: Message, user_id: int):
    active_guess_games[user_id] = {"history": [], "wrong": [], "busy": False}
    await target.answer(
        "🔮 <b>Men topaman!</b>\n\n"
        "Biror narsani <b>o'ylang</b> — mashhur inson, hayvon, narsa, ovqat, multfilm qahramoni... "
        "Hech qayerga yozmang, faqat xayolingizda tuting 🤫\n\n"
        f"Men {GUESS_MAX_QUESTIONS} tagacha savol berib, topishga harakat qilaman. "
        "Siz faqat tugmalarni bosasiz 👇"
    )
    await _guess_next_step(target, user_id)


@dp.message(Command("guess", "topaman"))
async def cmd_topaman(message: Message, state: FSMContext):
    await state.clear()
    await _guess_start(message, message.from_user.id)


@dp.callback_query(F.data.startswith("gs:"))
async def handle_guess_action(callback: CallbackQuery):
    action = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id

    if action == "new":
        await callback.answer()
        await _guess_start(callback.message, user_id)
        return

    game = active_guess_games.get(user_id)
    if not game:
        await callback.answer("Bu o'yin tugagan. Yangisi: /guess", show_alert=True)
        return
    if game.get("busy"):
        await callback.answer("⏳ Bir soniya, o'ylayapman...")
        return

    if action in _GUESS_ANSWERS:
        game["history"].append((game.get("last_question", ""), _GUESS_ANSWERS[action]))
        await callback.answer()
        try:
            await callback.message.edit_text(
                f"🔮 {html.escape(game.get('last_question', ''))}\n\n👉 <b>{_GUESS_ANSWERS[action]}</b>",
                reply_markup=None,
            )
        except Exception:
            pass
        await _guess_next_step(callback.message, user_id)
        return

    if action == "win":
        n = len(game["history"])
        guess = game.get("last_guess", "")
        active_guess_games.pop(user_id, None)
        await callback.answer("🎉")
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(
            f"🎉 <b>Yutdim!</b> Men {n} ta savolda <b>{html.escape(guess)}</b> ni topdim 😎\n\n"
            "Yana o'ynaymizmi?",
            reply_markup=_guess_end_keyboard(),
        )
        return

    if action == "lose":
        game["wrong"].append(game.get("last_guess", ""))
        game["history"].append((f"Sizning o'ylaganingiz {game.get('last_guess', '')} mi?", "Yo'q"))
        await callback.answer()
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        if len(game["history"]) >= GUESS_MAX_QUESTIONS or len(game["wrong"]) >= 3:
            active_guess_games.pop(user_id, None)
            await callback.message.answer(
                "🏳️ <b>Yutqazdim!</b> Siz meni chalg'itdingiz 👏\n\nYana bir marta urinib ko'raymi?",
                reply_markup=_guess_end_keyboard(),
            )
            return
        await callback.message.answer("🤔 Hmm, xato qildim. Yana bir oz o'ylab ko'raman...")
        await _guess_next_step(callback.message, user_id)
        return

    await callback.answer()


# --- Interaktiv hikoya (/story) -----------------------------------------
# AI har qadamda sahna yozadi, foydalanuvchi 3 ta tugmadan birini tanlaydi —
# tanlov hikoyani o'zgartiradi va oxirida o'ziga xos yakun bor.

STORY_STEPS = 6
STORY_GENRES = [
    ("🕵️", "Detektiv", "detective mystery in a rainy old city"),
    ("🚀", "Kosmos", "space adventure on a lost starship"),
    ("🏰", "Fantaziya", "fantasy quest with magic and dragons"),
    ("🧟", "Omon qolish", "survival story after a mysterious disaster"),
    ("🏴\u200d☠️", "Qaroqchilar", "pirate treasure hunt on the open sea"),
    ("⏳", "Vaqt sayohati", "time travel gone wrong"),
]
active_stories: dict[int, dict] = {}


def _story_genre_keyboard() -> InlineKeyboardMarkup:
    rows, row = [], []
    for i, (emoji, name, _d) in enumerate(STORY_GENRES):
        row.append(InlineKeyboardButton(text=f"{emoji} {name}", callback_data=f"st:g:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _story_choice_keyboard(choices: list) -> InlineKeyboardMarkup:
    labels = ["1️⃣", "2️⃣", "3️⃣"]
    rows = [[InlineKeyboardButton(text=f"{labels[i]} {c}"[:60], callback_data=f"st:c:{i}")]
            for i, c in enumerate(choices[:3])]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _generate_story_step_sync(story: dict, choice_text: str | None) -> dict:
    step = story["step"]  # shu sahna raqami (1..STORY_STEPS)
    is_last = step >= STORY_STEPS
    recap = "\n".join(story["log"][-4:]) or "(hikoya endi boshlanmoqda)"
    prompt = (
        f"You are a gripping interactive-fiction narrator. Genre: {story['desc']}. "
        "Write in UZBEK (Latin script), vivid, suspenseful, second person ('siz'). "
        f"Story so far (most recent last):\n{recap}\n\n"
        + (f"The player just chose: {choice_text}\n\n" if choice_text else "This is the opening scene.\n\n")
        + f"Write scene {step} of {STORY_STEPS}. 3-5 short sentences. "
        + ("This is the FINAL scene: give a memorable ending that depends on earlier choices, "
           "set \"ending\" to true and \"choices\" to an empty list. "
           if is_last else
           "End on tension. Give exactly 3 distinct, short (max 6 words) choices with real consequences. "
           "Set \"ending\" to false. ")
        + 'Reply ONLY as JSON: {"scene": "...", "choices": ["...","...","..."], "ending": false, "title": "short ending title or empty"}'
    )
    data = _llm_json_sync(prompt, temperature=0.95)
    scene = str(data.get("scene", "")).strip()
    if not scene:
        raise ValueError("Bo'sh sahna")
    choices = [str(c).strip() for c in (data.get("choices") or []) if str(c).strip()][:3]
    if not is_last and len(choices) < 2:
        raise ValueError("Tanlovlar yetarli emas")
    return {"scene": scene, "choices": choices, "ending": is_last, "title": str(data.get("title", "")).strip()}


async def _story_advance(chat_message: Message, user_id: int, choice_text: str | None):
    story = active_stories.get(user_id)
    if not story:
        return
    await bot.send_chat_action(chat_message.chat.id, "typing")
    try:
        res = await asyncio.to_thread(_generate_story_step_sync, story, choice_text)
    except Exception as e:
        logger.exception("Hikoya sahnasini yaratishda xatolik: %s", e)
        await chat_message.answer("❌ Hikoyani davom ettirib bo'lmadi. Qayta urinish uchun /story yuboring.")
        active_stories.pop(user_id, None)
        return

    story["log"].append(f"Scene {story['step']}: {res['scene']}")
    story["choices"] = res["choices"]
    header = f"{story['emoji']} <b>{story['name']}</b> — {story['step']}/{STORY_STEPS}"

    if res["ending"]:
        active_stories.pop(user_id, None)
        title = f"\n🏁 <b>{html.escape(res['title'])}</b>" if res["title"] else ""
        await chat_message.answer(
            f"{header}\n{title}\n\n{html.escape(res['scene'])}\n\n"
            "🎭 <b>Hikoya yakunlandi!</b> Boshqa tanlov — boshqa yakun. Yana o'ynaymizmi? 👇",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Yangi hikoya", callback_data="st:new")]
            ]),
        )
        return

    await chat_message.answer(
        f"{header}\n\n{html.escape(res['scene'])}\n\n<i>Nima qilasiz? 👇</i>",
        reply_markup=_story_choice_keyboard(res["choices"]),
    )


@dp.message(Command("story", "hikoya"))
async def cmd_hikoya(message: Message, state: FSMContext):
    if await state.get_state() is not None:
        await message.answer("⏳ Avval joriy testni tugating yoki /stop yuboring.")
        return
    active_stories.pop(message.from_user.id, None)
    await message.answer(
        "📖 <b>Interaktiv hikoya</b>\n\n"
        "Men hikoya boshlayman, siz esa har qadamda yo'lni tanlaysiz — "
        f"{STORY_STEPS} qadamdan keyin yakun sizning tanlovlaringizga bog'liq bo'ladi 🎲\n\n"
        "Janrni tanlang 👇",
        reply_markup=_story_genre_keyboard(),
    )


@dp.callback_query(F.data.startswith("st:"))
async def handle_story_action(callback: CallbackQuery):
    user_id = callback.from_user.id
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "new":
        await callback.answer()
        await callback.message.answer("📖 Janrni tanlang 👇", reply_markup=_story_genre_keyboard())
        return

    if action == "g":
        try:
            emoji, name, desc = STORY_GENRES[int(parts[2])]
        except (ValueError, IndexError):
            await callback.answer("Xatolik", show_alert=True)
            return
        await callback.answer(f"{emoji} {name}")
        active_stories[user_id] = {"emoji": emoji, "name": name, "desc": desc,
                                   "step": 1, "log": [], "choices": []}
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await _story_advance(callback.message, user_id, None)
        return

    if action == "c":
        story = active_stories.get(user_id)
        if not story or not story.get("choices"):
            await callback.answer("Bu hikoya tugagan. /story bilan yangisini boshlang.", show_alert=True)
            return
        try:
            choice = story["choices"][int(parts[2])]
        except (ValueError, IndexError):
            await callback.answer("Xatolik", show_alert=True)
            return
        story["choices"] = []  # ikki marta bosishdan himoya
        await callback.answer()
        story["log"].append(f"Player chose: {choice}")
        story["step"] += 1
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.message.answer(f"👉 <b>Siz tanladingiz:</b> {html.escape(choice)}")
        except Exception:
            pass
        await _story_advance(callback.message, user_id, choice)
        return

    await callback.answer()


# --- Yolg'onni top (/lie) ---------------------------------------------
# AI 3 ta "fakt" aytadi: 2 tasi rost, 1 tasi yolg'on. Foydalanuvchi yolg'onni
# topadi. Ketma-ket topishlar soni sessiya davomida hisoblanadi.

active_lies: dict[int, dict] = {}
_LIE_TOPICS = [
    "hayvonlar", "inson tanasi", "kosmos", "tarix", "okeanlar", "oziq-ovqat",
    "texnologiya", "geografiya", "o'simliklar", "dunyo rekordlari", "tillar", "ixtirolar",
]


def _generate_lie_sync(topic: str) -> dict:
    data = _llm_json_sync(
        f"Make a 'two truths and a lie' round about: {topic}. Write in UZBEK (Latin script). "
        "Give exactly 3 surprising short statements (1 sentence each). Exactly TWO must be 100% true, "
        "verifiable facts and ONE must be a believable but FALSE statement. "
        "Randomize which position the lie takes. Also give a short explanation (1-2 sentences) with the truth about the lie. "
        'Reply ONLY as JSON: {"statements": ["...","...","..."], "lie_index": 0, "explanation": "..."}',
        temperature=0.9,
    )
    st = [str(x).strip() for x in (data.get("statements") or []) if str(x).strip()]
    idx = int(data.get("lie_index", -1))
    if len(st) != 3 or idx not in (0, 1, 2):
        raise ValueError("Noto'g'ri format")
    return {"statements": st, "lie_index": idx, "explanation": str(data.get("explanation", "")).strip(), "topic": topic}


def _lie_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="1️⃣ yolg'on", callback_data="lie:0"),
        InlineKeyboardButton(text="2️⃣ yolg'on", callback_data="lie:1"),
        InlineKeyboardButton(text="3️⃣ yolg'on", callback_data="lie:2"),
    ]])


async def _send_lie_round(chat_message: Message, user_id: int):
    import random
    prev = active_lies.get(user_id, {})
    await bot.send_chat_action(chat_message.chat.id, "typing")
    try:
        rnd = await asyncio.to_thread(_generate_lie_sync, random.choice(_LIE_TOPICS))
    except Exception as e:
        logger.exception("Yolg'on raundini yaratishda xatolik: %s", e)
        await chat_message.answer("❌ Raund tayyorlanmadi. /lie bilan qayta urinib ko'ring.")
        return
    rnd["streak"] = prev.get("streak", 0)
    rnd["best"] = prev.get("best", 0)
    rnd["answered"] = False
    active_lies[user_id] = rnd
    body = "\n\n".join(f"{i + 1}️⃣ {html.escape(t)}" for i, t in enumerate(rnd["statements"]))
    streak_line = f"\n🔥 Ketma-ket: <b>{rnd['streak']}</b>" if rnd["streak"] else ""
    await chat_message.answer(
        f"🕵️ <b>Yolg'onni top!</b>{streak_line}\n\n{body}\n\n"
        "Ulardan <b>bittasi yolg'on</b>. Qaysi biri? 👇",
        reply_markup=_lie_keyboard(),
    )


@dp.message(Command("lie", "yolgon"))
async def cmd_yolgon(message: Message, state: FSMContext):
    if await state.get_state() is not None:
        await message.answer("⏳ Avval joriy testni tugating yoki /stop yuboring.")
        return
    active_lies.pop(message.from_user.id, None)
    await _send_lie_round(message, message.from_user.id)


@dp.callback_query(F.data.startswith("lie:"))
async def handle_lie_action(callback: CallbackQuery):
    user_id = callback.from_user.id
    action = callback.data.split(":", 1)[1]

    if action == "next":
        await callback.answer()
        await _send_lie_round(callback.message, user_id)
        return

    rnd = active_lies.get(user_id)
    if not rnd or rnd.get("answered"):
        await callback.answer("Bu raund tugagan. /lie bilan yangisini boshlang.", show_alert=True)
        return
    try:
        choice = int(action)
    except ValueError:
        await callback.answer()
        return

    rnd["answered"] = True
    lie = rnd["lie_index"]
    correct = choice == lie
    if correct:
        rnd["streak"] += 1
        rnd["best"] = max(rnd["best"], rnd["streak"])
        head = f"🎯 <b>Topdingiz!</b> 🔥 Ketma-ket: <b>{rnd['streak']}</b>"
        await callback.answer("🎉 To'g'ri!")
    else:
        was = rnd["streak"]
        rnd["streak"] = 0
        head = "😅 <b>Yolg'on tuzog'iga tushdingiz!</b>" + (f" (seriya {was} da to'xtadi)" if was else "")
        await callback.answer("Xato 😅")

    lines = []
    for i, t in enumerate(rnd["statements"]):
        mark = "❌ yolg'on" if i == lie else "✅ rost"
        lines.append(f"{i + 1}️⃣ {html.escape(t)}\n   {mark}")
    text = f"{head}\n\n" + "\n\n".join(lines)
    if rnd["explanation"]:
        text += f"\n\n💡 <i>{html.escape(rnd['explanation'])}</i>"
    if rnd["best"] > 1:
        text += f"\n\n🏆 Eng yaxshi seriya: {rnd['best']}"
    try:
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="➡️ Keyingisi", callback_data="lie:next")]]))
    except Exception:
        await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="➡️ Keyingisi", callback_data="lie:next")]]))


# --- "Nima bo'ladi?" (/whatif) ---------------------------------------------
# Fan asosidagi qiziq vaziyat beriladi (masalan: "Suvga natriy tashlasangiz..."),
# foydalanuvchi 3 ta variantdan nima bo'lishini taxmin qiladi, so'ng
# ilmiy tushuntirish va hayratlanarli fakt ko'rsatiladi.

_WHAT_TOPICS = [
    "fizika", "kimyo", "biologiya", "kosmos va astronomiya", "inson tanasi",
    "geologiya va Yer", "iqlim va ob-havo", "texnika va mexanika", "dengiz va okeanlar", "energiya va elektr",
]
active_what: dict[int, dict] = {}


def _generate_what_sync(topic: str) -> dict:
    data = _llm_json_sync(
        f"Create a 'What would happen if...?' science riddle about: {topic}. Write in UZBEK (Latin script). "
        "The scenario must be a vivid, concrete, scientifically answerable situation that makes people curious "
        "(NOT dangerous-to-try instructions; no instructions for harming anyone). "
        "Give exactly 3 plausible outcomes; exactly ONE is scientifically correct; the wrong ones must be tempting. "
        "Randomize the position of the correct one. Explain the real science simply (2-3 sentences) and add one "
        "mind-blowing related fact (1 sentence). Reply ONLY as JSON: "
        '{"scenario": "...", "options": ["...","...","..."], "correct_index": 0, "explanation": "...", "wow": "..."}',
        temperature=0.9,
    )
    opts = [str(x).strip() for x in (data.get("options") or []) if str(x).strip()]
    idx = int(data.get("correct_index", -1))
    scenario = str(data.get("scenario", "")).strip()
    if len(opts) != 3 or idx not in (0, 1, 2) or not scenario:
        raise ValueError("Noto'g'ri format")
    return {"scenario": scenario, "options": opts, "correct_index": idx,
            "explanation": str(data.get("explanation", "")).strip(),
            "wow": str(data.get("wow", "")).strip(), "topic": topic}


def _what_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="1️⃣", callback_data="wh:0"),
        InlineKeyboardButton(text="2️⃣", callback_data="wh:1"),
        InlineKeyboardButton(text="3️⃣", callback_data="wh:2"),
    ]])


async def _send_what_round(chat_message: Message, user_id: int):
    import random
    prev = active_what.get(user_id, {})
    await bot.send_chat_action(chat_message.chat.id, "typing")
    try:
        rnd = await asyncio.to_thread(_generate_what_sync, random.choice(_WHAT_TOPICS))
    except Exception as e:
        logger.exception("'Nima bo'ladi' savolini yaratishda xatolik: %s", e)
        await chat_message.answer("❌ Savol tayyorlanmadi. /whatif bilan qayta urinib ko'ring.")
        return
    rnd["right"] = prev.get("right", 0)
    rnd["total"] = prev.get("total", 0)
    rnd["answered"] = False
    active_what[user_id] = rnd
    body = "\n\n".join(f"{i + 1}️⃣ {html.escape(t)}" for i, t in enumerate(rnd["options"]))
    score = f"  •  🎯 {rnd['right']}/{rnd['total']}" if rnd["total"] else ""
    await chat_message.answer(
        f"🔬 <b>Nima bo'ladi?</b>{score}\n\n{html.escape(rnd['scenario'])}\n\n{body}\n\n<i>Sizningcha-chi? 👇</i>",
        reply_markup=_what_keyboard(),
    )


@dp.message(Command("whatif", "nima"))
async def cmd_nima(message: Message, state: FSMContext):
    if await state.get_state() is not None:
        await message.answer("⏳ Avval joriy testni tugating yoki /stop yuboring.")
        return
    active_what.pop(message.from_user.id, None)
    await _send_what_round(message, message.from_user.id)


@dp.callback_query(F.data.startswith("wh:"))
async def handle_what_action(callback: CallbackQuery):
    user_id = callback.from_user.id
    action = callback.data.split(":", 1)[1]

    if action == "next":
        await callback.answer()
        await _send_what_round(callback.message, user_id)
        return

    rnd = active_what.get(user_id)
    if not rnd or rnd.get("answered"):
        await callback.answer("Bu savol tugagan. /whatif bilan yangisini oling.", show_alert=True)
        return
    try:
        choice = int(action)
    except ValueError:
        await callback.answer()
        return

    rnd["answered"] = True
    rnd["total"] += 1
    good = rnd["correct_index"]
    if choice == good:
        rnd["right"] += 1
        head = "🎉 <b>To'g'ri taxmin!</b>"
        await callback.answer("🎉 To'g'ri!")
    else:
        head = "🤯 <b>Aksincha bo'ldi!</b>"
        await callback.answer("Kutilmagan 🤯")

    lines = []
    for i, t in enumerate(rnd["options"]):
        mark = "✅" if i == good else ("❌" if i == choice else "▫️")
        lines.append(f"{mark} {i + 1}️⃣ {html.escape(t)}")
    text = f"{head}\n\n" + "\n".join(lines)
    if rnd["explanation"]:
        text += f"\n\n🔎 <b>Ilm nima deydi:</b> {html.escape(rnd['explanation'])}"
    if rnd["wow"]:
        text += f"\n\n🤩 <i>{html.escape(rnd['wow'])}</i>"
    text += f"\n\n🎯 Natijangiz: <b>{rnd['right']}/{rnd['total']}</b>"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="➡️ Yana bitta", callback_data="wh:next")]])
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)


# --- Ingliz tili grammatikasi (/grammar) ---------------------------------
# Daraja -> mavzu tanlanadi, AI o'zbek tilida qisqa dars (qoida, misollar,
# keng tarqalgan xato) yozadi va 5 ta mashq beradi. Mashqlar tugma bilan yechiladi.

GRAMMAR_LEVELS = [
    ("🟢", "A1–A2 (boshlang'ich)", "beginner (A1-A2)"),
    ("🟡", "B1–B2 (o'rta)", "intermediate (B1-B2)"),
    ("🔴", "C1 (yuqori)", "advanced (C1)"),
]
GRAMMAR_TOPICS = [
    ["Present Simple", "Past Simple", "to be (am/is/are)", "Articles: a / an / the",
     "There is / There are", "Plurals va this/that", "Prepositions of place/time", "Can / Can't"],
    ["Present Perfect", "Past Continuous", "Conditionals (0, 1, 2)", "Modal verbs (must, should, have to)",
     "Comparatives & Superlatives", "Passive Voice", "Reported Speech", "Gerund vs Infinitive"],
    ["Third Conditional & Mixed", "Inversion", "Wish / If only", "Relative clauses (advanced)",
     "Future Perfect & Continuous", "Subjunctive", "Cleft sentences", "Linking words (however, whereas...)"],
]
GRAMMAR_QUESTIONS = 5
active_grammar: dict[int, dict] = {}


def _grammar_levels_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=f"{e} {name}", callback_data=f"gr:l:{i}")]
            for i, (e, name, _d) in enumerate(GRAMMAR_LEVELS)]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _grammar_topics_keyboard(level: int) -> InlineKeyboardMarkup:
    rows, row = [], []
    for j, t in enumerate(GRAMMAR_TOPICS[level]):
        row.append(InlineKeyboardButton(text=t[:30], callback_data=f"gr:t:{level}:{j}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🎲 Tasodifiy mavzu", callback_data=f"gr:t:{level}:r")])
    rows.append([InlineKeyboardButton(text="⬅️ Darajalar", callback_data="gr:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _generate_grammar_sync(topic: str, level_desc: str) -> dict:
    data = _llm_json_sync(
        f"You are an excellent English grammar teacher for UZBEK learners. Topic: '{topic}'. "
        f"Level: {level_desc}. Write the explanation in UZBEK (Latin script), simple and friendly; "
        "English examples stay in English. Reply ONLY as JSON with this schema: "
        '{"title": "...", "rule": "clear explanation in 3-5 short sentences incl. the formula/structure", '
        '"examples": [{"en": "...", "uz": "..."}, {"en": "...", "uz": "..."}, {"en": "...", "uz": "..."}], '
        '"mistake": "one very common mistake uzbek speakers make and the correct form", '
        f'"exercises": [exactly {GRAMMAR_QUESTIONS} items: {{"sentence": "English sentence with ___ gap", '
        '"options": ["A","B","C","D"], "correct_index": 0, "explanation": "short Uzbek explanation"}]} '
        "Exercises must test ONLY this topic, have exactly ONE correct option, and vary the correct position.",
        temperature=0.7,
    )
    rule = str(data.get("rule", "")).strip()
    ex = []
    for item in data.get("exercises") or []:
        try:
            opts = [str(o).strip() for o in item["options"]]
            ci = int(item["correct_index"])
            sentence = str(item["sentence"]).strip()
        except (KeyError, TypeError, ValueError):
            continue
        if len(opts) >= 3 and 0 <= ci < len(opts) and sentence:
            ex.append({"sentence": sentence, "options": opts[:4], "correct_index": ci,
                       "explanation": str(item.get("explanation", "")).strip()})
    ex = [q for q in ex if q["correct_index"] < len(q["options"])]
    if not rule or len(ex) < 3:
        raise ValueError("Grammar kontenti to'liq emas")
    examples = []
    for e in data.get("examples") or []:
        if isinstance(e, dict) and e.get("en"):
            examples.append((str(e.get("en")).strip(), str(e.get("uz", "")).strip()))
    return {"title": str(data.get("title") or topic).strip(), "rule": rule, "examples": examples[:3],
            "mistake": str(data.get("mistake", "")).strip(), "exercises": ex}


def _grammar_question_view(g: dict) -> tuple[str, InlineKeyboardMarkup]:
    q = g["exercises"][g["idx"]]
    letters = ["A", "B", "C", "D"]
    opts = "\n".join(f"<b>{letters[i]})</b> {html.escape(o)}" for i, o in enumerate(q["options"]))
    text = (f"📝 <b>Mashq {g['idx'] + 1}/{len(g['exercises'])}</b>\n\n"
            f"{html.escape(q['sentence'])}\n\n{opts}")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=letters[i], callback_data=f"gr:a:{i}") for i in range(len(q["options"]))]])
    return text, kb


@dp.message(Command("grammar"))
async def cmd_grammar(message: Message, state: FSMContext):
    if await state.get_state() is not None:
        await message.answer("⏳ Avval joriy testni tugating yoki /stop yuboring.")
        return
    active_grammar.pop(message.from_user.id, None)
    await message.answer(
        "📝 <b>Ingliz tili grammatikasi</b>\n\n"
        "Darajangizni tanlang — qisqa dars va 5 ta mashq tayyorlayman 👇",
        reply_markup=_grammar_levels_keyboard(),
    )


@dp.callback_query(F.data.startswith("gr:"))
async def handle_grammar(callback: CallbackQuery):
    import random
    user_id = callback.from_user.id
    parts = callback.data.split(":")
    action = parts[1] if len(parts) > 1 else ""
    msg = callback.message

    if action == "menu":
        await callback.answer()
        try:
            await msg.edit_text("📝 <b>Grammatika</b>\n\nDarajani tanlang 👇", reply_markup=_grammar_levels_keyboard())
        except Exception:
            await msg.answer("📝 <b>Grammatika</b>\n\nDarajani tanlang 👇", reply_markup=_grammar_levels_keyboard())
        return

    if action == "l":
        try:
            lvl = int(parts[2])
            emoji, name, _d = GRAMMAR_LEVELS[lvl]
        except (ValueError, IndexError):
            await callback.answer("Xatolik", show_alert=True)
            return
        await callback.answer()
        text = f"{emoji} <b>{name}</b>\n\nMavzuni tanlang 👇"
        try:
            await msg.edit_text(text, reply_markup=_grammar_topics_keyboard(lvl))
        except Exception:
            await msg.answer(text, reply_markup=_grammar_topics_keyboard(lvl))
        return

    if action == "t":
        try:
            lvl = int(parts[2])
            topics = GRAMMAR_TOPICS[lvl]
            topic = random.choice(topics) if parts[3] == "r" else topics[int(parts[3])]
            level_desc = GRAMMAR_LEVELS[lvl][2]
        except (ValueError, IndexError):
            await callback.answer("Xatolik", show_alert=True)
            return
        await callback.answer("⏳ Dars tayyorlanmoqda...")
        try:
            await msg.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await bot.send_chat_action(msg.chat.id, "typing")
        try:
            data = await asyncio.to_thread(_generate_grammar_sync, topic, level_desc)
        except Exception as e:
            logger.exception("Grammar darsini yaratishda xatolik: %s", e)
            await msg.answer("❌ Dars tayyorlanmadi. /grammar bilan qayta urinib ko'ring.")
            return
        examples = "\n".join(f"▫️ <b>{html.escape(en)}</b>\n    <i>{html.escape(uz)}</i>" for en, uz in data["examples"])
        lesson = f"📘 <b>{html.escape(data['title'])}</b>\n\n{html.escape(data['rule'])}"
        if examples:
            lesson += f"\n\n✨ <b>Misollar:</b>\n{examples}"
        if data["mistake"]:
            lesson += f"\n\n⚠️ <b>Ko'p uchraydigan xato:</b> {html.escape(data['mistake'])}"
        active_grammar[user_id] = {"topic": topic, "level": lvl, "exercises": data["exercises"],
                                   "idx": 0, "score": 0, "answered": True}
        await msg.answer(lesson, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📝 Mashqni boshlash", callback_data="gr:go")]]))
        return

    g = active_grammar.get(user_id)
    if not g:
        await callback.answer("Bu sessiya tugagan. /grammar bilan qayta boshlang.", show_alert=True)
        return

    if action == "go":
        if g.get("started"):
            await callback.answer()
            return
        g["started"] = True
        g["answered"] = False
        await callback.answer()
        try:
            await msg.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        text, kb = _grammar_question_view(g)
        await msg.answer(text, reply_markup=kb)
        return

    if action == "a":
        if g["answered"]:
            await callback.answer()
            return
        try:
            choice = int(parts[2])
        except (ValueError, IndexError):
            await callback.answer()
            return
        g["answered"] = True
        q = g["exercises"][g["idx"]]
        letters = ["A", "B", "C", "D"]
        good = q["correct_index"]
        if choice == good:
            g["score"] += 1
            result = "✅ <b>To'g'ri!</b>"
            await callback.answer("✅ To'g'ri!")
        else:
            result = f"❌ <b>Noto'g'ri.</b> To'g'ri javob: <b>{letters[good]}) {html.escape(q['options'][good])}</b>"
            await callback.answer("❌ Noto'g'ri")
        done = (f"📝 <b>Mashq {g['idx'] + 1}/{len(g['exercises'])}</b>\n\n{html.escape(q['sentence'])}\n\n{result}")
        if q["explanation"]:
            done += f"\n\n💡 <i>{html.escape(q['explanation'])}</i>"
        try:
            await msg.edit_text(done, reply_markup=None)
        except Exception:
            pass

        g["idx"] += 1
        if g["idx"] < len(g["exercises"]):
            g["answered"] = False
            text, kb = _grammar_question_view(g)
            await msg.answer(text, reply_markup=kb)
            return

        total = len(g["exercises"])
        score = g["score"]
        pct = round(score / total * 100)
        verdict = ("🏆 Ajoyib! Bu mavzuni yaxshi o'zlashtirdingiz." if pct >= 80 else
                   "👍 Yomon emas! Yana bir bor mashq qilsangiz, mukammal bo'ladi." if pct >= 50 else
                   "📚 Dars matnini qayta o'qib, yana urinib ko'ring — albatta chiqadi!")
        topic, lvl = g["topic"], g["level"]
        active_grammar.pop(user_id, None)
        await msg.answer(
            f"🏁 <b>{html.escape(topic)}</b> — natija\n\n🎯 <b>{score}/{total}</b> ({pct}%)\n\n{verdict}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔁 Shu darajadan boshqa mavzu", callback_data=f"gr:l:{lvl}")],
                [InlineKeyboardButton(text="⬅️ Darajalar", callback_data="gr:menu")],
            ]),
        )
        return

    await callback.answer()


# --- 💎 Premium: IELTS Writing tahlili (⭐ Telegram Stars) -----------------
# Foydalanuvchi WRITING_PRICE_STARS ⭐ to'lab, inshosini yuboradi — AI band ball, xatolar va
# yaxshilangan variantni beradi. Bot egasi (admin) uchun BEPUL.
# To'lov muvaffaqiyatli bo'lsa "kredit" yoziladi (Redis + xotira); tahlil
# muvaffaqiyatli yuborilgandagina sarflanadi, shuning uchun xatolikda pul yo'qolmaydi.

WRITING_CREDIT_PREFIX = "ielts_bot:writing_credit:"
WRITING_MIN_WORDS = 120   # mock uchun (rasmiy imtihonda Task 2 — kamida 250 so'z)
WRITING_MAX_CHARS = 4500
_writing_credit_mem: set = set()
awaiting_essay: dict = {}   # user_id -> berilgan IELTS Writing topshirig'i (matn)

_FALLBACK_WRITING_TASKS = [
    "Some people believe that children should start learning a foreign language at primary school, while others think it is better to start in secondary school. Discuss both views and give your own opinion.",
    "In many countries, more and more people are choosing to work from home. Do the advantages of this trend outweigh the disadvantages?",
    "Some people think that governments should spend money on public transport rather than building new roads. To what extent do you agree or disagree?",
    "Nowadays many people spend a lot of time on social media. What are the causes of this, and what effects does it have on society?",
    "Some argue that university education should be free for everyone. Others believe students should pay for it. Discuss both views and give your opinion.",
]
_WRITING_TASK_TYPES = [
    "opinion (to what extent do you agree or disagree)", "discussion (discuss both views and give your opinion)",
    "problem and solution", "advantages and disadvantages", "causes and effects",
]


def _generate_writing_task_sync() -> str:
    import random
    topic = random.choice(["education", "technology", "environment", "health", "work and careers", "society and family",
                           "culture and globalisation", "transport and cities", "crime and law", "media and advertising"])
    data = _llm_json_sync(
        f"Write ONE original IELTS Writing Task 2 question about {topic}. Essay type: {random.choice(_WRITING_TASK_TYPES)}. "
        "Use authentic IELTS wording, 1-3 sentences, English only. "
        'Reply ONLY as JSON: {"task": "..."}',
        temperature=1.0,
    )
    task = str(data.get("task", "")).strip()
    if len(task) < 30:
        raise ValueError("Task juda qisqa")
    return task


async def _give_writing_task(message: Message, uid: int, intro: str = ""):
    """Mock testdagidek: IELTS Writing Task 2 topshirig'ini beradi va inshoni kutadi."""
    import random
    await bot.send_chat_action(message.chat.id, "typing")
    try:
        task = await asyncio.to_thread(_generate_writing_task_sync)
    except Exception as e:
        logger.warning("Writing task generatsiyasida xatolik, zaxira ishlatiladi: %s", e)
        task = random.choice(_FALLBACK_WRITING_TASKS)
    awaiting_essay[uid] = task
    await message.answer(
        f"{intro}✍️ <b>IELTS WRITING — Task 2</b>\n\n"
        f"📋 <b>Topshiriq:</b>\n<i>{html.escape(task)}</i>\n\n"
        f"⏱ Vaqt: 40 daqiqa (mock uchun cheklov yo'q)\n"
        f"📝 Kamida <b>{WRITING_MIN_WORDS} so'z</b> (rasmiy imtihonda 250) — inshoni bitta xabar qilib yuboring.\n"
        "📌 Reja: kirish → 2 ta asosiy paragraf → xulosa.\n\n"
        "<i>Bekor qilish: /stop</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔄 Boshqa mavzu", callback_data="wr:new")]]),
    )


def _is_admin(user_id: int) -> bool:
    return bool(ADMIN_USER_ID) and str(user_id) == str(ADMIN_USER_ID)


async def _has_writing_credit(user_id: int) -> bool:
    if user_id in _writing_credit_mem:
        return True
    if redis_client is not None:
        try:
            return bool(await redis_client.exists(f"{WRITING_CREDIT_PREFIX}{user_id}"))
        except Exception as e:
            logger.warning("Writing kreditini o'qishda xatolik: %s", e)
    return False


async def _grant_writing_credit(user_id: int):
    _writing_credit_mem.add(user_id)
    if redis_client is not None:
        try:
            await redis_client.set(f"{WRITING_CREDIT_PREFIX}{user_id}", "1", ex=30 * 24 * 3600)
        except Exception as e:
            logger.warning("Writing kreditini yozishda xatolik: %s", e)


async def _consume_writing_credit(user_id: int):
    _writing_credit_mem.discard(user_id)
    if redis_client is not None:
        try:
            await redis_client.delete(f"{WRITING_CREDIT_PREFIX}{user_id}")
        except Exception as e:
            logger.warning("Writing kreditini o'chirishda xatolik: %s", e)


def _analyze_essay_sync(essay: str, task: str = "") -> dict:
    return _llm_json_sync(
        "You are a strict but encouraging certified IELTS Writing examiner. Evaluate the following essay "
        "written for this IELTS Writing Task 2 question (judge Task Response against it): "
        f"\"{task or 'free topic'}\". Explain in UZBEK (Latin script); keep quoted fragments and corrections in English. "
        "Use IELTS half-band scores (e.g. 6.5). Reply ONLY as JSON: "
        '{"task_response": 6.0, "coherence": 6.0, "lexical": 6.0, "grammar": 6.0, "overall": 6.0, '
        '"strengths": ["...","..."], '
        '"errors": [{"wrong": "quote from essay", "right": "corrected", "why": "short Uzbek reason"}] (3-5 items), '
        '"vocab": ["better word/phrase suggestion", "..."] (3-5 items), '
        '"improved_paragraph": "rewrite the weakest paragraph at a higher band level (English)", '
        '"next_steps": ["short Uzbek actionable tip", "..."] (2-3 items)}\n\nESSAY:\n' + essay,
        temperature=0.4,
    )


def _format_essay_report(d: dict) -> str:
    def band(k):
        try:
            return f"{float(d.get(k, 0)):.1f}"
        except (TypeError, ValueError):
            return "—"
    e = html.escape
    out = [
        "📝 <b>IELTS WRITING — TAHLIL</b>\n",
        f"🎯 Task Response: <b>{band('task_response')}</b>",
        f"🔗 Coherence & Cohesion: <b>{band('coherence')}</b>",
        f"📚 Lexical Resource: <b>{band('lexical')}</b>",
        f"✍️ Grammar: <b>{band('grammar')}</b>",
        f"\n🏆 <b>Overall Band: {band('overall')}</b>",
    ]
    st = [x for x in (d.get("strengths") or []) if str(x).strip()]
    if st:
        out.append("\n💪 <b>Kuchli tomonlar:</b>\n" + "\n".join(f"✅ {e(str(x))}" for x in st[:4]))
    errs = [x for x in (d.get("errors") or []) if isinstance(x, dict)]
    if errs:
        out.append("\n🔧 <b>Xatolar va tuzatishlar:</b>")
        for x in errs[:5]:
            out.append(f"❌ <i>{e(str(x.get('wrong', '')))}</i>\n✅ <b>{e(str(x.get('right', '')))}</b>\n💡 {e(str(x.get('why', '')))}")
    vc = [x for x in (d.get("vocab") or []) if str(x).strip()]
    if vc:
        out.append("\n📚 <b>Boy lug'at:</b>\n" + "\n".join(f"▫️ {e(str(x))}" for x in vc[:5]))
    ip = str(d.get("improved_paragraph", "")).strip()
    if ip:
        out.append(f"\n✨ <b>Yaxshilangan paragraf:</b>\n<i>{e(ip)}</i>")
    ns = [x for x in (d.get("next_steps") or []) if str(x).strip()]
    if ns:
        out.append("\n🚀 <b>Keyingi qadamlar:</b>\n" + "\n".join(f"{i}. {e(str(x))}" for i, x in enumerate(ns[:3], 1)))
    return "\n".join(out)


async def _send_long(message: Message, text: str):
    while text:
        if len(text) <= 4000:
            await message.answer(text)
            return
        cut = text.rfind("\n", 0, 4000)
        cut = cut if cut > 1000 else 4000
        await message.answer(text[:cut])
        text = text[cut:].lstrip("\n")


@dp.message(Command("writing"))
async def cmd_writing(message: Message, state: FSMContext):
    uid = message.from_user.id
    if await state.get_state() is not None:
        await message.answer("⏳ Avval joriy testni tugating yoki /stop yuboring.")
        return

    pro_ok = False
    if not _is_admin(uid) and not await _has_writing_credit(uid) and await _is_pro(uid):
        pro_ok = await _pro_use(uid, "writing")
        if not pro_ok:
            await message.answer(_pr("limit", l=PRO_DAILY_LIMIT))
    if _is_admin(uid) or pro_ok or await _has_writing_credit(uid):
        free = "🎁 <i>Siz uchun bepul.</i>\n\n" if _is_admin(uid) else ("⭐ <i>PRO — tekin.</i>\n\n" if pro_ok else "✅ <i>To'lov qabul qilingan.</i>\n\n")
        await _give_writing_task(message, uid, f"💎 {free}")
        return

    await message.answer(
        "💎 <b>IELTS Writing tahlili (Premium)</b>\n\n"
        "Xuddi mock testdagidek: men IELTS <b>Writing Task 2</b> topshirig'ini beraman, "
        "siz insho yozasiz — men esa IELTS mezonlari bo'yicha tekshiraman:\n"
        "🏆 band ballni (4 ta mezon bo'yicha)\n"
        "🔧 xatolar va ularning tuzatilishini\n"
        "📚 boy lug'at takliflarini\n"
        "✨ yaxshilangan paragrafni beraman.\n\n"
        f"Narxi: <b>{WRITING_PRICE_STARS} ⭐ Telegram Stars</b>"
    )
    try:
        await bot.send_invoice(
            chat_id=message.chat.id,
            title="IELTS Writing tahlili",
            description="Inshongiz uchun band ball, xatolar tahlili va yaxshilangan variant.",
            payload=f"writing:{uid}",
            currency="XTR",
            prices=[LabeledPrice(label="Writing tahlili", amount=WRITING_PRICE_STARS)],
            provider_token="",
        )
    except Exception as e:
        logger.exception("Invoice yuborishda xatolik: %s", e)
        await message.answer("❌ To'lov oynasini ochib bo'lmadi. Birozdan so'ng qayta urinib ko'ring.")


def _parse_payload(payload: str) -> tuple[str, str, bool]:
    parts = (payload or "").split(":")
    return (parts[0] if parts else ""), (parts[1] if len(parts) > 1 else ""), (len(parts) > 2 and parts[2] == "app")


@dp.pre_checkout_query()
async def handle_pre_checkout(query: PreCheckoutQuery):
    product, uid_s, _app = _parse_payload(query.invoice_payload)
    price = {"writing": WRITING_PRICE_STARS, "speaking": SPEAKING_PRICE_STARS, "pro": PRO_PRICE_STARS}.get(product)
    ok = query.currency == "XTR" and price is not None and uid_s == str(query.from_user.id) and price == query.total_amount
    if ok:
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message="Payment details do not match. Please try again from the bot menu.")


@dp.message(F.successful_payment)
async def handle_successful_payment(message: Message, state: FSMContext):
    pay = message.successful_payment
    uid = message.from_user.id
    product, _uid_s, from_app = _parse_payload(pay.invoice_payload)
    logger.info("Stars to'lov: user=%s amount=%s payload=%s charge_id=%s", uid, pay.total_amount,
                pay.invoice_payload, pay.telegram_payment_charge_id)
    asyncio.create_task(log_activity(uid, message.from_user.full_name or "", message.from_user.username or "",
                                     f"⭐ to'ladi: {pay.invoice_payload.split(':')[0]} ({pay.total_amount}⭐)"))
    if product == "pro":
        exp = await _grant_pro(uid)
        await message.answer(_pr("thanks", date=_pro_date(exp), l=PRO_DAILY_LIMIT),
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_app_button()]]))
        return
    if product == "speaking":
        await _grant_speaking_credit(uid)
        if from_app:
            await message.answer(tr("paid_app"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_app_button("english")]]))
        else:
            await _start_full_speaking(message, state, uid, _sp("thanks"))
        return
    await _grant_writing_credit(uid)
    if from_app:
        await message.answer(tr("paid_app"), reply_markup=InlineKeyboardMarkup(inline_keyboard=[[_app_button("english")]]))
    else:
        await _give_writing_task(message, uid, "✅ <b>To'lov qabul qilindi, rahmat!</b> ⭐\n\n")


@dp.callback_query(F.data == "wr:new")
async def handle_writing_new_task(callback: CallbackQuery):
    uid = callback.from_user.id
    if uid not in awaiting_essay:
        await callback.answer("Bu sessiya tugagan. /writing bilan qayta boshlang.", show_alert=True)
        return
    await callback.answer()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await _give_writing_task(callback.message, uid)


async def _essay_filter(message: Message) -> bool:
    return bool(message.text) and not message.text.startswith("/") and message.from_user.id in awaiting_essay


@dp.message(F.text, _essay_filter)
async def handle_essay(message: Message):
    uid = message.from_user.id
    essay = message.text.strip()
    words = len(essay.split())
    if words < WRITING_MIN_WORDS:
        await message.answer(f"✍️ Insho juda qisqa ({words} so'z). Kamida {WRITING_MIN_WORDS} so'z yozing va qayta yuboring.")
        return
    if len(essay) > WRITING_MAX_CHARS:
        await message.answer(f"✂️ Insho juda uzun. Iltimos, {WRITING_MAX_CHARS} belgidan oshmasin.")
        return

    task = awaiting_essay.pop(uid)
    await message.answer("🔎 Inshoyingiz tahlil qilinmoqda, 20–40 soniya kuting...")
    await bot.send_chat_action(message.chat.id, "typing")
    try:
        data = await asyncio.to_thread(_analyze_essay_sync, essay, task)
        report = _format_essay_report(data)
    except Exception as e:
        logger.exception("Essay tahlilida xatolik: %s", e)
        awaiting_essay[uid] = task  # kredit saqlanadi — qayta yuborish mumkin
        await message.answer("❌ Tahlilda xatolik yuz berdi. To'lovingiz saqlandi — inshongizni qayta yuboring.")
        return

    await _send_long(message, f"📋 <i>{html.escape(task)}</i>\n📏 So'zlar soni: <b>{words}</b>\n\n" + report)
    if not _is_admin(uid):
        await _consume_writing_credit(uid)
        await message.answer("🔁 Yana tahlil qildirish uchun /writing yuboring.")


# --- 💎 Premium: IELTS Speaking FULL MOCK (⭐ Telegram Stars) ---------------
# Haqiqiy imtihondagidek: Part 1 (3 mavzu, ~10 savol) + Part 2 (cue card) + Part 3 (5 savol).
# Imtihon davomida har javobga izoh berilmaydi (real imtihondagidek) — oxirida bitta batafsil
# hisobot: 4 mezon bo'yicha band, xatolar, lug'at, reja. Bot egasi (admin) uchun BEPUL.
# Kredit faqat hisobot muvaffaqiyatli yuborilgandan keyin sarflanadi.

SPEAKING_CREDIT_PREFIX = "ielts_bot:speaking_credit:"
_speaking_credit_mem: set = set()


class FullSpeakStates(StatesGroup):
    active = State()


_SP = {
    "intro": {
        "uz": "💎 <b>IELTS Speaking — TO'LIQ MOCK (Premium)</b>\n\nHaqiqiy imtihondek, 3 qismli:\n🗣 <b>Part 1</b> — tanishuv va 3 ta mavzu (~10 savol)\n🖼 <b>Part 2</b> — 2 ta rasmni tasvirlash va solishtirish (1–2 daqiqa)\n💭 <b>Part 3</b> — chuqur muhokama (5 savol)\n\nImtihon davomida izoh berilmaydi (real imtihondek), examiner siz bilan jonli suhbatlashgandek gapiradi. Oxirida batafsil hisobot: 4 mezon bo'yicha band, xatolar va tuzatishlar, lug'at takliflari, o'quv rejasi.\n\nNarxi: <b>{p} ⭐ Telegram Stars</b>",
        "ru": "💎 <b>IELTS Speaking — ПОЛНЫЙ МОК (Premium)</b>\n\nКак на настоящем экзамене, 3 части:\n🗣 <b>Part 1</b> — знакомство и 3 темы (~10 вопросов)\n🖼 <b>Part 2</b> — описание и сравнение 2 картинок (1–2 минуты)\n💭 <b>Part 3</b> — углублённое обсуждение (5 вопросов)\n\nВо время экзамена комментариев нет (как на реальном). В конце — подробный отчёт: балл по 4 критериям, ошибки и исправления, лексика, план обучения.\n\nЦена: <b>{p} ⭐ Telegram Stars</b>",
        "en": "💎 <b>IELTS Speaking — FULL MOCK (Premium)</b>\n\nJust like the real exam, 3 parts:\n🗣 <b>Part 1</b> — introduction and 3 topics (~10 questions)\n🖼 <b>Part 2</b> — describe and compare 2 pictures (1–2 minutes)\n💭 <b>Part 3</b> — in-depth discussion (5 questions)\n\nNo feedback during the exam (like the real thing). At the end you get a detailed report: band for all 4 criteria, mistakes and corrections, vocabulary upgrades and a study plan.\n\nPrice: <b>{p} ⭐ Telegram Stars</b>",
    },
    "inv_title": {"uz": "IELTS Speaking to'liq mock", "ru": "Полный мок IELTS Speaking", "en": "IELTS Speaking full mock"},
    "inv_desc": {
        "uz": "3 qismli speaking imtihoni va batafsil band hisoboti.",
        "ru": "Экзамен Speaking из 3 частей и подробный отчёт с баллами.",
        "en": "A 3-part speaking exam and a detailed band report.",
    },
    "inv_label": {"uz": "Speaking mock", "ru": "Speaking мок", "en": "Speaking mock"},
    "inv_fail": {
        "uz": "❌ To'lov oynasini ochib bo'lmadi. Birozdan so'ng qayta urinib ko'ring.",
        "ru": "❌ Не удалось открыть окно оплаты. Попробуйте чуть позже.",
        "en": "❌ Couldn't open the payment window. Please try again shortly.",
    },
    "busy": {
        "uz": "⏳ Avval joriy testni tugating yoki /stop yuboring.",
        "ru": "⏳ Сначала завершите текущий тест или отправьте /stop.",
        "en": "⏳ Finish your current test first or send /stop.",
    },
    "free": {"uz": "🎁 <i>Siz uchun bepul.</i>\n\n", "ru": "🎁 <i>Для вас бесплатно.</i>\n\n", "en": "🎁 <i>Free for you.</i>\n\n"},
    "paid": {"uz": "✅ <i>To'lov qabul qilingan.</i>\n\n", "ru": "✅ <i>Оплата получена.</i>\n\n", "en": "✅ <i>Payment received.</i>\n\n"},
    "thanks": {
        "uz": "✅ <b>To'lov qabul qilindi, rahmat!</b> ⭐\n\n",
        "ru": "✅ <b>Оплата получена, спасибо!</b> ⭐\n\n",
        "en": "✅ <b>Payment received, thank you!</b> ⭐\n\n",
    },
    "preparing": {
        "uz": "⏳ Imtihon savollari tayyorlanmoqda...",
        "ru": "⏳ Готовлю вопросы экзамена...",
        "en": "⏳ Preparing your exam questions...",
    },
    "gen_fail": {
        "uz": "❌ Savollarni tayyorlab bo'lmadi. To'lovingiz saqlandi — /speaking bilan qayta urinib ko'ring.",
        "ru": "❌ Не удалось подготовить вопросы. Оплата сохранена — попробуйте снова через /speaking.",
        "en": "❌ Couldn't prepare the questions. Your payment is saved — try again with /speaking.",
    },
    "start": {
        "uz": "🎙 <b>Imtihon boshlandi!</b>\nHar bir savolga <b>ovozli xabar</b> bilan javob bering. Savol matn va ovoz ko'rinishida keladi. To'xtatish: /stop",
        "ru": "🎙 <b>Экзамен начался!</b>\nОтвечайте на каждый вопрос <b>голосовым сообщением</b>. Вопрос приходит текстом и голосом. Остановить: /stop",
        "en": "🎙 <b>The exam has started!</b>\nAnswer every question with a <b>voice message</b>. Each question comes as text and audio. To stop: /stop",
    },
    "p1": {
        "uz": "🗣 <b>PART 1</b> — Introduction & Interview\nQisqa, tabiiy javob bering (2–4 gap).",
        "ru": "🗣 <b>PART 1</b> — Introduction & Interview\nОтвечайте коротко и естественно (2–4 предложения).",
        "en": "🗣 <b>PART 1</b> — Introduction & Interview\nGive short, natural answers (2–4 sentences).",
    },
    "p2": {
        "uz": "🖼 <b>PART 2</b> — Picture Description\n⏱ Rasmlarga qarang va 1 daqiqa tayyorlaning. So'ng <b>1–2 daqiqa</b> to'xtamasdan gapirib, bitta ovozli xabar yuboring: har bir rasmda nima borligini tasvirlang, ularni solishtiring va qaysi biri sizga ko'proq yoqishini asoslang.",
        "ru": "🖼 <b>PART 2</b> — Picture Description\n⏱ Посмотрите на картинки и подготовьтесь 1 минуту. Затем говорите <b>1–2 минуты</b> без остановки и отправьте одно голосовое: опишите, что на каждой картинке, сравните их и объясните, какая вам нравится больше.",
        "en": "🖼 <b>PART 2</b> — Picture Description\n⏱ Look at the pictures and take 1 minute to prepare. Then speak for <b>1–2 minutes</b> without stopping and send one voice message: describe what you see in each picture, compare them, and explain which one you prefer.",
    },
    "pic_cap_a": {"uz": "🅰️ Rasm A", "ru": "🅰️ Картинка A", "en": "🅰️ Picture A"},
    "pic_cap_b": {"uz": "🅱️ Rasm B", "ru": "🅱️ Картинка B", "en": "🅱️ Picture B"},
    "pic_fallback": {
        "uz": "⚠️ Rasmlarni yuklab bo'lmadi, shuning uchun ularni matn bilan tasvirlayman:\n\n🅰️ {a}\n🅱️ {b}",
        "ru": "⚠️ Не удалось загрузить картинки, поэтому описываю их словами:\n\n🅰️ {a}\n🅱️ {b}",
        "en": "⚠️ I couldn't load the pictures, so here they are in words:\n\n🅰️ {a}\n🅱️ {b}",
    },
    "p3": {
        "uz": "💭 <b>PART 3</b> — Discussion\nFikringizni asoslab, kengroq javob bering (4–6 gap).",
        "ru": "💭 <b>PART 3</b> — Discussion\nОбосновывайте мнение, отвечайте развёрнуто (4–6 предложений).",
        "en": "💭 <b>PART 3</b> — Discussion\nSupport your opinions and give fuller answers (4–6 sentences).",
    },
    "q": {"uz": "🗣 <b>Savol {i}/{n}:</b>\n{q}", "ru": "🗣 <b>Вопрос {i}/{n}:</b>\n{q}", "en": "🗣 <b>Question {i}/{n}:</b>\n{q}"},
    "cue": {"uz": "🎴 <b>Topshiriq:</b>\n{q}", "ru": "🎴 <b>Задание:</b>\n{q}", "en": "🎴 <b>Task:</b>\n{q}"},
    "ack": {"uz": "✅ Qabul qilindi.", "ru": "✅ Принято.", "en": "✅ Noted."},
    "voice_only": {
        "uz": "🎙 Iltimos, javobni faqat <b>ovozli xabar</b> bilan yuboring.",
        "ru": "🎙 Пожалуйста, отвечайте только <b>голосовым сообщением</b>.",
        "en": "🎙 Please answer with a <b>voice message</b> only.",
    },
    "dl_fail": {
        "uz": "❌ Ovozli xabarni yuklab olishda xatolik. Qayta yuboring.",
        "ru": "❌ Ошибка при загрузке голосового. Отправьте ещё раз.",
        "en": "❌ Couldn't download the voice message. Please send it again.",
    },
    "stt_fail": {
        "uz": "❌ Ovozni matnga aylantirib bo'lmadi. Qayta yuboring.",
        "ru": "❌ Не удалось распознать речь. Отправьте ещё раз.",
        "en": "❌ Couldn't transcribe your voice. Please send it again.",
    },
    "no_speech": {
        "uz": "⚠️ Nutq aniqlanmadi. Aniqroq gapirib, qayta yuboring.",
        "ru": "⚠️ Речь не распознана. Говорите чётче и отправьте ещё раз.",
        "en": "⚠️ No speech detected. Please speak clearly and send it again.",
    },
    "analyzing": {
        "uz": "✅ <b>Imtihon tugadi!</b>\n\n📊 Hisobot tayyorlanmoqda (20–40 soniya)...",
        "ru": "✅ <b>Экзамен завершён!</b>\n\n📊 Готовлю отчёт (20–40 секунд)...",
        "en": "✅ <b>Exam finished!</b>\n\n📊 Preparing your report (20–40 seconds)...",
    },
    "report_fail": {
        "uz": "❌ Hisobotni tayyorlashda xatolik. To'lovingiz saqlandi — /speaking bilan qayta urinib ko'ring.",
        "ru": "❌ Ошибка при подготовке отчёта. Оплата сохранена — попробуйте снова через /speaking.",
        "en": "❌ Couldn't build the report. Your payment is saved — try again with /speaking.",
    },
    "again": {
        "uz": "🔁 Yana mock topshirish uchun /speaking yuboring.",
        "ru": "🔁 Чтобы пройти мок ещё раз, отправьте /speaking.",
        "en": "🔁 Send /speaking to take another mock.",
    },
    "h_title": {"uz": "🏁 <b>IELTS SPEAKING — TO'LIQ HISOBOT</b>", "ru": "🏁 <b>IELTS SPEAKING — ПОЛНЫЙ ОТЧЁТ</b>", "en": "🏁 <b>IELTS SPEAKING — FULL REPORT</b>"},
    "h_overall": {"uz": "🏆 <b>Overall Band: {b}</b>", "ru": "🏆 <b>Overall Band: {b}</b>", "en": "🏆 <b>Overall Band: {b}</b>"},
    "h_summary": {"uz": "💬 <b>Examiner xulosasi:</b>", "ru": "💬 <b>Вывод экзаменатора:</b>", "en": "💬 <b>Examiner's summary:</b>"},
    "h_strong": {"uz": "💪 <b>Kuchli tomonlar:</b>", "ru": "💪 <b>Сильные стороны:</b>", "en": "💪 <b>Strengths:</b>"},
    "h_errors": {"uz": "🔧 <b>Xatolar va tuzatishlar:</b>", "ru": "🔧 <b>Ошибки и исправления:</b>", "en": "🔧 <b>Mistakes and corrections:</b>"},
    "h_vocab": {"uz": "📚 <b>Lug'atni boyiting:</b>", "ru": "📚 <b>Обогатите лексику:</b>", "en": "📚 <b>Vocabulary upgrades:</b>"},
    "h_plan": {"uz": "🚀 <b>Keyingi qadamlar:</b>", "ru": "🚀 <b>Следующие шаги:</b>", "en": "🚀 <b>Next steps:</b>"},
    "h_note": {
        "uz": "ℹ️ <i>Talaffuz bahosi taxminiy: u matnga aylantirilgan nutq asosida chiqarilgan, ovozning o'zini tahlil qilmaydi.</i>",
        "ru": "ℹ️ <i>Оценка произношения приблизительная: она основана на расшифрованном тексте, а не на анализе самого звука.</i>",
        "en": "ℹ️ <i>The pronunciation score is an estimate: it is based on the transcribed text, not on analysis of the audio itself.</i>",
    },
}


def _sp(key: str, **kw) -> str:
    d = _SP[key]
    t = d.get(cur_lang()) or d["en"]
    return t.format(**kw) if kw else t


async def _has_speaking_credit(user_id: int) -> bool:
    if user_id in _speaking_credit_mem:
        return True
    if redis_client is not None:
        try:
            return bool(await redis_client.exists(f"{SPEAKING_CREDIT_PREFIX}{user_id}"))
        except Exception as e:
            logger.warning("Speaking kreditini o'qishda xatolik: %s", e)
    return False


async def _grant_speaking_credit(user_id: int):
    _speaking_credit_mem.add(user_id)
    if redis_client is not None:
        try:
            await redis_client.set(f"{SPEAKING_CREDIT_PREFIX}{user_id}", "1", ex=30 * 24 * 3600)
        except Exception as e:
            logger.warning("Speaking kreditini yozishda xatolik: %s", e)


async def _consume_speaking_credit(user_id: int):
    _speaking_credit_mem.discard(user_id)
    if redis_client is not None:
        try:
            await redis_client.delete(f"{SPEAKING_CREDIT_PREFIX}{user_id}")
        except Exception as e:
            logger.warning("Speaking kreditini o'chirishda xatolik: %s", e)


def _generate_full_speaking_sync() -> list:
    """To'liq mock uchun tartiblangan savollar ro'yxatini qaytaradi:
    [{"part": 1|2|3, "q": str, "lead": str (ixtiyoriy), ...}, ...]
    Part 2 elementida: "pics": [qidiruv A, qidiruv B], "desc": [matn A, matn B]."""
    d = _llm_json_sync(
        "You are a real, friendly but professional IELTS Speaking examiner running a live face-to-face test. "
        "Create a fresh FULL Speaking test in the examiner's natural spoken voice. Reply ONLY as JSON: "
        '{"part1": {"topics": [{"lead": "Let\'s talk about your studies.", "questions": ["q1","q2","q3","q4"]}, '
        '{"lead": "Now let\'s talk about ...", "questions": ["q1","q2","q3"]}, '
        '{"lead": "Now I\'d like to ask you about ...", "questions": ["q1","q2","q3"]}]}, '
        '"part2": {"theme": "short theme", "pic_a_query": "2-4 word photo search phrase", "pic_b_query": "2-4 word photo search phrase", '
        '"pic_a_desc": "one-sentence description of a typical photo for query A", "pic_b_desc": "same for B"}, '
        '"part3": ["q1","q2","q3","q4","q5"]}. '
        "Rules: the first Part 1 topic is the candidate's work or studies (or home/hometown); the other two are everyday topics "
        "(food, free time, technology, transport, weather, shopping...). Questions are short and sound like speech "
        "(e.g. 'Do you work or are you a student?', 'What do you enjoy most about it?'). "
        "Part 2 is a TWO-PICTURE task: the two photos show two contrasting but comparable situations on one theme "
        "(e.g. two ways of travelling, two kinds of homes, studying alone vs in a group). Search phrases must describe concrete, "
        "photographable scenes (no abstract words, no people's names, no brands). "
        "Part 3: five abstract, analytical questions linked to the Part 2 theme. All text in English.",
        temperature=0.9,
    )
    items = []
    for t in (d.get("part1") or {}).get("topics") or []:
        qs = [str(q).strip() for q in (t.get("questions") or []) if str(q).strip()]
        lead = str(t.get("lead", "")).strip()
        for i, q in enumerate(qs):
            items.append({"part": 1, "q": q, "lead": lead if i == 0 else ""})
    p2 = d.get("part2") or {}
    qa, qb = str(p2.get("pic_a_query", "")).strip(), str(p2.get("pic_b_query", "")).strip()
    da, db = str(p2.get("pic_a_desc", "")).strip(), str(p2.get("pic_b_desc", "")).strip()
    theme = str(p2.get("theme", "")).strip() or "everyday life"
    p3 = [str(q).strip() for q in (d.get("part3") or []) if str(q).strip()]
    if len([i for i in items if i["part"] == 1]) < 6 or not (qa and qb) or len(p3) < 3:
        raise ValueError("Speaking test generation returned incomplete data")
    # Real imtihondagidek: tanishuv savollari har doim boshida
    items = [
        {"part": 1, "q": "Could you tell me your full name, please? And what should I call you?",
         "lead": "Good day! 😊 I'm your examiner today. This is the Speaking test."},
        {"part": 1, "q": "And where are you from?", "lead": ""},
    ] + items
    items.append({
        "part": 2,
        "q": "I'm going to show you two pictures. Please describe each one, say how they are similar or different, "
             "and explain which one you would prefer and why. You have one minute to prepare, then please speak for one to two minutes.",
        "lead": "Now we're moving on to Part 2.",
        "pics": [qa, qb], "desc": [da or qa, db or qb], "theme": theme,
    })
    for i, q in enumerate(p3[:5]):
        items.append({"part": 3, "q": q, "lead": "Thank you. Let's talk about this topic in more general terms. 💭" if i == 0 else ""})
    return items


def _plain_speech(t: str) -> str:
    """TTS uchun emoji va maxsus belgilarni olib tashlaydi."""
    return re.sub(r"\s+", " ", t.encode("ascii", "ignore").decode()).strip()


async def _fetch_commons_photo(query: str, used: set) -> bytes | None:
    """Wikimedia Commons'dan so'rovga mos JPEG fotosuratni yuklab oladi (kalit talab qilinmaydi)."""
    import aiohttp
    params = {
        "action": "query", "generator": "search", "gsrsearch": f"{query} filetype:bitmap",
        "gsrnamespace": "6", "gsrlimit": "20", "prop": "imageinfo", "iiprop": "url|size|mime",
        "iiurlwidth": "960", "format": "json",
    }
    headers = {"User-Agent": "TalkBot/1.0 (educational Telegram bot; contact via Telegram)"}
    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as sess:
        async with sess.get("https://commons.wikimedia.org/w/api.php", params=params) as r:
            data = await r.json(content_type=None)
        pages = list(((data or {}).get("query") or {}).get("pages", {}).values())
        cands = []
        for pg in pages:
            ii = (pg.get("imageinfo") or [{}])[0]
            if ii.get("mime") != "image/jpeg":
                continue
            if (ii.get("width") or 0) < 800 or (ii.get("height") or 0) < 500:
                continue
            url = ii.get("thumburl") or ii.get("url")
            if url and url not in used:
                cands.append(url)
        random.shuffle(cands)
        for url in cands[:4]:
            try:
                async with sess.get(url) as r:
                    if r.status == 200:
                        body = await r.read()
                        if 20_000 < len(body) < 9_000_000:
                            used.add(url)
                            return body
            except Exception as e:
                logger.warning("Rasm yuklashda xatolik: %s", e)
    return None


async def _fetch_two_pictures(queries: list) -> list | None:
    used: set = set()
    try:
        res = await asyncio.gather(*[_fetch_commons_photo(q, used) for q in queries[:2]], return_exceptions=True)
    except Exception:
        return None
    pics = [r for r in res if isinstance(r, (bytes, bytearray))]
    return pics if len(pics) == 2 else None


def _evaluate_full_speaking_sync(answers: list) -> dict:
    lines = []
    for i, a in enumerate(answers, 1):
        words = len(a["text"].split())
        lines.append(f"{i}. [Part {a['part']}] Q: {a['q']}\n   ({a['sec']} sec, {words} words) A: {a['text'][:1200]}")
    return _llm_json_sync(
        "You are a strict, experienced IELTS Speaking examiner. Below is the full transcript (speech-to-text) of a "
        "mock Speaking test. Judge by official band descriptors for Fluency & Coherence, Lexical Resource, "
        "Grammatical Range & Accuracy and Pronunciation. Pronunciation can only be ESTIMATED from the transcript "
        "(clarity, natural phrasing, signs of mis-recognised words) — be conservative. Take answer length and speaking "
        "speed (words per second) into account; very short Part 2 answers must lower fluency/coherence. "
        "Part 2 was a TWO-PICTURE task (describe both pictures, compare them, give a preference with reasons); "
        "you cannot see the pictures, so judge only the language, structure (describing, comparing, speculating, justifying) and "
        "coherence, not the accuracy of the content. "
        "Use half-band scores. Explain in UZBEK (Latin script); keep quotes and corrections in English. "
        "Reply ONLY as JSON: "
        '{"fluency": 6.0, "lexical": 6.0, "grammar": 6.0, "pronunciation": 6.0, "overall": 6.0, '
        '"summary": "3-4 sentence honest examiner summary", '
        '"strengths": ["...","..."] (2-3 items), '
        '"errors": [{"wrong": "exact quote from candidate", "right": "corrected English", "why": "short reason"}] (4-6 items), '
        '"vocab": ["weak word/phrase -> stronger alternative", "..."] (4-5 items), '
        '"plan": ["concrete study action", "..."] (3 items)}\n\nTRANSCRIPT:\n' + "\n".join(lines),
        temperature=0.3,
    )


def _format_full_speaking_report(d: dict) -> str:
    def band(k):
        try:
            return f"{float(d.get(k, 0)):.1f}"
        except (TypeError, ValueError):
            return "—"
    e = html.escape
    out = [
        _sp("h_title") + "\n",
        f"🗣 Fluency & Coherence: <b>{band('fluency')}</b>",
        f"📚 Lexical Resource: <b>{band('lexical')}</b>",
        f"✍️ Grammatical Range & Accuracy: <b>{band('grammar')}</b>",
        f"🔊 Pronunciation (est.): <b>{band('pronunciation')}</b>",
        "\n" + _sp("h_overall", b=band("overall")),
    ]
    summ = str(d.get("summary", "")).strip()
    if summ:
        out.append("\n" + _sp("h_summary") + "\n" + e(summ))
    st = [x for x in (d.get("strengths") or []) if str(x).strip()]
    if st:
        out.append("\n" + _sp("h_strong") + "\n" + "\n".join(f"✅ {e(str(x))}" for x in st[:3]))
    errs = [x for x in (d.get("errors") or []) if isinstance(x, dict)]
    if errs:
        out.append("\n" + _sp("h_errors"))
        for x in errs[:6]:
            out.append(f"❌ <i>{e(str(x.get('wrong', '')))}</i>\n✅ <b>{e(str(x.get('right', '')))}</b>\n💡 {e(str(x.get('why', '')))}")
    vc = [x for x in (d.get("vocab") or []) if str(x).strip()]
    if vc:
        out.append("\n" + _sp("h_vocab") + "\n" + "\n".join(f"▫️ {e(str(x))}" for x in vc[:5]))
    pl = [x for x in (d.get("plan") or []) if str(x).strip()]
    if pl:
        out.append("\n" + _sp("h_plan") + "\n" + "\n".join(f"{i}. {e(str(x))}" for i, x in enumerate(pl[:3], 1)))
    out.append("\n" + _sp("h_note"))
    return "\n".join(out)


async def _start_full_speaking(message: Message, state: FSMContext, uid: int, prefix: str = ""):
    prep = await message.answer(prefix + _sp("preparing"))
    try:
        items = await asyncio.to_thread(_generate_full_speaking_sync)
    except Exception as e:
        logger.exception("Full speaking savollarini yaratishda xatolik: %s", e)
        await prep.edit_text(_sp("gen_fail"))
        return
    try:
        await prep.delete()
    except Exception:
        pass
    await state.clear()
    await state.set_state(FullSpeakStates.active)
    await state.update_data(sp_items=items, sp_idx=0, sp_answers=[])
    await message.answer(_sp("start"))
    await _ask_full_speaking(message, items, 0)


async def _ask_full_speaking(message: Message, items: list, idx: int):
    it = items[idx]
    part = it["part"]
    lead = it.get("lead") or ""
    if idx == 0 or items[idx - 1]["part"] != part:
        await message.answer(_sp({1: "p1", 2: "p2", 3: "p3"}[part]))
    if part == 2:
        pics = await _fetch_two_pictures(it.get("pics") or [])
        if pics:
            try:
                await bot.send_chat_action(message.chat.id, "upload_photo")
                await message.answer_media_group([
                    InputMediaPhoto(media=BufferedInputFile(pics[0], "picture_a.jpg"), caption=_sp("pic_cap_a")),
                    InputMediaPhoto(media=BufferedInputFile(pics[1], "picture_b.jpg"), caption=_sp("pic_cap_b")),
                ])
            except Exception as e:
                logger.warning("Rasmlarni yuborishda xatolik: %s", e)
                pics = None
        if not pics:
            da, db = (it.get("desc") or ["", ""])[:2]
            await message.answer(_sp("pic_fallback", a=html.escape(da), b=html.escape(db)))
        text = (lead + "\n\n" if lead else "") + _sp("cue", q=it["q"])
        await send_text_and_voice(message, text, tts_text=_plain_speech((lead + " " if lead else "") + it["q"]))
        return
    same = [x for x in items if x["part"] == part]
    n = len(same)
    i = same.index(it) + 1
    text = (lead + "\n\n" if lead else "") + _sp("q", i=i, n=n, q=it["q"])
    await send_text_and_voice(message, text, tts_text=_plain_speech((lead + " " if lead else "") + it["q"]))


@dp.message(Command("speaking"))
async def cmd_speaking(message: Message, state: FSMContext):
    uid = message.from_user.id
    if await state.get_state() is not None:
        await message.answer(_sp("busy"))
        return
    cleanup_user_dir(uid)

    if _is_admin(uid):
        await _start_full_speaking(message, state, uid, _sp("free"))
        return
    if await _has_speaking_credit(uid):
        await _start_full_speaking(message, state, uid, _sp("paid"))
        return
    if await _is_pro(uid):
        if await _pro_use(uid, "speaking"):
            await _start_full_speaking(message, state, uid, "⭐ <i>PRO</i>\n\n")
            return
        await message.answer(_pr("limit", l=PRO_DAILY_LIMIT))

    await message.answer(_sp("intro", p=SPEAKING_PRICE_STARS))
    try:
        await bot.send_invoice(
            chat_id=message.chat.id,
            title=_sp("inv_title"),
            description=_sp("inv_desc"),
            payload=f"speaking:{uid}",
            currency="XTR",
            prices=[LabeledPrice(label=_sp("inv_label"), amount=SPEAKING_PRICE_STARS)],
            provider_token="",
        )
    except Exception as e:
        logger.exception("Speaking invoice yuborishda xatolik: %s", e)
        await message.answer(_sp("inv_fail"))


@dp.message(FullSpeakStates.active, F.voice)
async def handle_full_speaking_voice(message: Message, state: FSMContext):
    uid = message.from_user.id
    data = await state.get_data()
    items = data.get("sp_items") or []
    idx = data.get("sp_idx", 0)
    answers = data.get("sp_answers", [])
    if idx >= len(items):
        await state.clear()
        return

    user_dir = _user_dir(uid)
    ogg_path = os.path.join(user_dir, f"fs_{message.voice.file_unique_id}.ogg")
    try:
        f = await bot.get_file(message.voice.file_id)
        await bot.download_file(f.file_path, destination=ogg_path)
    except Exception as e:
        logger.exception("Full speaking: voice yuklashda xatolik: %s", e)
        await message.answer(_sp("dl_fail"))
        return
    try:
        transcript = await asyncio.to_thread(_transcribe_sync, ogg_path)
    except Exception as e:
        logger.exception("Full speaking: transkripsiya xatoligi: %s", e)
        await message.answer(_sp("stt_fail"))
        return
    finally:
        if os.path.exists(ogg_path):
            try:
                os.remove(ogg_path)
            except OSError:
                pass
    if not transcript:
        await message.answer(_sp("no_speech"))
        return

    it = items[idx]
    answers.append({"part": it["part"], "q": it["q"] + (f" [pictures theme: {it['theme']}]" if it.get("theme") else ""), "text": transcript, "sec": int(message.voice.duration or 0)})
    idx += 1
    await state.update_data(sp_idx=idx, sp_answers=answers)

    if idx < len(items):
        await _ask_full_speaking(message, items, idx)
        return

    await message.answer(_sp("analyzing"))
    await bot.send_chat_action(message.chat.id, "typing")
    try:
        report = await asyncio.to_thread(_evaluate_full_speaking_sync, answers)
        text = _format_full_speaking_report(report)
    except Exception as e:
        logger.exception("Full speaking hisobotida xatolik: %s", e)
        await state.clear()
        cleanup_user_dir(uid)
        await message.answer(_sp("report_fail"))
        return

    await state.clear()
    cleanup_user_dir(uid)
    await _send_long(message, text)
    if not _is_admin(uid):
        await _consume_speaking_credit(uid)
    await message.answer(_sp("again"))


@dp.message(FullSpeakStates.active)
async def handle_full_speaking_wrong_type(message: Message):
    if message.text and message.text.startswith("/"):
        await message.answer(_sp("busy"))
        return
    await message.answer(_sp("voice_only"))


# --- ⭐ PRO obuna (Telegram Stars) ---------------------------------------------
# PRO_DAYS kunlik PRO: /writing va /speaking tekin (adolatli foydalanish limiti bilan).
# Bir martalik to'lov (avtomatik yangilanmaydi) — muddat tugagach qayta olish mumkin;
# faol PRO'da qayta to'lansa muddat ustiga qo'shiladi. Admin uchun doim bepul.

PRO_DAILY_LIMIT = 5          # PRO'da kuniga har bir premium funksiya (writing / speaking) uchun
PRO_KEY_PREFIX = "ielts_bot:pro:"
PRO_USE_PREFIX = "ielts_bot:pro_use:"
_pro_mem: dict = {}          # user_id -> tugash vaqti (unix)
_pro_use_mem: dict = {}      # (kind, user_id, sana) -> son

_PRO = {
    "offer": {
        "uz": "⭐ <b>PRO obuna</b>\n\nUshbu imkoniyatlar {d} kun davomida:\n💎 <b>/writing</b> — IELTS Writing tahlili tekin (kuniga {l} tagacha)\n🎙 <b>/speaking</b> — to'liq Speaking mock tekin (kuniga {l} tagacha)\n🏅 Profilda ⭐ PRO belgisi\n\nAlohida olsangiz: Writing {w} ⭐ + Speaking {s} ⭐ — PRO bilan esa hammasi bir joyda.\n\nNarxi: <b>{p} ⭐ Telegram Stars</b> ({d} kun, avtomatik yangilanmaydi)",
        "ru": "⭐ <b>PRO-подписка</b>\n\nНа {d} дней вы получаете:\n💎 <b>/writing</b> — разбор IELTS Writing бесплатно (до {l} в день)\n🎙 <b>/speaking</b> — полный Speaking мок бесплатно (до {l} в день)\n🏅 Значок ⭐ PRO в профиле\n\nПо отдельности: Writing {w} ⭐ + Speaking {s} ⭐ — с PRO всё в одном.\n\nЦена: <b>{p} ⭐ Telegram Stars</b> ({d} дней, без автопродления)",
        "en": "⭐ <b>PRO subscription</b>\n\nFor {d} days you get:\n💎 <b>/writing</b> — IELTS Writing review included (up to {l} per day)\n🎙 <b>/speaking</b> — full Speaking mock included (up to {l} per day)\n🏅 A ⭐ PRO badge on your profile\n\nBought separately: Writing {w} ⭐ + Speaking {s} ⭐ — with PRO it's all in one.\n\nPrice: <b>{p} ⭐ Telegram Stars</b> ({d} days, does not auto-renew)",
    },
    "active": {
        "uz": "⭐ <b>PRO faol!</b>\n\n📅 Tugash sanasi: <b>{date}</b> ({left} kun qoldi)\n💎 /writing va 🎙 /speaking — kuniga {l} tagacha tekin.\n\nMuddatni uzaytirish uchun pastdagi to'lovni amalga oshiring: +{d} kun ({p} ⭐).",
        "ru": "⭐ <b>PRO активна!</b>\n\n📅 Действует до: <b>{date}</b> (осталось дней: {left})\n💎 /writing и 🎙 /speaking — бесплатно до {l} раз в день.\n\nЧтобы продлить, оплатите ниже: +{d} дней ({p} ⭐).",
        "en": "⭐ <b>PRO is active!</b>\n\n📅 Valid until: <b>{date}</b> ({left} days left)\n💎 /writing and 🎙 /speaking — free up to {l} times a day.\n\nTo extend, pay below: +{d} days ({p} ⭐).",
    },
    "admin": {
        "uz": "⭐ Siz bot egasisiz — barcha premium funksiyalar siz uchun doim tekin.",
        "ru": "⭐ Вы владелец бота — все премиум-функции для вас всегда бесплатны.",
        "en": "⭐ You're the bot owner — all premium features are always free for you.",
    },
    "inv_title": {"uz": "PRO obuna", "ru": "PRO-подписка", "en": "PRO subscription"},
    "inv_desc": {
        "uz": "{d} kun: Writing va Speaking mock tekin.",
        "ru": "{d} дней: Writing и Speaking мок бесплатно.",
        "en": "{d} days: Writing and Speaking mock included.",
    },
    "inv_label": {"uz": "PRO {d} kun", "ru": "PRO {d} дней", "en": "PRO {d} days"},
    "thanks": {
        "uz": "🎉 <b>PRO faollashtirildi, rahmat!</b> ⭐\n\n📅 Tugash sanasi: <b>{date}</b>\n💎 /writing va 🎙 /speaking endi tekin (kuniga {l} tagacha). Mashq qilishni boshlang!",
        "ru": "🎉 <b>PRO активирована, спасибо!</b> ⭐\n\n📅 Действует до: <b>{date}</b>\n💎 /writing и 🎙 /speaking теперь бесплатны (до {l} в день). Начинайте тренироваться!",
        "en": "🎉 <b>PRO activated, thank you!</b> ⭐\n\n📅 Valid until: <b>{date}</b>\n💎 /writing and 🎙 /speaking are now included (up to {l} per day). Start practising!",
    },
    "limit": {
        "uz": "⏳ PRO'da bugungi limit ({l} ta) tugadi. Ertaga yana tekin bo'ladi, hozir esa bitta marta to'lab foydalanishingiz mumkin.",
        "ru": "⏳ Дневной лимит PRO ({l}) исчерпан. Завтра снова бесплатно, а сейчас можно оплатить разовое использование.",
        "en": "⏳ Today's PRO limit ({l}) is used up. It's free again tomorrow, or you can pay for a single use now.",
    },
    "fail": {
        "uz": "❌ To'lov oynasini ochib bo'lmadi. Birozdan so'ng qayta urinib ko'ring.",
        "ru": "❌ Не удалось открыть окно оплаты. Попробуйте чуть позже.",
        "en": "❌ Couldn't open the payment window. Please try again shortly.",
    },
}


def _pr(key: str, **kw) -> str:
    d = _PRO[key]
    t = d.get(cur_lang()) or d["en"]
    return t.format(**kw) if kw else t


def _pro_date(ts: float) -> str:
    return datetime.fromtimestamp(ts, UZ_TZ).strftime("%d.%m.%Y")


async def _pro_expiry(user_id: int) -> float:
    """PRO tugash vaqti (unix). Faol bo'lmasa 0."""
    now = datetime.now(timezone.utc).timestamp()
    exp = _pro_mem.get(user_id, 0)
    if exp <= now and redis_client is not None:
        try:
            raw = await redis_client.get(f"{PRO_KEY_PREFIX}{user_id}")
            if raw:
                exp = float(raw.decode() if isinstance(raw, bytes) else raw)
                _pro_mem[user_id] = exp
        except Exception as e:
            logger.warning("PRO holatini o'qishda xatolik: %s", e)
    return exp if exp > now else 0


async def _is_pro(user_id: int) -> bool:
    return await _pro_expiry(user_id) > 0


async def _grant_pro(user_id: int) -> float:
    now = datetime.now(timezone.utc).timestamp()
    base = max(now, await _pro_expiry(user_id))
    exp = base + PRO_DAYS * 86400
    _pro_mem[user_id] = exp
    if redis_client is not None:
        try:
            await redis_client.set(f"{PRO_KEY_PREFIX}{user_id}", str(exp), ex=int(exp - now) + 86400)
        except Exception as e:
            logger.warning("PRO holatini yozishda xatolik: %s", e)
    return exp


async def _pro_use(user_id: int, kind: str, consume: bool = True) -> bool:
    """PRO foydalanuvchi bugungi limitdan oshmagan bo'lsa True (consume=True bo'lsa hisoblaydi)."""
    day = datetime.now(UZ_TZ).strftime("%Y-%m-%d")
    mk = (kind, user_id, day)
    count = _pro_use_mem.get(mk, 0)
    rk = f"{PRO_USE_PREFIX}{kind}:{user_id}:{day}"
    if redis_client is not None:
        try:
            raw = await redis_client.get(rk)
            if raw:
                count = max(count, int(raw.decode() if isinstance(raw, bytes) else raw))
        except Exception as e:
            logger.warning("PRO limitini o'qishda xatolik: %s", e)
    if count >= PRO_DAILY_LIMIT:
        return False
    if consume:
        _pro_use_mem[mk] = count + 1
        if redis_client is not None:
            try:
                await redis_client.set(rk, str(count + 1), ex=2 * 86400)
            except Exception as e:
                logger.warning("PRO limitini yozishda xatolik: %s", e)
    return True


async def _send_pro_invoice(message: Message):
    uid = message.chat.id
    try:
        await bot.send_invoice(
            chat_id=message.chat.id,
            title=_pr("inv_title"),
            description=_pr("inv_desc", d=PRO_DAYS),
            payload=f"pro:{uid}",
            currency="XTR",
            prices=[LabeledPrice(label=_pr("inv_label", d=PRO_DAYS), amount=PRO_PRICE_STARS)],
            provider_token="",
        )
    except Exception as e:
        logger.exception("PRO invoice yuborishda xatolik: %s", e)
        await message.answer(_pr("fail"))


@dp.message(Command("pro"))
async def cmd_pro(message: Message):
    uid = message.from_user.id
    if _is_admin(uid):
        await message.answer(_pr("admin"))
        return
    exp = await _pro_expiry(uid)
    if exp:
        left = max(1, int((exp - datetime.now(timezone.utc).timestamp()) // 86400) + 1)
        await message.answer(_pr("active", date=_pro_date(exp), left=left, l=PRO_DAILY_LIMIT, d=PRO_DAYS, p=PRO_PRICE_STARS))
    else:
        await message.answer(_pr("offer", d=PRO_DAYS, l=PRO_DAILY_LIMIT, w=WRITING_PRICE_STARS,
                                 s=SPEAKING_PRICE_STARS, p=PRO_PRICE_STARS))
    await _send_pro_invoice(message)


class EnglishGateMiddleware(BaseMiddleware):
    """Ingliz tiliga oid chat buyruqlari (Grammar, Reading, Listening, Speaking Part 1) faqat PRO uchun.
    Erkin ingliz suhbati (/english) hamma uchun bepul. O'chirish: GATE_CHAT_ENGLISH = False."""
    GATED = {"grammar", "test", "reading_test", "listening_test"}

    async def __call__(self, handler, event, data):
        if GATE_CHAT_ENGLISH and isinstance(event, Message) and event.text and event.text.startswith("/") and event.from_user:
            cmd = event.text[1:].split()[0].split("@")[0].lower() if event.text[1:].strip() else ""
            if cmd in self.GATED and not await _is_premium(event.from_user.id):
                await event.answer(
                    tr("gate", p=PRO_PRICE_STARS, d=PRO_DAYS),
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text=tr("btn_pro_go", p=PRO_PRICE_STARS), callback_data="pw:buy")],
                        [_app_button()],
                    ]),
                )
                return
        return await handler(event, data)


dp.message.outer_middleware(EnglishGateMiddleware())


# --- Kunlik fan + ingliz tili maslahati ----------------------------------
# Har kuni (Toshkent vaqti bilan) navbatdagi fan tanlanadi: qiziqarli fakt,
# 5 ta savollik test va ingliz tilidan foydali maslahat. Kontent kuniga BIR marta
# generatsiya qilinib Redis'da saqlanadi — hamma foydalanuvchi bir xil narsani
# oladi va LLM'ga ortiqcha murojaat bo'lmaydi.

UZ_TZ = timezone(timedelta(hours=5))  # O'zbekiston (UTC+5, yozgi vaqt yo'q)
DAILY_PUSH_ENABLED = os.getenv("DAILY_PUSH_ENABLED", "1") != "0"
DAILY_PUSH_HOUR = int(os.getenv("DAILY_PUSH_HOUR", "9"))  # shu soatdan keyin (Toshkent vaqti) yuboriladi
DAILY_PUSH_WINDOW_HOURS = 6  # bot kech yoqilsa ham, shu oynadan keyin o'sha kun uchun yubormaydi
DAILY_QUIZ_QUESTIONS = 5
DAILY_OPTOUT_KEY = "ielts_bot:daily_optout"
DAILY_SENT_KEY_PREFIX = "ielts_bot:daily_sent:"
DAILY_CONTENT_KEY_PREFIX = "ielts_bot:daily_content:"

ENGLISH_TIP_KINDS = [
    "grammatika qoidasi (oddiy va aniq)",
    "foydali frazal fe'l (phrasal verb)",
    "mashhur idioma (iboralar)",
    "o'zbek tilida so'zlovchilar ko'p qiladigan xato",
    "mavzuli 5 ta yangi so'z",
    "talaffuz bo'yicha maslahat",
    "rasmiy va norasmiy ifodalar farqi",
    "kundalik suhbatda ishlatiladigan tayyor iboralar",
]

_daily_cache: dict[str, dict] = {}
_daily_lock = asyncio.Lock()


def _today_str() -> str:
    return datetime.now(UZ_TZ).strftime("%Y-%m-%d")


def _llm_json_sync(prompt: str, temperature: float = 0.9) -> dict:
    completion = groq_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=temperature,
        response_format={"type": "json_object"},
    )
    return json.loads(completion.choices[0].message.content)


def _generate_daily_content_sync(date_str: str) -> dict:
    """Berilgan kun uchun kontent yaratadi: fan (sana bo'yicha navbatlanadi),
    qiziqarli fakt, 5 ta savol va ingliz tili maslahati."""
    ordinal = datetime.strptime(date_str, "%Y-%m-%d").date().toordinal()
    emoji, subject_name, desc = SUBJECTS[ordinal % len(SUBJECTS)]
    tip_kind = ENGLISH_TIP_KINDS[ordinal % len(ENGLISH_TIP_KINDS)]

    fact_data = _llm_json_sync(
        f"{desc} bo'yicha juda qiziqarli, kam ma'lum va ishonchli bitta fakt yozing. "
        "2-3 gap, o'zbek tilida, odamni hayratlantiradigan bo'lsin. "
        'Javobni FAQAT JSON formatida qaytaring: {"fact": "..."}'
    )
    fact = (fact_data.get("fact") or "").strip()
    if not fact:
        raise ValueError("Kunlik fakt generatsiya qilinmadi")

    try:
        questions = _generate_quiz_batch_sync(DAILY_QUIZ_QUESTIONS, subject_name)
    except Exception as e:
        logger.exception("Kunlik savollarni generatsiya qilishda xatolik: %s", e)
        questions = []

    tip = None
    try:
        tip_data = _llm_json_sync(
            "Siz ingliz tili o'qituvchisisiz. O'zbek tilida so'zlovchilar uchun bugungi "
            f"qisqa va foydali maslahat yozing. Mavzu turi: {tip_kind}. "
            "Tushuntirishni o'zbek tilida bering, misollarni esa ingliz tilida (o'zbekcha tarjimasi bilan) yozing. "
            "3-5 ta qisqa misol bering. Umumiy hajm 80-130 so'z bo'lsin. "
            'Javobni FAQAT JSON formatida qaytaring: {"title": "...", "explanation": "...", '
            '"examples": ["English sentence — o\'zbekcha tarjima", "..."]}'
        )
        if tip_data.get("title") and tip_data.get("explanation"):
            tip = {
                "title": tip_data["title"],
                "explanation": tip_data["explanation"],
                "examples": [str(x) for x in (tip_data.get("examples") or [])][:6],
            }
    except Exception as e:
        logger.exception("Kunlik ingliz tili maslahatini generatsiya qilishda xatolik: %s", e)

    return {
        "date": date_str,
        "subject": f"{emoji} {subject_name}",
        "fact": fact,
        "questions": questions,
        "tip": tip,
    }


async def get_daily_content(lang: str | None = None) -> dict | None:
    """Bugungi kunlik kontentni (foydalanuvchi tilida) qaytaradi: avval xotiradan, keyin Redis'dan,
    yo'q bo'lsa yangi generatsiya qilib saqlaydi."""
    lang = lang or cur_lang()
    date_str = _today_str()
    cache_key = date_str + ("" if lang == "uz" else ":" + lang)
    cached = _daily_cache.get(cache_key)
    if cached:
        return cached

    async with _daily_lock:
        cached = _daily_cache.get(cache_key)
        if cached:
            return cached

        key = DAILY_CONTENT_KEY_PREFIX + cache_key
        if redis_client is not None:
            try:
                raw = await redis_client.get(key)
                if raw:
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    content = json.loads(raw)
                    for k in [k for k in _daily_cache if not k.startswith(date_str)]:
                        _daily_cache.pop(k, None)
                    _daily_cache[cache_key] = content
                    return content
            except Exception as e:
                logger.warning("Kunlik kontentni Redis'dan o'qishda xatolik: %s", e)

        try:
            current_lang.set(lang)
            content = await asyncio.to_thread(_generate_daily_content_sync, date_str)
        except Exception as e:
            logger.exception("Kunlik kontentni generatsiya qilishda xatolik: %s", e)
            return None

        if redis_client is not None:
            try:
                await redis_client.set(key, json.dumps(content), ex=172800)
            except Exception as e:
                logger.warning("Kunlik kontentni Redis'ga yozishda xatolik: %s", e)

        for k in [k for k in _daily_cache if not k.startswith(date_str)]:
            _daily_cache.pop(k, None)
        _daily_cache[cache_key] = content
        return content


def _format_daily_text(content: dict) -> str:
    e = html.escape
    parts = [
        f"📅 <b>Bugungi fan: {e(content['subject'])}</b>",
        f"💡 <b>Bilasizmi?</b>\n{e(content['fact'])}",
    ]
    tip = content.get("tip")
    if tip:
        tip_text = f"🇬🇧 <b>Kunlik ingliz tili: {e(tip['title'])}</b>\n{e(tip['explanation'])}"
        if tip.get("examples"):
            tip_text += "\n\n" + "\n".join(f"▫️ {e(x)}" for x in tip["examples"])
        parts.append(tip_text)
    return "\n\n".join(parts)


def _daily_keyboard(content: dict) -> InlineKeyboardMarkup | None:
    n = len(content.get("questions") or [])
    if not n:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎯 Kunlik testni boshlash ({n} savol)", callback_data="daily:start")],
    ])


@dp.message(Command("daily", "kunlik"))
async def cmd_kunlik(message: Message):
    prep_msg = await message.answer("⏳ Bugungi kontent tayyorlanmoqda...")
    content = await get_daily_content()
    if not content:
        await prep_msg.edit_text("❌ Bugungi kontentni tayyorlab bo'lmadi. Birozdan keyin /daily bilan qayta urinib ko'ring.")
        return
    await prep_msg.edit_text(_format_daily_text(content), reply_markup=_daily_keyboard(content))


@dp.callback_query(F.data == "daily:start")
async def handle_daily_start(callback: CallbackQuery):
    await callback.answer()
    content = await get_daily_content()
    questions = (content or {}).get("questions") or []
    if not questions:
        await callback.message.answer("⚠️ Bugungi test hozir mavjud emas. Boshqa viktorina uchun /quiz yuboring.")
        return
    user_id = callback.from_user.id
    active_quizzes[user_id] = {
        "questions": [dict(q) for q in questions],
        "idx": 0,
        "score": 0,
        "mode": "daily",
        "subject": content["subject"],
    }
    text, keyboard = _build_quiz_question_view(active_quizzes[user_id], 0)
    await callback.message.answer(text, reply_markup=keyboard)


@dp.message(Command("notify", "xabarnoma"))
async def cmd_xabarnoma(message: Message):
    """Kunlik avtomatik xabarni (har kuni ertalab fan + ingliz tili maslahati) yoqadi/o'chiradi."""
    if redis_client is None:
        await message.answer("⚠️ Bu xizmat hozir sozlanmagan.")
        return
    uid = str(message.from_user.id)
    try:
        opted_out = await redis_client.sismember(DAILY_OPTOUT_KEY, uid)
        if opted_out:
            await redis_client.srem(DAILY_OPTOUT_KEY, uid)
            await message.answer("🔔 Kunlik xabarlar <b>yoqildi</b>. Har kuni yangi fan va ingliz tili maslahatini olasiz!")
        else:
            await redis_client.sadd(DAILY_OPTOUT_KEY, uid)
            await message.answer("🔕 Kunlik xabarlar <b>o'chirildi</b>. Qayta yoqish uchun yana /notify yuboring.\n\nKunlik kontentni /daily orqali o'zingiz ham ochishingiz mumkin.")
    except Exception as e:
        logger.exception("Xabarnoma sozlamasida xatolik: %s", e)
        await message.answer("❌ Xatolik yuz berdi. Keyinroq qayta urinib ko'ring.")


async def broadcast_daily_content(content: dict | None = None) -> tuple[int, int]:
    """Kunlik kontentni barcha foydalanuvchilarga (o'chirib qo'yganlardan tashqari) yuboradi.
    (yuborildi, xato) sonini qaytaradi. Telegram limitiga tushmaslik uchun sekin yuboradi."""
    from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

    users = await redis_client.smembers(BOT_USERS_SET_KEY) or []
    optouts = set(str(x) for x in (await redis_client.smembers(DAILY_OPTOUT_KEY) or []))
    lang_map = {}
    try:
        lang_map = await redis_client.hgetall(BOT_USER_LANG_KEY) or {}
    except Exception:
        pass
    lang_map = {(k.decode() if isinstance(k, bytes) else str(k)): (v.decode() if isinstance(v, bytes) else str(v)) for k, v in lang_map.items()}
    per_lang = {}

    sent = failed = 0
    for uid in users:
        uid = str(uid)
        if uid in optouts:
            continue
        ulang = lang_map.get(uid, DEFAULT_LANG)
        if ulang not in LANGS:
            ulang = DEFAULT_LANG
        if ulang not in per_lang:
            c = content if (ulang == "uz" and content) else await get_daily_content(ulang)
            if not c:
                c = content or await get_daily_content("uz")
            if not c:
                failed += 1
                continue
            per_lang[ulang] = (c, _format_daily_text(c) + "\n\n<i>Kunlik xabarni o'chirish: /notify</i>", _daily_keyboard(c))
        _c, text, keyboard = per_lang[ulang]
        current_lang.set(ulang)
        try:
            try:
                await bot.send_message(int(uid), text, reply_markup=keyboard)
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after + 1)
                await bot.send_message(int(uid), text, reply_markup=keyboard)
            sent += 1
        except TelegramForbiddenError:
            failed += 1  # foydalanuvchi botni bloklagan
        except Exception as e:
            failed += 1
            logger.warning("Kunlik xabar yuborilmadi (%s): %s", uid, e)
        await asyncio.sleep(0.06)  # ~16 xabar/soniya — Telegram limitidan past
    return sent, failed


async def daily_broadcast_loop():
    """Fon vazifasi: har 5 daqiqada tekshiradi; Toshkent vaqti bilan belgilangan soatdan
    keyin bugun uchun xabar hali yuborilmagan bo'lsa, bir marta yuboradi. Qayta yuborilishining
    oldi Redis'dagi 'bugun yuborilgan' belgisi (SET NX) bilan olinadi — deploy paytida ikki
    instance bir vaqtda ishlasa ham xabar takrorlanmaydi."""
    logger.info("Kunlik xabar vazifasi ishga tushdi (soat %02d:00, Toshkent vaqti).", DAILY_PUSH_HOUR)
    while True:
        try:
            now = datetime.now(UZ_TZ)
            in_window = DAILY_PUSH_HOUR <= now.hour < DAILY_PUSH_HOUR + DAILY_PUSH_WINDOW_HOURS
            if in_window and redis_client is not None:
                today = _today_str()
                sent_key = DAILY_SENT_KEY_PREFIX + today
                claimed = await redis_client.set(sent_key, "1", nx=True, ex=172800)
                if claimed:
                    content = await get_daily_content("uz")
                    if not content:
                        # Kontent yo'q — keyingi urinish uchun belgini qaytaramiz
                        await redis_client.delete(sent_key)
                        logger.warning("Kunlik xabar: kontent tayyor emas, 5 daqiqadan keyin qayta uriniladi.")
                    else:
                        sent, failed = await broadcast_daily_content(content)
                        logger.info("Kunlik xabar yuborildi: %s ta, xato: %s ta.", sent, failed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception("Kunlik xabar vazifasida xatolik: %s", e)
        await asyncio.sleep(300)



@dp.message(Command("certificate", "sertifikat"))
async def cmd_sertifikat(message: Message):
    f"""Ketma-ket {CERTIFICATE_QUIZ_LENGTH} ta aralash fanlardan iborat "sertifikat testi"ni
    boshlaydi. Agar natija {CERTIFICATE_PASS_PERCENT}% dan yuqori bo'lsa, test yakunida
    foydalanuvchiga rasmli (PNG) sertifikat yuboriladi."""
    prep_msg = await message.answer(
        f"⏳ Sertifikat testi uchun {CERTIFICATE_QUIZ_LENGTH} ta savol tayyorlanmoqda... "
        "(bu biroz vaqt olishi mumkin, iltimos kuting)"
    )

    try:
        questions = await asyncio.to_thread(_generate_quiz_set_sync, CERTIFICATE_QUIZ_LENGTH)
        if len(questions) < CERTIFICATE_QUIZ_LENGTH // 2:
            # Agar savollarning yarmidan ko'pi generatsiya qilinmagan bo'lsa,
            # test sifatsiz bo'lib qolmasligi uchun xatolik deb hisoblaymiz.
            raise ValueError(f"Yetarli savol generatsiya qilinmadi: {len(questions)}")
    except Exception as e:
        logger.exception("Sertifikat testini generatsiya qilishda xatolik: %s", e)
        await prep_msg.edit_text("❌ Savollarni tayyorlashda xatolik yuz berdi. Iltimos /certificate bilan qayta urinib ko'ring.")
        return

    active_quizzes[message.from_user.id] = {
        "questions": questions,
        "idx": 0,
        "score": 0,
        "mode": "certificate",
    }

    intro = (
        f"🎓 <b>Sertifikat testi boshlandi!</b>\n\n"
        f"📝 Jami {len(questions)} ta savol. Agar natijangiz "
        f"<b>{CERTIFICATE_PASS_PERCENT}%</b> yoki undan yuqori bo'lsa, "
        "test yakunida shaxsiy sertifikat rasmini olasiz 🏅\n\n"
        "Omad! 🍀"
    )
    await message.answer(intro)

    text, keyboard = _build_quiz_question_view(active_quizzes[message.from_user.id], 0)
    await prep_msg.edit_text(text, reply_markup=keyboard)


@dp.message(Command("reading_test"))
async def cmd_reading_test(message: Message):
    """AI orqali original IELTS Academic Reading uslubidagi passage va
    savollarni generatsiya qilib, testni boshlaydi."""
    prep_msg = await message.answer("⏳ Reading testi tayyorlanmoqda... (bu biroz vaqt olishi mumkin)")

    try:
        data = await asyncio.to_thread(_generate_reading_test_sync)
        questions = data["questions"]
        passage = data["passage"]
        if not questions or not passage:
            raise ValueError("Reading testi to'liq generatsiya qilinmadi")
    except Exception as e:
        logger.exception("Reading testini generatsiya qilishda xatolik: %s", e)
        await prep_msg.edit_text("❌ Testni tayyorlashda xatolik yuz berdi. Iltimos /reading_test bilan qayta urinib ko'ring.")
        return

    active_quizzes[message.from_user.id] = {
        "questions": questions,
        "idx": 0,
        "score": 0,
        "mode": "reading",
    }

    try:
        await prep_msg.delete()
    except Exception:
        pass

    await message.answer(f"📖 <b>Reading Passage</b>\n\n{passage}")
    await message.answer(f"📝 Endi quyidagi {len(questions)} ta savolga javob bering 👇")

    text, keyboard = _build_quiz_question_view(active_quizzes[message.from_user.id], 0)
    await message.answer(text, reply_markup=keyboard)


@dp.message(Command("listening_test"))
async def cmd_listening_test(message: Message):
    """AI orqali original IELTS Listening uslubidagi skript generatsiya qilib,
    uni ovozga aylantirib yuboradi va savollar bilan testni boshlaydi."""
    prep_msg = await message.answer("⏳ Listening testi tayyorlanmoqda... (bu biroz vaqt olishi mumkin)")

    try:
        data = await asyncio.to_thread(_generate_listening_test_sync)
        questions = data["questions"]
        script = data["script"]
        if not questions or not script:
            raise ValueError("Listening testi to'liq generatsiya qilinmadi")
    except Exception as e:
        logger.exception("Listening testini generatsiya qilishda xatolik: %s", e)
        await prep_msg.edit_text("❌ Testni tayyorlashda xatolik yuz berdi. Iltimos /listening_test bilan qayta urinib ko'ring.")
        return

    active_quizzes[message.from_user.id] = {
        "questions": questions,
        "idx": 0,
        "score": 0,
        "mode": "listening",
    }

    try:
        await prep_msg.delete()
    except Exception:
        pass

    user_dir = _user_dir(message.from_user.id)
    ogg_path = os.path.join(user_dir, f"listening_test_{secrets.token_hex(6)}.ogg")
    try:
        await asyncio.to_thread(_text_to_speech_ogg_sync, script, ogg_path)
        await message.answer("🎧 <b>Listening — tinglang va savollarga javob bering:</b>")
        await message.answer_voice(FSInputFile(ogg_path))
    except Exception as e:
        logger.exception("Listening audio yaratishda xatolik: %s", e)
        await message.answer("⚠️ Audio yaratib bo'lmadi, lekin matn asosida davom etamiz:\n\n" + script)
    finally:
        if os.path.exists(ogg_path):
            try:
                os.remove(ogg_path)
            except OSError:
                pass

    await message.answer(f"📝 Endi quyidagi {len(questions)} ta savolga javob bering 👇")

    text, keyboard = _build_quiz_question_view(active_quizzes[message.from_user.id], 0)
    await message.answer(text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("qans:"))
async def handle_quiz_answer(callback: CallbackQuery):
    user_id = callback.from_user.id
    quiz = active_quizzes.get(user_id)

    if not quiz:
        await callback.answer("⏱ Bu sessiya tugagan yoki muddati o'tgan. Yangi test uchun /quiz yuboring.", show_alert=True)
        return

    try:
        selected_index = int(callback.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer("Xatolik yuz berdi.", show_alert=True)
        return

    idx = quiz["idx"]
    total = len(quiz["questions"])
    q = quiz["questions"][idx]
    correct_index = q["correct_index"]
    letters = ["A", "B", "C", "D", "E", "F"]
    mode = quiz.get("mode", "quiz")
    mode_emoji = {"certificate": "🎓", "reading": "📖", "listening": "🎧", "daily": "📅"}.get(mode, "🧩")

    is_correct = selected_index == correct_index
    if is_correct:
        quiz["score"] += 1
        result_line = "✅ <b>To'g'ri javob!</b>"
    else:
        correct_letter = letters[correct_index]
        correct_option = q["options"][correct_index]
        result_line = f"❌ <b>Noto'g'ri.</b> To'g'ri javob: <b>{correct_letter}) {correct_option}</b>"

    explanation = q.get("explanation", "")
    final_text = f"{mode_emoji} <b>Savol {idx + 1}/{total}:</b>\n\n{q['question']}\n\n{result_line}"
    if explanation:
        final_text += f"\n\n💡 <i>{explanation}</i>"

    # Javob berilgan savoldagi tugmalarni olib tashlab, natijani ko'rsatamiz
    try:
        await callback.message.edit_text(final_text, reply_markup=None)
    except Exception:
        pass

    await callback.answer("To'g'ri! 🎉" if is_correct else "Noto'g'ri 😔")

    quiz["idx"] += 1

    if quiz["idx"] < total:
        # Keyingi savolni yangi xabar sifatida yuboramiz
        next_text, next_keyboard = _build_quiz_question_view(quiz, quiz["idx"])
        await callback.message.answer(next_text, reply_markup=next_keyboard)
        return

    # --- Test yakunlandi ---
    score = quiz["score"]
    percentage = round((score / total) * 100)
    active_quizzes.pop(user_id, None)

    if mode == "certificate":
        await update_leaderboard(LEADERBOARD_CERT_KEY, user_id, percentage)

        if percentage >= CERTIFICATE_PASS_PERCENT:
            full_name = callback.from_user.full_name or callback.from_user.username or "Foydalanuvchi"
            await callback.message.answer(
                f"🏁 <b>Sertifikat testi yakunlandi!</b>\n\n"
                f"🎯 Natijangiz: <b>{score}/{total}</b> ({percentage}%)\n\n"
                "🎉 Tabriklaymiz! Sertifikatingiz tayyorlanmoqda..."
            )
            cert_path = None
            try:
                cert_path = await asyncio.to_thread(
                    _generate_certificate_image_sync, full_name, score, total, percentage
                )
                await callback.message.answer_photo(
                    FSInputFile(cert_path),
                    caption=(
                        f"🎓 <b>Tabriklaymiz, {full_name}!</b>\n"
                        f"Siz {total} savoldan {score} tasiga to'g'ri javob berib, "
                        f"({percentage}%) sertifikatga sazovor bo'ldingiz!\n\n"
                        "🏆 Reytingni /leaderboard orqali ko'rishingiz mumkin."
                    ),
                )
            except Exception as e:
                logger.exception("Sertifikat rasmini yaratish/yuborishda xatolik: %s", e)
                await callback.message.answer("⚠️ Sertifikat rasmini yaratishda xatolik yuz berdi, lekin natijangiz hisobga olindi.")
            finally:
                if cert_path and os.path.exists(cert_path):
                    try:
                        os.remove(cert_path)
                    except OSError:
                        pass
        else:
            await callback.message.answer(
                f"🏁 <b>Sertifikat testi yakunlandi!</b>\n\n"
                f"🎯 Natijangiz: <b>{score}/{total}</b> ({percentage}%)\n\n"
                f"😔 Afsuski, sertifikat olish uchun kamida {CERTIFICATE_PASS_PERCENT}% kerak. "
                "Ko'proq mashq qilib, yana urinib ko'ring!\n\n"
                "🔁 Qayta urinish uchun /certificate yuboring."
            )
        return

    if mode in ("reading", "listening"):
        leaderboard_key = LEADERBOARD_READING_KEY if mode == "reading" else LEADERBOARD_LISTENING_KEY
        await update_leaderboard(leaderboard_key, user_id, percentage)

        icon = "📖" if mode == "reading" else "🎧"
        label = "Reading" if mode == "reading" else "Listening"
        next_cmd = "/reading_test" if mode == "reading" else "/listening_test"

        if percentage >= 80:
            verdict = "🏆 Ajoyib natija!"
        elif percentage >= 50:
            verdict = "👍 Yomon emas, yana mashq qiling!"
        else:
            verdict = "📚 Ko'proq mashq qilish kerak."

        summary = (
            f"🏁 <b>{icon} {label} testi yakunlandi!</b>\n\n"
            f"🎯 Natijangiz: <b>{score}/{total}</b> ({percentage}%)\n\n"
            f"{verdict}\n\n"
            f"🔁 Yangi test uchun {next_cmd}, reytingni ko'rish uchun /leaderboard yuboring."
        )
        await callback.message.answer(summary)
        return

    if mode == "daily":
        if percentage >= 80:
            verdict = "🏆 Zo'r! Bugungi fanni yaxshi bilar ekansiz."
        elif percentage >= 50:
            verdict = "👍 Yaxshi, ertaga yangi fan sizni kutadi!"
        else:
            verdict = "📚 Hechqisi yo'q — yangi narsa o'rganish ham yutuq!"
        await callback.message.answer(
            f"🏁 <b>Kunlik test yakunlandi!</b>\n\n"
            f"🎯 Natijangiz: <b>{score}/{total}</b> ({percentage}%)\n\n"
            f"{verdict}\n\n"
            "🧩 Yana o'ynash uchun /quiz, jumboq uchun /teaser yuboring."
        )
        return

    # Oddiy /quiz viktorinasi yakuni
    await update_leaderboard(LEADERBOARD_QUIZ_KEY, user_id, percentage)

    if percentage >= 80:
        verdict = "🏆 Ajoyib natija! Bilimingiz juda yaxshi."
    elif percentage >= 50:
        verdict = "👍 Yomon emas! Yana mashq qilsangiz yanada yaxshi bo'ladi."
    else:
        verdict = "📚 Ko'proq o'rganishga arziydi, lekin harakat qildingiz!"

    summary = (
        f"🏁 <b>Viktorina yakunlandi!</b>\n\n"
        f"🎯 Natijangiz: <b>{score}/{total}</b> ({percentage}%)\n\n"
        f"{verdict}\n\n"
        "🔁 Yana o'ynash uchun /quiz, sertifikat testi uchun /certificate, "
        "reytingni ko'rish uchun /leaderboard yuboring."
    )
    await callback.message.answer(summary)


@dp.message(Command("test"))
async def cmd_test(message: Message, state: FSMContext):
    await state.clear()
    cleanup_user_dir(message.from_user.id)

    prep_msg = await message.answer("⏳ IELTS Speaking testi tayyorlanmoqda, biroz kuting...")

    try:
        questions = await asyncio.to_thread(_generate_questions_sync)
    except Exception as e:
        logger.exception("Savollarni generatsiya qilishda xatolik: %s", e)
        await prep_msg.edit_text("❌ Savollarni generatsiya qilishda xatolik yuz berdi. Iltimos /test bilan qayta urinib ko'ring.")
        return

    await state.update_data(
        questions=questions,
        part1_idx=0,
        part3_idx=0,
        answers=[],
    )
    await state.set_state(ExamStates.part1)

    try:
        await prep_msg.delete()
    except Exception:
        pass

    await message.answer(
        "✅ <b>Test tayyor!</b>\n\n🎤 <b>PART 1</b> — Introduction & Interview (qisqa test)\n"
        "🎙 Har bir savolga ovozli xabar bilan javob bering."
    )
    q0 = questions["part1"][0]
    await send_text_and_voice(message, f"🗣 <b>Savol 1/{PART1_QUESTIONS_COUNT}:</b>\n{q0}", tts_text=q0)


async def _process_voice(message: Message, question: str) -> tuple[str, dict] | None:
    """Voice xabarni yuklab oladi, transkripsiya qiladi va baholaydi.
    Xatolik bo'lsa None qaytaradi (foydalanuvchiga xabar allaqachon yuborilgan)."""
    user_dir = _user_dir(message.from_user.id)
    ogg_path = os.path.join(user_dir, f"voice_{message.voice.file_unique_id}.ogg")

    try:
        file = await bot.get_file(message.voice.file_id)
        await bot.download_file(file.file_path, destination=ogg_path)
    except Exception as e:
        logger.exception("Voice yuklab olishda xatolik: %s", e)
        await message.answer("❌ Ovozli xabarni yuklab olishda xatolik. Iltimos qayta yuboring.")
        return None

    try:
        transcript = await asyncio.to_thread(_transcribe_sync, ogg_path)
    except Exception as e:
        logger.exception("Transkripsiya xatoligi: %s", e)
        await message.answer("❌ Ovozni matnga aylantirishda xatolik. Iltimos qayta yuboring.")
        return None
    finally:
        if os.path.exists(ogg_path):
            try:
                os.remove(ogg_path)
            except OSError:
                pass

    if not transcript:
        await message.answer("⚠️ Ovozingizda nutq aniqlanmadi. Iltimos, aniqroq gapirib qayta yuboring.")
        return None

    try:
        scores = await asyncio.to_thread(_evaluate_answer_sync, question, transcript)
    except Exception as e:
        logger.exception("Baholashda xatolik: %s", e)
        scores = {
            "fluency_coherence": 0,
            "lexical_resource": 0,
            "grammar_range_accuracy": 0,
            "pronunciation_estimate": 0,
            "short_comment": "Baholab bo'lmadi (texnik xatolik).",
        }

    return transcript, scores


@dp.message(ExamStates.part1, F.voice)
async def handle_part1_voice(message: Message, state: FSMContext):
    data = await state.get_data()
    questions = data["questions"]
    idx = data["part1_idx"]
    question = questions["part1"][idx]

    result = await _process_voice(message, question)
    if result is None:
        return
    transcript, scores = result

    answers = data["answers"]
    answers.append({"part": "Part 1", "question": question, "transcript": transcript, "scores": scores})

    idx += 1
    await state.update_data(part1_idx=idx, answers=answers)

    comment = scores.get("short_comment", "")
    await message.answer(f"📝 <i>{comment}</i>" if comment else "📝 Javob qabul qilindi.")

    if idx < PART1_QUESTIONS_COUNT:
        next_q = questions["part1"][idx]
        await send_text_and_voice(message, f"🗣 <b>Savol {idx + 1}/{PART1_QUESTIONS_COUNT}:</b>\n{next_q}", tts_text=next_q)
    else:
        await message.answer("✅ <b>Part 1 tugadi!</b>\n\n📊 Yakuniy natija tayyorlanmoqda, biroz kuting...")
        try:
            report = await asyncio.to_thread(_generate_final_report_sync, answers)
        except Exception as e:
            logger.exception("Yakuniy hisobotni tayyorlashda xatolik: %s", e)
            await message.answer("❌ Yakuniy hisobotni tayyorlashda xatolik yuz berdi.")
            await state.clear()
            cleanup_user_dir(message.from_user.id)
            return

        avg = report["averages"]
        final_text = (
            "🏁 <b>IELTS SPEAKING PART 1 — YAKUNIY NATIJA</b>\n\n"
            f"🗣 Fluency & Coherence: <b>{avg.get('fluency_coherence', 0)}</b>\n"
            f"📚 Lexical Resource: <b>{avg.get('lexical_resource', 0)}</b>\n"
            f"✍️ Grammatical Range & Accuracy: <b>{avg.get('grammar_range_accuracy', 0)}</b>\n"
            f"🔊 Pronunciation: <b>{avg.get('pronunciation_estimate', 0)}</b>\n\n"
            f"🏆 <b>Overall Band Score: {report['overall_band']}</b>\n\n"
            f"💬 <b>Examiner fikri:</b>\n{report['text']}"
        )
        await send_text_and_voice(message, final_text, tts_text=report["text"])

        await state.clear()
        cleanup_user_dir(message.from_user.id)
        await message.answer("🔁 Yangi test uchun /test, to'liq 3 qismli mock uchun /speaking yuboring.")


@dp.message(ExamStates.part2, F.voice)
async def handle_part2_voice(message: Message, state: FSMContext):
    data = await state.get_data()
    questions = data["questions"]
    question = questions["part2"]["cue_card"]

    result = await _process_voice(message, question)
    if result is None:
        return
    transcript, scores = result

    answers = data["answers"]
    answers.append({"part": "Part 2", "question": question, "transcript": transcript, "scores": scores})
    await state.update_data(answers=answers, part3_idx=0)
    await state.set_state(ExamStates.part3)

    comment = scores.get("short_comment", "")
    await message.answer(f"📝 <i>{comment}</i>" if comment else "📝 Javob qabul qilindi.")

    await message.answer("✅ <b>Part 2 tugadi!</b>\n\n💭 <b>PART 3</b> — Discussion")
    q0 = questions["part3"][0]
    await send_text_and_voice(message, f"🗣 <b>Savol 1/{PART3_QUESTIONS_COUNT}:</b>\n{q0}", tts_text=q0)


@dp.message(ExamStates.part3, F.voice)
async def handle_part3_voice(message: Message, state: FSMContext):
    data = await state.get_data()
    questions = data["questions"]
    idx = data["part3_idx"]
    question = questions["part3"][idx]

    result = await _process_voice(message, question)
    if result is None:
        return
    transcript, scores = result

    answers = data["answers"]
    answers.append({"part": "Part 3", "question": question, "transcript": transcript, "scores": scores})

    idx += 1
    await state.update_data(part3_idx=idx, answers=answers)

    comment = scores.get("short_comment", "")
    await message.answer(f"📝 <i>{comment}</i>" if comment else "📝 Javob qabul qilindi.")

    if idx < PART3_QUESTIONS_COUNT:
        next_q = questions["part3"][idx]
        await send_text_and_voice(message, f"🗣 <b>Savol {idx + 1}/{PART3_QUESTIONS_COUNT}:</b>\n{next_q}", tts_text=next_q)
    else:
        await message.answer("✅ <b>Part 3 tugadi!</b>\n\n📊 Yakuniy natija tayyorlanmoqda, biroz kuting...")
        try:
            report = await asyncio.to_thread(_generate_final_report_sync, answers)
        except Exception as e:
            logger.exception("Yakuniy hisobotni tayyorlashda xatolik: %s", e)
            await message.answer("❌ Yakuniy hisobotni tayyorlashda xatolik yuz berdi.")
            await state.clear()
            cleanup_user_dir(message.from_user.id)
            return

        avg = report["averages"]
        final_text = (
            "🏁 <b>IELTS SPEAKING — YAKUNIY NATIJA</b>\n\n"
            f"🗣 Fluency & Coherence: <b>{avg.get('fluency_coherence', 0)}</b>\n"
            f"📚 Lexical Resource: <b>{avg.get('lexical_resource', 0)}</b>\n"
            f"✍️ Grammatical Range & Accuracy: <b>{avg.get('grammar_range_accuracy', 0)}</b>\n"
            f"🔊 Pronunciation: <b>{avg.get('pronunciation_estimate', 0)}</b>\n\n"
            f"🏆 <b>Overall Band Score: {report['overall_band']}</b>\n\n"
            f"💬 <b>Examiner fikri:</b>\n{report['text']}"
        )
        await send_text_and_voice(message, final_text, tts_text=report["text"])

        await state.clear()
        cleanup_user_dir(message.from_user.id)
        await message.answer("🔁 Yangi test uchun /test buyrug'ini yuboring.")


# Foydalanuvchi voice o'rniga matn yuborsa (imtihon davomida)
@dp.message(StateFilter(ExamStates.part1, ExamStates.part2, ExamStates.part3))
async def handle_wrong_content_type(message: Message):
    await message.answer("🎙 Iltimos, javobingizni faqat <b>ovozli xabar (voice)</b> shaklida yuboring.")


# --- ODDIY SUHBAT (Default Chat) ----------------------------------------
# Imtihon holatidan tashqarida yuborilgan har qanday matnli yoki ovozli
# xabar — hech qanday maxsus buyruq (masalan /chat) talab qilinmaydi,
# bot avtomatik ravishda erkin suhbatdosh sifatida javob beradi.

@dp.message(F.text)
async def handle_default_chat_text(message: Message, state: FSMContext):
    if message.text.startswith("/"):
        await message.answer(
            "❓ Bunday buyruqni tanimadim. Lekin menga oddiygina yozsangiz — "
            "suhbatlashishga tayyorman!"
        )
        return

    data = await state.get_data()
    history = data.get("chat_history", [])
    history.append({"role": "user", "content": message.text})

    await bot.send_chat_action(message.chat.id, "typing")

    try:
        reply = await asyncio.to_thread(_generate_chat_reply_sync, history)
    except Exception as e:
        logger.exception("Suhbat javobini generatsiya qilishda xatolik: %s", e)
        await message.answer("❌ Javob berishda xatolik yuz berdi. Iltimos, qayta urinib ko'ring.")
        return

    history.append({"role": "assistant", "content": reply})
    await state.update_data(chat_history=history[-CHAT_HISTORY_LIMIT:])
    await message.answer(reply)


@dp.message(F.voice)
async def handle_default_chat_voice(message: Message, state: FSMContext):
    """Imtihondan tashqarida yuborilgan ovozli xabarni matnga aylantirib,
    xuddi yozma xabardek suhbat javobini beradi."""
    user_dir = _user_dir(message.from_user.id)
    ogg_path = os.path.join(user_dir, f"chat_voice_{message.voice.file_unique_id}.ogg")

    try:
        file = await bot.get_file(message.voice.file_id)
        await bot.download_file(file.file_path, destination=ogg_path)
        transcript = await asyncio.to_thread(_transcribe_sync, ogg_path)
    except Exception as e:
        logger.exception("Suhbatda ovozni tanib olishda xatolik: %s", e)
        await message.answer("❌ Ovozli xabarni tushunishda xatolik yuz berdi. Iltimos, qayta urinib ko'ring.")
        return
    finally:
        if os.path.exists(ogg_path):
            try:
                os.remove(ogg_path)
            except OSError:
                pass

    if not transcript:
        await message.answer("⚠️ Ovozingizda nutq aniqlanmadi. Iltimos, aniqroq gapirib qayta yuboring.")
        return

    data = await state.get_data()
    history = data.get("chat_history", [])
    history.append({"role": "user", "content": transcript})

    await bot.send_chat_action(message.chat.id, "typing")

    try:
        reply = await asyncio.to_thread(_generate_chat_reply_sync, history)
    except Exception as e:
        logger.exception("Suhbat javobini generatsiya qilishda xatolik: %s", e)
        await message.answer("❌ Javob berishda xatolik yuz berdi. Iltimos, qayta urinib ko'ring.")
        return

    history.append({"role": "assistant", "content": reply})
    await state.update_data(chat_history=history[-CHAT_HISTORY_LIMIT:])
    await message.answer(f"🎙 <i>Siz aytdingiz:</i> {transcript}\n\n{reply}")


# Boshqa hech qaysi handlerga to'g'ri kelmagan xabar turlari (sticker, rasm va h.k.)
@dp.message()
async def handle_fallback(message: Message):
    await message.answer(
        "ℹ️ Menga oddiy matn yoki ovozli xabar yuborishingiz mumkin — suhbatlashamiz. "
        "IELTS testi uchun /test, to'xtatish uchun /stop buyrug'ini yuboring."
    )


# --------------------------------------------------------------------------
# 7) DASHBOARD MINI APP (Telegram WebApp) — Profil, Reading, Listening, Grammar
# --------------------------------------------------------------------------

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover">
<title>Talk</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root {
    --bg: var(--tg-theme-bg-color, #0e1117);
    --card: var(--tg-theme-secondary-bg-color, #181c27);
    --text: var(--tg-theme-text-color, #eef0f6);
    --muted: var(--tg-theme-hint-color, #8b93a7);
    --accent: var(--tg-theme-button-color, #5b8cff);
    --accent-text: var(--tg-theme-button-text-color, #ffffff);
    --line: rgba(127,135,160,.25);
    --good: #3ecf8e; --bad: #ff6b6b; --gold: #f5b942;
    --r: 16px;
  }
  * { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
  html, body { margin: 0; background: var(--bg); color: var(--text);
    font: 15px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
  body { padding: 0 16px calc(84px + env(safe-area-inset-bottom)); min-height: 100vh; }
  h1, h2, h3, p { margin: 0; }
  .top { display: flex; align-items: center; gap: 12px; padding: 18px 0 8px; }
  .avatar { width: 46px; height: 46px; border-radius: 50%; background: linear-gradient(135deg, var(--accent), #b06cff);
    display: grid; place-items: center; font-weight: 700; font-size: 19px; color: #fff; flex: none; }
  .top h1 { font-size: 19px; line-height: 1.2; }
  .sub { color: var(--muted); font-size: 13px; }
  .badge { display: inline-block; font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 99px;
    background: linear-gradient(90deg, #f5b942, #ff8a5c); color: #1b1305; vertical-align: 2px; margin-left: 6px; }
  .lock { font-size: 11px; font-weight: 700; color: var(--gold); }
  .section-title { font-size: 12px; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); margin: 20px 2px 8px; font-weight: 600; }
  .card { background: var(--card); border-radius: var(--r); padding: 16px; margin-bottom: 12px; }
  .promo { background: linear-gradient(135deg, rgba(245,185,66,.18), rgba(176,108,255,.18)); border: 1px solid rgba(245,185,66,.35); }
  .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
  .stat { background: var(--card); border-radius: var(--r); padding: 14px; }
  .stat .v { font-size: 26px; font-weight: 700; letter-spacing: -.02em; }
  .stat .l { color: var(--muted); font-size: 12px; margin-top: 2px; }
  .stat .r { color: var(--gold); font-size: 12px; margin-top: 4px; font-weight: 600; }
  .tile { background: var(--card); border: 0; color: var(--text); border-radius: var(--r); padding: 14px 12px; text-align: left;
    font: inherit; cursor: pointer; display: flex; flex-direction: column; gap: 6px; min-height: 92px; position: relative; }
  .tile:active { transform: scale(.98); }
  .tile .e { font-size: 26px; }
  .tile .n { font-size: 13.5px; font-weight: 600; line-height: 1.25; }
  .tile .d { font-size: 11.5px; color: var(--muted); line-height: 1.3; }
  .tile .lk { position: absolute; top: 10px; right: 10px; font-size: 12px; }
  .btn { display: block; width: 100%; border: 0; border-radius: 12px; padding: 13px 16px; font: inherit; font-weight: 600;
    background: var(--accent); color: var(--accent-text); cursor: pointer; text-align: center; margin-top: 10px; }
  .btn.ghost { background: transparent; color: var(--accent); border: 1px solid var(--line); }
  .btn.gold { background: linear-gradient(90deg, #f5b942, #ff8a5c); color: #1b1305; }
  .btn.good { background: var(--good); color: #06240f; }
  .btn.bad { background: var(--bad); color: #fff; }
  .btn[disabled] { opacity: .55; }
  .row { display: flex; gap: 10px; }
  .row > * { flex: 1; }
  .muted { color: var(--muted); }
  .small { font-size: 13px; }
  .list-item { background: var(--card); border-radius: var(--r); padding: 14px 16px; margin-bottom: 10px; cursor: pointer; display: flex; align-items: center; gap: 12px; }
  .list-item .t { flex: 1; font-weight: 600; }
  .list-item .m { color: var(--muted); font-size: 12px; font-weight: 400; margin-top: 2px; }
  .chev { color: var(--muted); font-size: 18px; }
  .seg { display: flex; background: var(--card); border-radius: 12px; padding: 3px; margin-bottom: 14px; overflow-x: auto; }
  .seg button { flex: 1; border: 0; background: transparent; color: var(--muted); padding: 9px 8px; border-radius: 10px; font: inherit;
    font-size: 13px; font-weight: 600; cursor: pointer; white-space: nowrap; }
  .seg button.on { background: var(--accent); color: var(--accent-text); }
  .perk { display: flex; gap: 10px; padding: 7px 0; }
  .perk .ic { font-size: 20px; width: 28px; text-align: center; }
  .big { font-size: 34px; font-weight: 800; letter-spacing: -.02em; }
  .pre { white-space: pre-wrap; line-height: 1.65; }
  .opt { display: block; width: 100%; text-align: left; background: var(--bg); color: var(--text); border: 1px solid var(--line);
    border-radius: 12px; padding: 12px 14px; margin-top: 8px; font: inherit; cursor: pointer; }
  .opt.correct { border-color: var(--good); background: rgba(62,207,142,.16); }
  .opt.wrong { border-color: var(--bad); background: rgba(255,107,107,.16); }
  .opt[disabled] { opacity: 1; cursor: default; }
  .sat-split { display: grid; gap: 12px; }
  .sat-passage { background: var(--bg); border: 1px solid var(--line); border-left: 3px solid var(--accent); border-radius: 12px; padding: 12px 14px; font-size: 16.5px; line-height: 1.6; white-space: pre-wrap; }
  .sat-q { font-size: 17px; line-height: 1.55; font-weight: 600; margin: 4px 0 10px; }
  .sat-opt { font-size: 16px; line-height: 1.45; padding: 13px 14px; }
  .katex { font-size: 1.08em; }
  @media (min-width: 760px) { .sat-split { grid-template-columns: 1fr 1fr; align-items: start; } }
  .progress { height: 5px; background: var(--line); border-radius: 9px; overflow: hidden; margin-bottom: 12px; }
  .progress i { display: block; height: 100%; background: var(--accent); transition: width .25s; }
  .center { text-align: center; padding: 44px 12px; color: var(--muted); }
  .spin { width: 28px; height: 28px; border: 3px solid var(--line); border-top-color: var(--accent); border-radius: 50%;
    animation: sp .8s linear infinite; margin: 0 auto 12px; }
  @keyframes sp { to { transform: rotate(360deg); } }
  .back { color: var(--accent); background: none; border: 0; font: inherit; padding: 14px 0 6px; cursor: pointer; }
  nav { position: fixed; left: 0; right: 0; bottom: 0; display: flex; background: var(--card); border-top: 1px solid var(--line);
    padding: 6px 4px calc(6px + env(safe-area-inset-bottom)); z-index: 20; }
  nav button { flex: 1; border: 0; background: none; color: var(--muted); font: inherit; font-size: 10.5px; padding: 6px 0;
    display: flex; flex-direction: column; align-items: center; gap: 2px; cursor: pointer; font-weight: 600; }
  nav button .i { font-size: 20px; }
  nav button.on { color: var(--accent); }
  .toast { position: fixed; left: 50%; bottom: 96px; transform: translateX(-50%); background: #000d; color: #fff; padding: 10px 16px;
    border-radius: 99px; font-size: 13px; z-index: 60; max-width: 90%; text-align: center; }
  .sheet-bg { position: fixed; inset: 0; background: rgba(0,0,0,.55); z-index: 40; display: flex; align-items: flex-end; }
  .sheet { background: var(--bg); width: 100%; border-radius: 22px 22px 0 0; padding: 20px 18px calc(22px + env(safe-area-inset-bottom));
    max-height: 88vh; overflow: auto; }
  textarea { width: 100%; min-height: 220px; background: var(--bg); color: var(--text); border: 1px solid var(--line); border-radius: 12px;
    padding: 12px; font: inherit; resize: vertical; }
  .wc { text-align: right; font-size: 12px; margin-top: 4px; }
  .rec { width: 86px; height: 86px; border-radius: 50%; border: 0; background: var(--bad); color: #fff; font-size: 34px; margin: 10px auto;
    display: grid; place-items: center; cursor: pointer; box-shadow: 0 0 0 0 rgba(255,107,107,.6); }
  .rec.on { animation: pulse 1.2s infinite; }
  @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(255,107,107,.55); } 100% { box-shadow: 0 0 0 22px rgba(255,107,107,0); } }
  .pics { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin: 10px 0; }
  .pics figure { margin: 0; }
  .pics img { width: 100%; height: 150px; object-fit: cover; border-radius: 12px; display: block; background: var(--line); }
  .pics figcaption { font-size: 12px; color: var(--muted); text-align: center; margin-top: 4px; }
  .errbox { border-left: 3px solid var(--bad); padding: 4px 0 4px 10px; margin: 10px 0; }
  .qtext { font-size: 17px; font-weight: 600; line-height: 1.4; }
  .scene { font-size: 16px; line-height: 1.65; white-space: pre-wrap; }
  .lb-row { display: flex; gap: 10px; padding: 9px 0; border-bottom: 1px solid var(--line); align-items: center; }
  .lb-row:last-child { border-bottom: 0; }
  .lb-row.me { color: var(--gold); font-weight: 700; }
  .lb-row .nm { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
</style>
</head>
<body>
<div id="app"><div class="center"><div class="spin"></div></div></div>
<nav id="nav"></nav>
<script>
(function () {
  "use strict";
  var tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  if (tg) { try { tg.ready(); tg.expand(); } catch (e) {} }
  var initData = tg && tg.initData ? tg.initData : "";
  var T = {
    uz: {
      home: "Bosh", science: "Ilm", english: "Ingliz tili", top: "Reyting", pro: "PRO",
      loading: "Yuklanmoqda…", preparing: "Tayyorlanmoqda… (5–20 soniya)", err: "Xatolik. Qayta urinib ko'ring.", expired: "Sessiya tugadi. Qaytadan boshlang.",
      noTg: "Bu ilovani Telegramdagi bot orqali oching.", hello: "Salom", back: "← Orqaga", again: "🔁 Yana", done: "Tayyor", next: "Keyingisi ➜", start: "Boshlash",
      freeTag: "Bepul", proTag: "PRO",
      promoT: "⭐ PRO bilan hammasi ochiladi", promoD: "Writing, Speaking, Grammar, Reading, Listening va ingliz tili viktorinalari — {p} ⭐ / {d} kun.", promoBtn: "PRO ni ko'rish",
      statQuiz: "Viktorina (eng yaxshi)", statCert: "Sertifikat (eng yaxshi)", statRead: "Reading yechilgan", statListen: "Listening yechilgan", rank: "O'rin",
      daily: "Kunlik fan va fakt", dailyD: "Bugungi fakt + 5 ta savol", quiz: "Viktorina", quizD: "Istalgan fandan {n} ta savol", cert: "Sertifikat testi", certD: "{n} savol, {p}%+ bo'lsa sertifikat",
      teaser: "Miya jumboqi", teaserD: "O'ylang, maslahat oling", guess: "Men topaman", guessD: "Siz o'ylang, men topaman", story: "Interaktiv hikoya", storyD: "Tanlovingiz yakunni o'zgartiradi",
      lie: "Yolg'onni top", lieD: "3 ta gapdan yolg'oni qaysi?", whatif: "Nima bo'ladi?", whatifD: "Ilmiy jumboqlar",
      chat: "Ingliz tilida suhbat", chatD: "Bepul · botda yozing yoki gapiring", speakFull: "Speaking to'liq mock", speakFullD: "Part 1–3, mikrofon bilan, band hisoboti",
      speakShort: "Speaking Part 1", speakShortD: "Qisqa mashq (5 savol)", writing: "Writing tahlili", writingD: "Task 2 insho → band va xatolar", grammar: "Grammar", grammarD: "Dars + 5 mashq",
      reading: "Reading testi", readingD: "Passage + 6 savol", listening: "Listening testi", listeningD: "Audio + 5 savol", library: "Kutubxona", libraryD: "Tayyor Reading / Listening / Grammar",
      pickSubject: "Fanni tanlang", random: "Tasodifiy", pickLevel: "Daraja", pickTopic: "Mavzu", randomTopic: "Tasodifiy mavzu", pickGenre: "Janrni tanlang",
      question: "Savol", correct: "To'g'ri! 🎉", wrong: "Noto'g'ri", result: "Natija", score: "Ball", certSent: "🎓 Sertifikat botga (chatga) yuborildi!", certFail: "Sertifikatni yuborib bo'lmadi, lekin natija hisobga olindi.", certNeed: "Sertifikat uchun kamida {p}% kerak.",
      fact: "Bugungi fakt", tip: "Ingliz tili maslahati", tipLocked: "🔒 Ingliz tili maslahati — PRO'da", passage: "Matn", script: "Audio matni", listenHint: "Audioni tinglab, savollarga javob bering", play: "▶️ Tinglash",
      lessonRule: "Qoida", lessonEx: "Misollar", lessonMistake: "Ko'p uchraydigan xato", startEx: "Mashqni boshlash",
      hint: "💡 Maslahat", answer: "✅ Javob", newOne: "🔄 Yangisi", yes: "✅ Ha", no: "❌ Yo'q", maybe: "🤷 Bilmayman", rightGuess: "🎯 To'g'ri!", wrongGuess: "🙅 Xato",
      iWon: "Yutdim! 😎", iLost: "Yutqazdim! Siz meni chalg'itdingiz 👏", guessIntro: "Biror narsani o'ylang — mashhur inson, hayvon, narsa, ovqat yoki qahramon. Hech qayerga yozmang. Men {n} tagacha savol beraman.",
      guessQ: "Savol", guessMine: "Menimcha, siz o'ylagan:", thinking: "O'ylayapman…", storyEnd: "Hikoya yakunlandi 🎭", whatDo: "Nima qilasiz?", streak: "Ketma-ket", round: "Raund", findLie: "Ulardan bittasi yolg'on. Qaysi?", whichHappens: "Sizningcha-chi?", stop: "To'xtatish",
      sci: "Ilmiy izoh",
      wTask: "Topshiriq", wPlace: "Inshoni shu yerga yozing (inglizcha)…", wWords: "so'z", wMin: "Kamida {n} so'z kerak", wSubmit: "Tekshirishga yuborish", wAnalyzing: "Tahlil qilinmoqda (20–40 soniya)…",
      rTask: "Task Response", rCoh: "Coherence & Cohesion", rLex: "Lexical Resource", rGra: "Grammar", rOver: "Overall Band", rStrong: "Kuchli tomonlar", rErrors: "Xatolar va tuzatishlar", rVocab: "Lug'at", rImproved: "Yaxshilangan paragraf", rNext: "Keyingi qadamlar",
      sFlu: "Fluency & Coherence", sLex: "Lexical Resource", sGra: "Grammatical Range & Accuracy", sPro: "Pronunciation (taxminiy)", sSummary: "Examiner xulosasi", sPlan: "O'quv rejasi", sNote: "Talaffuz bahosi taxminiy: u matnga aylantirilgan nutqqa asoslangan.",
      examiner: "Examiner", listenQ: "🔊 Savolni tinglash", rec: "Yozishni boshlash", recStop: "To'xtatish", recSend: "Yuborish", recRedo: "Qayta yozish", recProcessing: "Javob qayta ishlanmoqda…", recHint: "Mikrofon tugmasini bosing va ingliz tilida javob bering.",
      part: "Part", prep: "Tayyorlanish", picA: "Rasm A", picB: "Rasm B", picsFail: "Rasmlar yuklanmadi, tavsifi:", micDenied: "Mikrofonga ruxsat berilmadi yoki ishlamayapti.", micChat: "Chatda ovozli xabar bilan davom etish", noSpeech: "Nutq aniqlanmadi. Aniqroq gapirib qayta yozing.", tooBig: "Yozuv juda uzun.", maxTime: "Maksimal vaqt tugadi",
      pwT: "🔒 Bu PRO funksiya", pwD: "Ingliz tiliga oid hamma vositalar (Writing, Speaking, Grammar, Reading, Listening, Kutubxona) PRO'da. Ingliz tilida erkin suhbat esa bepul.", pwGet: "⭐ PRO olish", later: "Hozir emas",
      payT: "Premium funksiya", payD: "PRO bilan hammasi tekin yoki bir martalik to'lang.", payOnce: "Faqat shu safar", limitT: "PRO limiti tugadi", limitD: "Bugungi limit ({n} ta) tugadi. Ertaga yana tekin yoki bir martalik to'lang.",
      upT: "🎉 Zo'r o'ynadingiz!", upD: "PRO bilan ingliz tilidagi Writing, Speaking, Grammar, Reading va Listening ham ochiladi. {p} ⭐ / {d} kun.", upBtn: "⭐ PRO ga o'tish", upLater: "Keyinroq",
      proActive: "PRO faol", until: "Tugash sanasi", daysLeft: "kun qoldi", extend: "Muddatni uzaytirish", buy: "PRO olish", offerT: "PRO obuna", days: "kun",
      perk1: "IELTS Writing tahlili", perk2: "To'liq Speaking mock", perk3: "Grammar, Reading, Listening, Kutubxona", perk4: "Ingliz tili viktorinalari", perk5: "⭐ PRO belgisi", perkLimit: "Writing va Speaking: kuniga {n} tagacha. Avtomatik yangilanmaydi.",
      owner: "Siz bot egasisiz — hammasi doim tekin.", paidOk: "To'lov qabul qilindi! ⭐", cancelled: "To'lov bekor qilindi.", checking: "Tekshirilmoqda…", asSeparate: "Alohida: Writing {w} ⭐ + Speaking {s} ⭐",
      empty: "Hozircha kontent yo'q.", questions: "ta savol", readOnly: "O'qish / tinglash", noRank: "Hozircha natijalar yo'q", boardQuiz: "Viktorina", boardCert: "Sertifikat", boardRead: "Reading", boardListen: "Listening",
      sat: "SAT (xalqaro)",
      satD: "Digital SAT: Reading & Writing + Math",
      satMock: "To'liq mock (adaptiv)",
      satMockD: "4 modul · 2-modul qiyinligi natijangizga qarab tanlanadi · taxminiy ball",
      satPractice: "Mavzu bo'yicha mashq",
      satPracticeD: "10 savol · tushuntirishlar bilan",
      satRW: "Reading & Writing",
      satMath: "Math",
      satAllDomains: "Aralash",
      satStage: "Modul",
      satSubmit: "Modulni topshirish",
      satPrev: "←",
      satFlag: "🚩 Belgilash",
      satNumPlace: "Javobni yozing (son yoki kasr, masalan 3/4)",
      satNumHint: "Son yoki kasr kiriting",
      satRef: "Formulalar (Reference)", satTimeUp: "Vaqt tugadi — xohlasangiz davom etishingiz mumkin.",
      satConfirm: "Javob berilmagan savollar:",
      satSure: "Topshiramizmi?",
      satYes: "Ha, topshirish",
      satNo: "Yo'q",
      satModDone: "Modul topshirildi",
      satNextMod: "Keyingi modul",
      satHarder: "Qiyinroq modul keladi 🔥",
      satEasier: "Yengilroq modul keladi",
      satEst: "Taxminiy ball",
      satTotal: "Jami (400–1600)",
      satEstNote: "Bu rasmiy College Board bali emas, AI savollari asosidagi taxminiy baho.",
      satDomains: "Bo'limlar bo'yicha",
      satWeak: "Zaif tomonlar",
      satReview: "Xatolarni ko'rish",
      satAllQ: "Hammasi",
      satWrongOnly: "Faqat xatolar",
      satYour: "Sizning javobingiz",
      satRight: "To'g'ri javob",
      satNone: "— javob yo'q —",
      satSec: "Bo'lim",
      satRoute: "Yo'nalish",
      satHardR: "qiyin",
      satEasyR: "yengil",
      satScoreOf: "to'g'ri",
      satLeft: "qoldi",
      satGenFail: "Savollar tayyorlanmadi. Qayta urinib ko'ring.",
      chatLink: "Botga o'tish"
    },
    ru: {
      home: "Главная", science: "Наука", english: "Английский", top: "Рейтинг", pro: "PRO",
      loading: "Загрузка…", preparing: "Готовлю… (5–20 секунд)", err: "Ошибка. Попробуйте ещё раз.", expired: "Сессия закончилась. Начните заново.",
      noTg: "Откройте приложение через бота в Telegram.", hello: "Привет", back: "← Назад", again: "🔁 Ещё раз", done: "Готово", next: "Дальше ➜", start: "Начать",
      freeTag: "Бесплатно", proTag: "PRO",
      promoT: "⭐ С PRO открывается всё", promoD: "Writing, Speaking, Grammar, Reading, Listening и английские викторины — {p} ⭐ / {d} дней.", promoBtn: "Узнать про PRO",
      statQuiz: "Викторина (лучший)", statCert: "Сертификат (лучший)", statRead: "Reading пройдено", statListen: "Listening пройдено", rank: "Место",
      daily: "Наука и факт дня", dailyD: "Факт дня + 5 вопросов", quiz: "Викторина", quizD: "{n} вопросов по любой теме", cert: "Тест с сертификатом", certD: "{n} вопросов, от {p}% — сертификат",
      teaser: "Головоломка", teaserD: "Думайте, берите подсказку", guess: "Я угадаю", guessD: "Загадайте — я угадаю", story: "Интерактивная история", storyD: "Ваш выбор меняет финал",
      lie: "Найди ложь", lieD: "Какое из 3 утверждений ложь?", whatif: "Что будет?", whatifD: "Научные загадки",
      chat: "Разговор на английском", chatD: "Бесплатно · пишите или говорите в боте", speakFull: "Полный Speaking мок", speakFullD: "Part 1–3, с микрофоном, отчёт с баллами",
      speakShort: "Speaking Part 1", speakShortD: "Короткая практика (5 вопросов)", writing: "Разбор Writing", writingD: "Эссе Task 2 → балл и ошибки", grammar: "Grammar", grammarD: "Урок + 5 упражнений",
      reading: "Тест Reading", readingD: "Текст + 6 вопросов", listening: "Тест Listening", listeningD: "Аудио + 5 вопросов", library: "Библиотека", libraryD: "Готовые Reading / Listening / Grammar",
      pickSubject: "Выберите тему", random: "Случайная", pickLevel: "Уровень", pickTopic: "Тема", randomTopic: "Случайная тема", pickGenre: "Выберите жанр",
      question: "Вопрос", correct: "Верно! 🎉", wrong: "Неверно", result: "Результат", score: "Баллы", certSent: "🎓 Сертификат отправлен вам в чат с ботом!", certFail: "Не удалось отправить сертификат, но результат засчитан.", certNeed: "Для сертификата нужно минимум {p}%.",
      fact: "Факт дня", tip: "Совет по английскому", tipLocked: "🔒 Совет по английскому — в PRO", passage: "Текст", script: "Текст аудио", listenHint: "Прослушайте аудио и ответьте на вопросы", play: "▶️ Слушать",
      lessonRule: "Правило", lessonEx: "Примеры", lessonMistake: "Частая ошибка", startEx: "Начать упражнения",
      hint: "💡 Подсказка", answer: "✅ Ответ", newOne: "🔄 Новая", yes: "✅ Да", no: "❌ Нет", maybe: "🤷 Не знаю", rightGuess: "🎯 Верно!", wrongGuess: "🙅 Нет",
      iWon: "Я выиграл! 😎", iLost: "Я проиграл! Вы меня запутали 👏", guessIntro: "Загадайте что-нибудь — известного человека, животное, предмет, еду или героя. Нигде не пишите. Я задам до {n} вопросов.",
      guessQ: "Вопрос", guessMine: "Думаю, вы загадали:", thinking: "Думаю…", storyEnd: "История закончена 🎭", whatDo: "Что вы сделаете?", streak: "Подряд", round: "Раунд", findLie: "Одно из них ложь. Какое?", whichHappens: "А как думаете вы?", stop: "Закончить",
      sci: "Научное пояснение",
      wTask: "Задание", wPlace: "Пишите эссе здесь (на английском)…", wWords: "слов", wMin: "Нужно минимум {n} слов", wSubmit: "Отправить на проверку", wAnalyzing: "Идёт разбор (20–40 секунд)…",
      rTask: "Task Response", rCoh: "Coherence & Cohesion", rLex: "Lexical Resource", rGra: "Grammar", rOver: "Overall Band", rStrong: "Сильные стороны", rErrors: "Ошибки и исправления", rVocab: "Лексика", rImproved: "Улучшенный абзац", rNext: "Следующие шаги",
      sFlu: "Fluency & Coherence", sLex: "Lexical Resource", sGra: "Grammatical Range & Accuracy", sPro: "Pronunciation (оценка)", sSummary: "Вывод экзаменатора", sPlan: "План обучения", sNote: "Оценка произношения приблизительная: она основана на расшифрованной речи.",
      examiner: "Экзаменатор", listenQ: "🔊 Прослушать вопрос", rec: "Начать запись", recStop: "Остановить", recSend: "Отправить", recRedo: "Записать заново", recProcessing: "Обрабатываю ответ…", recHint: "Нажмите кнопку микрофона и отвечайте по-английски.",
      part: "Part", prep: "Подготовка", picA: "Картинка A", picB: "Картинка B", picsFail: "Картинки не загрузились, описание:", micDenied: "Нет доступа к микрофону или он не работает.", micChat: "Продолжить голосовыми в чате", noSpeech: "Речь не распознана. Говорите чётче и запишите снова.", tooBig: "Запись слишком длинная.", maxTime: "Время записи вышло",
      pwT: "🔒 Это функция PRO", pwD: "Все инструменты английского (Writing, Speaking, Grammar, Reading, Listening, библиотека) — в PRO. Свободный разговор на английском бесплатен.", pwGet: "⭐ Получить PRO", later: "Не сейчас",
      payT: "Премиум-функция", payD: "С PRO всё бесплатно, либо оплатите разово.", payOnce: "Только в этот раз", limitT: "Лимит PRO исчерпан", limitD: "Дневной лимит ({n}) исчерпан. Завтра снова бесплатно, либо оплатите разово.",
      upT: "🎉 Отлично сыграли!", upD: "С PRO откроются Writing, Speaking, Grammar, Reading и Listening по английскому. {p} ⭐ / {d} дней.", upBtn: "⭐ Перейти на PRO", upLater: "Позже",
      proActive: "PRO активна", until: "Действует до", daysLeft: "дн. осталось", extend: "Продлить", buy: "Получить PRO", offerT: "PRO-подписка", days: "дней",
      perk1: "Разбор IELTS Writing", perk2: "Полный Speaking мок", perk3: "Grammar, Reading, Listening, библиотека", perk4: "Английские викторины", perk5: "⭐ Значок PRO", perkLimit: "Writing и Speaking: до {n} в день. Без автопродления.",
      owner: "Вы владелец бота — для вас всё бесплатно.", paidOk: "Оплата получена! ⭐", cancelled: "Оплата отменена.", checking: "Проверяем…", asSeparate: "По отдельности: Writing {w} ⭐ + Speaking {s} ⭐",
      empty: "Пока нет материалов.", questions: "вопросов", readOnly: "Для чтения / прослушивания", noRank: "Пока нет результатов", boardQuiz: "Викторина", boardCert: "Сертификат", boardRead: "Reading", boardListen: "Listening",
      sat: "SAT (международный)",
      satD: "Digital SAT: Reading & Writing + Math",
      satMock: "Полный мок (адаптивный)",
      satMockD: "4 модуля · сложность 2-го модуля зависит от результата · ориентировочный балл",
      satPractice: "Практика по темам",
      satPracticeD: "10 вопросов · с объяснениями",
      satRW: "Reading & Writing",
      satMath: "Math",
      satAllDomains: "Микс",
      satStage: "Модуль",
      satSubmit: "Сдать модуль",
      satPrev: "←",
      satFlag: "🚩 Отметить",
      satNumPlace: "Введите ответ (число или дробь, например 3/4)",
      satNumHint: "Введите число или дробь",
      satRef: "Формулы (Reference)", satTimeUp: "Время вышло — можно продолжить, если хотите.",
      satConfirm: "Вопросов без ответа:",
      satSure: "Сдаём?",
      satYes: "Да, сдать",
      satNo: "Нет",
      satModDone: "Модуль сдан",
      satNextMod: "Следующий модуль",
      satHarder: "Следующий модуль будет сложнее 🔥",
      satEasier: "Следующий модуль будет проще",
      satEst: "Ориентировочный балл",
      satTotal: "Всего (400–1600)",
      satEstNote: "Это не официальный балл College Board, а оценка по вопросам, созданным ИИ.",
      satDomains: "По разделам",
      satWeak: "Слабые места",
      satReview: "Разбор ошибок",
      satAllQ: "Все",
      satWrongOnly: "Только ошибки",
      satYour: "Ваш ответ",
      satRight: "Верный ответ",
      satNone: "— нет ответа —",
      satSec: "Раздел",
      satRoute: "Маршрут",
      satHardR: "сложный",
      satEasyR: "лёгкий",
      satScoreOf: "верно",
      satLeft: "осталось",
      satGenFail: "Не удалось подготовить вопросы. Попробуйте ещё раз.",
      chatLink: "Перейти в бота"
    },
    en: {
      home: "Home", science: "Science", english: "English", top: "Ranking", pro: "PRO",
      loading: "Loading…", preparing: "Preparing… (5–20 seconds)", err: "Something went wrong. Please try again.", expired: "Session expired. Please start again.",
      noTg: "Please open this app from the bot inside Telegram.", hello: "Hi", back: "← Back", again: "🔁 Again", done: "Done", next: "Next ➜", start: "Start",
      freeTag: "Free", proTag: "PRO",
      promoT: "⭐ PRO unlocks everything", promoD: "Writing, Speaking, Grammar, Reading, Listening and English quizzes — {p} ⭐ / {d} days.", promoBtn: "See PRO",
      statQuiz: "Quiz (best)", statCert: "Certificate (best)", statRead: "Reading completed", statListen: "Listening completed", rank: "Rank",
      daily: "Daily science & fact", dailyD: "Today's fact + 5 questions", quiz: "Quiz", quizD: "{n} questions on any subject", cert: "Certificate test", certD: "{n} questions, certificate at {p}%+",
      teaser: "Brain teaser", teaserD: "Think, ask for a hint", guess: "I'll guess", guessD: "You think, I guess", story: "Interactive story", storyD: "Your choices change the ending",
      lie: "Spot the lie", lieD: "Which of 3 statements is false?", whatif: "What happens?", whatifD: "Science puzzles",
      chat: "Chat in English", chatD: "Free · write or speak in the bot", speakFull: "Full Speaking mock", speakFullD: "Parts 1–3, with your mic, band report",
      speakShort: "Speaking Part 1", speakShortD: "Quick practice (5 questions)", writing: "Writing review", writingD: "Task 2 essay → band and mistakes", grammar: "Grammar", grammarD: "Lesson + 5 exercises",
      reading: "Reading test", readingD: "Passage + 6 questions", listening: "Listening test", listeningD: "Audio + 5 questions", library: "Library", libraryD: "Ready-made Reading / Listening / Grammar",
      pickSubject: "Pick a subject", random: "Random", pickLevel: "Level", pickTopic: "Topic", randomTopic: "Random topic", pickGenre: "Pick a genre",
      question: "Question", correct: "Correct! 🎉", wrong: "Wrong", result: "Result", score: "Score", certSent: "🎓 Your certificate was sent to your chat with the bot!", certFail: "Couldn't send the certificate, but your result counted.", certNeed: "You need at least {p}% for the certificate.",
      fact: "Today's fact", tip: "English tip", tipLocked: "🔒 English tip — in PRO", passage: "Passage", script: "Audio script", listenHint: "Listen to the audio and answer the questions", play: "▶️ Listen",
      lessonRule: "Rule", lessonEx: "Examples", lessonMistake: "Common mistake", startEx: "Start exercises",
      hint: "💡 Hint", answer: "✅ Answer", newOne: "🔄 New one", yes: "✅ Yes", no: "❌ No", maybe: "🤷 Don't know", rightGuess: "🎯 Correct!", wrongGuess: "🙅 Wrong",
      iWon: "I win! 😎", iLost: "I lost! You fooled me 👏", guessIntro: "Think of something — a famous person, an animal, an object, a food or a character. Don't type it anywhere. I'll ask up to {n} questions.",
      guessQ: "Question", guessMine: "I think you're thinking of:", thinking: "Thinking…", storyEnd: "The story is over 🎭", whatDo: "What do you do?", streak: "Streak", round: "Round", findLie: "One of them is a lie. Which?", whichHappens: "What do you think?", stop: "Finish",
      sci: "The science",
      wTask: "Task", wPlace: "Write your essay here (in English)…", wWords: "words", wMin: "At least {n} words needed", wSubmit: "Submit for review", wAnalyzing: "Reviewing your essay (20–40 seconds)…",
      rTask: "Task Response", rCoh: "Coherence & Cohesion", rLex: "Lexical Resource", rGra: "Grammar", rOver: "Overall Band", rStrong: "Strengths", rErrors: "Mistakes and corrections", rVocab: "Vocabulary", rImproved: "Improved paragraph", rNext: "Next steps",
      sFlu: "Fluency & Coherence", sLex: "Lexical Resource", sGra: "Grammatical Range & Accuracy", sPro: "Pronunciation (est.)", sSummary: "Examiner's summary", sPlan: "Study plan", sNote: "The pronunciation score is an estimate based on the transcribed speech.",
      examiner: "Examiner", listenQ: "🔊 Hear the question", rec: "Start recording", recStop: "Stop", recSend: "Send", recRedo: "Record again", recProcessing: "Processing your answer…", recHint: "Tap the microphone and answer in English.",
      part: "Part", prep: "Preparation", picA: "Picture A", picB: "Picture B", picsFail: "The pictures didn't load, described in words:", micDenied: "Microphone access was denied or isn't available.", micChat: "Continue with voice messages in chat", noSpeech: "No speech detected. Please speak clearly and record again.", tooBig: "The recording is too long.", maxTime: "Max recording time reached",
      pwT: "🔒 This is a PRO feature", pwD: "All English tools (Writing, Speaking, Grammar, Reading, Listening, Library) are in PRO. Free chat in English stays free.", pwGet: "⭐ Get PRO", later: "Not now",
      payT: "Premium feature", payD: "Included with PRO, or pay once for just this.", payOnce: "Just this once", limitT: "PRO limit reached", limitD: "Today's limit ({n}) is used up. Free again tomorrow, or pay for a single use.",
      upT: "🎉 Nice game!", upD: "PRO also unlocks English Writing, Speaking, Grammar, Reading and Listening. {p} ⭐ / {d} days.", upBtn: "⭐ Go PRO", upLater: "Later",
      proActive: "PRO is active", until: "Valid until", daysLeft: "days left", extend: "Extend", buy: "Get PRO", offerT: "PRO subscription", days: "days",
      perk1: "IELTS Writing review", perk2: "Full Speaking mock", perk3: "Grammar, Reading, Listening, Library", perk4: "English quizzes", perk5: "⭐ PRO badge", perkLimit: "Writing and Speaking: up to {n} per day. Does not auto-renew.",
      owner: "You're the bot owner — everything is always free for you.", paidOk: "Payment received! ⭐", cancelled: "Payment cancelled.", checking: "Checking…", asSeparate: "Bought separately: Writing {w} ⭐ + Speaking {s} ⭐",
      empty: "No content yet.", questions: "questions", readOnly: "For reading / listening", noRank: "No results yet", boardQuiz: "Quiz", boardCert: "Certificate", boardRead: "Reading", boardListen: "Listening",
      sat: "SAT (international)",
      satD: "Digital SAT: Reading & Writing + Math",
      satMock: "Full mock (adaptive)",
      satMockD: "4 modules · module 2 difficulty adapts to your result · estimated score",
      satPractice: "Practice by topic",
      satPracticeD: "10 questions · with explanations",
      satRW: "Reading & Writing",
      satMath: "Math",
      satAllDomains: "Mixed",
      satStage: "Module",
      satSubmit: "Submit module",
      satPrev: "←",
      satFlag: "🚩 Flag",
      satNumPlace: "Type your answer (number or fraction, e.g. 3/4)",
      satNumHint: "Enter a number or fraction",
      satRef: "Reference sheet", satTimeUp: "Time is up — you may keep going if you wish.",
      satConfirm: "Unanswered questions:",
      satSure: "Submit now?",
      satYes: "Yes, submit",
      satNo: "No",
      satModDone: "Module submitted",
      satNextMod: "Next module",
      satHarder: "A harder module is coming 🔥",
      satEasier: "An easier module is coming",
      satEst: "Estimated score",
      satTotal: "Total (400–1600)",
      satEstNote: "This is not an official College Board score; it is an estimate from AI-written questions.",
      satDomains: "By domain",
      satWeak: "Weak spots",
      satReview: "Review answers",
      satAllQ: "All",
      satWrongOnly: "Wrong only",
      satYour: "Your answer",
      satRight: "Correct answer",
      satNone: "— no answer —",
      satSec: "Section",
      satRoute: "Route",
      satHardR: "hard",
      satEasyR: "easy",
      satScoreOf: "correct",
      satLeft: "left",
      satGenFail: "Couldn't prepare the questions. Try again.",
      chatLink: "Go to the bot"
    }
  };

  var guess = ((tg && tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.language_code) || "en").slice(0, 2);
  var lang = T[guess] ? guess : "en";
  function S(k, vars) {
    var s = (T[lang] && T[lang][k]) || T.en[k] || k;
    if (vars) Object.keys(vars).forEach(function (x) { s = s.split("{" + x + "}").join(vars[x]); });
    return s;
  }

  var me = null, tab = "home", app = document.getElementById("app"), nav = document.getElementById("nav");
  var cleanupFn = null, lastUpsell = 0;

  function h(tag, attrs) {
    var el = document.createElement(tag);
    attrs = attrs || {};
    Object.keys(attrs).forEach(function (k) {
      if (k === "class") el.className = attrs[k];
      else if (k === "text") el.textContent = attrs[k];
      else if (k.indexOf("on") === 0) el.addEventListener(k.slice(2), attrs[k]);
      else if (attrs[k] !== false && attrs[k] != null) el.setAttribute(k, attrs[k]);
    });
    (function add(list) {
      for (var i = 0; i < list.length; i++) {
        var c = list[i];
        if (c == null || c === false) continue;
        if (Array.isArray(c)) { add(c); continue; }
        el.appendChild(typeof c === "string" ? document.createTextNode(c) : c);
      }
    })(Array.prototype.slice.call(arguments, 2));
    return el;
  }
  function clear(el) { while (el.firstChild) el.removeChild(el.firstChild); }
  function mount() {
    if (cleanupFn) { try { cleanupFn(); } catch (e) {} cleanupFn = null; }
    clear(app);
    for (var i = 0; i < arguments.length; i++) if (arguments[i]) app.appendChild(arguments[i]);
    window.scrollTo(0, 0);
  }
  function spinner(msg) { return h("div", { class: "center" }, h("div", { class: "spin" }), h("div", { text: msg || S("loading") })); }
  function toast(msg) {
    var t = h("div", { class: "toast", text: msg }); document.body.appendChild(t);
    setTimeout(function () { if (t.parentNode) t.parentNode.removeChild(t); }, 2400);
  }
  function haptic(kind) { try { tg.HapticFeedback.notificationOccurred(kind); } catch (e) {} }
  function isPremium() { return !!(me && (me.admin || me.pro.active)); }

  function api(path, opts) {
    opts = opts || {};
    var headers = { "X-Init-Data": initData };
    if (opts.json !== undefined) { headers["Content-Type"] = "application/json"; opts.body = JSON.stringify(opts.json); delete opts.json; }
    opts.headers = Object.assign(headers, opts.headers || {});
    return fetch(path, opts).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (d) {
        if (!r.ok) { var e = new Error("http"); e.status = r.status; e.data = d; throw e; }
        return d;
      });
    });
  }
  function post(path, json) { return api(path, { method: "POST", json: json || {} }); }
  function refreshMe() { return api("/api/app/me").then(function (d) { me = d; }); }

  // ---- umumiy xato ishlovchi ----
  function fail(e, retry) {
    var st = e && e.status, d = (e && e.data) || {};
    if (st === 403 && d.error === "pro_required") { paywall(); return; }
    if (st === 402) { payPrompt(d.product, retry); return; }
    if (st === 429) { limitSheet(); return; }
    if (st === 410) { toast(S("expired")); show(); return; }
    toast(S("err")); show();
  }

  // ---- bottom sheet ----
  function sheet() {
    var box = h("div", { class: "sheet" });
    for (var i = 0; i < arguments.length; i++) if (arguments[i]) box.appendChild(arguments[i]);
    var bg = h("div", { class: "sheet-bg", onclick: function (ev) { if (ev.target === bg) close(); } }, box);
    function close() { if (bg.parentNode) bg.parentNode.removeChild(bg); }
    document.body.appendChild(bg);
    return { close: close };
  }
  function perks() {
    return h("div", { style: "margin:10px 0" },
      [["💎", "perk1"], ["🎙", "perk2"], ["📝", "perk3"], ["🌐", "perk4"]].map(function (x) {
        return h("div", { class: "perk" }, h("div", { class: "ic", text: x[0] }), h("div", { text: S(x[1]) }));
      }));
  }
  function paywall() {
    var s = sheet(h("h2", { text: S("pwT") }), h("p", { class: "muted", style: "margin-top:8px", text: S("pwD") }), perks(),
      h("button", { class: "btn gold", onclick: function () { s.close(); buyProduct("pro"); }, text: S("pwGet") + " · ⭐ " + me.prices.pro + " / " + me.prices.pro_days + " " + S("days") }),
      h("button", { class: "btn ghost", onclick: function () { s.close(); }, text: S("later") }));
  }
  function payPrompt(product, retry) {
    var price = product === "writing" ? me.prices.writing : me.prices.speaking;
    var s = sheet(h("h2", { text: S("payT") }), h("p", { class: "muted", style: "margin-top:8px", text: S("payD") }),
      h("button", { class: "btn gold", onclick: function () { s.close(); buyProduct("pro", retry); }, text: S("pwGet") + " · ⭐ " + me.prices.pro + " / " + me.prices.pro_days + " " + S("days") }),
      h("button", { class: "btn", onclick: function () { s.close(); buyProduct(product, retry); }, text: S("payOnce") + " · ⭐ " + price }),
      h("button", { class: "btn ghost", onclick: function () { s.close(); show(); }, text: S("later") }));
  }
  function limitSheet() {
    var s = sheet(h("h2", { text: S("limitT") }), h("p", { class: "muted", style: "margin-top:8px", text: S("limitD", { n: me.prices.pro_daily_limit }) }),
      h("button", { class: "btn ghost", onclick: function () { s.close(); show(); }, text: S("done") }));
  }
  function upsell() {
    if (isPremium()) return;
    var now = Date.now();
    if (now - lastUpsell < 150000) return;
    lastUpsell = now;
    var s = sheet(h("h2", { text: S("upT") }), h("p", { class: "muted", style: "margin-top:8px", text: S("upD", { p: me.prices.pro, d: me.prices.pro_days }) }), perks(),
      h("button", { class: "btn gold", onclick: function () { s.close(); buyProduct("pro"); }, text: S("upBtn") }),
      h("button", { class: "btn ghost", onclick: function () { s.close(); }, text: S("upLater") }));
  }

  function buyProduct(product, onPaid) {
    post("/api/app/invoice", { product: product }).then(function (r) {
      if (!tg || !tg.openInvoice) { window.location.href = r.link; return; }
      tg.openInvoice(r.link, function (status) {
        if (status === "paid") {
          haptic("success"); toast(S("paidOk"));
          var tries = 0;
          (function poll() {
            refreshMe().then(function () {
              var ok = product === "pro" ? me.pro.active : (product === "writing" ? me.credits.writing : me.credits.speaking);
              if (ok || tries++ > 8) { if (onPaid) onPaid(); else show(); }
              else setTimeout(poll, 1200);
            }).catch(function () { show(); });
          })();
        } else if (status === "cancelled") toast(S("cancelled"));
      });
    }).catch(function () { toast(S("err")); });
  }

  function openBot(param) {
    var url = "https://t.me/" + me.bot + (param ? "?start=" + param : "");
    try { tg.openTelegramLink(url); } catch (e) { window.location.href = url; }
    setTimeout(function () { try { tg.close(); } catch (e) {} }, 300);
  }

  // ---- navigatsiya ----
  function renderNav() {
    clear(nav);
    [["home", "🏠"], ["science", "🔬"], ["english", "🇬🇧"], ["top", "🏆"], ["pro", "⭐"]].forEach(function (x) {
      nav.appendChild(h("button", { class: tab === x[0] ? "on" : "", onclick: function () { tab = x[0]; show(); } },
        h("span", { class: "i", text: x[1] }), h("span", { text: S(x[0]) })));
    });
  }
  function show() {
    renderNav();
    if (!me) return;
    ({ home: viewHome, science: viewScience, english: viewEnglish, top: viewTop, pro: viewPro })[tab]();
  }
  function backBtn(fn) { return h("button", { class: "back", onclick: fn || show, text: S("back") }); }
  function title(txt, sub) {
    return h("div", { class: "top" }, h("div", {}, h("h1", { text: txt }), sub ? h("div", { class: "sub", text: sub }) : null));
  }
  function tile(emoji, name, desc, onclick, opts) {
    opts = opts || {};
    return h("button", { class: "tile", onclick: onclick },
      h("div", { class: "e", text: emoji }), h("div", { class: "n", text: name }),
      desc ? h("div", { class: "d", text: desc }) : null,
      opts.lock ? h("div", { class: "lk", text: "🔒" }) : null,
      opts.price ? h("div", { class: "lock", text: "⭐ " + opts.price }) : null);
  }
  function stat(v, l, rank) {
    return h("div", { class: "stat" }, h("div", { class: "v", text: v }), h("div", { class: "l", text: l }),
      rank ? h("div", { class: "r", text: S("rank") + " #" + rank }) : null);
  }

  // ================= BOSH SAHIFA =================
  function viewHome() {
    var st = me.stats, initial = (me.name || "?").trim().charAt(0).toUpperCase() || "?";
    var nm = h("h1", { text: me.name || "" });
    if (me.admin || me.pro.active) nm.appendChild(h("span", { class: "badge", text: me.admin ? "OWNER" : "PRO" }));
    var parts = [h("div", { class: "top" }, h("div", { class: "avatar", text: initial }), h("div", {}, nm, h("div", { class: "sub", text: S("hello") + " 👋" })))];
    if (!isPremium()) {
      parts.push(h("div", { class: "card promo" }, h("h3", { text: S("promoT") }),
        h("p", { class: "muted small", style: "margin-top:6px", text: S("promoD", { p: me.prices.pro, d: me.prices.pro_days }) }),
        h("button", { class: "btn gold", onclick: function () { tab = "pro"; show(); }, text: S("promoBtn") })));
    }
    parts.push(h("div", { class: "section-title", text: "🚀" }),
      h("div", { class: "grid2" },
        tile("📅", S("daily"), S("dailyD"), function () { startQuiz({ kind: "daily" }); }),
        tile("🧩", S("quiz"), S("quizD", { n: me.limits.quiz_len }), quizPicker)));
    parts.push(h("div", { class: "section-title", text: "📊" }),
      h("div", { class: "grid2" },
        stat(st.quiz_best != null ? st.quiz_best + "%" : "—", S("statQuiz"), st.quiz_rank),
        stat(st.cert_best != null ? st.cert_best + "%" : "—", S("statCert"), st.cert_rank),
        stat(String(st.reading_done), S("statRead")), stat(String(st.listening_done), S("statListen"))));
    mount.apply(null, parts);
  }

  function viewScience() {
    var L = me.limits;
    mount(title(S("science")),
      h("div", { class: "grid2" },
        tile("📅", S("daily"), S("dailyD"), function () { startQuiz({ kind: "daily" }); }),
        tile("🧩", S("quiz"), S("quizD", { n: L.quiz_len }), quizPicker),
        tile("🎓", S("cert"), S("certD", { n: L.cert_len, p: L.cert_pass }), function () { startQuiz({ kind: "certificate" }); }),
        tile("🧠", S("teaser"), S("teaserD"), teaserGame),
        tile("🔮", S("guess"), S("guessD"), guessGame),
        tile("📖", S("story"), S("storyD"), storyPicker),
        tile("🕵️", S("lie"), S("lieD"), function () { roundGame("lie"); }),
        tile("🔬", S("whatif"), S("whatifD"), function () { roundGame("whatif"); })));
  }

  function viewEnglish() {
    var P = isPremium(), pr = me.prices;
    mount(title(S("english")),
      h("div", { class: "grid2" },
        tile("💬", S("chat"), S("chatD"), function () { openBot("run_english"); }),
        tile("🎙", S("speakFull"), S("speakFullD"), function () { speakingFlow("full"); }, { price: P ? 0 : pr.speaking }),
        tile("💎", S("writing"), S("writingD"), writingFlow, { price: P ? 0 : pr.writing }),
        tile("🎤", S("speakShort"), S("speakShortD"), function () { P ? speakingFlow("short") : paywall(); }, { lock: !P }),
        tile("📝", S("grammar"), S("grammarD"), function () { P ? grammarPicker() : paywall(); }, { lock: !P }),
        tile("📖", S("reading"), S("readingD"), function () { P ? startQuiz({ kind: "reading" }) : paywall(); }, { lock: !P }),
        tile("🎧", S("listening"), S("listeningD"), function () { P ? startQuiz({ kind: "listening" }) : paywall(); }, { lock: !P }),
        tile("📚", S("library"), S("libraryD"), function () { P ? viewLibrary("reading") : paywall(); }, { lock: !P }),
        tile("🎓", S("sat"), S("satD"), viewSat, { lock: !P })));
  }

  // ================= REYTING =================
  var boardKey = "quiz";
  function viewTop() {
    var seg = h("div", { class: "seg" });
    [["quiz", "boardQuiz"], ["certificate", "boardCert"], ["reading", "boardRead"], ["listening", "boardListen"]].forEach(function (x) {
      seg.appendChild(h("button", { class: boardKey === x[0] ? "on" : "", onclick: function () { boardKey = x[0]; viewTop(); } }, S(x[1])));
    });
    var box = h("div", { class: "card" }, spinner());
    mount(title(S("top")), seg, box);
    api("/api/app/leaderboard").then(function (d) {
      var b = d.boards.filter(function (x) { return x.key === boardKey; })[0];
      clear(box);
      if (!b || !b.rows.length) { box.appendChild(h("div", { class: "center", text: S("noRank") })); return; }
      var medals = ["🥇", "🥈", "🥉"];
      b.rows.forEach(function (r, i) {
        box.appendChild(h("div", { class: "lb-row" + (r.me ? " me" : "") },
          h("div", { text: medals[i] || String(i + 1) + "." }), h("div", { class: "nm", text: r.name }), h("div", { text: r.pct + "%" })));
      });
    }).catch(function () { clear(box); box.appendChild(h("div", { class: "center", text: S("err") })); });
  }

  // ================= PRO =================
  function viewPro() {
    var p = me.pro, parts = [title("⭐ " + S("offerT"))];
    if (me.admin) { mount(parts[0], h("div", { class: "card", text: S("owner") })); return; }
    if (p.active) parts.push(h("div", { class: "card" }, h("h3", { text: "✅ " + S("proActive") }),
      h("p", { class: "muted", style: "margin-top:6px", text: S("until") + ": " + p.until + " (" + p.left + " " + S("daysLeft") + ")" })));
    parts.push(h("div", { class: "card" },
      [["💎", "perk1"], ["🎙", "perk2"], ["📝", "perk3"], ["🌐", "perk4"], ["🏅", "perk5"]].map(function (x) {
        return h("div", { class: "perk" }, h("div", { class: "ic", text: x[0] }), h("div", { text: S(x[1]) }));
      }),
      h("p", { class: "muted small", style: "margin-top:6px", text: S("perkLimit", { n: me.prices.pro_daily_limit }) }),
      h("p", { class: "muted small", text: S("asSeparate", { w: me.prices.writing, s: me.prices.speaking }) })));
    parts.push(h("button", { class: "btn gold", onclick: function () { buyProduct("pro"); },
      text: (p.active ? S("extend") : S("buy")) + " · ⭐ " + me.prices.pro + " / " + me.prices.pro_days + " " + S("days") }));
    mount.apply(null, parts);
  }

  // ================= VIKTORINA TURLARI =================
  function quizPicker() {
    var grid = h("div", { class: "grid2" });
    me.subjects.forEach(function (s) {
      grid.appendChild(tile(s.emoji, s.name, null, function () {
        if (s.pro && !isPremium()) { paywall(); return; }
        startQuiz({ kind: "quiz", subject: s.i });
      }, { lock: s.pro && !isPremium() }));
    });
    grid.appendChild(tile("🎲", S("random"), null, function () { startQuiz({ kind: "quiz", subject: "r" }); }));
    mount(backBtn(), title("🧩 " + S("pickSubject")), grid);
  }

  function grammarPicker() {
    var idx = 0;
    function draw() {
      var seg = h("div", { class: "seg" });
      me.levels.forEach(function (l, i) {
        seg.appendChild(h("button", { class: idx === i ? "on" : "", onclick: function () { idx = i; draw(); } }, l.emoji + " " + l.name.split(" ")[0]));
      });
      var lv = me.levels[idx], list = h("div", {});
      lv.topics.forEach(function (t, j) {
        list.appendChild(h("div", { class: "list-item", onclick: function () { startQuiz({ kind: "grammar", level: idx, topic: j }); } },
          h("div", { class: "t", text: t }), h("div", { class: "chev", text: "›" })));
      });
      list.appendChild(h("div", { class: "list-item", onclick: function () { startQuiz({ kind: "grammar", level: idx, topic: "r" }); } },
        h("div", { class: "t", text: "🎲 " + S("randomTopic") }), h("div", { class: "chev", text: "›" })));
      mount(backBtn(viewEnglish), title("📝 " + S("grammar"), lv.name), seg, list);
    }
    draw();
  }

  function startQuiz(body) {
    mount(spinner(S("preparing")));
    post("/api/app/quiz/start", body).then(function (d) { quizIntro(d, body); }).catch(function (e) { fail(e, function () { startQuiz(body); }); });
  }

  function exitFor(body) { return body.kind === "reading" || body.kind === "listening" || body.kind === "grammar" ? viewEnglish : viewScience; }

  function quizIntro(d, body) {
    var parts = [];
    if (d.mode === "daily") {
      parts.push(h("div", { class: "card" }, h("div", { class: "muted small", text: S("fact") + " · " + d.title }), h("p", { style: "margin-top:8px", class: "pre", text: d.fact })));
      if (d.tip && d.tip.locked) parts.push(h("div", { class: "card", onclick: paywall }, h("div", { text: S("tipLocked") })));
      else if (d.tip) parts.push(h("div", { class: "card" }, h("h3", { text: "💡 " + d.tip.title }), h("p", { class: "pre muted", style: "margin-top:6px", text: d.tip.explanation }),
        (d.tip.examples || []).map(function (x) { return h("p", { class: "small", style: "margin-top:4px", text: "• " + x }); })));
    } else if (d.lesson) {
      var L = d.lesson;
      parts.push(h("div", { class: "card" }, h("h3", { text: "📘 " + L.title }), h("div", { class: "muted small", style: "margin-top:8px", text: S("lessonRule") }), h("p", { class: "pre", text: L.rule }),
        L.examples.length ? h("div", { class: "muted small", style: "margin-top:10px", text: S("lessonEx") }) : null,
        L.examples.map(function (x) { return h("div", { style: "margin-top:6px" }, h("b", { text: x.en }), h("div", { class: "muted small", text: x.uz })); }),
        L.mistake ? h("div", { class: "muted small", style: "margin-top:10px", text: "⚠️ " + S("lessonMistake") }) : null, L.mistake ? h("p", { text: L.mistake }) : null));
    } else if (d.mode === "reading") {
      parts.push(h("div", { class: "card" }, h("div", { class: "muted small", text: "📖 " + S("passage") }), h("p", { class: "pre", style: "margin-top:8px", text: d.passage })));
    } else if (d.mode === "listening") {
      parts.push(h("div", { class: "card" }, h("div", { class: "muted small", text: "🎧 " + S("listenHint") }),
        h("audio", { controls: "", src: "/api/app/audio/" + d.sid, style: "width:100%;margin-top:10px" })));
    }
    if (d.mode === "reading" || d.mode === "listening") { quizPlay(d, body, parts); return; }
    if (parts.length) {
      parts.push(h("button", { class: "btn", onclick: function () { quizPlay(d, body, []); }, text: S(d.lesson ? "startEx" : "start") }));
      mount.apply(null, [backBtn(exitFor(body))].concat(parts));
    } else quizPlay(d, body, []);
  }

  function quizPlay(d, body, topParts) {
    var i = 0;
    function render() {
      var q = d.questions[i], answered = false;
      var optBox = h("div", {}), info = h("div", {});
      q.options.forEach(function (o, k) {
        var b = h("button", { class: "opt", text: o, onclick: function () {
          if (answered) return; answered = true;
          Array.prototype.forEach.call(optBox.children, function (x) { x.setAttribute("disabled", ""); });
          post("/api/app/quiz/answer", { sid: d.sid, i: i, choice: k }).then(function (r) {
            optBox.children[r.correct_index].classList.add("correct");
            if (!r.correct) optBox.children[k].classList.add("wrong");
            haptic(r.correct ? "success" : "error");
            info.appendChild(h("p", { style: "margin-top:12px;font-weight:600", text: r.correct ? S("correct") : S("wrong") }));
            if (r.explanation) info.appendChild(h("p", { class: "muted small pre", style: "margin-top:6px", text: "💡 " + r.explanation }));
            var last = i === d.total - 1;
            info.appendChild(h("button", { class: "btn", onclick: function () { if (last) finish(); else { i++; render(); } }, text: last ? S("done") : S("next") }));
          }).catch(function (e) { fail(e); });
        } });
        optBox.appendChild(b);
      });
      mount.apply(null, [backBtn(exitFor(body))].concat(topParts, [h("div", { class: "card" },
        h("div", { class: "progress" }, h("i", { style: "width:" + Math.round(i / d.total * 100) + "%" })),
        h("div", { class: "muted small", text: (d.title ? d.title + " · " : "") + S("question") + " " + (i + 1) + "/" + d.total }),
        h("div", { class: "qtext", style: "margin-top:6px", text: q.question }), optBox, info)]));
    }
    function finish() {
      mount(spinner());
      post("/api/app/quiz/finish", { sid: d.sid }).then(function (r) { quizResult(r, body); }).catch(function (e) { fail(e); });
    }
    render();
  }

  function quizResult(r, body) {
    var parts = [h("div", { class: "card", style: "text-align:center" }, h("div", { class: "muted small", text: S("result") }),
      h("div", { class: "big", text: r.score + "/" + r.total }), h("div", { class: "muted", text: r.percentage + "%" }))];
    if (r.mode === "certificate") {
      var msg = r.certificate === "sent" ? S("certSent") : r.certificate === "failed" ? S("certFail") : S("certNeed", { p: r.pass_percent });
      parts.push(h("div", { class: "card", text: msg }));
    }
    if (r.script) parts.push(h("div", { class: "card" }, h("div", { class: "muted small", text: S("script") }), h("p", { class: "pre", style: "margin-top:6px", text: r.script })));
    parts.push(h("button", { class: "btn", onclick: function () { startQuiz(body); }, text: S("again") }), h("button", { class: "btn ghost", onclick: exitFor(body), text: S("back") }));
    haptic(r.percentage >= 60 ? "success" : "warning");
    mount.apply(null, parts);
    refreshMe().catch(function () {});
    setTimeout(upsell, 900);
  }

  // ================= YOLG'ON / NIMA BO'LADI (raundlar) =================
  function roundGame(kind) {
    var round = 0, right = 0, streak = 0;
    function next() {
      mount(spinner(S("preparing")));
      post("/api/app/quiz/start", { kind: kind }).then(function (d) {
        round++;
        var q = d.questions[0], answered = false, optBox = h("div", {}), info = h("div", {});
        q.options.forEach(function (o, k) {
          optBox.appendChild(h("button", { class: "opt", text: (k + 1) + ". " + o, onclick: function () {
            if (answered) return; answered = true;
            Array.prototype.forEach.call(optBox.children, function (x) { x.setAttribute("disabled", ""); });
            post("/api/app/quiz/answer", { sid: d.sid, i: 0, choice: k }).then(function (r) {
              optBox.children[r.correct_index].classList.add("correct");
              if (!r.correct) optBox.children[k].classList.add("wrong");
              if (r.correct) { right++; streak++; } else streak = 0;
              haptic(r.correct ? "success" : "error");
              info.appendChild(h("p", { style: "margin-top:12px;font-weight:600", text: r.correct ? S("correct") : S("wrong") }));
              if (r.explanation) info.appendChild(h("p", { class: "pre muted small", style: "margin-top:6px", text: "🔎 " + r.explanation }));
              info.appendChild(h("div", { class: "row" },
                h("button", { class: "btn", onclick: function () { if (round % 5 === 0) upsell(); next(); }, text: S("next") }),
                h("button", { class: "btn ghost", onclick: function () { end(); }, text: S("stop") })));
            }).catch(function (e) { fail(e); });
          } }));
        });
        var head = kind === "lie" ? "🕵️ " + S("lie") + (streak ? " · 🔥 " + streak : "") : "🔬 " + S("whatif") + " · 🎯 " + right + "/" + (round - 1);
        mount(backBtn(viewScience), h("div", { class: "card" }, h("div", { class: "muted small", text: head + " · " + S("round") + " " + round }),
          kind === "lie" ? h("div", { class: "qtext", style: "margin-top:6px", text: S("findLie") }) : h("div", { class: "qtext", style: "margin-top:6px", text: q.question }),
          optBox, info));
      }).catch(function (e) { fail(e, next); });
    }
    function end() {
      mount(h("div", { class: "card", style: "text-align:center" }, h("div", { class: "muted small", text: S("result") }),
        h("div", { class: "big", text: right + "/" + (round) }), h("div", { class: "muted", text: "🔥 " + S("streak") + ": " + streak })),
        h("button", { class: "btn", onclick: function () { roundGame(kind); }, text: S("again") }), h("button", { class: "btn ghost", onclick: viewScience, text: S("back") }));
      setTimeout(upsell, 700);
    }
    next();
  }

  // ================= JUMBOQ =================
  function teaserGame() {
    mount(spinner(S("preparing")));
    post("/api/app/teaser/new").then(function (d) {
      var extra = h("div", {});
      var hintBtn = h("button", { class: "btn ghost", text: S("hint"), onclick: function () {
        post("/api/app/teaser/reveal", { sid: d.sid, what: "hint" }).then(function (r) { extra.appendChild(h("p", { class: "muted", style: "margin-top:10px", text: "💡 " + r.hint })); }).catch(function (e) { fail(e); });
      } });
      var ansBtn = h("button", { class: "btn", text: S("answer"), onclick: function () {
        post("/api/app/teaser/reveal", { sid: d.sid, what: "answer" }).then(function (r) {
          clear(extra); hintBtn.remove(); ansBtn.remove();
          extra.appendChild(h("p", { style: "margin-top:10px;font-weight:700", text: "✅ " + r.answer }));
          if (r.explanation) extra.appendChild(h("p", { class: "muted small", style: "margin-top:6px", text: r.explanation }));
          setTimeout(upsell, 1200);
        }).catch(function (e) { fail(e); });
      } });
      mount(backBtn(viewScience), h("div", { class: "card" }, h("div", { class: "muted small", text: "🧠 " + S("teaser") }), h("div", { class: "qtext", style: "margin-top:8px", text: d.puzzle }),
        h("div", { class: "row" }, hintBtn, ansBtn), extra),
        h("button", { class: "btn ghost", onclick: teaserGame, text: S("newOne") }));
    }).catch(function (e) { fail(e, teaserGame); });
  }

  // ================= MEN TOPAMAN =================
  function guessGame() {
    mount(backBtn(viewScience), h("div", { class: "card" }, h("h3", { text: "🔮 " + S("guess") }), h("p", { class: "muted", style: "margin-top:8px", text: S("guessIntro", { n: me.limits.guess_max }) })),
      h("button", { class: "btn", onclick: begin, text: S("start") }));
    function begin() {
      mount(spinner(S("thinking")));
      post("/api/app/guess/start").then(step).catch(function (e) { fail(e, guessGame); });
    }
    function step(d) {
      if (d.type === "win" || d.type === "lose") {
        mount(h("div", { class: "card", style: "text-align:center" }, h("div", { class: "big", text: d.type === "win" ? "🎉" : "🏳️" }),
          h("h3", { style: "margin-top:8px", text: d.type === "win" ? S("iWon") : S("iLost") }), d.type === "win" ? h("p", { class: "muted", style: "margin-top:6px", text: d.text + " · " + d.n + " " + S("guessQ").toLowerCase() }) : null),
          h("button", { class: "btn", onclick: guessGame, text: S("again") }), h("button", { class: "btn ghost", onclick: viewScience, text: S("back") }));
        setTimeout(upsell, 900);
        return;
      }
      function send(a) { mount(spinner(S("thinking"))); post("/api/app/guess/answer", { sid: sid, a: a }).then(step).catch(function (e) { fail(e); }); }
      var sid = d.sid || step.sid; step.sid = sid;
      if (d.type === "ask") {
        mount(h("div", { class: "card" }, h("div", { class: "progress" }, h("i", { style: "width:" + Math.round(d.n / d.max * 100) + "%" })),
          h("div", { class: "muted small", text: "🔮 " + S("guessQ") + " " + d.n + "/" + d.max }), h("div", { class: "qtext", style: "margin-top:8px", text: d.text }),
          h("div", { class: "row" }, h("button", { class: "btn good", onclick: function () { send("y"); }, text: S("yes") }), h("button", { class: "btn bad", onclick: function () { send("n"); }, text: S("no") })),
          h("button", { class: "btn ghost", onclick: function () { send("m"); }, text: S("maybe") })));
      } else {
        mount(h("div", { class: "card", style: "text-align:center" }, h("div", { class: "muted small", text: "🔮 " + S("guessMine") }), h("div", { class: "big", style: "margin-top:8px;font-size:28px", text: d.text }),
          h("div", { class: "row" }, h("button", { class: "btn good", onclick: function () { send("win"); }, text: S("rightGuess") }), h("button", { class: "btn bad", onclick: function () { send("lose"); }, text: S("wrongGuess") }))));
      }
    }
  }

  // ================= HIKOYA =================
  function storyPicker() {
    var grid = h("div", { class: "grid2" });
    me.genres.forEach(function (g) { grid.appendChild(tile(g.emoji, g.name, null, function () { storyPlay(g); })); });
    mount(backBtn(viewScience), title("📖 " + S("pickGenre")), grid);
  }
  function storyPlay(g) {
    var sid = null;
    mount(spinner(S("preparing")));
    post("/api/app/story/start", { genre: g.i }).then(function (d) { sid = d.sid; scene(d); }).catch(function (e) { fail(e, function () { storyPlay(g); }); });
    function scene(d) {
      var parts = [h("div", { class: "card" }, h("div", { class: "progress" }, h("i", { style: "width:" + Math.round(d.step / d.total * 100) + "%" })),
        h("div", { class: "muted small", text: g.emoji + " " + g.name + " · " + d.step + "/" + d.total }),
        d.ending && d.title ? h("h3", { style: "margin-top:8px", text: "🏁 " + d.title }) : null, h("div", { class: "scene", style: "margin-top:10px", text: d.scene }))];
      if (d.ending) {
        parts.push(h("div", { class: "card", style: "text-align:center" }, h("div", { text: S("storyEnd") })),
          h("button", { class: "btn", onclick: storyPicker, text: S("again") }), h("button", { class: "btn ghost", onclick: viewScience, text: S("back") }));
        mount.apply(null, parts); setTimeout(upsell, 1200); return;
      }
      parts.push(h("div", { class: "muted small", style: "margin:4px 2px", text: S("whatDo") }));
      d.choices.forEach(function (c, k) {
        parts.push(h("button", { class: "opt", text: (k + 1) + ". " + c, onclick: function () {
          mount(spinner(S("preparing")));
          post("/api/app/story/choose", { sid: sid, choice: k }).then(scene).catch(function (e) { fail(e); });
        } }));
      });
      mount.apply(null, parts);
    }
  }

  // ================= SAT =================
  // ---- KaTeX (formulalar) ----
  var katexQ = [], katexState = 0;
  function loadKatex(cb) {
    if (katexState === 2) { cb(); return; }
    katexQ.push(cb);
    if (katexState === 1) return;
    katexState = 1;
    var base = "https://cdnjs.cloudflare.com/ajax/libs/KaTeX/0.16.9/";
    var css = document.createElement("link"); css.rel = "stylesheet"; css.href = base + "katex.min.css"; document.head.appendChild(css);
    function add(src, next) { var sc = document.createElement("script"); sc.src = src; sc.onload = next; sc.onerror = function () { katexState = 0; katexQ = []; }; document.head.appendChild(sc); }
    add(base + "katex.min.js", function () { add(base + "contrib/auto-render.min.js", function () { katexState = 2; var q = katexQ; katexQ = []; q.forEach(function (f) { f(); }); }); });
  }
  function mj(el) {
    if (!el) return el;
    loadKatex(function () {
      try { window.renderMathInElement(el, { delimiters: [{ left: "$$", right: "$$", display: true }, { left: "$", right: "$", display: false }], throwOnError: false }); } catch (e) {}
    });
    return el;
  }
  var SAT_REF = [
    ["$A=\\pi r^2$", "$C=2\\pi r$"], ["$A=\\ell w$", "$A=\\tfrac12 bh$"], ["$a^2+b^2=c^2$", "$V=\\ell wh$"],
    ["$V=\\pi r^2h$", "$V=\\tfrac43\\pi r^3$"], ["$V=\\tfrac13\\pi r^2h$", "$V=\\tfrac13\\ell wh$"],
    ["30°-60°-90°: $x,\\ x\\sqrt3,\\ 2x$", "45°-45°-90°: $s,\\ s,\\ s\\sqrt2$"],
    ["$360^\\circ=2\\pi$ rad", "$\\triangle$: $180^\\circ$"]
  ];
  function satRef() {
    var rows = SAT_REF.map(function (r) { return h("div", { class: "row", style: "margin:8px 0;font-size:16px" }, r.map(function (x) { return h("div", { text: x }); })); });
    var box = h("div", {}, rows);
    var s = sheet(h("h2", { text: "📐 " + S("satRef") }), mj(box), h("button", { class: "btn ghost", onclick: function () { s.close(); }, text: S("later") }));
  }

  var SAT_DOMAINS = { rw: ["Craft and Structure", "Information and Ideas", "Standard English Conventions", "Expression of Ideas"],
    math: ["Algebra", "Advanced Math", "Problem-Solving and Data Analysis", "Geometry and Trigonometry"] };

  function viewSat() {
    var P = isPremium();
    function go(body) { if (!P) { paywall(); return; } satStart(body); }
    var parts = [backBtn(viewEnglish), title("🎓 " + S("sat"), S("satD")),
      h("button", { class: "tile wide", style: "width:100%;margin-bottom:14px", onclick: function () { go({ mode: "mock" }); } },
        h("div", { class: "e", text: "🏁" }), h("div", { class: "n", text: S("satMock") }), h("div", { class: "d", text: S("satMockD") }),
        P ? null : h("div", { class: "lk", text: "🔒" }))];
    ["rw", "math"].forEach(function (sec) {
      parts.push(h("h3", { style: "margin:14px 2px 8px", text: (sec === "rw" ? "📖 " : "➗ ") + S(sec === "rw" ? "satRW" : "satMath") + " — " + S("satPractice") }));
      var g = h("div", { class: "grid2" });
      g.appendChild(tile("🎲", S("satAllDomains"), S("satPracticeD"), function () { go({ mode: "practice", section: sec }); }, { lock: !P }));
      SAT_DOMAINS[sec].forEach(function (d) { g.appendChild(tile("▫️", d, S("satPracticeD"), function () { go({ mode: "practice", section: sec, domain: d }); }, { lock: !P })); });
      parts.push(g);
    });
    mount.apply(null, parts);
  }

  function satStart(body) {
    mount(spinner(S("preparing")));
    post("/api/app/sat/start", body).then(function (d) { satPlay(d.sid, d.module, d, body); })
      .catch(function (e) { if (e.status === 502) { toast(S("satGenFail")); viewSat(); } else fail(e, function () { satStart(body); }); });
  }

  function fmtMS(s) { s = Math.max(0, Math.round(s)); return Math.floor(s / 60) + ":" + ("0" + (s % 60)).slice(-2); }

  function satPlay(sid, mod, meta, startBody) {
    var n = mod.n, idx = 0, answers = [], flags = {}, left = mod.seconds, tick = null, warned = false;
    for (var k = 0; k < n; k++) answers.push(null);
    var timerEl = h("div", { class: "big", style: "font-size:20px", text: fmtMS(left) });
    var body = h("div", {});
    function stop() { if (tick) { clearInterval(tick); tick = null; } }
    tick = setInterval(function () {
      left--; timerEl.textContent = (left >= 0 ? "" : "+") + fmtMS(Math.abs(left));
      timerEl.style.color = left < 60 ? "var(--bad)" : "";
      if (left === 0 && !warned) { warned = true; toast(S("satTimeUp")); haptic("warning"); }
    }, 1000);
    cleanupFn = stop;

    function draw() {
      clear(body);
      var q = mod.questions[idx];
      var head = h("div", {},
        h("div", { class: "progress" }, h("i", { style: "width:" + Math.round((idx + 1) / n * 100) + "%" })),
        h("div", { class: "row", style: "align-items:center;margin-bottom:8px" },
          h("div", { class: "muted small", text: (idx + 1) + "/" + n + " · " + q.domain }),
          h("button", { class: "btn ghost", style: "flex:0 0 auto;padding:4px 10px;margin:0;width:auto", onclick: function () { flags[idx] = !flags[idx]; draw(); },
            text: flags[idx] ? "🚩" : S("satFlag") })));
      var right = h("div", { class: "sat-right" }, mj(h("div", { class: "sat-q", text: q.question })));
      var split = h("div", { class: "sat-split" },
        q.passage ? mj(h("div", { class: "sat-passage", text: q.passage })) : null, right);
      var card = h("div", { class: "card" }, head, split);
      if (q.type === "mc") {
        q.options.forEach(function (o, k) {
          var on = answers[idx] === k;
          right.appendChild(mj(h("button", { class: "opt sat-opt", style: on ? "border-color:var(--accent);background:rgba(90,140,255,.18)" : "",
            text: "ABCD".charAt(k) + ")  " + o, onclick: function () { answers[idx] = on ? null : k; haptic("light"); draw(); } })));
        });
      } else {
        var inp = h("input", { type: "text", inputmode: "decimal", maxlength: "12", placeholder: S("satNumPlace"), value: answers[idx] == null ? "" : String(answers[idx]),
          style: "width:100%;margin-top:12px;padding:12px;border-radius:12px;border:1px solid var(--line);background:var(--bg);color:var(--text);font:inherit;font-size:18px" });
        inp.addEventListener("input", function () { answers[idx] = inp.value.trim() === "" ? null : inp.value.trim(); paintGrid(); });
        right.appendChild(inp); right.appendChild(h("div", { class: "muted small", style: "margin-top:6px", text: S("satNumHint") }));
      }
      body.appendChild(card);
      var nav = h("div", { class: "row" },
        h("button", { class: "btn ghost", onclick: function () { if (idx > 0) { idx--; draw(); } }, text: "←", disabled: idx === 0 ? "" : false }),
        idx < n - 1 ? h("button", { class: "btn", onclick: function () { idx++; draw(); }, text: S("next") })
                    : h("button", { class: "btn gold", onclick: confirmSubmit, text: S("satSubmit") }));
      body.appendChild(nav);
      body.appendChild(gridEl());
      if (idx < n - 1) body.appendChild(h("button", { class: "btn ghost", onclick: confirmSubmit, text: S("satSubmit") }));
    }
    var gridBox = null;
    function gridEl() {
      gridBox = h("div", { style: "display:flex;flex-wrap:wrap;gap:6px;margin:12px 0" });
      paintGrid(); return gridBox;
    }
    function paintGrid() {
      if (!gridBox) return; clear(gridBox);
      for (var i = 0; i < n; i++) (function (i) {
        var done = answers[i] != null && answers[i] !== "";
        gridBox.appendChild(h("button", { style: "width:38px;height:38px;border-radius:10px;font:inherit;font-weight:600;border:1px solid " + (i === idx ? "var(--accent)" : "var(--line)") +
          ";background:" + (done ? "var(--accent)" : "var(--card)") + ";color:" + (done ? "var(--accent-text)" : "var(--text)"),
          onclick: function () { idx = i; draw(); }, text: (flags[i] ? "🚩" : String(i + 1)) }));
      })(i);
    }
    function confirmSubmit() {
      var missing = answers.filter(function (a) { return a == null || a === ""; }).length;
      var s = sheet(h("h2", { text: S("satSure") }), missing ? h("p", { class: "muted", style: "margin-top:8px", text: S("satConfirm") + " " + missing }) : null,
        h("button", { class: "btn", onclick: function () { s.close(); submit(); }, text: S("satYes") }),
        h("button", { class: "btn ghost", onclick: function () { s.close(); }, text: S("satNo") }));
    }
    function submit() {
      stop();
      mount(spinner(meta.mode === "mock" ? S("preparing") : S("wAnalyzing")));
      post("/api/app/sat/submit", { sid: sid, answers: answers }).then(function (r) {
        if (r.done) { satReport(r, startBody); return; }
        satBetween(r, sid, meta, startBody);
      }).catch(function (e) {
        if (e.status === 502) { toast(S("satGenFail")); satPlay(sid, mod, meta, startBody); }
        else fail(e, function () { submit(); });
      });
    }
    mount(backBtn(function () { stop(); viewSat(); }),
      h("div", { class: "row", style: "align-items:center;margin:4px 2px 10px" },
        h("div", { class: "muted", text: "🎓 " + mod.label + (meta.mode === "mock" ? " · " + S("satStage") + " " + ((mod.stage % 2) + 1) + "/2" : "") }),
        h("div", { style: "display:flex;gap:8px;align-items:center;justify-content:flex-end;flex:0 0 auto" },
          mod.section === "math" ? h("button", { class: "btn ghost", style: "width:auto;margin:0;padding:4px 10px", onclick: satRef, text: "📐" }) : null,
          h("button", { class: "btn ghost", style: "width:auto;margin:0;padding:4px 10px", onclick: function () { timerEl.style.visibility = timerEl.style.visibility === "hidden" ? "visible" : "hidden"; }, text: "⏱" }),
          timerEl)), body);
    cleanupFn = stop;
    draw();
  }

  function satBetween(r, sid, meta, startBody) {
    var nxt = r.module, harder = r.prev.stage === 0 || r.prev.stage === 2 ? r.prev.pct >= 60 : null;
    haptic("success");
    mount(title("✅ " + S("satModDone")),
      h("div", { class: "card promo", style: "text-align:center" },
        h("div", { class: "muted small", text: S("satNextMod") + ": " + nxt.label + " · " + S("satStage") + " " + ((nxt.stage % 2) + 1) + "/2" }),
        harder === null ? null : h("p", { style: "margin-top:8px", text: harder ? S("satHarder") : S("satEasier") })),
      h("button", { class: "btn", onclick: function () { satPlay(sid, nxt, meta, startBody); }, text: S("start") }));
  }

  function satReport(r, startBody) {
    var parts = [];
    haptic("success");
    if (r.mode === "mock") {
      parts.push(title("🏁 SAT · " + S("satEst")),
        h("div", { class: "card promo", style: "text-align:center" }, h("div", { class: "muted small", text: S("satTotal") }), h("div", { class: "big", text: String(r.total) })),
        h("div", { class: "grid2" },
          ["rw", "math"].map(function (sec) {
            var x = r.sections[sec];
            return h("div", { class: "stat" }, h("div", { class: "v", text: String(x.score) }), h("div", { class: "l", text: S(sec === "rw" ? "satRW" : "satMath") }),
              h("div", { class: "r", text: x.m1 + "% → " + x.m2 + "% · " + S("satRoute") + ": " + S(x.hard ? "satHardR" : "satEasyR") }));
          })));
    } else {
      parts.push(title("🏁 " + r.right + "/" + r.n + " " + S("satScoreOf")));
    }
    var weak = r.domains.filter(function (d) { return d.pct < 60; });
    parts.push(h("div", { class: "card" }, h("h3", { text: "📊 " + S("satDomains") }),
      r.domains.map(function (d) {
        return h("div", { style: "margin-top:10px" }, h("div", { class: "row", style: "justify-content:space-between" }, h("div", { class: "small", text: d.domain }), h("div", { class: "small", text: d.ok + "/" + d.n + " · " + d.pct + "%" })),
          h("div", { class: "progress", style: "margin:4px 0 0" }, h("i", { style: "width:" + d.pct + "%;background:" + (d.pct >= 60 ? "var(--good)" : "var(--bad)") })));
      })));
    if (weak.length) parts.push(h("div", { class: "card" }, h("h3", { text: "🎯 " + S("satWeak") }), weak.map(function (d) { return h("p", { style: "margin-top:6px", text: "• " + d.domain + " (" + d.pct + "%)" }); })));
    if (r.mode === "mock") parts.push(h("p", { class: "muted small", style: "margin:8px 2px", text: "ℹ️ " + S("satEstNote") }));
    parts.push(h("button", { class: "btn", onclick: function () { satReview(r, startBody); }, text: "🔍 " + S("satReview") }),
      h("button", { class: "btn ghost", onclick: function () { satStart(startBody); }, text: S("again") }),
      h("button", { class: "btn ghost", onclick: viewSat, text: S("back") }));
    mount.apply(null, parts);
    refreshMe().catch(function () {});
  }

  function satReview(r, startBody, onlyWrong) {
    var list = r.review.map(function (x, i) { x.n = i + 1; return x; }).filter(function (x) { return !onlyWrong || !x.ok; });
    var seg = h("div", { class: "seg" },
      h("button", { class: onlyWrong ? "" : "on", onclick: function () { satReview(r, startBody, false); }, text: S("satAllQ") }),
      h("button", { class: onlyWrong ? "on" : "", onclick: function () { satReview(r, startBody, true); }, text: S("satWrongOnly") }));
    var parts = [backBtn(function () { satReport(r, startBody); }), title("🔍 " + S("satReview")), seg];
    list.forEach(function (x) {
      parts.push(h("div", { class: "card", style: "border-left:4px solid " + (x.ok ? "var(--good)" : "var(--bad)") },
        h("div", { class: "muted small", text: (x.ok ? "✅ " : "❌ ") + "#" + x.n + " · " + x.domain }),
        x.passage ? mj(h("div", { class: "sat-passage", style: "margin:6px 0", text: x.passage })) : null,
        mj(h("div", { class: "sat-q", style: "margin-top:6px", text: x.question })),
        x.options ? x.options.map(function (o, k) { return mj(h("div", { class: "small muted", style: "margin-top:3px", text: "ABCD".charAt(k) + ") " + o })); }) : null,
        mj(h("p", { style: "margin-top:8px", text: S("satYour") + ": " + (x.your == null ? S("satNone") : x.your) })),
        mj(h("p", { style: "color:var(--good);font-weight:600", text: S("satRight") + ": " + x.right })),
        x.explanation ? mj(h("p", { class: "small", style: "margin-top:6px", text: "💡 " + x.explanation })) : null));
    });
    if (!list.length) parts.push(h("div", { class: "center", text: "🎉" }));
    mount.apply(null, parts);
  }


  // ================= WRITING =================
  function writingFlow() {
    mount(spinner(S("preparing")));
    post("/api/app/writing/task").then(function (d) {
      var ta = h("textarea", { placeholder: S("wPlace") }), wc = h("div", { class: "wc muted", text: "0 " + S("wWords") }), btn;
      ta.addEventListener("input", function () {
        var n = ta.value.trim() ? ta.value.trim().split(/\s+/).length : 0;
        wc.textContent = n + " " + S("wWords") + (n < d.min_words ? " · " + S("wMin", { n: d.min_words }) : "");
        wc.style.color = n >= d.min_words ? "var(--good)" : "";
      });
      btn = h("button", { class: "btn", text: S("wSubmit"), onclick: function () {
        var n = ta.value.trim() ? ta.value.trim().split(/\s+/).length : 0;
        if (n < d.min_words) { toast(S("wMin", { n: d.min_words })); return; }
        mount(spinner(S("wAnalyzing")));
        post("/api/app/writing/submit", { sid: d.sid, essay: ta.value }).then(function (r) { writingReport(r); })
          .catch(function (e) { if (e.status === 422) { toast(S("wMin", { n: d.min_words })); writingFlow(); } else fail(e); });
      } });
      mount(backBtn(viewEnglish), h("div", { class: "card" }, h("div", { class: "muted small", text: "💎 " + S("wTask") }), h("div", { class: "qtext", style: "margin-top:8px", text: d.task })),
        ta, wc, btn);
    }).catch(function (e) { fail(e, writingFlow); });
  }

  function band(v) { var n = parseFloat(v); return isNaN(n) ? "—" : n.toFixed(1); }
  function bandCard(label, v) { return h("div", { class: "stat" }, h("div", { class: "v", text: band(v) }), h("div", { class: "l", text: label })); }
  function listBlock(titleTxt, items, mark) {
    items = (items || []).filter(function (x) { return String(x).trim(); });
    if (!items.length) return null;
    return h("div", { class: "card" }, h("h3", { text: titleTxt }), items.map(function (x) { return h("p", { style: "margin-top:6px", text: (mark || "•") + " " + x }); }));
  }
  function errorsBlock(titleTxt, errs) {
    errs = (errs || []).filter(function (x) { return x && typeof x === "object"; });
    if (!errs.length) return null;
    return h("div", { class: "card" }, h("h3", { text: titleTxt }), errs.slice(0, 6).map(function (x) {
      return h("div", { class: "errbox" }, h("div", { class: "muted", style: "text-decoration:line-through", text: String(x.wrong || "") }),
        h("div", { style: "color:var(--good);font-weight:600", text: String(x.right || "") }), h("div", { class: "small", text: String(x.why || "") }));
    }));
  }

  function writingReport(r) {
    var d = r.report || {};
    var parts = [title("📝 IELTS Writing"),
      h("div", { class: "grid2" }, bandCard(S("rTask"), d.task_response), bandCard(S("rCoh"), d.coherence), bandCard(S("rLex"), d.lexical), bandCard(S("rGra"), d.grammar)),
      h("div", { class: "card promo", style: "text-align:center;margin-top:12px" }, h("div", { class: "muted small", text: S("rOver") }), h("div", { class: "big", text: band(d.overall) }),
        h("div", { class: "muted small", text: r.words + " " + S("wWords") })),
      listBlock("💪 " + S("rStrong"), d.strengths, "✅"), errorsBlock("🔧 " + S("rErrors"), d.errors), listBlock("📚 " + S("rVocab"), d.vocab, "▫️"),
      d.improved_paragraph ? h("div", { class: "card" }, h("h3", { text: "✨ " + S("rImproved") }), h("p", { class: "pre", style: "margin-top:6px;font-style:italic", text: d.improved_paragraph })) : null,
      listBlock("🚀 " + S("rNext"), d.next_steps, "→"),
      h("button", { class: "btn", onclick: writingFlow, text: S("again") }), h("button", { class: "btn ghost", onclick: viewEnglish, text: S("back") })];
    haptic("success"); mount.apply(null, parts); refreshMe().catch(function () {});
  }

  // ================= SPEAKING =================
  function speakingFlow(mode) {
    mount(spinner(S("preparing")));
    post("/api/app/speaking/start", { mode: mode }).then(function (d) { speakItem(d.sid, d.item, mode); })
      .catch(function (e) { fail(e, function () { speakingFlow(mode); }); });
  }

  function pickMime() {
    var c = ["audio/webm;codecs=opus", "audio/webm", "audio/mp4", "audio/ogg;codecs=opus"];
    for (var i = 0; i < c.length; i++) { try { if (window.MediaRecorder && MediaRecorder.isTypeSupported(c[i])) return c[i]; } catch (e) {} }
    return "";
  }
  function fmtTime(s) { s = Math.max(0, Math.round(s)); return Math.floor(s / 60) + ":" + ("0" + (s % 60)).slice(-2); }

  function speakItem(sid, item, mode) {
    var stream = null, rec = null, chunks = [], mime = pickMime(), startedAt = 0, tick = null, blob = null, previewUrl = null, audioEl = null, prepTick = null;
    var maxSec = item.part === 2 ? 150 : 90;
    var ctl = h("div", { style: "text-align:center" }), status = h("div", { class: "muted small", text: S("recHint") });
    function stopAll() {
      if (tick) { clearInterval(tick); tick = null; }
      if (prepTick) { clearInterval(prepTick); prepTick = null; }
      try { if (rec && rec.state !== "inactive") { rec.onstop = null; rec.stop(); } } catch (e) {}
      if (stream) { stream.getTracks().forEach(function (t) { t.stop(); }); stream = null; }
      if (audioEl) { try { audioEl.pause(); } catch (e) {} }
      if (previewUrl) { URL.revokeObjectURL(previewUrl); previewUrl = null; }
    }
    function playQ() {
      try { if (audioEl) audioEl.pause(); audioEl = new Audio("/api/app/speaking/tts/" + sid + "/" + item.i); audioEl.play().catch(function () {}); } catch (e) {}
    }
    function idle() {
      clear(ctl);
      status.textContent = S("recHint");
      ctl.appendChild(h("button", { class: "rec", onclick: begin, "aria-label": S("rec"), text: "🎙" }));
      ctl.appendChild(status);
    }
    function begin() {
      if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia || !window.MediaRecorder) { micFallback(); return; }
      navigator.mediaDevices.getUserMedia({ audio: true }).then(function (s) {
        stream = s; chunks = [];
        try { rec = new MediaRecorder(s, mime ? { mimeType: mime } : undefined); } catch (e) { micFallback(); return; }
        rec.ondataavailable = function (ev) { if (ev.data && ev.data.size) chunks.push(ev.data); };
        rec.onstop = review;
        rec.start();
        startedAt = Date.now();
        clear(ctl);
        var timer = h("div", { class: "big", style: "font-size:26px", text: "0:00" });
        ctl.appendChild(h("button", { class: "rec on", onclick: function () { try { rec.stop(); } catch (e) {} }, text: "⏹" }));
        ctl.appendChild(timer); ctl.appendChild(h("div", { class: "muted small", text: S("recStop") }));
        tick = setInterval(function () {
          var el = (Date.now() - startedAt) / 1000; timer.textContent = fmtTime(el);
          if (el >= maxSec) { toast(S("maxTime")); try { rec.stop(); } catch (e) {} }
        }, 250);
      }).catch(function () { micFallback(); });
    }
    function micFallback() {
      stopAll(); clear(ctl);
      ctl.appendChild(h("div", { class: "card" }, h("p", { text: "🎙 " + S("micDenied") }),
        h("button", { class: "btn", onclick: function () { openBot("run_speaking"); }, text: S("micChat") })));
    }
    function review() {
      if (tick) { clearInterval(tick); tick = null; }
      var dur = (Date.now() - startedAt) / 1000;
      if (stream) { stream.getTracks().forEach(function (t) { t.stop(); }); stream = null; }
      blob = new Blob(chunks, { type: (rec && rec.mimeType) || mime || "audio/webm" });
      if (previewUrl) URL.revokeObjectURL(previewUrl);
      previewUrl = URL.createObjectURL(blob);
      clear(ctl);
      ctl.appendChild(h("audio", { controls: "", src: previewUrl, style: "width:100%;margin:8px 0" }));
      ctl.appendChild(h("div", { class: "row" }, h("button", { class: "btn ghost", onclick: idle, text: S("recRedo") }),
        h("button", { class: "btn", onclick: function () { send(dur); }, text: S("recSend") })));
    }
    function send(dur) {
      clear(ctl); ctl.appendChild(spinner(item.i === item.total - 1 ? S("wAnalyzing") : S("recProcessing")));
      var fd = new FormData();
      fd.append("sid", sid); fd.append("duration", String(Math.round(dur)));
      var ext = blob.type.indexOf("mp4") >= 0 ? "mp4" : blob.type.indexOf("ogg") >= 0 ? "ogg" : "webm";
      fd.append("audio", blob, "answer." + ext);
      fetch("/api/app/speaking/answer", { method: "POST", headers: { "X-Init-Data": initData }, body: fd }).then(function (r) {
        return r.json().catch(function () { return {}; }).then(function (j) { return { status: r.status, j: j }; });
      }).then(function (res) {
        if (res.status === 200) {
          stopAll();
          if (res.j.done) speakReport(res.j.report, mode); else speakItem(sid, res.j.item, mode);
        } else if (res.status === 422) { toast(S("noSpeech")); idle(); }
        else if (res.status === 413) { toast(S("tooBig")); idle(); }
        else if (res.status === 410) { stopAll(); toast(S("expired")); viewEnglish(); }
        else { toast(S("err")); review(); }
      }).catch(function () { toast(S("err")); review(); });
    }

    var card = h("div", { class: "card" },
      h("div", { class: "progress" }, h("i", { style: "width:" + Math.round(item.i / item.total * 100) + "%" })),
      h("div", { class: "muted small", text: "🎙 " + S("part") + " " + item.part + " · " + (item.i + 1) + "/" + item.total }),
      item.lead ? h("p", { class: "muted", style: "margin-top:8px", text: "🧑‍🏫 " + item.lead }) : null);
    if (item.part === 2) {
      if (item.pics) card.appendChild(h("div", { class: "pics" },
        h("figure", {}, h("img", { src: "/api/app/speaking/pic/" + sid + "/0", alt: "" }), h("figcaption", { text: "🅰️ " + S("picA") })),
        h("figure", {}, h("img", { src: "/api/app/speaking/pic/" + sid + "/1", alt: "" }), h("figcaption", { text: "🅱️ " + S("picB") }))));
      else card.appendChild(h("div", { style: "margin:10px 0" }, h("div", { class: "muted small", text: S("picsFail") }),
        (item.desc || []).map(function (x, k) { return h("p", { style: "margin-top:4px", text: (k ? "🅱️ " : "🅰️ ") + x }); })));
      var prep = h("div", { class: "muted small", text: "⏱ " + S("prep") + " 1:00" }), left = 60;
      card.appendChild(prep);
      prepTick = setInterval(function () { left--; prep.textContent = left > 0 ? "⏱ " + S("prep") + " " + fmtTime(left) : "✅"; if (left <= 0) { clearInterval(prepTick); prepTick = null; } }, 1000);
    }
    card.appendChild(h("div", { class: "qtext", style: "margin-top:8px", text: item.q }));
    card.appendChild(h("button", { class: "btn ghost", onclick: playQ, text: S("listenQ") }));
    mount(card, h("div", { class: "card" }, ctl));
    cleanupFn = stopAll;
    idle();
    setTimeout(playQ, 300);
  }

  function speakReport(d, mode) {
    d = d || {};
    haptic("success");
    mount.apply(null, [title("🏁 IELTS Speaking"),
      h("div", { class: "grid2" }, bandCard(S("sFlu"), d.fluency), bandCard(S("sLex"), d.lexical), bandCard(S("sGra"), d.grammar), bandCard(S("sPro"), d.pronunciation)),
      h("div", { class: "card promo", style: "text-align:center;margin-top:12px" }, h("div", { class: "muted small", text: S("rOver") }), h("div", { class: "big", text: band(d.overall) })),
      d.summary ? h("div", { class: "card" }, h("h3", { text: "💬 " + S("sSummary") }), h("p", { class: "pre", style: "margin-top:6px", text: d.summary })) : null,
      listBlock("💪 " + S("rStrong"), d.strengths, "✅"), errorsBlock("🔧 " + S("rErrors"), d.errors), listBlock("📚 " + S("rVocab"), d.vocab, "▫️"), listBlock("🚀 " + S("sPlan"), d.plan, "→"),
      h("p", { class: "muted small", style: "margin:8px 2px", text: "ℹ️ " + S("sNote") }),
      h("button", { class: "btn", onclick: function () { speakingFlow(mode); }, text: S("again") }), h("button", { class: "btn ghost", onclick: viewEnglish, text: S("back") })]);
    refreshMe().catch(function () {});
  }

  // ================= KUTUBXONA =================
  function viewLibrary(type) {
    var seg = h("div", { class: "seg" });
    [["reading", "reading"], ["listening", "listening"], ["grammar", "grammar"]].forEach(function (x) {
      seg.appendChild(h("button", { class: type === x[0] ? "on" : "", onclick: function () { viewLibrary(x[0]); } }, S(x[1])));
    });
    var box = h("div", {}, spinner());
    mount(backBtn(viewEnglish), title("📚 " + S("library")), seg, box);
    api("/api/dashboard/content/" + type).then(function (items) {
      clear(box);
      if (!items.length) { box.appendChild(h("div", { class: "center", text: S("empty") })); return; }
      items.forEach(function (it) {
        box.appendChild(h("div", { class: "list-item", onclick: function () { openItem(type, it.id); } },
          h("div", { class: "t" }, document.createTextNode(it.title), h("div", { class: "m", text: it.has_questions ? it.question_count + " " + S("questions") : S("readOnly") })),
          h("div", { class: "chev", text: "›" })));
      });
    }).catch(function (e) { clear(box); if (e.status === 403) paywall(); else box.appendChild(h("div", { class: "center", text: S("err") })); });
  }

  function openItem(type, id) {
    mount(spinner());
    api("/api/dashboard/content/" + type + "/" + id).then(function (item) {
      var card = h("div", { class: "card" }, h("h3", { text: item.title }));
      if (item.body) card.appendChild(h("div", { class: "pre", style: "margin-top:10px", text: item.body }));
      var area = h("div", {});
      if (type === "listening") {
        var au = h("audio", { controls: "", style: "width:100%;margin-top:12px" });
        card.appendChild(au);
        fetch("/api/dashboard/audio/" + id, { headers: { "X-Init-Data": initData } }).then(function (r) { return r.blob(); })
          .then(function (b) { au.src = URL.createObjectURL(b); }).catch(function () {});
      }
      mount(backBtn(function () { viewLibrary(type); }), card, area);
      if (item.questions && item.questions.length) libQuiz(type, id, item.questions, area);
    }).catch(function (e) { if (e.status === 403) paywall(); else { toast(S("err")); viewLibrary(type); } });
  }

  function libQuiz(type, id, qs, area) {
    var answers = [], i = 0;
    function ask() {
      clear(area);
      var q = qs[i], card = h("div", { class: "card" }, h("div", { class: "progress" }, h("i", { style: "width:" + Math.round(i / qs.length * 100) + "%" })),
        h("div", { class: "muted small", text: S("question") + " " + (i + 1) + "/" + qs.length }), h("div", { class: "qtext", style: "margin-top:6px", text: q.question }));
      q.options.forEach(function (o, k) {
        card.appendChild(h("button", { class: "opt", text: o, onclick: function () { answers.push(k); i++; if (i < qs.length) ask(); else finish(); } }));
      });
      area.appendChild(card);
    }
    function finish() {
      clear(area); area.appendChild(spinner());
      post("/api/dashboard/content/" + type + "/" + id + "/submit", { answers: answers }).then(function (r) {
        clear(area); haptic(r.percentage >= 60 ? "success" : "warning");
        area.appendChild(h("div", { class: "card", style: "text-align:center" }, h("div", { class: "muted small", text: S("result") }), h("div", { class: "big", text: r.score + "/" + r.total }),
          h("div", { class: "muted", text: r.percentage + "%" }), h("button", { class: "btn ghost", onclick: function () { viewLibrary(type); }, text: S("back") })));
        refreshMe().catch(function () {});
      }).catch(function () { clear(area); area.appendChild(h("div", { class: "center", text: S("err") })); });
    }
    ask();
  }

  // ================= BOSHLASH =================
  if (!initData) { renderNav(); mount(h("div", { class: "center", text: S("noTg") })); return; }
  renderNav();
  mount(spinner());
  refreshMe().then(function () {
    if (T[me.lang]) lang = me.lang;
    var r = new URLSearchParams(window.location.search).get("r");
    if (r && ["home", "science", "english", "top", "pro"].indexOf(r) >= 0) tab = r;
    show();
  }).catch(function () { mount(h("div", { class: "center", text: S("err") })); });

})();
</script>
</body>
</html>
"""


def _verify_init_data(init_data: str, max_age: int = 86400) -> dict | None:
    """Telegram Mini App initData imzosini (HMAC-SHA256) tekshiradi va foydalanuvchi ma'lumotini qaytaradi.
    Imzo noto'g'ri yoki eskirgan bo'lsa None — shu tufayli foydalanuvchi o'zini boshqasi deb ko'rsata olmaydi."""
    if not init_data:
        return None
    try:
        pairs = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        received = pairs.pop("hash", None)
        if not received:
            return None
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc, received):
            return None
        age = datetime.now(timezone.utc).timestamp() - int(pairs.get("auth_date", "0"))
        if age > max_age:
            return None
        user = json.loads(pairs.get("user", "{}"))
        return user if isinstance(user, dict) and user.get("id") else None
    except Exception:
        return None


def _auth_user(request: web.Request) -> dict | None:
    return _verify_init_data(request.headers.get("X-Init-Data", ""))


_bot_username_cache: str = ""


async def _bot_username() -> str:
    global _bot_username_cache
    if not _bot_username_cache:
        try:
            _bot_username_cache = (await bot.get_me()).username or ""
        except Exception as e:
            logger.warning("Bot username olinmadi: %s", e)
    return _bot_username_cache


async def dashboard_page(request: web.Request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html", headers={"Cache-Control": "no-store"})


async def _profile_stats(uid: str) -> dict:
    stats = {"quiz_best": None, "cert_best": None, "quiz_rank": None, "cert_rank": None,
             "reading_done": 0, "listening_done": 0}
    if redis_client is None:
        return stats
    for key, best, rank in ((LEADERBOARD_QUIZ_KEY, "quiz_best", "quiz_rank"), (LEADERBOARD_CERT_KEY, "cert_best", "cert_rank")):
        try:
            score = await redis_client.zscore(key, uid)
            if score is not None:
                stats[best] = int(float(score))
                entries = await get_leaderboard_entries(key, top_n=10_000)
                for i, (member, _) in enumerate(entries):
                    if member == uid:
                        stats[rank] = i + 1
                        break
        except Exception as e:
            logger.warning("Profil (%s) o'qishda xatolik: %s", best, e)
    try:
        stats["reading_done"] = int(await redis_client.scard(f"ielts_bot:done:reading:{uid}") or 0)
        stats["listening_done"] = int(await redis_client.scard(f"ielts_bot:done:listening:{uid}") or 0)
    except Exception as e:
        logger.warning("Profil (done-sets) o'qishda xatolik: %s", e)
    return stats


def _dash_tiles(lang: str, items: list) -> list:
    out = []
    for emoji, cmd in items:
        if cmd in ("pro", "dashboard"):
            continue
        label = (MENU_LABELS.get(cmd) or {}).get(lang) or (MENU_LABELS.get(cmd) or {}).get("en") or cmd
        price = {"writing": WRITING_PRICE_STARS, "speaking": SPEAKING_PRICE_STARS}.get(cmd)
        out.append({"cmd": cmd, "emoji": emoji, "label": label, "price": price})
    return out


async def api_dashboard_content_list(request: web.Request):
    _u = _auth_user(request)
    if not _u:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not await _is_premium(int(_u["id"])):
        return web.json_response({"error": "pro_required"}, status=403)
    content_type = request.match_info.get("type")
    if content_type not in CONTENT_TYPES:
        return web.json_response({"error": "unknown content type"}, status=404)

    items = await list_content_items(content_type)
    result = []
    for item in items:
        questions = item.get("questions", [])
        result.append({
            "id": item["id"],
            "title": item.get("title", ""),
            "has_questions": bool(questions),
            "question_count": len(questions),
        })
    return web.json_response(result)


async def api_dashboard_content_detail(request: web.Request):
    _u = _auth_user(request)
    if not _u:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not await _is_premium(int(_u["id"])):
        return web.json_response({"error": "pro_required"}, status=403)
    content_type = request.match_info.get("type")
    if content_type not in CONTENT_TYPES:
        return web.json_response({"error": "unknown content type"}, status=404)

    try:
        item_id = int(request.match_info.get("id"))
    except ValueError:
        return web.json_response({"error": "invalid id"}, status=400)

    item = await get_content_item(content_type, item_id)
    if item is None:
        return web.json_response({"error": "not found"}, status=404)

    # Diqqat: to'g'ri javob indekslari (correct_index) MIJOZGA yuborilmaydi —
    # aks holda foydalanuvchi brauzer orqali to'g'ri javoblarni ko'rib olishi mumkin.
    safe_questions = [
        {"question": q["question"], "options": q["options"]}
        for q in item.get("questions", [])
    ]

    return web.json_response({
        "id": item_id,
        "title": item.get("title", ""),
        "body": item.get("body"),
        "questions": safe_questions,
    })


async def api_dashboard_content_submit(request: web.Request):
    content_type = request.match_info.get("type")
    if content_type not in CONTENT_TYPES:
        return web.json_response({"error": "unknown content type"}, status=404)

    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not await _is_premium(int(user["id"])):
        return web.json_response({"error": "pro_required"}, status=403)
    try:
        item_id = int(request.match_info.get("id"))
        body = await request.json()
        uid = str(user["id"])
        user_answers = body.get("answers", [])
    except Exception:
        return web.json_response({"error": "invalid request"}, status=400)

    item = await get_content_item(content_type, item_id)
    if item is None:
        return web.json_response({"error": "not found"}, status=404)

    questions = item.get("questions", [])
    total = len(questions)
    score = 0
    for i, q in enumerate(questions):
        if i < len(user_answers) and user_answers[i] == q.get("correct_index"):
            score += 1

    percentage = round((score / total) * 100) if total else 0

    if redis_client is not None:
        try:
            await redis_client.sadd(f"ielts_bot:done:{content_type}:{uid}", str(item_id))
        except Exception as e:
            logger.warning("Done-set yozishda xatolik: %s", e)

    return web.json_response({"score": score, "total": total, "percentage": percentage})


async def api_dashboard_audio(request: web.Request):
    _u = _auth_user(request)
    if not _u:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not await _is_premium(int(_u["id"])):
        return web.json_response({"error": "pro_required"}, status=403)
    """Listening kontenti uchun audio faylni Telegram'dan (file_id orqali)
    olib, brauzerga proxy qiladi — bot tokenini mijozga hech qachon ochmaydi."""
    try:
        item_id = int(request.match_info.get("id"))
    except ValueError:
        return web.Response(status=400)

    item = await get_content_item("listening", item_id)
    if item is None or "file_id" not in item:
        return web.Response(status=404)

    try:
        file = await bot.get_file(item["file_id"])
        buf = await bot.download_file(file.file_path)
        data = buf.read()
    except Exception as e:
        logger.exception("Audio proxy xatolik: %s", e)
        return web.Response(status=500)

    return web.Response(body=data, content_type="audio/ogg")


# --------------------------------------------------------------------------
# 8) AIOHTTP WEB SERVER — HEALTH-CHECK, TELEGRAM WEBHOOK, DASHBOARD MINI APP
#    (Render "Web Service" doim ochiq portni talab qiladi; shu server orqali
#    Telegram to'g'ridan-to'g'ri /webhook manziliga yangilanish yuboradi)
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# MINI APP O'YIN DVIGATELI
# Chatdagi barcha funksiyalar Mini App ichida ham ishlaydi. Server savollarni/o'yinlarni
# mavjud generatorlar (_generate_*_sync) bilan yaratadi va sessiyani xotirada saqlaydi;
# to'g'ri javoblar mijozga faqat javob berilgandan keyin yuboriladi.
# --------------------------------------------------------------------------

APP_SESSIONS: dict = {}
APP_SESSION_TTL = 3 * 3600
APP_MAX_SESSIONS_PER_USER = 8
SPEAKING_MAX_AUDIO_BYTES = 8 * 1024 * 1024

_SUBJECT_L10N = {
    "Geografiya": ("География", "Geography"), "Fizika": ("Физика", "Physics"), "Kimyo": ("Химия", "Chemistry"),
    "Biologiya": ("Биология", "Biology"), "Matematika": ("Математика", "Mathematics"),
    "Texnologiya va IT": ("Технологии и IT", "Technology & IT"),
    "Adabiyot va madaniyat": ("Литература и культура", "Literature & culture"),
    "San'at va musiqa": ("Искусство и музыка", "Art & music"), "Koinot": ("Космос", "Space"),
    "Iqtisodiyot va moliya": ("Экономика и финансы", "Economics & finance"), "Psixologiya": ("Психология", "Psychology"),
    "Sport": ("Спорт", "Sport"), "Salomatlik va ovqatlanish": ("Здоровье и питание", "Health & nutrition"),
    "Ingliz tili": ("Английский язык", "English"),
}
_GENRE_L10N = {
    "Detektiv": ("Детектив", "Detective"), "Kosmos": ("Космос", "Space"), "Fantaziya": ("Фэнтези", "Fantasy"),
    "Omon qolish": ("Выживание", "Survival"), "Qaroqchilar": ("Пираты", "Pirates"),
    "Vaqt sayohati": ("Путешествие во времени", "Time travel"),
}
_GLEVEL_L10N = ["A1–A2", "B1–B2", "C1"]
_GLEVEL_WORD = {
    "uz": ["boshlang'ich", "o'rta", "yuqori"], "ru": ["начальный", "средний", "продвинутый"],
    "en": ["beginner", "intermediate", "advanced"],
}


def _l10n(name: str, table: dict, lang: str) -> str:
    row = table.get(name)
    if not row or lang == "uz":
        return name
    return row[0] if lang == "ru" else row[1]


def _app_purge_sessions(uid: int | None = None):
    now = datetime.now(timezone.utc).timestamp()
    for k in [k for k, v in APP_SESSIONS.items() if now - v["t"] > APP_SESSION_TTL]:
        APP_SESSIONS.pop(k, None)
    if uid is not None:
        mine = sorted(((k, v["t"]) for k, v in APP_SESSIONS.items() if v["uid"] == uid), key=lambda x: x[1])
        for k, _t in mine[:-APP_MAX_SESSIONS_PER_USER]:
            APP_SESSIONS.pop(k, None)


def _app_new_session(uid: int, kind: str, **data) -> tuple[str, dict]:
    _app_purge_sessions(uid)
    sid = secrets.token_urlsafe(16)
    s = {"uid": uid, "kind": kind, "t": datetime.now(timezone.utc).timestamp(), **data}
    APP_SESSIONS[sid] = s
    return sid, s


def _app_session(sid: str, uid: int | None = None, kinds: tuple | None = None) -> dict | None:
    s = APP_SESSIONS.get(str(sid))
    if not s:
        return None
    if uid is not None and s["uid"] != uid:
        return None
    if kinds and s["kind"] not in kinds:
        return None
    s["t"] = datetime.now(timezone.utc).timestamp()
    return s


_APP_ACTIVITY = {
    "api_app_quiz_start": "📱 App: o'yin", "api_app_quiz_finish": "📱 App: o'yinni tugatdi",
    "api_app_teaser_new": "📱 App: brain teaser", "api_app_guess_start": "📱 App: taxmin o'yini",
    "api_app_story_start": "📱 App: hikoya", "api_app_writing_task": "📱 App: Writing",
    "api_app_writing_submit": "📱 App: Writing yubordi", "api_app_speaking_start": "📱 App: Speaking",
    "api_app_invoice": "📱 App: to'lov oynasi", "api_app_sat_start": "📱 App: SAT", "api_app_sat_submit": "📱 App: SAT modulini topshirdi",
}


def app_api(fn):
    """Mini App API handler uchun o'ram: initData imzosini tekshiradi, foydalanuvchi tilini o'rnatadi,
    kutilmagan xatolarni JSON qilib qaytaradi."""
    async def wrapper(request: web.Request):
        user = _auth_user(request)
        if not user:
            return web.json_response({"error": "unauthorized"}, status=401)
        uid = int(user["id"])
        lang = (await get_lang(uid)) or UNSET_LANG
        current_lang.set(lang)
        try:
            resp = await fn(request, uid, lang, user)
            try:
                if getattr(resp, "status", 500) == 200 and fn.__name__ in _APP_ACTIVITY:
                    label = _APP_ACTIVITY[fn.__name__]
                    if fn.__name__ == "api_app_quiz_start":
                        label += " " + str((await _body(request)).get("kind", ""))
                    elif fn.__name__ == "api_app_invoice":
                        label += " " + str((await _body(request)).get("product", ""))
                    nm = " ".join(x for x in (user.get("first_name"), user.get("last_name")) if x).strip()
                    asyncio.create_task(log_activity(uid, nm, user.get("username") or "", label))
            except Exception:
                pass
            return resp
        except web.HTTPException:
            raise
        except Exception as e:
            logger.exception("Mini App API xatosi (%s): %s", fn.__name__, e)
            return web.json_response({"error": "server"}, status=500)
    wrapper.__name__ = fn.__name__
    return wrapper


async def _body(request: web.Request) -> dict:
    try:
        d = await request.json()
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _err(code: str, status: int = 400, **extra):
    return web.json_response({"error": code, **extra}, status=status)


async def _is_premium(uid: int) -> bool:
    return _is_admin(uid) or await _is_pro(uid)


async def _app_premium_access(uid: int, kind: str) -> str | None:
    """Writing/Speaking uchun ruxsat: 'admin' | 'credit' | 'pro' | 'limit' | None (to'lov kerak)."""
    if _is_admin(uid):
        return "admin"
    has_credit = await (_has_writing_credit(uid) if kind == "writing" else _has_speaking_credit(uid))
    if has_credit:
        return "credit"
    if await _is_pro(uid):
        return "pro" if await _pro_use(uid, kind) else "limit"
    return None


def _paywall(kind: str, access: str | None):
    if access == "limit":
        return _err("limit", 429, limit=PRO_DAILY_LIMIT)
    return _err("payment_required", 402, product=kind)


# ---- /me --------------------------------------------------------------------
async def api_app_me(request: web.Request):
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    uid = int(user["id"])
    lang = (await get_lang(uid)) or UNSET_LANG
    name = " ".join(x for x in (user.get("first_name"), user.get("last_name")) if x).strip() \
        or (f"@{user['username']}" if user.get("username") else "User")
    exp = await _pro_expiry(uid)
    now_ts = datetime.now(timezone.utc).timestamp()
    pro = {"active": bool(exp), "until": _pro_date(exp) if exp else "",
           "left": max(1, int((exp - now_ts) // 86400) + 1) if exp else 0}
    subjects = [{"i": i, "emoji": e, "name": _l10n(n, _SUBJECT_L10N, lang), "pro": n == "Ingliz tili"}
                for i, (e, n, _d) in enumerate(SUBJECTS)]
    genres = [{"i": i, "emoji": e, "name": _l10n(n, _GENRE_L10N, lang)} for i, (e, n, _d) in enumerate(STORY_GENRES)]
    levels = []
    for i, (e, _n, _d) in enumerate(GRAMMAR_LEVELS):
        levels.append({"i": i, "emoji": e, "name": f"{_GLEVEL_L10N[i]} ({_GLEVEL_WORD[lang][i]})",
                       "topics": [t.replace(" va ", " & ") for t in GRAMMAR_TOPICS[i]]})
    return web.json_response({
        "uid": uid, "name": name, "lang": lang, "admin": _is_admin(uid), "pro": pro,
        "credits": {"writing": await _has_writing_credit(uid), "speaking": await _has_speaking_credit(uid)},
        "stats": await _profile_stats(str(uid)),
        "bot": await _bot_username(),
        "subjects": subjects, "genres": genres, "levels": levels,
        "prices": {"pro": PRO_PRICE_STARS, "pro_days": PRO_DAYS, "pro_daily_limit": PRO_DAILY_LIMIT,
                   "writing": WRITING_PRICE_STARS, "speaking": SPEAKING_PRICE_STARS},
        "limits": {"writing_min_words": WRITING_MIN_WORDS, "writing_max_chars": WRITING_MAX_CHARS,
                   "quiz_len": QUIZ_LENGTH, "cert_len": CERTIFICATE_QUIZ_LENGTH,
                   "cert_pass": CERTIFICATE_PASS_PERCENT, "guess_max": GUESS_MAX_QUESTIONS, "story_steps": STORY_STEPS},
    })


# ---- To'lov (Stars) ---------------------------------------------------------
@app_api
async def api_app_invoice(request: web.Request, uid: int, lang: str, user: dict):
    product = (await _body(request)).get("product")
    if product == "pro":
        title, desc, label, amount = _pr("inv_title"), _pr("inv_desc", d=PRO_DAYS), _pr("inv_label", d=PRO_DAYS), PRO_PRICE_STARS
    elif product == "writing":
        title, desc, label, amount = "IELTS Writing", "IELTS Writing Task 2 review", "Writing", WRITING_PRICE_STARS
    elif product == "speaking":
        title, desc, label, amount = _sp("inv_title"), _sp("inv_desc"), _sp("inv_label"), SPEAKING_PRICE_STARS
    else:
        return _err("bad_product")
    if _is_admin(uid):
        return _err("owner")
    link = await bot.create_invoice_link(
        title=title, description=desc, payload=f"{product}:{uid}:app", provider_token="", currency="XTR",
        prices=[LabeledPrice(label=label, amount=amount)],
    )
    return web.json_response({"link": link})


# ---- Test/viktorina turlari (ko'p variantli) ------------------------------
def _norm_q(q: dict) -> dict:
    return {"question": str(q.get("question", "")), "options": [str(o) for o in q.get("options", [])],
            "correct_index": int(q.get("correct_index", 0)), "explanation": str(q.get("explanation", "") or "")}


def _public_questions(qs: list) -> list:
    return [{"question": q["question"], "options": q["options"]} for q in qs]


QUIZ_PRO_KINDS = ("grammar", "reading", "listening", "certificate")


@app_api
async def api_app_quiz_start(request: web.Request, uid: int, lang: str, user: dict):
    import random
    b = await _body(request)
    kind = b.get("kind")
    if kind not in ("quiz", "certificate", "daily", "reading", "listening", "grammar", "lie", "whatif"):
        return _err("bad_kind")
    premium = await _is_premium(uid)
    if kind in QUIZ_PRO_KINDS and not premium:
        return _err("pro_required", 403)

    extra: dict = {}
    title = ""
    if kind == "quiz":
        subject_name = None
        sub = b.get("subject")
        if sub not in (None, "r"):
            try:
                emoji, subject_name, _d = SUBJECTS[int(sub)]
            except (ValueError, IndexError, TypeError):
                return _err("bad_subject")
            title = f"{emoji} {_l10n(subject_name, _SUBJECT_L10N, lang)}"
        else:
            title = "🎲"
        if subject_name == "Ingliz tili" and not premium:
            return _err("pro_required", 403)
        questions = await asyncio.to_thread(_generate_quiz_set_sync, QUIZ_LENGTH, subject_name)
        if not questions:
            return _err("generation_failed", 502)
        questions = [_norm_q(q) for q in questions]
    elif kind == "certificate":
        questions = await asyncio.to_thread(_generate_quiz_set_sync, CERTIFICATE_QUIZ_LENGTH)
        if len(questions) < CERTIFICATE_QUIZ_LENGTH // 2:
            return _err("generation_failed", 502)
        questions = [_norm_q(q) for q in questions]
    elif kind == "daily":
        content = await get_daily_content(lang)
        if not content or not content.get("questions"):
            return _err("generation_failed", 502)
        questions = [_norm_q(q) for q in content["questions"]]
        title = content.get("subject", "")
        tip = content.get("tip")
        extra = {"fact": content.get("fact", ""),
                 "tip": tip if (tip and premium) else ({"locked": True} if tip else None)}
    elif kind == "reading":
        data = await asyncio.to_thread(_generate_reading_test_sync)
        if not data["questions"] or not data["passage"]:
            return _err("generation_failed", 502)
        questions = [_norm_q(q) for q in data["questions"]]
        extra = {"passage": data["passage"]}
    elif kind == "listening":
        data = await asyncio.to_thread(_generate_listening_test_sync)
        if not data["questions"] or not data["script"]:
            return _err("generation_failed", 502)
        questions = [_norm_q(q) for q in data["questions"]]
        extra = {"audio": True}
    elif kind == "grammar":
        try:
            lvl = int(b.get("level"))
            topics = GRAMMAR_TOPICS[lvl]
            t = b.get("topic")
            topic = random.choice(topics) if t in (None, "r") else topics[int(t)]
            level_desc = GRAMMAR_LEVELS[lvl][2]
        except (ValueError, IndexError, TypeError):
            return _err("bad_topic")
        g = await asyncio.to_thread(_generate_grammar_sync, topic, level_desc)
        questions = [{"question": e["sentence"], "options": e["options"], "correct_index": e["correct_index"],
                      "explanation": e["explanation"]} for e in g["exercises"]]
        title = g["title"]
        extra = {"lesson": {"title": g["title"], "rule": g["rule"],
                            "examples": [{"en": en, "uz": uz} for en, uz in g["examples"]], "mistake": g["mistake"]}}
    elif kind == "lie":
        rnd = await asyncio.to_thread(_generate_lie_sync, random.choice(_LIE_TOPICS))
        questions = [{"question": "", "options": rnd["statements"], "correct_index": rnd["lie_index"],
                      "explanation": rnd["explanation"]}]
    else:  # whatif
        rnd = await asyncio.to_thread(_generate_what_sync, random.choice(_WHAT_TOPICS))
        expl = rnd["explanation"] + (("\n\n🤩 " + rnd["wow"]) if rnd["wow"] else "")
        questions = [{"question": rnd["scenario"], "options": rnd["options"], "correct_index": rnd["correct_index"],
                      "explanation": expl}]

    script = data["script"] if kind == "listening" else ""
    sid, _s = _app_new_session(uid, "quiz", mode=kind, questions=questions, score=0, answered=set(), script=script)
    return web.json_response({"sid": sid, "mode": kind, "title": title, "total": len(questions),
                              "questions": _public_questions(questions), **extra})


@app_api
async def api_app_quiz_answer(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    s = _app_session(b.get("sid"), uid, ("quiz",))
    if not s:
        return _err("expired", 410)
    try:
        i, choice = int(b.get("i")), int(b.get("choice"))
        q = s["questions"][i]
    except (ValueError, TypeError, IndexError):
        return _err("bad_request")
    if i in s["answered"]:
        return _err("already_answered", 409)
    s["answered"].add(i)
    correct = choice == q["correct_index"]
    if correct:
        s["score"] += 1
    return web.json_response({"correct": correct, "correct_index": q["correct_index"],
                              "explanation": q["explanation"], "score": s["score"]})


@app_api
async def api_app_quiz_finish(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    sid = b.get("sid")
    s = _app_session(sid, uid, ("quiz",))
    if not s:
        return _err("expired", 410)
    total = len(s["questions"])
    score = s["score"]
    pct = round(score / total * 100) if total else 0
    mode = s["mode"]
    out = {"score": score, "total": total, "percentage": pct, "mode": mode}
    board = {"quiz": LEADERBOARD_QUIZ_KEY, "certificate": LEADERBOARD_CERT_KEY,
             "reading": LEADERBOARD_READING_KEY, "listening": LEADERBOARD_LISTENING_KEY}.get(mode)
    if board and total:
        await update_leaderboard(board, uid, pct)
    if mode == "listening":
        out["script"] = s.get("script", "")
    if mode == "certificate":
        out["pass_percent"] = CERTIFICATE_PASS_PERCENT
        if pct >= CERTIFICATE_PASS_PERCENT:
            full_name = user.get("first_name") or user.get("username") or "User"
            if user.get("last_name"):
                full_name += " " + user["last_name"]
            cert_path = None
            try:
                cert_path = await asyncio.to_thread(_generate_certificate_image_sync, full_name, score, total, pct)
                await bot.send_photo(uid, FSInputFile(cert_path),
                                     caption=f"🎓 {html.escape(full_name)} — {score}/{total} ({pct}%)")
                out["certificate"] = "sent"
            except Exception as e:
                logger.exception("Sertifikatni yuborishda xatolik: %s", e)
                out["certificate"] = "failed"
            finally:
                if cert_path and os.path.exists(cert_path):
                    try:
                        os.remove(cert_path)
                    except OSError:
                        pass
    APP_SESSIONS.pop(sid, None)
    return web.json_response(out)


async def api_app_audio(request: web.Request):
    """Listening testi uchun TTS audio (sid — maxfiy, taxmin qilib bo'lmaydigan token)."""
    s = _app_session(request.match_info.get("sid", ""), None, ("quiz",))
    if not s or not s.get("script"):
        return web.Response(status=404)
    if "audio" not in s:
        try:
            s["audio"] = await asyncio.to_thread(_tts_mp3_bytes_sync, s["script"][:4500])
        except Exception as e:
            logger.exception("Listening audio xatosi: %s", e)
            return web.Response(status=500)
    return web.Response(body=s["audio"], content_type="audio/mpeg", headers={"Cache-Control": "private, max-age=3600"})


def _tts_mp3_bytes_sync(text: str) -> bytes:
    import io
    buf = io.BytesIO()
    gTTS(text=text, lang="en").write_to_fp(buf)
    return buf.getvalue()


# ---- Miya jumboqi -----------------------------------------------------------
@app_api
async def api_app_teaser_new(request: web.Request, uid: int, lang: str, user: dict):
    t = await asyncio.to_thread(_generate_teaser_sync)
    sid, _s = _app_new_session(uid, "teaser", teaser=t)
    return web.json_response({"sid": sid, "puzzle": str(t["puzzle"])})


@app_api
async def api_app_teaser_reveal(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    s = _app_session(b.get("sid"), uid, ("teaser",))
    if not s:
        return _err("expired", 410)
    teaser = s["teaser"]
    if b.get("what") == "hint":
        return web.json_response({"hint": str(teaser.get("hint", ""))})
    return web.json_response({"answer": str(teaser.get("answer", "")), "explanation": str(teaser.get("explanation", ""))})


# ---- Men topaman ------------------------------------------------------------
_GUESS_ANS = {"y": "Ha", "n": "Yo'q", "m": "Ba'zan / bilmayman"}


async def _guess_app_step(s: dict) -> dict:
    n = len(s["history"])
    step = await asyncio.to_thread(_guess_step_sync, s["history"], n >= GUESS_MAX_QUESTIONS - 1, s["wrong"])
    if step.get("action") == "guess" and step.get("guess"):
        s["last_guess"] = str(step["guess"])
        return {"type": "guess", "text": s["last_guess"], "n": n}
    s["last_question"] = str(step.get("question") or "Bu tirik mavjudotmi?")
    return {"type": "ask", "text": s["last_question"], "n": n + 1, "max": GUESS_MAX_QUESTIONS}


@app_api
async def api_app_guess_start(request: web.Request, uid: int, lang: str, user: dict):
    if not await _is_premium(uid):
        return _err("pro_required", 403)
    sid, s = _app_new_session(uid, "guess", history=[], wrong=[], last_question="", last_guess="")
    return web.json_response({"sid": sid, **(await _guess_app_step(s))})


@app_api
async def api_app_guess_answer(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    s = _app_session(b.get("sid"), uid, ("guess",))
    if not s:
        return _err("expired", 410)
    a = b.get("a")
    if a in _GUESS_ANS:
        s["history"].append((s.get("last_question", ""), _GUESS_ANS[a]))
    elif a == "win":
        n = len(s["history"])
        APP_SESSIONS.pop(b.get("sid"), None)
        return web.json_response({"type": "win", "text": s.get("last_guess", ""), "n": n})
    elif a == "lose":
        s["wrong"].append(s.get("last_guess", ""))
        s["history"].append((f"Sizning o'ylaganingiz {s.get('last_guess', '')} mi?", "Yo'q"))
        if len(s["history"]) >= GUESS_MAX_QUESTIONS or len(s["wrong"]) >= 3:
            APP_SESSIONS.pop(b.get("sid"), None)
            return web.json_response({"type": "lose"})
    else:
        return _err("bad_request")
    return web.json_response(await _guess_app_step(s))


# ---- Interaktiv hikoya --------------------------------------------------------
async def _story_app_step(s: dict, choice_text: str | None) -> dict:
    res = await asyncio.to_thread(_generate_story_step_sync, s["story"], choice_text)
    st = s["story"]
    st["log"].append(f"Scene {st['step']}: {res['scene']}")
    st["choices"] = res["choices"]
    return {"step": st["step"], "total": STORY_STEPS, "scene": res["scene"], "choices": res["choices"],
            "ending": bool(res["ending"]), "title": res["title"]}


@app_api
async def api_app_story_start(request: web.Request, uid: int, lang: str, user: dict):
    if not await _is_premium(uid):
        return _err("pro_required", 403)
    b = await _body(request)
    try:
        emoji, name, desc = STORY_GENRES[int(b.get("genre"))]
    except (ValueError, IndexError, TypeError):
        return _err("bad_genre")
    story = {"emoji": emoji, "name": name, "desc": desc, "step": 1, "log": [], "choices": []}
    sid, s = _app_new_session(uid, "story", story=story)
    return web.json_response({"sid": sid, **(await _story_app_step(s, None))})


@app_api
async def api_app_story_choose(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    sid = b.get("sid")
    s = _app_session(sid, uid, ("story",))
    if not s or not s["story"].get("choices"):
        return _err("expired", 410)
    st = s["story"]
    try:
        choice = st["choices"][int(b.get("choice"))]
    except (ValueError, IndexError, TypeError):
        return _err("bad_request")
    st["choices"] = []
    st["log"].append(f"Player chose: {choice}")
    st["step"] += 1
    try:
        out = await _story_app_step(s, choice)
    except Exception:
        st["step"] -= 1
        raise
    if out["ending"]:
        APP_SESSIONS.pop(sid, None)
    return web.json_response(out)


# ---- Writing ------------------------------------------------------------------
@app_api
async def api_app_writing_task(request: web.Request, uid: int, lang: str, user: dict):
    access = await _app_premium_access(uid, "writing")
    if access in (None, "limit"):
        return _paywall("writing", access)
    task = await asyncio.to_thread(_generate_writing_task_sync)
    sid, _s = _app_new_session(uid, "writing", task=task, access=access)
    return web.json_response({"sid": sid, "task": task, "access": access,
                              "min_words": WRITING_MIN_WORDS, "max_chars": WRITING_MAX_CHARS})


@app_api
async def api_app_writing_submit(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    s = _app_session(b.get("sid"), uid, ("writing",))
    if not s:
        return _err("expired", 410)
    essay = str(b.get("essay", "")).strip()
    words = len(essay.split())
    if words < WRITING_MIN_WORDS:
        return _err("too_short", 422, words=words, min=WRITING_MIN_WORDS)
    if len(essay) > WRITING_MAX_CHARS:
        return _err("too_long", 422, max=WRITING_MAX_CHARS)
    data = await asyncio.to_thread(_analyze_essay_sync, essay, s["task"])
    if s["access"] == "credit":
        await _consume_writing_credit(uid)
    APP_SESSIONS.pop(b.get("sid"), None)
    return web.json_response({"report": data, "words": words, "task": s["task"]})


# ---- Speaking (mikrofon bilan) -------------------------------------------------
def _speak_item_public(s: dict, i: int) -> dict:
    it = s["items"][i]
    out = {"i": i, "total": len(s["items"]), "part": it["part"], "lead": it.get("lead", ""), "q": it["q"]}
    if it["part"] == 2:
        out["pics"] = bool(s.get("pics"))
        out["desc"] = it.get("desc") or []
    return out


async def _speak_prepare_item(s: dict, i: int):
    """Part 2 uchun rasmlarni yuklab oladi (bir marta)."""
    it = s["items"][i]
    if it["part"] == 2 and "pics" not in s:
        s["pics"] = await _fetch_two_pictures(it.get("pics") or []) or None


@app_api
async def api_app_speaking_start(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    mode = "short" if b.get("mode") == "short" else "full"
    if mode == "short":
        if not await _is_premium(uid):
            return _err("pro_required", 403)
        access = "free"
    else:
        access = await _app_premium_access(uid, "speaking")
        if access in (None, "limit"):
            return _paywall("speaking", access)
    items = await asyncio.to_thread(_generate_full_speaking_sync)
    if mode == "short":
        items = [x for x in items if x["part"] == 1][:5]
    sid, s = _app_new_session(uid, "speaking", items=items, answers=[], idx=0, access=access, mode=mode)
    await _speak_prepare_item(s, 0)
    return web.json_response({"sid": sid, "mode": mode, "access": access, "item": _speak_item_public(s, 0)})


async def api_app_speaking_tts(request: web.Request):
    s = _app_session(request.match_info.get("sid", ""), None, ("speaking",))
    try:
        i = int(request.match_info.get("i", ""))
        it = s["items"][i]
    except (TypeError, ValueError, IndexError, KeyError):
        return web.Response(status=404)
    cache = s.setdefault("tts", {})
    if i not in cache:
        text = _plain_speech(((it.get("lead") or "") + " " + it["q"]).strip())[:900]
        try:
            cache[i] = await asyncio.to_thread(_tts_mp3_bytes_sync, text)
        except Exception as e:
            logger.exception("Speaking TTS xatosi: %s", e)
            return web.Response(status=500)
    return web.Response(body=cache[i], content_type="audio/mpeg", headers={"Cache-Control": "private, max-age=3600"})


async def api_app_speaking_pic(request: web.Request):
    s = _app_session(request.match_info.get("sid", ""), None, ("speaking",))
    try:
        n = int(request.match_info.get("n", ""))
        data = s["pics"][n]
    except (TypeError, ValueError, IndexError, KeyError):
        return web.Response(status=404)
    return web.Response(body=bytes(data), content_type="image/jpeg", headers={"Cache-Control": "private, max-age=3600"})


@app_api
async def api_app_speaking_answer(request: web.Request, uid: int, lang: str, user: dict):
    sid, audio, duration = None, None, 0
    ext = "webm"
    reader = await request.multipart()
    async for part in reader:
        if part.name == "sid":
            sid = (await part.text()).strip()
        elif part.name == "duration":
            try:
                duration = int(float(await part.text()))
            except ValueError:
                duration = 0
        elif part.name == "audio":
            ctype = (part.headers.get("Content-Type") or "").lower()
            ext = ("mp4" if "mp4" in ctype else "ogg" if "ogg" in ctype else "wav" if "wav" in ctype
                   else "mp3" if "mpeg" in ctype else "webm")
            chunks, size = [], 0
            while True:
                chunk = await part.read_chunk(256 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > SPEAKING_MAX_AUDIO_BYTES:
                    return _err("too_big", 413)
                chunks.append(chunk)
            audio = b"".join(chunks)
    s = _app_session(sid or "", uid, ("speaking",))
    if not s:
        return _err("expired", 410)
    if not audio or len(audio) < 1500:
        return _err("no_speech", 422)

    try:
        def _stt():
            tr = groq_client.audio.transcriptions.create(
                file=(f"answer.{ext}", audio), model=STT_MODEL, language="en", response_format="json")
            return (tr.text or "").strip()
        transcript = await asyncio.to_thread(_stt)
    except Exception as e:
        logger.exception("Speaking transkripsiya xatosi: %s", e)
        return _err("stt_failed", 502)
    if not transcript:
        return _err("no_speech", 422)

    i = s["idx"]
    it = s["items"][i]
    s["answers"].append({"part": it["part"], "q": it["q"] + (f" [pictures theme: {it['theme']}]" if it.get("theme") else ""),
                         "text": transcript, "sec": duration})
    s["idx"] = i + 1
    if s["idx"] < len(s["items"]):
        await _speak_prepare_item(s, s["idx"])
        return web.json_response({"done": False, "item": _speak_item_public(s, s["idx"])})

    report = await asyncio.to_thread(_evaluate_full_speaking_sync, s["answers"])
    if s["access"] == "credit":
        await _consume_speaking_credit(uid)
    APP_SESSIONS.pop(sid, None)
    return web.json_response({"done": True, "report": report})


# ---- Reyting ------------------------------------------------------------------
_name_cache: dict = {}


async def _display_name(uid_str: str) -> str:
    hit = _name_cache.get(uid_str)
    now = datetime.now(timezone.utc).timestamp()
    if hit and now - hit[1] < 6 * 3600:
        return hit[0]
    try:
        chat = await bot.get_chat(int(uid_str))
        name = chat.full_name or (f"@{chat.username}" if chat.username else f"User {uid_str[-4:]}")
    except Exception:
        name = f"User {uid_str[-4:]}"
    _name_cache[uid_str] = (name, now)
    return name


# ======================= SAT (xalqaro, Digital SAT formati) =======================
SAT_RW_DOMAINS = ["Craft and Structure", "Information and Ideas", "Standard English Conventions", "Expression of Ideas"]
SAT_MATH_DOMAINS = ["Algebra", "Advanced Math", "Problem-Solving and Data Analysis", "Geometry and Trigonometry"]
SAT_RW_N, SAT_MATH_N, SAT_PRACTICE_N = 9, 8, 10
SAT_SEC_SECONDS = {"rw": 70, "math": 95}   # bitta savolga taxminiy vaqt (soniya)
_SAT_LABEL = {"rw": "Reading & Writing", "math": "Math"}


def _sat_parse_num(x) -> float | None:
    t = str(x or "").strip().replace(" ", "").replace(",", "").replace("−", "-")
    if not t:
        return None
    try:
        if "/" in t:
            a, b = t.split("/", 1)
            return float(a) / float(b)
        return float(t)
    except (ValueError, ZeroDivisionError):
        return None


def _sat_is_correct(q: dict, ans) -> bool:
    if ans is None:
        return False
    if q["type"] == "mc":
        return isinstance(ans, int) and not isinstance(ans, bool) and ans == q["correct_index"]
    a, b = _sat_parse_num(ans), _sat_parse_num(q["answer"])
    if a is None or b is None:
        return str(ans).strip().lower() == str(q["answer"]).strip().lower()
    return abs(a - b) <= max(1e-3, abs(b) * 0.002)


def _sat_fix_tex(t):
    """JSON'da bitta teskari chiziq (\frac, \times, \beta...) boshqaruv belgisiga aylanib qolgan bo'lsa, tiklaydi."""
    if not isinstance(t, str):
        return t
    for bad, good in (("\x0c", "\\f"), ("\x08", "\\b"), ("\t", "\\t"), ("\r", "\\r"), ("\x07", "\\a"), ("\x0b", "\\v")):
        t = t.replace(bad, good)
    return t


def _sat_fix_tex_obj(o):
    if isinstance(o, str):
        return _sat_fix_tex(o)
    if isinstance(o, list):
        return [_sat_fix_tex_obj(x) for x in o]
    if isinstance(o, dict):
        return {k: _sat_fix_tex_obj(v) for k, v in o.items()}
    return o


def _generate_sat_module_sync(section: str, domains: list, difficulty: str) -> list:
    """Bitta SAT moduli: savollar real Digital SAT uslubida, ingliz tilida; izohlar o'zbekcha."""
    n = len(domains)
    plan = "\n".join(f"{i + 1}. domain: {d}" for i, d in enumerate(domains))
    if section == "rw":
        spec = (
            "Digital SAT *Reading and Writing* questions. EACH question has its own short ORIGINAL passage "
            "(25-70 words; literature, science, history or social-science excerpt written by you, NOT copied) and ONE question about it, "
            "4 options (A-D), exactly one correct. 'Standard English Conventions' = grammar/punctuation/agreement in a blank or underlined portion; "
            "'Expression of Ideas' = transitions or rhetorical synthesis; 'Craft and Structure' = words in context, text structure, cross-text purpose; "
            "'Information and Ideas' = central idea, inference, evidence, quantitative detail. "
            'JSON item: {"domain": "...", "passage": "...", "question": "...", "options": ["...","...","...","..."], "correct_index": 0, "explanation": "..."}')
    else:
        spec = (
            "Digital SAT *Math* questions (no calculator needed but allowed). About one in four items must be a STUDENT-PRODUCED RESPONSE "
            '(no options; the answer is a single number, a fraction like 3/4 or a decimal): use "type": "num" and "answer": "...". '
            'The others are multiple choice: "type": "mc", 4 options, "correct_index". Write ALL math (question, passage, options AND explanation) in LaTeX between single dollar signs, '
            r'e.g. $\frac{3}{4}$, $x^2+5x-6=0$, $\sqrt{x}$, $f(x)=2x^2$, $\angle ABC=40^\circ$. '
            r"Inside the JSON every LaTeX backslash MUST be doubled (write \\frac, \\sqrt, \\pi, \\times). Plain numbers in normal prose need no dollars. "
            "The 'answer' of a num item stays plain text (7/2 or 3.5). "
            "Make sure the answer is mathematically CORRECT and unique; double-check each computation. 'passage' is a short word-problem context or an empty string. "
            'JSON item: {"domain": "...", "type": "mc", "passage": "", "question": "...", "options": ["...","...","...","..."], "correct_index": 0, "explanation": "..."} '
            'or {"domain": "...", "type": "num", "passage": "", "question": "...", "answer": "7/2", "explanation": "..."}')
    data = _llm_json_sync(
        f"You are an expert College Board SAT item writer. Write {n} questions, difficulty: {difficulty}, one per line of this plan:\n{plan}\n\n{spec}\n"
        "Write 'explanation' in UZBEK (Latin script): 1-3 short sentences showing why the correct answer is right. "
        "Everything else (passage, question, options) stays in English. Do not number the options. "
        'Reply ONLY as JSON: {"questions": [ ...items in plan order... ]}',
        temperature=0.6)
    out = []
    for i, it in enumerate(data.get("questions") or []):
        if not isinstance(it, dict):
            continue
        it = _sat_fix_tex_obj(it)
        question = str(it.get("question") or "").strip()
        passage = str(it.get("passage") or "").strip()
        dom = str(it.get("domain") or (domains[i] if i < len(domains) else "")).strip()
        expl = str(it.get("explanation") or "").strip()
        if not question or (section == "rw" and not passage):
            continue
        typ = "num" if (section == "math" and str(it.get("type")) == "num") else "mc"
        q = {"type": typ, "domain": dom, "passage": passage, "question": question, "explanation": expl, "difficulty": difficulty}
        if typ == "num":
            ans = str(it.get("answer") or "").strip()
            if _sat_parse_num(ans) is None:
                continue
            q["answer"] = ans
            q["options"] = None
        else:
            try:
                opts = [str(o).strip() for o in it["options"]][:4]
                ci = int(it["correct_index"])
            except (KeyError, TypeError, ValueError):
                continue
            if len(opts) != 4 or len(set(opts)) != 4 or not (0 <= ci < 4) or not all(opts):
                continue
            q["options"], q["correct_index"] = opts, ci
        out.append(q)
    if len(out) < max(3, int(n * 0.7)):
        raise ValueError("SAT moduli to'liq generatsiya bo'lmadi")
    return out


async def _sat_make_module(section: str, domains: list, difficulty: str) -> list:
    last = None
    for _ in range(2):
        try:
            return await asyncio.to_thread(_generate_sat_module_sync, section, domains, difficulty)
        except Exception as e:
            last = e
            logger.warning("SAT modul generatsiyasi: %s", e)
    raise last


def _sat_domains(section: str, n: int, only: str | None = None) -> list:
    pool = SAT_RW_DOMAINS if section == "rw" else SAT_MATH_DOMAINS
    if only in pool:
        return [only] * n
    return [pool[i % len(pool)] for i in range(n)]


def _sat_public(mod: dict) -> dict:
    qs = [{"i": i, "type": q["type"], "domain": q["domain"], "passage": q["passage"], "question": q["question"],
           "options": q["options"]} for i, q in enumerate(mod["questions"])]
    n = len(qs)
    return {"section": mod["section"], "label": _SAT_LABEL[mod["section"]], "stage": mod["stage"], "n": n,
            "seconds": n * SAT_SEC_SECONDS[mod["section"]], "questions": qs}


def _sat_est_score(m1: float, m2: float, hard: bool) -> int:
    base = 450 + 350 * (0.4 * m1 + 0.6 * m2) if hard else 200 + 350 * (0.4 * m1 + 0.6 * m2)
    return int(max(200, min(800, round(base / 10) * 10)))


def _sat_review(mod: dict) -> list:
    out = []
    for q, a in zip(mod["questions"], mod["answers"]):
        right = (q["options"][q["correct_index"]] if q["type"] == "mc" else q["answer"])
        yours = (None if a is None else (q["options"][a] if q["type"] == "mc" and isinstance(a, int) and 0 <= a < 4 else str(a)))
        out.append({"domain": q["domain"], "passage": q["passage"], "question": q["question"], "options": q["options"],
                    "your": yours, "right": right, "ok": _sat_is_correct(q, a), "explanation": q["explanation"],
                    "section": mod["section"]})
    return out


def _sat_domain_stats(mods: list) -> list:
    st: dict = {}
    for mod in mods:
        for q, a in zip(mod["questions"], mod["answers"]):
            d = st.setdefault(q["domain"], [0, 0])
            d[1] += 1
            d[0] += 1 if _sat_is_correct(q, a) else 0
    return [{"domain": k, "ok": v[0], "n": v[1], "pct": round(100 * v[0] / v[1])} for k, v in st.items()]


@app_api
async def api_app_sat_start(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    mode = b.get("mode")
    if mode not in ("mock", "practice"):
        return _err("bad_mode")
    if not await _is_premium(uid):
        return _err("pro_required", 403)
    section = b.get("section") if b.get("section") in ("rw", "math") else "rw"
    only = b.get("domain") if isinstance(b.get("domain"), str) else None
    if mode == "practice":
        n = SAT_PRACTICE_N
        domains = _sat_domains(section, n, only)
        diff = "mixed: 3 medium, 4 hard-ish, 3 easy" if only is None else "medium to hard"
    else:
        section, n = "rw", SAT_RW_N
        domains = _sat_domains("rw", n)
        diff = "medium (module 1 of an adaptive test: mix of easy, medium and a few hard)"
    # kunlik limit (har bir boshlash 1 ta hisoblanadi; admin cheksiz)
    if not _is_admin(uid) and not await _pro_use(uid, "sat"):
        return _err("limit", 429, limit=PRO_DAILY_LIMIT)
    try:
        qs = await _sat_make_module(section, domains, diff)
    except Exception:
        return _err("generation_failed", 502)
    mod = {"section": section, "stage": 0, "questions": qs, "answers": [], "difficulty": "mixed"}
    sid, s = _app_new_session(uid, "sat", mode=mode, mods=[], cur=mod, hard={}, total_stages=(4 if mode == "mock" else 1))
    return web.json_response({"sid": sid, "mode": mode, "total_stages": s["total_stages"], "module": _sat_public(mod)})


@app_api
async def api_app_sat_submit(request: web.Request, uid: int, lang: str, user: dict):
    b = await _body(request)
    s = _app_session(b.get("sid"), uid, ("sat",))
    if not s or not s.get("cur"):
        return _err("expired", 410)
    mod = s["cur"]
    raw = b.get("answers")
    if not isinstance(raw, list):
        return _err("bad_answers")
    answers = []
    for i, q in enumerate(mod["questions"]):
        a = raw[i] if i < len(raw) else None
        if q["type"] == "mc":
            a = a if (isinstance(a, int) and not isinstance(a, bool) and 0 <= a < 4) else None
        else:
            a = str(a).strip()[:12] if a not in (None, "") else None
        answers.append(a)
    mod["answers"] = answers
    mod["frac"] = sum(1 for q, a in zip(mod["questions"], answers) if _sat_is_correct(q, a)) / max(1, len(answers))
    s["mods"].append(mod)
    s["cur"] = None

    if s["mode"] == "practice":
        review = _sat_review(mod)
        right = sum(1 for r in review if r["ok"])
        return web.json_response({"done": True, "mode": "practice", "right": right, "n": len(review),
                                  "domains": _sat_domain_stats([mod]), "review": review})

    stage = mod["stage"]
    nxt = stage + 1
    if nxt >= 4:
        sections = {}
        for sec, (ia, ib) in (("rw", (0, 1)), ("math", (2, 3))):
            ma, mb = s["mods"][ia], s["mods"][ib]
            sections[sec] = {"score": _sat_est_score(ma["frac"], mb["frac"], bool(s["hard"].get(sec))),
                             "hard": bool(s["hard"].get(sec)), "m1": round(ma["frac"] * 100), "m2": round(mb["frac"] * 100)}
        review = [r for m_ in s["mods"] for r in _sat_review(m_)]
        total = sections["rw"]["score"] + sections["math"]["score"]
        return web.json_response({"done": True, "mode": "mock", "sections": sections, "total": total,
                                  "domains": _sat_domain_stats(s["mods"]), "review": review})
    # keyingi modul (adaptiv: 1-moduldan keyin qiyinlik tanlanadi)
    if nxt == 1:
        hard = mod["frac"] >= 0.6
        s["hard"]["rw"] = hard
        section, n, domains = "rw", SAT_RW_N, _sat_domains("rw", SAT_RW_N)
        diff = "hard" if hard else "easy to medium"
    elif nxt == 2:
        section, n, domains = "math", SAT_MATH_N, _sat_domains("math", SAT_MATH_N)
        diff = "medium (module 1 of an adaptive test: mix of easy, medium and a few hard)"
    else:
        hard = mod["frac"] >= 0.6
        s["hard"]["math"] = hard
        section, n, domains = "math", SAT_MATH_N, _sat_domains("math", SAT_MATH_N)
        diff = "hard" if hard else "easy to medium"
    try:
        qs = await _sat_make_module(section, domains, diff)
    except Exception:
        s["mods"].pop()  # foydalanuvchi qayta yuborishi mumkin
        s["cur"] = mod
        return _err("generation_failed", 502)
    new = {"section": section, "stage": nxt, "questions": qs, "answers": [], "difficulty": diff}
    s["cur"] = new
    return web.json_response({"done": False, "module": _sat_public(new), "prev": {"stage": stage, "pct": round(mod["frac"] * 100)}})


@app_api
async def api_app_leaderboard(request: web.Request, uid: int, lang: str, user: dict):
    boards = []
    for key, k in ((LEADERBOARD_QUIZ_KEY, "quiz"), (LEADERBOARD_CERT_KEY, "certificate"),
                   (LEADERBOARD_READING_KEY, "reading"), (LEADERBOARD_LISTENING_KEY, "listening")):
        entries = await get_leaderboard_entries(key, top_n=10)
        names = await asyncio.gather(*[_display_name(m) for m, _ in entries])
        boards.append({"key": k, "rows": [{"name": n, "pct": int(sc), "me": m == str(uid)}
                                           for (m, sc), n in zip(entries, names)]})
    return web.json_response({"boards": boards})



async def health(request):
    return web.Response(text="IELTS Speaking Bot is running (webhook mode).")


async def on_startup(app: web.Application):
    await bot.set_webhook(
        url=WEBHOOK_URL,
        secret_token=WEBHOOK_SECRET,
        drop_pending_updates=True,
    )
    logger.info("Webhook o'rnatildi: %s", WEBHOOK_URL)
    await setup_bot_commands()
    try:
        await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="🎓 App", web_app=WebAppInfo(url=_app_url())))
    except Exception as e:
        logger.warning("Menyu tugmasini o'rnatib bo'lmadi: %s", e)

    global _daily_task
    if DAILY_PUSH_ENABLED and redis_client is not None:
        _daily_task = asyncio.create_task(daily_broadcast_loop())
    else:
        logger.info("Kunlik avtomatik xabar o'chiq (DAILY_PUSH_ENABLED=0 yoki Redis ulanmagan).")


_daily_task: "asyncio.Task | None" = None


async def on_shutdown(app: web.Application):
    # MUHIM: bu yerda bot.delete_webhook() CHAQIRILMAYDI!
    # Render rolling-deploy paytida eski va yangi instance bir necha soniya
    # bir vaqtda ishlab turishi mumkin. Agar shu yerda webhook o'chirilsa,
    # eski (o'chayotgan) instance yangi instance endigina o'rnatgan
    # webhookni o'chirib yuboradi va bot butunlay javob bermay qoladi.
    # Shuning uchun shutdown paytida faqat bot sessiyasini yopamiz, xolos.
    if _daily_task is not None:
        _daily_task.cancel()
    await bot.session.close()
    logger.info("Bot sessiyasi yopildi (webhook tegilmadi).")


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", health)

    # Dashboard Mini App — sahifa va API endpointlari
    app.router.add_get("/miniapp/dashboard", dashboard_page)
    app.router.add_get("/api/app/me", api_app_me)
    app.router.add_post("/api/app/invoice", api_app_invoice)
    app.router.add_post("/api/app/quiz/start", api_app_quiz_start)
    app.router.add_post("/api/app/quiz/answer", api_app_quiz_answer)
    app.router.add_post("/api/app/quiz/finish", api_app_quiz_finish)
    app.router.add_get("/api/app/audio/{sid}", api_app_audio)
    app.router.add_post("/api/app/teaser/new", api_app_teaser_new)
    app.router.add_post("/api/app/teaser/reveal", api_app_teaser_reveal)
    app.router.add_post("/api/app/guess/start", api_app_guess_start)
    app.router.add_post("/api/app/guess/answer", api_app_guess_answer)
    app.router.add_post("/api/app/story/start", api_app_story_start)
    app.router.add_post("/api/app/story/choose", api_app_story_choose)
    app.router.add_post("/api/app/writing/task", api_app_writing_task)
    app.router.add_post("/api/app/writing/submit", api_app_writing_submit)
    app.router.add_post("/api/app/speaking/start", api_app_speaking_start)
    app.router.add_get("/api/app/speaking/tts/{sid}/{i}", api_app_speaking_tts)
    app.router.add_get("/api/app/speaking/pic/{sid}/{n}", api_app_speaking_pic)
    app.router.add_post("/api/app/speaking/answer", api_app_speaking_answer)
    app.router.add_post("/api/app/sat/start", api_app_sat_start)
    app.router.add_post("/api/app/sat/submit", api_app_sat_submit)
    app.router.add_get("/api/app/leaderboard", api_app_leaderboard)
    app.router.add_get("/api/dashboard/content/{type}", api_dashboard_content_list)
    app.router.add_get("/api/dashboard/content/{type}/{id}", api_dashboard_content_detail)
    app.router.add_post("/api/dashboard/content/{type}/{id}/submit", api_dashboard_content_submit)
    app.router.add_get("/api/dashboard/audio/{id}", api_dashboard_audio)

    SimpleRequestHandler(
        dispatcher=dp,
        bot=bot,
        secret_token=WEBHOOK_SECRET,
    ).register(app, path=WEBHOOK_PATH)

    setup_application(app, dp, bot=bot)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_shutdown)
    return app


# --------------------------------------------------------------------------
# 9) BOT BUYRUQLAR MENYUSI (Telegram'da "/" bosilganda avtomatik chiqadigan
#    ro'yxat — foydalanuvchi "/ta" deb yozsa, mos buyruqlar filtrlanib ko'rsatiladi)
# --------------------------------------------------------------------------

async def setup_bot_commands():
    commands = [
        BotCommand(command="language", description="Choose language"),
        BotCommand(command="menu", description="Main menu (Science / English)"),
        BotCommand(command="start", description="Start and all features"),
        BotCommand(command="quiz", description="Quiz on any subject"),
        BotCommand(command="teaser", description="Brain teasers and riddles"),
        BotCommand(command="guess", description="I guess what you're thinking (game)"),
        BotCommand(command="story", description="Interactive story — choose your path"),
        BotCommand(command="lie", description="Spot the lie among 3 statements"),
        BotCommand(command="whatif", description="What happens? Science puzzles"),
        BotCommand(command="daily", description="Today's science, fact and English tip"),
        BotCommand(command="writing", description=f"IELTS Writing review ({WRITING_PRICE_STARS} Stars)"),
        BotCommand(command="speaking", description=f"IELTS Speaking full mock ({SPEAKING_PRICE_STARS} Stars)"),
        BotCommand(command="pro", description=f"PRO: Writing + Speaking included ({PRO_PRICE_STARS} Stars)"),
        BotCommand(command="grammar", description="English grammar: lessons and practice"),
        BotCommand(command="english", description="Chat in English (corrects your mistakes)"),
        BotCommand(command="certificate", description="25-question certificate test"),
        BotCommand(command="leaderboard", description="TOP 10 leaderboards"),
        BotCommand(command="test", description="IELTS Speaking mock test"),
        BotCommand(command="reading_test", description="IELTS Reading test"),
        BotCommand(command="listening_test", description="IELTS Listening test"),
        BotCommand(command="dashboard", description="Profile and study materials"),
        BotCommand(command="notify", description="Turn daily message on/off"),
        BotCommand(command="users", description="User list (owner only)"),
        BotCommand(command="stats", description="Bot statistics"),
        BotCommand(command="stop", description="Stop current activity"),
    ]
    # Buyruqlar menyusi hamma uchun faqat inglizcha
    await bot.set_my_commands(commands)
    for lc in ("uz", "ru", "en"):
        try:
            await bot.set_my_commands(commands, language_code=lc)
        except Exception as e:
            logger.warning("%s tilidagi buyruqlar menyusini o'rnatib bo'lmadi: %s", lc, e)
    logger.info("Bot buyruqlar menyusi o'rnatildi.")


# --------------------------------------------------------------------------
# 9) MAIN
# --------------------------------------------------------------------------

def main():
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
