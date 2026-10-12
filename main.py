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
import json
import asyncio
import logging
import tempfile
import shutil
import secrets
import base64
import html
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
)
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


async def track_user(user_id: int):
    """Foydalanuvchini Upstash Redis'dagi noyob foydalanuvchilar to'plamiga
    qo'shadi va umumiy murojaatlar sonini oshiradi. Xatolik bo'lsa botni
    to'xtatmaydi, faqat log yozadi."""
    if redis_client is None:
        return
    try:
        await redis_client.sadd(BOT_USERS_SET_KEY, str(user_id))
        await redis_client.incr(BOT_STARTS_COUNTER_KEY)
    except Exception as e:
        logger.warning("Redis orqali statistika yozishda xatolik: %s", e)


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
            asyncio.create_task(track_user(user.id))
        return await handler(event, data)


dp.message.outer_middleware(UserTrackingMiddleware())
dp.callback_query.outer_middleware(UserTrackingMiddleware())


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
        rows.append([InlineKeyboardButton(text="📢 Kanalga obuna bo'lish", url=REQUIRED_CHANNEL_LINK)])
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data=CHECK_SUB_CALLBACK)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


SUBSCRIBE_TEXT = (
    "📢 <b>Botdan foydalanish uchun kanalimizga obuna bo'ling!</b>\n\n"
    "1️⃣ Pastdagi tugma orqali kanalga o'ting va obuna bo'ling.\n"
    "2️⃣ Keyin <b>✅ Tekshirish</b> tugmasini bosing."
)


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
            await event.answer("📢 Avval kanalga obuna bo'ling!", show_alert=True)
            if event.message:
                try:
                    await event.message.answer(SUBSCRIBE_TEXT, reply_markup=_subscribe_keyboard())
                except Exception:
                    pass
        elif isinstance(event, Message):
            await event.answer(SUBSCRIBE_TEXT, reply_markup=_subscribe_keyboard())
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

    center_text(120, "SERTIFIKAT", font_title, border_gold)
    center_text(215, "Bilim va yutuqlar sertifikati", font_subtitle, text_gray)

    draw.line([(300, 290), (width - 300, 290)], fill=border_gold, width=2)

    center_text(350, "Ushbu sertifikat quyidagi shaxsga topshiriladi:", font_body, text_dark)
    center_text(410, full_name, font_name, text_dark)

    draw.line([(400, 500), (width - 400, 500)], fill=border_gold, width=1)

    center_text(
        560,
        f"IELTS Speaking AI Examiner Bot tomonidan tashkil etilgan {total} savolli",
        font_body,
        text_dark,
    )
    center_text(605, "umumiy bilim sertifikat testidan muvaffaqiyatli o'tgani uchun.", font_body, text_dark)

    center_text(690, f"Natija: {score}/{total} ({percentage}%)", font_score, border_gold)

    date_str = datetime.now().strftime("%d.%m.%Y")
    center_text(770, f"Sana: {date_str}", font_body, text_gray)

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
    "🔮 /topaman — siz o'ylagan narsani men topaman!\n"
    "/kunlik — bugungi fan, qiziqarli fakt va test\n"
    "/hikoya — interaktiv hikoya: tanlovingiz yakunni o'zgartiradi 🎭\n"
    "/sertifikat — 25 savolli test, natija yaxshi bo'lsa sertifikat\n"
    "/reyting — eng yaxshilar jadvali\n\n"
    "🇬🇧 <b>Ingliz tili va IELTS</b>\n"
    "/english — ingliz tilida suhbat, xatolaringizni tuzatib boraman\n"
    "/test — IELTS Speaking mock testi (ovozli javoblar)\n"
    "/reading_test, /listening_test — IELTS uslubidagi testlar\n"
    "/dashboard — profil va o'quv materiallari\n\n"
    "🔔 Har kuni ertalab yangi fan va ingliz tili maslahati yuboraman — o'chirish: /xabarnoma\n"
    "🛑 Jarayonni to'xtatish: /stop"
)


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(WELCOME_TEXT)


@dp.message(Command("stop"))
async def cmd_stop(message: Message, state: FSMContext):
    current = await state.get_state()

    # Davom etayotgan viktorina/test (Quiz, Kunlik, Reading, Listening, Sertifikat) va jumboqni ham to'xtatamiz
    quiz_stopped = active_quizzes.pop(message.from_user.id, None) is not None
    active_teasers.pop(message.from_user.id, None)
    active_stories.pop(message.from_user.id, None)
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
    except Exception as e:
        logger.exception("Statistikani o'qishda xatolik: %s", e)
        await message.answer("❌ Statistikani olishda xatolik yuz berdi.")
        return

    total_starts = total_starts or 0

    await message.answer(
        "📊 <b>Bot statistikasi</b>\n\n"
        f"👥 Jami noyob foydalanuvchilar: <b>{total_users}</b>\n"
        f"📨 Jami murojaatlar (xabar/tugma): <b>{total_starts}</b>"
    )


@dp.message(Command("reyting"))
async def cmd_reyting(message: Message):
    """Viktorina (/quiz), sertifikat (/sertifikat), Reading va Listening testlari uchun
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


@dp.message(Command("dashboard"))
async def cmd_dashboard(message: Message):
    """Dashboard Mini App'ni ochadi: Profil, Reading, Listening, Grammar bo'limlari."""
    url = f"{WEBHOOK_BASE_URL.rstrip('/')}/miniapp/dashboard?uid={message.from_user.id}"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🎓 Dashboard'ni ochish", web_app=WebAppInfo(url=url))]]
    )
    await message.answer(
        "🎓 <b>IELTS Dashboard</b>\n\n"
        "Profilingiz, Reading, Listening va Grammar materiallarini shu yerdan ko'rishingiz mumkin:",
        reply_markup=keyboard,
    )


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
            await target.answer("⚠️ Kechirasiz, xatolik yuz berdi. Qayta boshlash: /topaman")
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


@dp.message(Command("topaman"))
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
        await callback.answer("Bu o'yin tugagan. Yangisi: /topaman", show_alert=True)
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


# --- Interaktiv hikoya (/hikoya) -----------------------------------------
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
        await chat_message.answer("❌ Hikoyani davom ettirib bo'lmadi. Qayta urinish uchun /hikoya yuboring.")
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


@dp.message(Command("hikoya"))
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
            await callback.answer("Bu hikoya tugagan. /hikoya bilan yangisini boshlang.", show_alert=True)
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


async def get_daily_content() -> dict | None:
    """Bugungi kunlik kontentni qaytaradi: avval xotiradan, keyin Redis'dan,
    yo'q bo'lsa yangi generatsiya qilib saqlaydi."""
    date_str = _today_str()
    cached = _daily_cache.get(date_str)
    if cached:
        return cached

    async with _daily_lock:
        cached = _daily_cache.get(date_str)
        if cached:
            return cached

        key = DAILY_CONTENT_KEY_PREFIX + date_str
        if redis_client is not None:
            try:
                raw = await redis_client.get(key)
                if raw:
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    content = json.loads(raw)
                    _daily_cache.clear()
                    _daily_cache[date_str] = content
                    return content
            except Exception as e:
                logger.warning("Kunlik kontentni Redis'dan o'qishda xatolik: %s", e)

        try:
            content = await asyncio.to_thread(_generate_daily_content_sync, date_str)
        except Exception as e:
            logger.exception("Kunlik kontentni generatsiya qilishda xatolik: %s", e)
            return None

        if redis_client is not None:
            try:
                await redis_client.set(key, json.dumps(content), ex=172800)
            except Exception as e:
                logger.warning("Kunlik kontentni Redis'ga yozishda xatolik: %s", e)

        _daily_cache.clear()
        _daily_cache[date_str] = content
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


@dp.message(Command("kunlik"))
async def cmd_kunlik(message: Message):
    prep_msg = await message.answer("⏳ Bugungi kontent tayyorlanmoqda...")
    content = await get_daily_content()
    if not content:
        await prep_msg.edit_text("❌ Bugungi kontentni tayyorlab bo'lmadi. Birozdan keyin /kunlik bilan qayta urinib ko'ring.")
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


@dp.message(Command("xabarnoma"))
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
            await message.answer("🔕 Kunlik xabarlar <b>o'chirildi</b>. Qayta yoqish uchun yana /xabarnoma yuboring.\n\nKunlik kontentni /kunlik orqali o'zingiz ham ochishingiz mumkin.")
    except Exception as e:
        logger.exception("Xabarnoma sozlamasida xatolik: %s", e)
        await message.answer("❌ Xatolik yuz berdi. Keyinroq qayta urinib ko'ring.")


async def broadcast_daily_content(content: dict) -> tuple[int, int]:
    """Kunlik kontentni barcha foydalanuvchilarga (o'chirib qo'yganlardan tashqari) yuboradi.
    (yuborildi, xato) sonini qaytaradi. Telegram limitiga tushmaslik uchun sekin yuboradi."""
    from aiogram.exceptions import TelegramForbiddenError, TelegramRetryAfter

    users = await redis_client.smembers(BOT_USERS_SET_KEY) or []
    optouts = set(str(x) for x in (await redis_client.smembers(DAILY_OPTOUT_KEY) or []))
    text = _format_daily_text(content) + "\n\n<i>Kunlik xabarni o'chirish: /xabarnoma</i>"
    keyboard = _daily_keyboard(content)

    sent = failed = 0
    for uid in users:
        uid = str(uid)
        if uid in optouts:
            continue
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
                    content = await get_daily_content()
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



@dp.message(Command("sertifikat"))
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
        await prep_msg.edit_text("❌ Savollarni tayyorlashda xatolik yuz berdi. Iltimos /sertifikat bilan qayta urinib ko'ring.")
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
                        "🏆 Reytingni /reyting orqali ko'rishingiz mumkin."
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
                "🔁 Qayta urinish uchun /sertifikat yuboring."
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
            f"🔁 Yangi test uchun {next_cmd}, reytingni ko'rish uchun /reyting yuboring."
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
        "🔁 Yana o'ynash uchun /quiz, sertifikat testi uchun /sertifikat, "
        "reytingni ko'rish uchun /reyting yuboring."
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
        "✅ <b>Test tayyor!</b>\n\n🎤 <b>PART 1</b> — Introduction & Interview\n"
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
        cue = questions["part2"]["cue_card"]
        await state.set_state(ExamStates.part2)
        await message.answer("✅ <b>Part 1 tugadi!</b>\n\n🎬 <b>PART 2</b> — Cue Card\n⏱ O'zingizni tayyorlang va so'ng 1-2 daqiqa gapirib, ovozli xabar sifatida yuboring.")
        await send_text_and_voice(message, f"🎴 <b>Cue Card:</b>\n{cue}", tts_text=cue)


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

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>IELTS Dashboard</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root {
    --bg: #0f1220; --card: #171b2e; --accent: #6c8bff; --accent2: #ff6cab;
    --text: #eef0ff; --muted: #9aa0c0; --good: #52d68a; --bad: #ff6b6b;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    background: var(--bg); color: var(--text); min-height: 100vh; padding-bottom: 80px;
  }
  header { padding: 18px 20px 10px; text-align: center; }
  header h1 { font-size: 19px; margin: 0; }
  #tabs {
    position: sticky; top: 0; z-index: 10; display: flex; background: var(--bg);
    border-bottom: 1px solid #262b48; padding: 0 8px;
  }
  .tab {
    flex: 1; text-align: center; padding: 12px 4px; font-size: 13px; color: var(--muted);
    cursor: pointer; border-bottom: 2px solid transparent;
  }
  .tab.active { color: var(--text); border-bottom-color: var(--accent); font-weight: 600; }
  #content { padding: 16px; }
  .card {
    background: var(--card); border-radius: 14px; padding: 16px; margin-bottom: 12px;
  }
  .card h3 { margin: 0 0 6px; font-size: 15px; }
  .card p { margin: 4px 0; color: var(--muted); font-size: 13px; }
  .pill {
    display: inline-block; background: #262b48; color: var(--muted); font-size: 11px;
    padding: 3px 9px; border-radius: 999px; margin-top: 6px;
  }
  button.primary {
    background: linear-gradient(90deg, var(--accent), var(--accent2)); color: white;
    border: none; border-radius: 10px; padding: 10px 18px; font-size: 14px;
    font-weight: 600; cursor: pointer; margin-top: 10px;
  }
  button.secondary {
    background: #262b48; color: var(--text); border: none; border-radius: 10px;
    padding: 10px 18px; font-size: 14px; cursor: pointer; margin-top: 10px; margin-right: 8px;
  }
  .option-btn {
    display: block; width: 100%; text-align: left; background: #1f2440; color: var(--text);
    border: 1px solid #2e3358; border-radius: 10px; padding: 12px 14px; margin-bottom: 8px;
    font-size: 14px; cursor: pointer;
  }
  .option-btn.correct { background: rgba(82,214,138,0.18); border-color: var(--good); }
  .option-btn.wrong { background: rgba(255,107,107,0.18); border-color: var(--bad); }
  .body-text { white-space: pre-wrap; line-height: 1.6; font-size: 14px; color: var(--text); }
  #back-btn { color: var(--accent); font-size: 13px; cursor: pointer; margin-bottom: 12px; display: inline-block; }
  .stat-row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #262b48; font-size: 13px; }
  .stat-row:last-child { border-bottom: none; }
  .loading { text-align: center; color: var(--muted); padding: 40px 0; font-size: 13px; }
  audio { width: 100%; margin-top: 10px; }
</style>
</head>
<body>
<header><h1>🎓 IELTS Dashboard</h1></header>
<div id="tabs">
  <div class="tab active" data-tab="profile">👤 Profil</div>
  <div class="tab" data-tab="reading">📖 Reading</div>
  <div class="tab" data-tab="listening">🎧 Listening</div>
  <div class="tab" data-tab="grammar">📚 Grammar</div>
</div>
<div id="content"><div class="loading">⏳ Yuklanmoqda…</div></div>

<script>
(function () {
  const tg = window.Telegram ? window.Telegram.WebApp : null;
  if (tg) { tg.ready(); tg.expand(); }

  const params = new URLSearchParams(window.location.search);
  let uid = params.get("uid") || "anonymous";
  try {
    if (tg && tg.initDataUnsafe && tg.initDataUnsafe.user && tg.initDataUnsafe.user.id) {
      uid = String(tg.initDataUnsafe.user.id);
    }
  } catch (e) {}

  const contentEl = document.getElementById("content");
  const tabs = document.querySelectorAll(".tab");
  let currentTab = "profile";

  tabs.forEach((t) => {
    t.addEventListener("click", () => {
      tabs.forEach((x) => x.classList.remove("active"));
      t.classList.add("active");
      currentTab = t.dataset.tab;
      render(currentTab);
    });
  });

  function escapeHtml(str) {
    const d = document.createElement("div");
    d.textContent = str;
    return d.innerHTML;
  }

  async function fetchJSON(url, opts) {
    const resp = await fetch(url, opts);
    return await resp.json();
  }

  async function render(tab) {
    contentEl.innerHTML = '<div class="loading">⏳ Yuklanmoqda…</div>';
    if (tab === "profile") return renderProfile();
    if (tab === "reading") return renderList("reading");
    if (tab === "listening") return renderList("listening");
    if (tab === "grammar") return renderList("grammar");
  }

  async function renderProfile() {
    try {
      const data = await fetchJSON("/api/dashboard/profile?uid=" + encodeURIComponent(uid));
      let html = '<div class="card"><h3>' + escapeHtml(data.name || "Foydalanuvchi") + '</h3>';
      html += '<div class="stat-row"><span>🏆 Viktorina (eng yaxshi)</span><span>' + (data.quiz_best !== null ? data.quiz_best + '%' : '—') + '</span></div>';
      html += '<div class="stat-row"><span>🎓 Sertifikat testi (eng yaxshi)</span><span>' + (data.cert_best !== null ? data.cert_best + '%' : '—') + '</span></div>';
      html += '<div class="stat-row"><span>📖 Reading testlari yechilgan</span><span>' + data.reading_done + '</span></div>';
      html += '<div class="stat-row"><span>🎧 Listening testlari yechilgan</span><span>' + data.listening_done + '</span></div>';
      if (data.quiz_rank) html += '<div class="stat-row"><span>Viktorina reytingidagi o\'rni</span><span>#' + data.quiz_rank + '</span></div>';
      if (data.cert_rank) html += '<div class="stat-row"><span>Sertifikat reytingidagi o\'rni</span><span>#' + data.cert_rank + '</span></div>';
      html += '</div>';
      contentEl.innerHTML = html;
    } catch (e) {
      contentEl.innerHTML = '<div class="loading">⚠️ Xatolik yuz berdi.</div>';
    }
  }

  async function renderList(type) {
    try {
      const items = await fetchJSON("/api/dashboard/content/" + type);
      if (!items.length) {
        contentEl.innerHTML = '<div class="loading">📭 Hozircha kontent yo\'q.</div>';
        return;
      }
      let html = "";
      items.forEach((item) => {
        html += '<div class="card" data-id="' + item.id + '">';
        html += '<h3>' + escapeHtml(item.title) + '</h3>';
        if (item.has_questions) html += '<span class="pill">📝 ' + item.question_count + ' ta savol</span>';
        else html += '<span class="pill">📖 O\'qish/tinglash uchun</span>';
        html += '<br><button class="primary" onclick="window.__openItem(\'' + type + '\',' + item.id + ')">▶️ Ochish</button>';
        html += '</div>';
      });
      contentEl.innerHTML = html;
    } catch (e) {
      contentEl.innerHTML = '<div class="loading">⚠️ Xatolik yuz berdi.</div>';
    }
  }

  window.__openItem = async function (type, id) {
    contentEl.innerHTML = '<div class="loading">⏳ Yuklanmoqda…</div>';
    const item = await fetchJSON("/api/dashboard/content/" + type + "/" + id);

    let html = '<div id="back-btn" onclick="window.__back(\'' + type + '\')">← Orqaga</div>';
    html += '<div class="card"><h3>' + escapeHtml(item.title) + '</h3>';
    if (item.body) html += '<div class="body-text">' + escapeHtml(item.body) + '</div>';
    if (type === "listening") {
      html += '<audio controls src="/api/dashboard/audio/' + id + '"></audio>';
    }
    html += '</div>';

    if (item.questions && item.questions.length) {
      html += '<div id="quiz-area"></div>';
      contentEl.innerHTML = html;
      renderQuiz(type, id, item.questions);
    } else {
      contentEl.innerHTML = html;
    }
  };

  window.__back = function (type) { render(type); };

  function renderQuiz(type, id, questions) {
    const quizArea = document.getElementById("quiz-area");
    const answers = new Array(questions.length).fill(null);
    let currentQ = 0;

    function renderQuestion() {
      const q = questions[currentQ];
      let html = '<div class="card"><p>Savol ' + (currentQ + 1) + '/' + questions.length + '</p>';
      html += '<h3>' + escapeHtml(q.question) + '</h3>';
      q.options.forEach((opt, i) => {
        html += '<button class="option-btn" data-i="' + i + '">' + escapeHtml(opt) + '</button>';
      });
      html += '</div>';
      quizArea.innerHTML = html;
      quizArea.querySelectorAll(".option-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
          answers[currentQ] = parseInt(btn.dataset.i, 10);
          currentQ++;
          if (currentQ < questions.length) renderQuestion();
          else submitQuiz();
        });
      });
    }

    async function submitQuiz() {
      quizArea.innerHTML = '<div class="loading">🧮 Baholanmoqda…</div>';
      const result = await fetchJSON("/api/dashboard/content/" + type + "/" + id + "/submit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ uid: uid, answers: answers }),
      });
      let html = '<div class="card"><h3>🏁 Natija: ' + result.score + '/' + result.total + ' (' + result.percentage + '%)</h3>';
      html += '<button class="secondary" onclick="window.__back(\'' + type + '\')">↩️ Ro\'yxatga qaytish</button>';
      html += '</div>';
      quizArea.innerHTML = html;
    }

    renderQuestion();
  }

  render(currentTab);
})();
</script>
</body>
</html>
"""


async def dashboard_page(request: web.Request):
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def api_dashboard_profile(request: web.Request):
    uid = request.query.get("uid", "")
    name = "Foydalanuvchi"
    try:
        chat = await bot.get_chat(int(uid))
        name = chat.full_name or (f"@{chat.username}" if chat.username else name)
    except Exception:
        pass

    quiz_best = None
    cert_best = None
    quiz_rank = None
    cert_rank = None

    if redis_client is not None:
        try:
            score = await redis_client.zscore(LEADERBOARD_QUIZ_KEY, uid)
            if score is not None:
                quiz_best = int(float(score))
                entries = await get_leaderboard_entries(LEADERBOARD_QUIZ_KEY, top_n=10_000)
                for i, (member, _) in enumerate(entries):
                    if member == uid:
                        quiz_rank = i + 1
                        break
        except Exception as e:
            logger.warning("Profil (viktorina) o'qishda xatolik: %s", e)

        try:
            score = await redis_client.zscore(LEADERBOARD_CERT_KEY, uid)
            if score is not None:
                cert_best = int(float(score))
                entries = await get_leaderboard_entries(LEADERBOARD_CERT_KEY, top_n=10_000)
                for i, (member, _) in enumerate(entries):
                    if member == uid:
                        cert_rank = i + 1
                        break
        except Exception as e:
            logger.warning("Profil (sertifikat) o'qishda xatolik: %s", e)

    reading_done = 0
    listening_done = 0
    if redis_client is not None:
        try:
            reading_done = await redis_client.scard(f"ielts_bot:done:reading:{uid}") or 0
            listening_done = await redis_client.scard(f"ielts_bot:done:listening:{uid}") or 0
        except Exception as e:
            logger.warning("Profil (done-sets) o'qishda xatolik: %s", e)

    return web.json_response({
        "name": name,
        "quiz_best": quiz_best,
        "cert_best": cert_best,
        "quiz_rank": quiz_rank,
        "cert_rank": cert_rank,
        "reading_done": int(reading_done),
        "listening_done": int(listening_done),
    })


async def api_dashboard_content_list(request: web.Request):
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

    try:
        item_id = int(request.match_info.get("id"))
        body = await request.json()
        uid = str(body.get("uid", "anonymous"))
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
    app.router.add_get("/api/dashboard/profile", api_dashboard_profile)
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
        BotCommand(command="start", description="Boshlash va barcha imkoniyatlar"),
        BotCommand(command="quiz", description="Istalgan fandan viktorina"),
        BotCommand(command="teaser", description="Miya jumboqlari va topishmoqlar"),
        BotCommand(command="topaman", description="Men o'ylagan narsani top (o'yin)"),
        BotCommand(command="hikoya", description="Interaktiv hikoya — yo'lni o'zingiz tanlang"),
        BotCommand(command="kunlik", description="Bugungi fan, fakt va ingliz tili maslahati"),
        BotCommand(command="english", description="Ingliz tilida suhbat (xatolarni tuzatadi)"),
        BotCommand(command="sertifikat", description="25 savolli sertifikat testi"),
        BotCommand(command="reyting", description="TOP 10 reyting jadvallari"),
        BotCommand(command="test", description="IELTS Speaking mock testi"),
        BotCommand(command="reading_test", description="IELTS Reading testi"),
        BotCommand(command="listening_test", description="IELTS Listening testi"),
        BotCommand(command="dashboard", description="Profil va o'quv materiallari"),
        BotCommand(command="xabarnoma", description="Kunlik xabarni yoqish/o'chirish"),
        BotCommand(command="stats", description="Bot statistikasi"),
        BotCommand(command="stop", description="Joriy jarayonni to'xtatish"),
    ]
    await bot.set_my_commands(commands)
    logger.info("Bot buyruqlar menyusi o'rnatildi.")


# --------------------------------------------------------------------------
# 9) MAIN
# --------------------------------------------------------------------------

def main():
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
