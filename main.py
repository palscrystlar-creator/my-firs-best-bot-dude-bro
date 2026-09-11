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
from datetime import datetime

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
LEADERBOARD_TARIX_KEY = "ielts_bot:leaderboard:tarix"
LEADERBOARD_CERT_KEY = "ielts_bot:leaderboard:sertifikat"
LEADERBOARD_READING_KEY = "ielts_bot:leaderboard:reading"
LEADERBOARD_LISTENING_KEY = "ielts_bot:leaderboard:listening"
LEADERBOARD_TOP_N = 10
# Ixtiyoriy: /stats buyrug'ini faqat shu Telegram user_id egasiga cheklash uchun.
# Agar bo'sh qoldirilsa, /stats hamma uchun ochiq bo'ladi.
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")

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


# --- Tarixdan ketma-ket savollar (History Quiz Session) ----------------

# Har bir foydalanuvchining hozirgi faol tarix viktorina sessiyasini saqlab turadi.
# user_id -> {"questions": [...], "idx": int, "score": int, "mode": "quiz"|"certificate"}
active_history_quizzes: dict[int, dict] = {}

HISTORY_QUIZ_LENGTH = 10
CERTIFICATE_QUIZ_LENGTH = 25
CERTIFICATE_PASS_PERCENT = 70  # sertifikat olish uchun kerakli minimal foiz
MAX_QUESTIONS_PER_LLM_CALL = 10  # bitta so'rovda ishonchli generatsiya qilinadigan maksimal savol soni

HISTORY_TOPICS = [
    "jahon tarixi",
    "O'zbekiston tarixi",
    "qadimgi Sharq tarixi",
    "O'rta asrlar tarixi",
    "Buyuk ipak yo'li",
    "Amir Temur va Temuriylar davri",
    "Ikkinchi jahon urushi",
    "qadimgi Yunoniston va Rim",
    "Mustaqillik davri O'zbekiston tarixi",
    "buyuk geografik kashfiyotlar",
]


def _generate_history_quiz_batch_sync(count: int) -> list:
    """Groq (LLM) orqali bir-biriga o'xshamaydigan `count` ta tarix savolidan
    iborat BITTA to'plamni bitta so'rovda JSON ko'rinishida generatsiya qiladi.
    `count` MAX_QUESTIONS_PER_LLM_CALL dan oshmasligi kerak (ishonchlilik uchun)."""
    topics_str = ", ".join(HISTORY_TOPICS)
    prompt = (
        "Siz tarix fanidan qiziqarli viktorina (trivia) savollari tuzuvchi mutaxassissiz. "
        f"Quyidagi mavzular doirasida bir-biriga o'xshamaydigan, xilma-xil {count} ta savol tuzing: "
        f"{topics_str}. Har bir savol turli mavzu va faktlarga oid bo'lsin, takrorlanmasin. "
        "Javobni FAQAT quyidagi JSON formatida qaytaring, boshqa hech qanday matn qo'shmang:\n"
        "{\n"
        '  "questions": [\n'
        "    {\n"
        '      "question": "savol matni o\'zbek tilida",\n'
        '      "options": ["variant A", "variant B", "variant C", "variant D"],\n'
        '      "correct_index": 0,\n'
        '      "explanation": "to\'g\'ri javobning qisqa (1-2 gapli) izohi o\'zbek tilida"\n'
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
    questions = data.get("questions", [])

    # Faqat to'g'ri formatdagi savollarni qoldiramiz (LLM xato format bersa ham bot yiqilmasin)
    valid = []
    for q in questions:
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
    return valid[:count]


def _generate_history_quiz_set_sync(count: int) -> list:
    """Kerakli umumiy savol sonini MAX_QUESTIONS_PER_LLM_CALL dan oshmaydigan
    bo'laklarga bo'lib, bir necha marta LLM'ga murojaat qilib yig'adi. Bu
    katta (masalan 25 ta) savol to'plamini so'rasak ham JSON kesilib qolish
    yoki formatning buzilish xavfini kamaytiradi."""
    all_questions = []
    remaining = count
    while remaining > 0 and len(all_questions) < count:
        batch_size = min(remaining, MAX_QUESTIONS_PER_LLM_CALL)
        try:
            batch = _generate_history_quiz_batch_sync(batch_size)
        except Exception as e:
            logger.exception("Savol partiyasini generatsiya qilishda xatolik: %s", e)
            batch = []
        all_questions.extend(batch)
        remaining -= batch_size
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


def _build_history_question_view(quiz: dict, idx: int):
    """Berilgan indeksdagi savol uchun matn va inline tugmalarni tayyorlaydi."""
    q = quiz["questions"][idx]
    total = len(quiz["questions"])
    letters = ["A", "B", "C", "D", "E", "F"]
    buttons = [
        [InlineKeyboardButton(text=f"{letters[i]}) {opt}", callback_data=f"histans:{i}")]
        for i, opt in enumerate(q["options"])
    ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    mode_labels = {
        "certificate": "🎓 Sertifikat testi",
        "quiz": "📜 Tarix savoli",
        "reading": "📖 Reading testi",
        "listening": "🎧 Listening testi",
    }
    mode_label = mode_labels.get(quiz.get("mode"), "📜 Savol")
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
    center_text(215, "Tarix bilimlari bo'yicha yutuqlar sertifikati", font_subtitle, text_gray)

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
    center_text(605, "tarix bo'yicha sertifikat testidan muvaffaqiyatli o'tgani uchun.", font_body, text_dark)

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
    "Siz do'stona, samimiy va foydali suhbatdosh yordamchisiz. Foydalanuvchi "
    "qaysi tilda yozsa, o'sha tilda javob bering (odatda o'zbek tilida). "
    "Javoblaringiz qisqa va tabiiy bo'lsin, keraksiz cho'zilib ketmang, "
    "lekin savolga to'liq va foydali javob bering."
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
    "👋 <b>Assalomu alaykum!</b>\n\n"
    "Men <b>IELTS Speaking AI Examiner</b> botiman. Men sizga to'liq IELTS Speaking "
    "Mock testini o'tkazib beraman: Part 1, Part 2 va Part 3, so'ngra batafsil "
    "baholash (Fluency, Lexical Resource, Grammar, Pronunciation, Overall Band Score) "
    "bilan natija beraman.\n\n"
    "📌 <b>Qoidalar:</b>\n"
    "• Savollarga faqat <b>ovozli xabar (voice)</b> bilan javob bering.\n"
    "• Har bir javobingiz avtomatik tinglanadi va tahlil qilinadi.\n"
    "• Testni istalgan vaqtda /stop bilan to'xtatishingiz mumkin.\n\n"
    "🎲 Bonus: /tarix buyrug'i bilan ketma-ket 10 ta tarix savolidan iborat viktorina o'ynashingiz mumkin!\n"
    "🎓 /sertifikat bilan esa 25 ta savolli test topshirib, natijangiz yetarli bo'lsa sertifikat olishingiz mumkin!\n"
    "📖 /reading_test va 🎧 /listening_test bilan AI generatsiya qilgan haqiqiy IELTS uslubidagi Reading/Listening testlarini yechishingiz mumkin!\n"
    "🏆 /reyting bilan eng yaxshi natijalar jadvalini ko'rishingiz mumkin!\n"
    "🇬🇧 /english bilan ingliz tilida erkin suhbatlashib, xatolaringizni tuzatib olishingiz mumkin!\n"
    "🎓 /dashboard bilan esa Profil, Reading, Listening va Grammar bo'limlariga ega shaxsiy oynani ochishingiz mumkin!\n"
    "💬 Yoki menga oddiygina yozing (yoki ovozli xabar yuboring) — hech qanday maxsus "
    "buyruqsiz erkin suhbatlashishimiz mumkin!\n\n"
    "🚀 Boshlash uchun /test buyrug'ini yuboring!"
)


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(WELCOME_TEXT)


@dp.message(Command("stop"))
async def cmd_stop(message: Message, state: FSMContext):
    current = await state.get_state()

    if current is None:
        # Faol imtihon yo'q, lekin suhbat xotirasi bo'lishi mumkin — uni tozalaymiz.
        await state.set_data({})
        await message.answer("🔄 Suhbat xotirasi tozalandi. Yangidan yozishingiz mumkin, yoki /test bilan imtihon boshlang.")
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
    """Tarix viktorinasi (/tarix) va sertifikat testi (/sertifikat) uchun
    alohida TOP 10 reyting jadvalini ko'rsatadi."""
    if redis_client is None:
        await message.answer(
            "⚠️ Reyting xizmati sozlanmagan.\n\n"
            "UPSTASH_REDIS_REST_URL va UPSTASH_REDIS_REST_TOKEN "
            "environment variable'larini qo'shing."
        )
        return

    await bot.send_chat_action(message.chat.id, "typing")

    tarix_text = await format_leaderboard_text(
        "🏆 <b>Tarix viktorinasi reytingi</b> (TOP 10)", LEADERBOARD_TARIX_KEY
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

    await message.answer(tarix_text)
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
    await message.answer("Endi o'qish matnini (passage) to'liq yuboring:")


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
    await message.answer("Endi maqola/qoidalar matnini to'liq yuboring:")


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
    await message.answer("Endi audio faylni yuboring (voice yoki audio fayl sifatida):")


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
            "Foydalanish: <code>/delete_content &lt;reading|listening|grammar&gt; &lt;id&gt;</code>\n"
            "Masalan: <code>/delete_content reading 3</code>"
        )
        return

    content_type, item_id_str = args[0], args[1]
    deleted = await delete_content_item(content_type, int(item_id_str))
    if deleted:
        await message.answer(f"🗑 {content_type} #{item_id_str} o'chirildi.")
    else:
        await message.answer(f"⚠️ {content_type} #{item_id_str} topilmadi.")



async def cmd_tarix(message: Message):
    f"""Ketma-ket {HISTORY_QUIZ_LENGTH} ta tasodifiy tarix savolidan iborat viktorina
    sessiyasini boshlaydi. Bu funksiya IELTS imtihon holatidan (FSM state) mustaqil
    ishlaydi — imtihon davomida ham chaqirish mumkin, chunki alohida
    (active_history_quizzes) xotirada saqlanadi va exam FSM ma'lumotlariga tegmaydi."""
    prep_msg = await message.answer(f"⏳ {HISTORY_QUIZ_LENGTH} ta tarix savoli tayyorlanmoqda...")

    try:
        questions = await asyncio.to_thread(_generate_history_quiz_set_sync, HISTORY_QUIZ_LENGTH)
        if not questions:
            raise ValueError("Yaroqli savollar generatsiya qilinmadi")
    except Exception as e:
        logger.exception("Tarix viktorinasini generatsiya qilishda xatolik: %s", e)
        await prep_msg.edit_text("❌ Savollarni tayyorlashda xatolik yuz berdi. Iltimos /tarix bilan qayta urinib ko'ring.")
        return

    active_history_quizzes[message.from_user.id] = {
        "questions": questions,
        "idx": 0,
        "score": 0,
        "mode": "quiz",
    }

    text, keyboard = _build_history_question_view(active_history_quizzes[message.from_user.id], 0)
    await prep_msg.edit_text(text, reply_markup=keyboard)


@dp.message(Command("sertifikat"))
async def cmd_sertifikat(message: Message):
    f"""Ketma-ket {CERTIFICATE_QUIZ_LENGTH} ta tarix savolidan iborat "sertifikat testi"ni
    boshlaydi. Agar natija {CERTIFICATE_PASS_PERCENT}% dan yuqori bo'lsa, test yakunida
    foydalanuvchiga rasmli (PNG) sertifikat yuboriladi."""
    prep_msg = await message.answer(
        f"⏳ Sertifikat testi uchun {CERTIFICATE_QUIZ_LENGTH} ta savol tayyorlanmoqda... "
        "(bu biroz vaqt olishi mumkin, iltimos kuting)"
    )

    try:
        questions = await asyncio.to_thread(_generate_history_quiz_set_sync, CERTIFICATE_QUIZ_LENGTH)
        if len(questions) < CERTIFICATE_QUIZ_LENGTH // 2:
            # Agar savollarning yarmidan ko'pi generatsiya qilinmagan bo'lsa,
            # test sifatsiz bo'lib qolmasligi uchun xatolik deb hisoblaymiz.
            raise ValueError(f"Yetarli savol generatsiya qilinmadi: {len(questions)}")
    except Exception as e:
        logger.exception("Sertifikat testini generatsiya qilishda xatolik: %s", e)
        await prep_msg.edit_text("❌ Savollarni tayyorlashda xatolik yuz berdi. Iltimos /sertifikat bilan qayta urinib ko'ring.")
        return

    active_history_quizzes[message.from_user.id] = {
        "questions": questions,
        "idx": 0,
        "score": 0,
        "mode": "certificate",
    }

    intro = (
        f"🎓 <b>Sertifikat testi boshlandi!</b>\n\n"
        f"Jami {len(questions)} ta savol. Agar natijangiz "
        f"<b>{CERTIFICATE_PASS_PERCENT}%</b> yoki undan yuqori bo'lsa, "
        "test yakunida shaxsiy sertifikat rasmini olasiz.\n\n"
        "Omad!"
    )
    await message.answer(intro)

    text, keyboard = _build_history_question_view(active_history_quizzes[message.from_user.id], 0)
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

    active_history_quizzes[message.from_user.id] = {
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
    await message.answer(f"Endi quyidagi {len(questions)} ta savolga javob bering:")

    text, keyboard = _build_history_question_view(active_history_quizzes[message.from_user.id], 0)
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

    active_history_quizzes[message.from_user.id] = {
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

    await message.answer(f"Endi quyidagi {len(questions)} ta savolga javob bering:")

    text, keyboard = _build_history_question_view(active_history_quizzes[message.from_user.id], 0)
    await message.answer(text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("histans:"))
async def handle_history_answer(callback: CallbackQuery):
    user_id = callback.from_user.id
    quiz = active_history_quizzes.get(user_id)

    if not quiz:
        await callback.answer("⏱ Bu sessiya tugagan yoki muddati o'tgan. Yangi test uchun /tarix yuboring.", show_alert=True)
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
    mode_emoji = "🎓" if mode == "certificate" else "📜"

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
        next_text, next_keyboard = _build_history_question_view(quiz, quiz["idx"])
        await callback.message.answer(next_text, reply_markup=next_keyboard)
        return

    # --- Test yakunlandi ---
    score = quiz["score"]
    percentage = round((score / total) * 100)
    active_history_quizzes.pop(user_id, None)

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

    # Oddiy /tarix viktorinasi yakuni
    await update_leaderboard(LEADERBOARD_TARIX_KEY, user_id, percentage)

    if percentage >= 80:
        verdict = "🏆 Ajoyib natija! Tarixni juda yaxshi bilasiz."
    elif percentage >= 50:
        verdict = "👍 Yomon emas! Yana mashq qilsangiz yanada yaxshi bo'ladi."
    else:
        verdict = "📚 Tarixni ko'proq o'qishga arziydi, lekin harakat qildingiz!"

    summary = (
        f"🏁 <b>Viktorina yakunlandi!</b>\n\n"
        f"🎯 Natijangiz: <b>{score}/{total}</b> ({percentage}%)\n\n"
        f"{verdict}\n\n"
        "🔁 Yana boshlash uchun /tarix, sertifikat testi uchun /sertifikat, "
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
        "✅ <b>Test tayyor!</b>\n\n<b>PART 1</b> — Introduction & Interview\n"
        "Har bir savolga ovozli xabar bilan javob bering."
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
        await message.answer("✅ <b>Part 1 tugadi!</b>\n\n<b>PART 2</b> — Cue Card\nO'zingizni tayyorlang va so'ng 1-2 daqiqa gapirib, ovozli xabar sifatida yuboring.")
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

    await message.answer("✅ <b>Part 2 tugadi!</b>\n\n<b>PART 3</b> — Discussion")
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
<div id="content"><div class="loading">Yuklanmoqda…</div></div>

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
    contentEl.innerHTML = '<div class="loading">Yuklanmoqda…</div>';
    if (tab === "profile") return renderProfile();
    if (tab === "reading") return renderList("reading");
    if (tab === "listening") return renderList("listening");
    if (tab === "grammar") return renderList("grammar");
  }

  async function renderProfile() {
    try {
      const data = await fetchJSON("/api/dashboard/profile?uid=" + encodeURIComponent(uid));
      let html = '<div class="card"><h3>' + escapeHtml(data.name || "Foydalanuvchi") + '</h3>';
      html += '<div class="stat-row"><span>🏆 Tarix viktorinasi (eng yaxshi)</span><span>' + (data.tarix_best !== null ? data.tarix_best + '%' : '—') + '</span></div>';
      html += '<div class="stat-row"><span>🎓 Sertifikat testi (eng yaxshi)</span><span>' + (data.cert_best !== null ? data.cert_best + '%' : '—') + '</span></div>';
      html += '<div class="stat-row"><span>📖 Reading testlari yechilgan</span><span>' + data.reading_done + '</span></div>';
      html += '<div class="stat-row"><span>🎧 Listening testlari yechilgan</span><span>' + data.listening_done + '</span></div>';
      if (data.tarix_rank) html += '<div class="stat-row"><span>Tarix reytingidagi o\'rni</span><span>#' + data.tarix_rank + '</span></div>';
      if (data.cert_rank) html += '<div class="stat-row"><span>Sertifikat reytingidagi o\'rni</span><span>#' + data.cert_rank + '</span></div>';
      html += '</div>';
      contentEl.innerHTML = html;
    } catch (e) {
      contentEl.innerHTML = '<div class="loading">Xatolik yuz berdi.</div>';
    }
  }

  async function renderList(type) {
    try {
      const items = await fetchJSON("/api/dashboard/content/" + type);
      if (!items.length) {
        contentEl.innerHTML = '<div class="loading">Hozircha kontent yo\'q.</div>';
        return;
      }
      let html = "";
      items.forEach((item) => {
        html += '<div class="card" data-id="' + item.id + '">';
        html += '<h3>' + escapeHtml(item.title) + '</h3>';
        if (item.has_questions) html += '<span class="pill">📝 ' + item.question_count + ' ta savol</span>';
        else html += '<span class="pill">O\'qish/tinglash uchun</span>';
        html += '<br><button class="primary" onclick="window.__openItem(\'' + type + '\',' + item.id + ')">Ochish</button>';
        html += '</div>';
      });
      contentEl.innerHTML = html;
    } catch (e) {
      contentEl.innerHTML = '<div class="loading">Xatolik yuz berdi.</div>';
    }
  }

  window.__openItem = async function (type, id) {
    contentEl.innerHTML = '<div class="loading">Yuklanmoqda…</div>';
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
      quizArea.innerHTML = '<div class="loading">Baholanmoqda…</div>';
      const result = await fetchJSON("/api/dashboard/content/" + type + "/" + id + "/submit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ uid: uid, answers: answers }),
      });
      let html = '<div class="card"><h3>🏁 Natija: ' + result.score + '/' + result.total + ' (' + result.percentage + '%)</h3>';
      html += '<button class="secondary" onclick="window.__back(\'' + type + '\')">Ro\'yxatga qaytish</button>';
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

    tarix_best = None
    cert_best = None
    tarix_rank = None
    cert_rank = None

    if redis_client is not None:
        try:
            score = await redis_client.zscore(LEADERBOARD_TARIX_KEY, uid)
            if score is not None:
                tarix_best = int(float(score))
                entries = await get_leaderboard_entries(LEADERBOARD_TARIX_KEY, top_n=10_000)
                for i, (member, _) in enumerate(entries):
                    if member == uid:
                        tarix_rank = i + 1
                        break
        except Exception as e:
            logger.warning("Profil (tarix) o'qishda xatolik: %s", e)

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
        "tarix_best": tarix_best,
        "cert_best": cert_best,
        "tarix_rank": tarix_rank,
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


async def on_shutdown(app: web.Application):
    # MUHIM: bu yerda bot.delete_webhook() CHAQIRILMAYDI!
    # Render rolling-deploy paytida eski va yangi instance bir necha soniya
    # bir vaqtda ishlab turishi mumkin. Agar shu yerda webhook o'chirilsa,
    # eski (o'chayotgan) instance yangi instance endigina o'rnatgan
    # webhookni o'chirib yuboradi va bot butunlay javob bermay qoladi.
    # Shuning uchun shutdown paytida faqat bot sessiyasini yopamiz, xolos.
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
        BotCommand(command="start", description="Botni boshlash va yo'riqnoma"),
        BotCommand(command="test", description="Yangi IELTS Speaking mock testini boshlash"),
        BotCommand(command="tarix", description="10 ta ketma-ket tarix savoli viktorinasi"),
        BotCommand(command="sertifikat", description="25 ta savolli sertifikat testi"),
        BotCommand(command="reading_test", description="AI generatsiya qilgan IELTS Reading testi"),
        BotCommand(command="listening_test", description="AI generatsiya qilgan IELTS Listening testi"),
        BotCommand(command="stats", description="Bot statistikasini ko'rish"),
        BotCommand(command="reyting", description="TOP 10 reyting jadvalini ko'rish"),
        BotCommand(command="english", description="Ingliz tili mashq rejimi (xatolarni tuzatadi)"),
        BotCommand(command="dashboard", description="Dashboard: Profil, Reading, Listening, Grammar"),
        BotCommand(command="stop", description="Joriy testni to'xtatish / suhbatni tozalash"),
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
