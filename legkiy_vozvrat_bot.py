import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.dispatcher.middlewares.base import BaseMiddleware


# ==========================
# Настройки
# ==========================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "legkiy_vozvrat.db")
DEFAULT_SUB_PRICE = 149  # Telegram Stars
DEFAULT_SUB_DAYS = 30
DEFAULT_FREE_CLAIMS = 3

# Укажи тут свою реальную @username, если хочешь жёстко зашить ссылку в постах.
# Если оставить пустым, бот подставит username, полученный из Bot API.
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("legkiy_vozvrat")


def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def ts_to_str(ts: int) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().strftime("%d.%m.%Y %H:%M")


def normalize_money(text: str) -> Optional[int]:
    """Пытаемся вытащить сумму в рублях из строки."""
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    try:
        value = int(digits)
        return value if value > 0 else None
    except ValueError:
        return None


def short_text(text: str, limit: int = 400) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def split_long_message(text: str, limit: int = 3900) -> List[str]:
    """Telegram ограничивает длину сообщения. Делим по абзацам, чтобы не падать."""
    if len(text) <= limit:
        return [text]

    parts: List[str] = []
    current = ""
    for paragraph in text.split("\n"):
        chunk = paragraph + "\n"
        if len(current) + len(chunk) > limit:
            if current.strip():
                parts.append(current.rstrip())
            current = chunk
        else:
            current += chunk

    if current.strip():
        parts.append(current.rstrip())

    # На случай совсем длинных абзацев режем их дополнительно
    safe_parts: List[str] = []
    for part in parts:
        if len(part) <= limit:
            safe_parts.append(part)
            continue
        start = 0
        while start < len(part):
            safe_parts.append(part[start : start + limit])
            start += limit
    return safe_parts


# ==========================
# Каталог кейсов
# ==========================

@dataclass(frozen=True)
class CaseItem:
    id: str
    title: str
    demand_key: str
    note: str
    group: str


CATEGORY_ORDER = [
    ("marketplaces", "🛒 Маркетплейсы", "Wildberries / Ozon / Я.Маркет / AliExpress"),
    ("banks", "🏦 Банки и кредиты", "Комиссии, страховки, списания, блокировки"),
    ("insurance", "🛡 Страховки", "ОСАГО, КАСКО, жизнь, путешествия"),
    ("stores", "🏪 Магазины и техника", "Офлайн-магазины, электроника, мебель"),
    ("travel", "✈️ Авиабилеты и туризм", "Билеты, багаж, отели, туры"),
    ("services", "📱 Услуги и цифровые", "Связь, подписки, доставка, онлайн-сервисы"),
]

CASES: List[CaseItem] = [
    # Маркетплейсы
    CaseItem("wb_defect", "Wildberries — брак / дефект", "refund", "товар с недостатком", "marketplaces"),
    CaseItem("wb_wrong", "Wildberries — пришёл не тот товар", "refund", "получен иной товар", "marketplaces"),
    CaseItem("wb_size", "Wildberries — не подошёл размер", "refund", "возврат товара надлежащего качества", "marketplaces"),
    CaseItem("wb_delivery", "Wildberries — товар не доставлен", "refund", "заказ не передан покупателю", "marketplaces"),
    CaseItem("wb_refuse", "Wildberries — отказ в возврате", "refund", "необоснованный отказ в возврате", "marketplaces"),
    CaseItem("wb_delay", "Wildberries — просрочка возврата денег", "refund", "деньги не возвращены вовремя", "marketplaces"),
    CaseItem("wb_set", "Wildberries — неполная комплектация", "refund", "не хватает частей / аксессуаров", "marketplaces"),
    CaseItem("wb_exchange", "Wildberries — прошу замену", "exchange", "замена товара ненадлежащего качества", "marketplaces"),

    CaseItem("oz_defect", "Ozon — брак / дефект", "refund", "товар с недостатком", "marketplaces"),
    CaseItem("oz_wrong", "Ozon — пришёл не тот товар", "refund", "получен иной товар", "marketplaces"),
    CaseItem("oz_size", "Ozon — не подошёл размер", "refund", "возврат товара надлежащего качества", "marketplaces"),
    CaseItem("oz_delivery", "Ozon — товар не доставлен", "refund", "заказ не передан покупателю", "marketplaces"),
    CaseItem("oz_refuse", "Ozon — отказ в возврате", "refund", "необоснованный отказ в возврате", "marketplaces"),
    CaseItem("oz_delay", "Ozon — просрочка возврата денег", "refund", "деньги не возвращены вовремя", "marketplaces"),
    CaseItem("oz_set", "Ozon — неполная комплектация", "refund", "не хватает частей / аксессуаров", "marketplaces"),
    CaseItem("oz_exchange", "Ozon — прошу замену", "exchange", "замена товара ненадлежащего качества", "marketplaces"),

    CaseItem("ym_defect", "Яндекс.Маркет — брак / дефект", "refund", "товар с недостатком", "marketplaces"),
    CaseItem("ym_wrong", "Яндекс.Маркет — пришёл не тот товар", "refund", "получен иной товар", "marketplaces"),
    CaseItem("ym_delay", "Яндекс.Маркет — задержка доставки", "refund", "срок доставки нарушен", "marketplaces"),
    CaseItem("ym_refuse", "Яндекс.Маркет — отказ в возврате", "refund", "необоснованный отказ в возврате", "marketplaces"),
    CaseItem("ym_set", "Яндекс.Маркет — неполная комплектация", "refund", "не хватает частей / аксессуаров", "marketplaces"),
    CaseItem("ym_exchange", "Яндекс.Маркет — прошу замену", "exchange", "замена товара ненадлежащего качества", "marketplaces"),

    CaseItem("ali_defect", "AliExpress — брак / дефект", "refund", "товар с недостатком", "marketplaces"),
    CaseItem("ali_wrong", "AliExpress — пришёл не тот товар", "refund", "получен иной товар", "marketplaces"),
    CaseItem("ali_no_delivery", "AliExpress — заказ не пришёл", "refund", "товар не доставлен", "marketplaces"),
    CaseItem("ali_delay", "AliExpress — сильная задержка", "refund", "срок доставки нарушен", "marketplaces"),
    CaseItem("ali_refuse", "AliExpress — отказ в споре/возврате", "refund", "необоснованный отказ в споре", "marketplaces"),
    CaseItem("ali_exchange", "AliExpress — прошу замену", "exchange", "замена товара ненадлежащего качества", "marketplaces"),
    CaseItem("ali_partial", "AliExpress — не хватает части заказа", "refund", "неполная комплектация", "marketplaces"),

    # Банки
    CaseItem("bank_fee", "Банк — списали комиссию незаконно", "refund", "навязанная / спорная комиссия", "banks"),
    CaseItem("bank_insurance", "Банк — навязали страховку", "refund", "дополнительная услуга без согласия", "banks"),
    CaseItem("bank_credit", "Банк — неверный перерасчёт кредита", "recalculate", "ошибка в начислениях / процентах", "banks"),
    CaseItem("bank_block", "Банк — заблокировал карту / счёт", "restore", "необоснованная блокировка", "banks"),
    CaseItem("bank_cashback", "Банк — не начислил кэшбэк", "recalculate", "неисполнение бонусной программы", "banks"),
    CaseItem("bank_transfer", "Банк — удержал перевод / комиссию", "refund", "спорное удержание при переводе", "banks"),
    CaseItem("bank_chargeback", "Банк — не вернул спорную операцию", "refund", "оспариваемое списание", "banks"),
    CaseItem("bank_service", "Банк — навязанный платный сервис", "refund", "подключён платный пакет без согласия", "banks"),

    # Страховки
    CaseItem("ins_osago", "Страховка — ОСАГО / КБМ", "recalculate", "неверный коэффициент / расчёт", "insurance"),
    CaseItem("ins_casco", "Страховка — КАСКО / отказ", "refund", "страховщик нарушил условия договора", "insurance"),
    CaseItem("ins_life", "Страховка — навязанная жизнь", "refund", "добровольная страховка при кредите", "insurance"),
    CaseItem("ins_travel", "Страховка — туристическая / выездная", "refund", "отказ в выплате / возврате", "insurance"),
    CaseItem("ins_delay", "Страховка — задержка выплаты", "refund", "страховая затягивает выплату", "insurance"),
    CaseItem("ins_cancel", "Страховка — отказ от полиса", "refund", "досрочное расторжение договора", "insurance"),

    # Магазины и техника
    CaseItem("store_defect", "Магазин — брак / дефект товара", "refund", "товар ненадлежащего качества", "stores"),
    CaseItem("store_wrong", "Магазин — не тот товар", "refund", "выдан другой товар", "stores"),
    CaseItem("store_size", "Магазин — не подошёл размер", "refund", "возврат товара надлежащего качества", "stores"),
    CaseItem("store_delay", "Магазин — просрочка доставки", "refund", "срок передачи товара нарушен", "stores"),
    CaseItem("store_refuse", "Магазин — отказ принять возврат", "refund", "необоснованный отказ", "stores"),
    CaseItem("store_warranty", "Магазин — гарантийный ремонт", "repair", "ремонт по гарантии", "stores"),
    CaseItem("store_set", "Магазин — неполная комплектация", "refund", "не хватает деталей / аксессуаров", "stores"),
    CaseItem("store_exchange", "Магазин — замена товара", "exchange", "замена на аналогичный товар", "stores"),

    # Авиабилеты и туризм
    CaseItem("flight_cancel", "Авиабилет — отмена рейса", "refund", "рейс отменён перевозчиком", "travel"),
    CaseItem("flight_delay", "Авиабилет — значительная задержка", "refund", "существенная задержка рейса", "travel"),
    CaseItem("flight_refund", "Авиабилет — возврат денег за билет", "refund", "добровольный / вынужденный возврат", "travel"),
    CaseItem("flight_baggage", "Авиабилет — потеря багажа", "refund", "багаж не доставлен", "travel"),
    CaseItem("tour_hotel", "Отель — не соответствует описанию", "refund", "услуга оказана ненадлежащим образом", "travel"),
    CaseItem("tour_cancel", "Тур — отмена поездки", "refund", "отказ / перенос тура", "travel"),
    CaseItem("tour_fine", "Тур — удержали штраф неправомерно", "refund", "необоснованное удержание", "travel"),

    # Услуги и цифровые товары
    CaseItem("svc_mobile", "Связь — списали лишнее", "refund", "некорректное списание", "services"),
    CaseItem("svc_subscription", "Подписка — автосписание без согласия", "refund", "нежелательная подписка", "services"),
    CaseItem("svc_delivery", "Доставка — услуга оказана плохо", "refund", "нарушен срок / качество доставки", "services"),
    CaseItem("svc_course", "Онлайн-курс — услуга не оказана", "refund", "курс не предоставлен / доступ закрыт", "services"),
    CaseItem("svc_app", "Приложение — платная функция не работает", "refund", "цифровая услуга ненадлежащего качества", "services"),
    CaseItem("svc_saas", "SaaS — неработающий платный доступ", "refund", "доступ к сервису не предоставлен", "services"),
    CaseItem("svc_other", "Другая услуга / спорное списание", "refund", "услуга оказана ненадлежащим образом", "services"),
]

CASES_BY_ID: Dict[str, CaseItem] = {case.id: case for case in CASES}

CATEGORY_TO_CASES: Dict[str, List[CaseItem]] = {}
for case in CASES:
    CATEGORY_TO_CASES.setdefault(case.group, []).append(case)


# ==========================
# FSM
# ==========================

class ClaimForm(StatesGroup):
    category = State()
    case = State()
    seller = State()
    doc_number = State()
    purchase_date = State()
    amount = State()
    problem = State()
    demand = State()
    full_name = State()
    contact = State()
    city = State()
    extra = State()
    preview = State()


# ==========================
# База данных
# ==========================

class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None
        self.lock = asyncio.Lock()

    async def connect(self):
        if self.conn is not None:
            return
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA synchronous=NORMAL;")
        await self.conn.execute("PRAGMA foreign_keys=ON;")
        await self.conn.execute("PRAGMA busy_timeout=5000;")
        await self.init_schema()

    async def init_schema(self):
        assert self.conn is not None
        async with self.lock:
            await self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    referrer_id INTEGER,
                    free_claims_left INTEGER NOT NULL DEFAULT 3,
                    total_claims INTEGER NOT NULL DEFAULT 0,
                    subscription_until INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    inviter_id INTEGER NOT NULL,
                    referred_id INTEGER NOT NULL UNIQUE,
                    created_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    case_id TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    claim_text TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                );

                CREATE INDEX IF NOT EXISTS idx_history_user_id ON history(user_id);
                CREATE INDEX IF NOT EXISTS idx_referrals_inviter_id ON referrals(inviter_id);
                """
            )
            await self.conn.commit()

    async def execute(self, query: str, params: tuple = ()):
        assert self.conn is not None
        async with self.lock:
            cur = await self.conn.execute(query, params)
            await self.conn.commit()
            return cur

    async def fetchone(self, query: str, params: tuple = ()):
        assert self.conn is not None
        async with self.lock:
            cur = await self.conn.execute(query, params)
            row = await cur.fetchone()
            return row

    async def fetchall(self, query: str, params: tuple = ()):
        assert self.conn is not None
        async with self.lock:
            cur = await self.conn.execute(query, params)
            rows = await cur.fetchall()
            return rows

    async def get_user(self, user_id: int) -> Optional[aiosqlite.Row]:
        return await self.fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))

    async def create_user_if_missing(self, user_id: int, username: str, full_name: str):
        existing = await self.get_user(user_id)
        ts = now_ts()
        if existing is None:
            await self.execute(
                """
                INSERT INTO users (user_id, username, full_name, referrer_id, free_claims_left, total_claims, subscription_until, created_at, updated_at)
                VALUES (?, ?, ?, NULL, ?, 0, 0, ?, ?)
                """,
                (user_id, username, full_name, DEFAULT_FREE_CLAIMS, ts, ts),
            )
        else:
            await self.execute(
                """
                UPDATE users
                SET username = ?, full_name = ?, updated_at = ?
                WHERE user_id = ?
                """,
                (username, full_name, ts, user_id),
            )

    async def register_referral(self, inviter_id: int, referred_id: int):
        if inviter_id == referred_id:
            return False

        existing = await self.fetchone("SELECT 1 FROM referrals WHERE referred_id = ?", (referred_id,))
        if existing is not None:
            return False

        await self.execute(
            "INSERT INTO referrals (inviter_id, referred_id, created_at) VALUES (?, ?, ?)",
            (inviter_id, referred_id, now_ts()),
        )
        await self.execute(
            "UPDATE users SET free_claims_left = free_claims_left + 1, updated_at = ? WHERE user_id = ?",
            (now_ts(), inviter_id),
        )
        return True

    async def consume_claim(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        if user is None:
            return False

        ts = now_ts()
        has_subscription = int(user["subscription_until"] or 0) > ts
        if has_subscription:
            await self.execute(
                "UPDATE users SET total_claims = total_claims + 1, updated_at = ? WHERE user_id = ?",
                (ts, user_id),
            )
            return True

        free_left = int(user["free_claims_left"] or 0)
        if free_left <= 0:
            return False

        await self.execute(
            """
            UPDATE users
            SET free_claims_left = free_claims_left - 1,
                total_claims = total_claims + 1,
                updated_at = ?
            WHERE user_id = ?
            """,
            (ts, user_id),
        )
        return True

    async def add_subscription_days(self, user_id: int, days: int):
        user = await self.get_user(user_id)
        if user is None:
            return
        ts = now_ts()
        current_until = int(user["subscription_until"] or 0)
        base = max(current_until, ts)
        new_until = base + days * 24 * 60 * 60
        await self.execute(
            "UPDATE users SET subscription_until = ?, updated_at = ? WHERE user_id = ?",
            (new_until, ts, user_id),
        )

    async def log_history(self, user_id: int, case_id: str, request_data: dict, claim_text: str):
        await self.execute(
            """
            INSERT INTO history (user_id, case_id, request_json, claim_text, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, case_id, json.dumps(request_data, ensure_ascii=False), claim_text, now_ts()),
        )

    async def get_referral_stats(self, user_id: int):
        referrals = await self.fetchone("SELECT COUNT(*) AS cnt FROM referrals WHERE inviter_id = ?", (user_id,))
        user = await self.get_user(user_id)
        return int(referrals["cnt"] or 0), user

    async def get_recent_history(self, user_id: int, limit: int = 5):
        return await self.fetchall(
            "SELECT * FROM history WHERE user_id = ? ORDER BY id DESC LIMIT ?",
            (user_id, limit),
        )


DB = Database(DB_PATH)


# ==========================
# Middleware ошибок
# ==========================

class ErrorCatchMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            return await handler(event, data)
        except Exception:
            logger.exception("Unhandled error")
            bot: Bot = data["bot"]
            if isinstance(event, Message):
                try:
                    await event.answer(
                        "⚠️ Что-то пошло не так. Попробуйте ещё раз или нажмите /menu."
                    )
                except Exception:
                    pass
            elif isinstance(event, CallbackQuery):
                try:
                    await event.answer("Ошибка. Нажмите /menu и повторите ещё раз.", show_alert=True)
                except Exception:
                    pass
            return None


# ==========================
# Роутер и утилиты интерфейса
# ==========================

router = Router()
router.message.middleware(ErrorCatchMiddleware())
router.callback_query.middleware(ErrorCatchMiddleware())


async def get_bot_username(bot: Bot) -> str:
    global BOT_USERNAME
    if BOT_USERNAME:
        return BOT_USERNAME
    me = await bot.get_me()
    BOT_USERNAME = me.username
    return BOT_USERNAME


def main_menu_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🧾 Создать претензию", callback_data="menu:create")
    kb.button(text="👥 Рефералы", callback_data="menu:refs")
    kb.button(text="⭐ Подписка", callback_data="menu:sub")
    kb.button(text="📚 История", callback_data="menu:history")
    kb.button(text="❓ Помощь", callback_data="menu:help")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def categories_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for cat_id, title, _ in CATEGORY_ORDER:
        kb.button(text=title, callback_data=f"cat:{cat_id}")
    kb.button(text="⬅️ Назад", callback_data="menu:home")
    kb.adjust(1)
    return kb.as_markup()


def cases_kb(category_id: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for case in CATEGORY_TO_CASES.get(category_id, []):
        kb.button(text=case.title, callback_data=f"case:{case.id}")
    kb.button(text="⬅️ К категориям", callback_data="menu:create")
    kb.adjust(1)
    return kb.as_markup()


def demand_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="💸 Вернуть деньги", callback_data="demand:refund")
    kb.button(text="🔁 Заменить товар", callback_data="demand:exchange")
    kb.button(text="🛠 Устранить недостатки", callback_data="demand:repair")
    kb.button(text="📉 Уменьшить цену", callback_data="demand:recalculate")
    kb.button(text="✅ Исполнить услугу / открыть доступ", callback_data="demand:restore")
    kb.button(text="✍️ Вписать своё", callback_data="demand:other")
    kb.adjust(2, 2, 2)
    return kb.as_markup()


def share_kb(bot_username: str, amount: Optional[int], user_id: int) -> InlineKeyboardMarkup:
    claim_amount = amount if amount else "Х"
    text = f"Я вернул {claim_amount} рублей за 3 минуты через @{bot_username}! Попробуй сам по ссылке: https://t.me/{bot_username}"
    share_url = "https://t.me/share/url?url={url}&text={text}".format(
        url=quote(f"https://t.me/{bot_username}?start=ref_{user_id}"),
        text=quote(text),
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="🔥 Поделиться успехом", url=share_url)
    kb.button(text="🧾 Новая претензия", callback_data="menu:create")
    kb.adjust(1)
    return kb.as_markup()


def back_home_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    return kb.as_markup()


def cancel_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="❌ Отменить", callback_data="form:cancel")
    return kb.as_markup()


def format_balance(user_row) -> str:
    free_left = int(user_row["free_claims_left"] or 0)
    sub_until = int(user_row["subscription_until"] or 0)
    ts = now_ts()
    if sub_until > ts:
        return f"⭐ Подписка активна до <b>{ts_to_str(sub_until)}</b>"
    return f"🧾 Бесплатных претензий осталось: <b>{free_left}</b>"


async def send_long_text(message_or_bot, chat_id: Optional[int], text: str):
    """Отправка длинного текста частями."""
    parts = split_long_message(text)
    if isinstance(message_or_bot, Message):
        for i, part in enumerate(parts):
            await message_or_bot.answer(part)
    else:
        assert chat_id is not None
        for part in parts:
            await message_or_bot.send_message(chat_id, part)


# ==========================
# Генератор претензии
# ==========================

DEMAND_TEXTS = {
    "refund": "вернуть уплаченную мной сумму в размере {amount} руб.",
    "exchange": "произвести замену товара на аналогичный товар надлежащего качества",
    "repair": "безвозмездно устранить выявленные недостатки в разумный срок",
    "recalculate": "произвести перерасчёт и вернуть излишне удержанные средства",
    "restore": "надлежащим образом исполнить обязательство и предоставить услугу / доступ",
    "other": "удовлетворить моё требование, указанное в настоящей претензии",
}

DUE_DATES = {
    "refund": "10 дней",
    "exchange": "7 дней",
    "repair": "45 дней",
    "recalculate": "10 дней",
    "restore": "10 дней",
    "other": "срок, установленный законом",
}


def build_claim(data: Dict[str, Any], case: CaseItem, user_row) -> str:
    amount = data.get("amount") or 0
    seller = escape(str(data.get("seller") or "Продавец / исполнитель"))
    doc_number = escape(str(data.get("doc_number") or "—"))
    purchase_date = escape(str(data.get("purchase_date") or "—"))
    problem = escape(str(data.get("problem") or case.note))
    full_name = escape(str(data.get("full_name") or user_row["full_name"]))
    contact = escape(str(data.get("contact") or "—"))
    city = escape(str(data.get("city") or "—"))
    extra = escape(str(data.get("extra") or "—"))
    demand_key = data.get("demand_key") or case.demand_key

    if demand_key == "other":
        demand_text = escape(str(data.get("custom_demand") or DEMAND_TEXTS["other"]))
    else:
        demand_text = DEMAND_TEXTS.get(demand_key, DEMAND_TEXTS["other"]).format(amount=amount or "_____" )
    due_text = DUE_DATES.get(demand_key, "срок, установленный законом")

    # Блок с правовыми ссылками адаптирован под изменения 2026 года.
    law_block = (
        "Закон РФ «О защите прав потребителей», включая действующую редакцию после изменений, "
        "внесённых Федеральным законом № 500-ФЗ от 28.12.2025, в том числе ст. 13 и ст. 23."
    )

    claim = f"""ПРЕТЕНЗИЯ
о защите прав потребителя

Кому: {seller}
От: {full_name}
Город: {city}
Контакты: {contact}

Мною {purchase_date} был оформлен заказ / заключён договор по поводу:
<b>{case.title}</b>.
Номер заказа / договора / полиса / билета: {doc_number}
Стоимость: {amount} руб.

Суть проблемы:
{problem}

Дополнительно:
{extra}

В связи с изложенным, руководствуясь {law_block}, прошу:
1. {demand_text}.
2. Устранить нарушение и письменно сообщить мне о результатах рассмотрения претензии.
3. Если требование связано с возвратом денежных средств, прошу произвести возврат в течение {due_text}.

Также уведомляю, что при нарушении срока исполнения требований буду вынужден(а) обратиться в суд и требовать:
- неустойку по действующей редакции ст. 23 Закона РФ «О защите прав потребителей»;
- штраф в размере 50% от суммы, присужденной судом в пользу потребителя, с учётом актуальных исключений, установленных законом;
- компенсацию морального вреда, убытков и судебных расходов.

Дата: {datetime.now().strftime('%d.%m.%Y')}
Подпись: {full_name}
"""
    return claim.strip()


def build_opening_text(user_row, bot_username: str) -> str:
    free_left = int(user_row["free_claims_left"] or 0)
    sub_until = int(user_row["subscription_until"] or 0)
    lines = [
        "👋 <b>ЛегкийВозврат Эксперт</b> — бот для быстрого создания официальной претензии по ЗоЗПП.",
        "",
        "🧠 Я задаю вопросы по шагам и собираю текст без внешних API и без OpenAI.",
        "",
        f"{format_balance(user_row)}",
    ]
    if sub_until > now_ts():
        lines.append(f"📅 Подписка действует до <b>{ts_to_str(sub_until)}</b>")
    lines.append("")
    lines.append("Нажми кнопку ниже и выбери тип ситуации.")
    lines.append(f"\n🔗 Ваша реф-ссылка: <code>https://t.me/{bot_username}?start=ref_{user_row['user_id']}</code>")
    return "\n".join(lines)


async def ensure_paying_access(bot: Bot, message: Message) -> bool:
    user = await DB.get_user(message.from_user.id)
    if user is None:
        await DB.create_user_if_missing(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
        user = await DB.get_user(message.from_user.id)

    has_subscription = int(user["subscription_until"] or 0) > now_ts()
    if has_subscription:
        return True

    if int(user["free_claims_left"] or 0) > 0:
        return True

    bot_username = await get_bot_username(bot)
    pay_text = (
        "🪙 Бесплатные претензии закончились.\n\n"
        f"Оформи <b>«Безлимит на 30 дней»</b> за <b>{DEFAULT_SUB_PRICE} Telegram Stars</b>.\n"
        "После оплаты бот откроет безлимитный режим автоматически."
    )
    link = await bot.create_invoice_link(
        title="ЛегкийВозврат Эксперт — 30 дней",
        description="Безлимитная генерация претензий на 30 дней",
        payload=f"sub30d:{message.from_user.id}",
        currency="XTR",
        prices=[LabeledPrice(label="30 дней", amount=DEFAULT_SUB_PRICE)],
        subscription_period=DEFAULT_SUB_DAYS * 24 * 60 * 60,
    )
    kb = InlineKeyboardBuilder()
    kb.button(text=f"⭐ Купить за {DEFAULT_SUB_PRICE} Stars", url=link)
    kb.button(text="👥 Пригласить друга и получить +1", callback_data="menu:refs")
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(1)
    await message.answer(pay_text, reply_markup=kb.as_markup())
    return False


# ==========================
# Команды
# ==========================

@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject, bot: Bot, state: FSMContext):
    await state.clear()
    await DB.create_user_if_missing(message.from_user.id, message.from_user.username or "", message.from_user.full_name)

    # Реферальная ссылка: /start ref_123456
    args = (command.args or "").strip() if command else ""
    if args.startswith("ref_"):
        inviter_str = args.removeprefix("ref_")
        if inviter_str.isdigit():
            inviter_id = int(inviter_str)
            if inviter_id != message.from_user.id:
                # Убедимся, что пригласивший есть в БД, чтобы +1 не потерялся.
                inviter_user = await DB.get_user(inviter_id)
                if inviter_user is None:
                    await DB.execute(
                        """
                        INSERT OR IGNORE INTO users (user_id, username, full_name, referrer_id, free_claims_left, total_claims, subscription_until, created_at, updated_at)
                        VALUES (?, '', '', NULL, ?, 0, 0, ?, ?)
                        """,
                        (inviter_id, DEFAULT_FREE_CLAIMS, now_ts(), now_ts()),
                    )
                added = await DB.register_referral(inviter_id, message.from_user.id)
                if added:
                    try:
                        await bot.send_message(
                            inviter_id,
                            f"🎉 Новый реферал! Вам начислена <b>+1 бесплатная претензия</b>."
                        )
                    except Exception:
                        pass

    user = await DB.get_user(message.from_user.id)
    bot_username = await get_bot_username(bot)
    await message.answer(
        build_opening_text(user, bot_username),
        reply_markup=main_menu_kb(),
    )


@router.message(Command("menu"))
async def cmd_menu(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    user = await DB.get_user(message.from_user.id)
    if user is None:
        await DB.create_user_if_missing(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
        user = await DB.get_user(message.from_user.id)
    bot_username = await get_bot_username(bot)
    await message.answer(build_opening_text(user, bot_username), reply_markup=main_menu_kb())


@router.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "❓ <b>Как пользоваться</b>\n\n"
        "1) Нажми <b>Создать претензию</b>.\n"
        "2) Выбери категорию и конкретный случай.\n"
        "3) Ответь на вопросы.\n"
        "4) Получи готовый текст претензии.\n\n"
        "⚠️ Это шаблонный генератор документов, а не замена юриста. Для сложных банковских и страховых споров полезно проверить текст вручную."
    )
    await message.answer(text, reply_markup=back_home_kb())


# ==========================
# Главное меню и навигация
# ==========================

@router.callback_query(F.data == "menu:home")
async def menu_home(callback: CallbackQuery, bot: Bot, state: FSMContext):
    await state.clear()
    await DB.create_user_if_missing(callback.from_user.id, callback.from_user.username or "", callback.from_user.full_name)
    user = await DB.get_user(callback.from_user.id)
    bot_username = await get_bot_username(bot)
    await callback.message.edit_text(build_opening_text(user, bot_username), reply_markup=main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:create")
async def menu_create(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(ClaimForm.category)
    text = "🧾 <b>Выберите категорию возврата / спора</b>\n\nЯ покажу частые ситуации и помогу собрать претензию по шагам."
    await callback.message.edit_text(text, reply_markup=categories_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:refs")
async def menu_refs(callback: CallbackQuery, bot: Bot):
    user = await DB.get_user(callback.from_user.id)
    if user is None:
        await DB.create_user_if_missing(callback.from_user.id, callback.from_user.username or "", callback.from_user.full_name)
        user = await DB.get_user(callback.from_user.id)
    ref_count, _ = await DB.get_referral_stats(callback.from_user.id)
    bot_username = await get_bot_username(bot)
    text = (
        "👥 <b>Реферальная система</b>\n\n"
        f"Вам начисляется <b>+1 бесплатная претензия</b> за каждого друга, который откроет бота по вашей ссылке и нажмёт /start.\n\n"
        f"Ваша ссылка:\n<code>https://t.me/{bot_username}?start=ref_{callback.from_user.id}</code>\n\n"
        f"Приглашено: <b>{ref_count}</b>"
    )
    await callback.message.edit_text(text, reply_markup=back_home_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:history")
async def menu_history(callback: CallbackQuery):
    rows = await DB.get_recent_history(callback.from_user.id, limit=5)
    if not rows:
        text = "📚 История пуста. После первой претензии она появится здесь."
    else:
        items = ["📚 <b>Последние претензии</b>\n"]
        for row in rows:
            case = CASES_BY_ID.get(row["case_id"])
            title = case.title if case else row["case_id"]
            items.append(f"• {escape(title)} — {ts_to_str(int(row['created_at']))}")
        text = "\n".join(items)
    await callback.message.edit_text(text, reply_markup=back_home_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:sub")
async def menu_sub(callback: CallbackQuery, bot: Bot):
    user = await DB.get_user(callback.from_user.id)
    if user is None:
        await DB.create_user_if_missing(callback.from_user.id, callback.from_user.username or "", callback.from_user.full_name)
        user = await DB.get_user(callback.from_user.id)

    bot_username = await get_bot_username(bot)
    link = await bot.create_invoice_link(
        title="ЛегкийВозврат Эксперт — 30 дней",
        description="Безлимитная генерация претензий на 30 дней",
        payload=f"sub30d:{callback.from_user.id}",
        currency="XTR",
        prices=[LabeledPrice(label="30 дней", amount=DEFAULT_SUB_PRICE)],
        subscription_period=DEFAULT_SUB_DAYS * 24 * 60 * 60,
    )
    text = (
        "⭐ <b>Подписка «Безлимит на 30 дней»</b>\n\n"
        f"Стоимость: <b>{DEFAULT_SUB_PRICE} Telegram Stars</b>\n"
        f"Срок: <b>{DEFAULT_SUB_DAYS} дней</b>\n\n"
        "После оплаты генератор претензий станет безлимитным на весь период.\n"
        f"Текущий бот: @{bot_username}"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text=f"⭐ Купить за {DEFAULT_SUB_PRICE} Stars", url=link)
    kb.button(text="🏠 Главное меню", callback_data="menu:home")
    kb.adjust(1)
    await callback.message.edit_text(text, reply_markup=kb.as_markup())
    await callback.answer()


# ==========================
# Выбор категории и случая
# ==========================

@router.callback_query(F.data.startswith("cat:"))
async def pick_category(callback: CallbackQuery, state: FSMContext):
    category_id = callback.data.split(":", 1)[1]
    category = next((c for c in CATEGORY_ORDER if c[0] == category_id), None)
    if category is None:
        await callback.answer("Категория не найдена", show_alert=True)
        return

    await state.update_data(category_id=category_id)
    await state.set_state(ClaimForm.case)
    title = category[1]
    text = f"Выбрано: <b>{title}</b>\n\nТеперь выберите конкретную ситуацию."
    await callback.message.edit_text(text, reply_markup=cases_kb(category_id))
    await callback.answer()


@router.callback_query(F.data.startswith("case:"))
async def pick_case(callback: CallbackQuery, state: FSMContext):
    case_id = callback.data.split(":", 1)[1]
    case = CASES_BY_ID.get(case_id)
    if case is None:
        await callback.answer("Случай не найден", show_alert=True)
        return

    await state.update_data(case_id=case_id)
    await state.set_state(ClaimForm.seller)
    await callback.message.edit_text(
        f"✅ <b>{case.title}</b>\n\nШаг 1/9: напишите <b>название продавца / банка / страховой / сервиса</b>.\n"
        f"Можно указать так, как оно написано в заказе, договоре или чеке.",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("demand:"))
async def pick_demand(callback: CallbackQuery, state: FSMContext):
    demand_key = callback.data.split(":", 1)[1]
    if demand_key == "other":
        await state.update_data(demand_key=demand_key)
        await state.set_state(ClaimForm.demand)
        await callback.message.edit_text(
            "✍️ Напишите, какое <b>требование</b> вы хотите включить в претензию своими словами.\n\n"
            "Например: вернуть деньги, заменить товар, произвести ремонт, открыть доступ к сервису.",
            reply_markup=cancel_kb(),
        )
        await callback.answer()
        return

    await state.update_data(demand_key=demand_key)
    await state.set_state(ClaimForm.full_name)
    await callback.message.edit_text(
        "Шаг 8/9: напишите <b>ФИО</b> для подписи в претензии.",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


# ==========================
# Отмена и универсальный возврат
# ==========================

@router.callback_query(F.data == "form:cancel")
async def cancel_form(callback: CallbackQuery, state: FSMContext, bot: Bot):
    await state.clear()
    user = await DB.get_user(callback.from_user.id)
    if user is None:
        await DB.create_user_if_missing(callback.from_user.id, callback.from_user.username or "", callback.from_user.full_name)
        user = await DB.get_user(callback.from_user.id)
    bot_username = await get_bot_username(bot)
    await callback.message.edit_text(build_opening_text(user, bot_username), reply_markup=main_menu_kb())
    await callback.answer("Отменено")


# ==========================
# Шаги формы
# ==========================

@router.message(ClaimForm.seller)
async def step_seller(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 2:
        await message.answer("Напишите название продавца / компании текстом.")
        return
    await state.update_data(seller=text)
    await state.set_state(ClaimForm.doc_number)
    await message.answer(
        "Шаг 2/9: укажите <b>номер заказа / договора / полиса / билета</b>.\n"
        "Если номера нет — напишите <b>нет</b>.",
        reply_markup=cancel_kb(),
    )


@router.message(ClaimForm.doc_number)
async def step_doc_number(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    await state.update_data(doc_number=text)
    await state.set_state(ClaimForm.purchase_date)
    await message.answer(
        "Шаг 3/9: напишите <b>дату покупки / оформления</b>.\n"
        "Формат можно любой: 12.03.2026, вчера, 03.2026 и т.д.",
        reply_markup=cancel_kb(),
    )


@router.message(ClaimForm.purchase_date)
async def step_purchase_date(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    await state.update_data(purchase_date=text)
    await state.set_state(ClaimForm.amount)
    await message.answer(
        "Шаг 4/9: укажите <b>сумму</b> в рублях.\n"
        "Например: 1990 или 19 990.",
        reply_markup=cancel_kb(),
    )


@router.message(ClaimForm.amount)
async def step_amount(message: Message, state: FSMContext):
    amount = normalize_money(message.text or "")
    if amount is None:
        await message.answer("Не вижу сумму. Напишите число, например <b>1990</b>.")
        return
    await state.update_data(amount=amount)
    await state.set_state(ClaimForm.problem)
    await message.answer(
        "Шаг 5/9: опишите <b>что случилось</b> своими словами.\n"
        "Например: товар пришёл с трещиной, деньги не вернули, рейс отменили.",
        reply_markup=cancel_kb(),
    )


@router.message(ClaimForm.problem)
async def step_problem(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Опишите проблему чуть подробнее.")
        return
    await state.update_data(problem=text)
    await state.set_state(ClaimForm.demand)
    await message.answer(
        "Шаг 6/9: выберите <b>требование</b>.",
        reply_markup=demand_kb(),
    )


@router.message(ClaimForm.full_name)
async def step_full_name(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Напишите ФИО полностью.")
        return
    await state.update_data(full_name=text)
    await state.set_state(ClaimForm.contact)
    await message.answer(
        "Шаг 7/9: укажите <b>контакт</b> — телефон или e-mail.\n"
        "Например: +7 999 123-45-67, ivan@mail.ru",
        reply_markup=cancel_kb(),
    )


@router.message(ClaimForm.contact)
async def step_contact(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Укажите телефон или e-mail.")
        return
    await state.update_data(contact=text)
    await state.set_state(ClaimForm.city)
    await message.answer(
        "Шаг 8/9: напишите <b>город</b> для шапки претензии.\n"
        "Если не хотите указывать — напишите <b>нет</b>.",
        reply_markup=cancel_kb(),
    )


@router.message(ClaimForm.city)
async def step_city(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    await state.update_data(city=text if text.lower() != "нет" else "—")
    await state.set_state(ClaimForm.extra)
    await message.answer(
        "Шаг 9/9: добавьте <b>дополнительные детали</b> или напишите <b>нет</b>.\n"
        "Например: отправлял несколько обращений, есть чек, есть скриншоты.",
        reply_markup=cancel_kb(),
    )


@router.message(ClaimForm.extra)
async def step_extra(message: Message, state: FSMContext, bot: Bot):
    user_text = (message.text or "").strip()
    await state.update_data(extra=user_text if user_text.lower() != "нет" else "—")

    # До генерации проверяем лимит. Если бесплатные закончились — показываем оплату.
    if not await ensure_paying_access(bot, message):
        await state.clear()
        return

    data = await state.get_data()
    case = CASES_BY_ID.get(data.get("case_id"))
    if case is None:
        await state.clear()
        await message.answer("Не удалось определить выбранный случай. Нажмите /menu.")
        return

    user_row = await DB.get_user(message.from_user.id)
    if user_row is None:
        await DB.create_user_if_missing(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
        user_row = await DB.get_user(message.from_user.id)

    claim_text = build_claim(data, case, user_row)
    ok = await DB.consume_claim(message.from_user.id)
    if not ok:
        await state.clear()
        await message.answer("Лимит бесплатных претензий закончился. Нажмите /menu и оформите подписку.")
        return

    await DB.log_history(message.from_user.id, case.id, data, claim_text)
    await state.clear()

    await message.answer("✅ <b>Претензия готова</b>\n\nНиже текст, который можно копировать и отправлять:")
    for part in split_long_message(claim_text):
        await message.answer(part)

    bot_username = await get_bot_username(bot)
    amount = data.get("amount")
    await message.answer(
        "🔥 <b>Успех!</b>\n"
        "Если хотите, можете поделиться результатом и привлечь друзей.",
        reply_markup=share_kb(bot_username, amount, message.from_user.id),
    )


@router.message(ClaimForm.demand)
async def step_demand_custom(message: Message, state: FSMContext):
    # Этот хендлер работает только если пользователь выбрал "Вписать своё"
    data = await state.get_data()
    if data.get("demand_key") != "other":
        await message.answer("Пожалуйста, выберите требование кнопкой ниже.", reply_markup=demand_kb())
        return

    text = (message.text or "").strip()
    if len(text) < 5:
        await message.answer("Напишите требование чуть подробнее.")
        return
    await state.update_data(custom_demand=text)
    await state.set_state(ClaimForm.full_name)
    await message.answer("Шаг 8/9: напишите <b>ФИО</b> для подписи в претензии.", reply_markup=cancel_kb())


# ==========================
# Успешная оплата Stars
# ==========================

@router.pre_checkout_query()
async def pre_checkout(pre_checkout_query, bot: Bot):
    # Для Telegram Stars pre-checkout всё равно нужно подтвердить.
    try:
        await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
    except Exception:
        logger.exception("Failed to answer pre-checkout")


@router.message(F.successful_payment)
async def successful_payment(message: Message):
    payment = message.successful_payment
    if not payment:
        return

    if payment.currency != "XTR":
        await message.answer("Платёж получен, но валюта не совпадает с Telegram Stars.")
        return

    if not payment.invoice_payload.startswith("sub30d:"):
        await message.answer("Платёж получен, но payload не распознан.")
        return

    await DB.add_subscription_days(message.from_user.id, DEFAULT_SUB_DAYS)
    user = await DB.get_user(message.from_user.id)
    until = ts_to_str(int(user["subscription_until"])) if user else "—"
    await message.answer(
        f"✅ <b>Подписка активирована</b>\n\nБезлимит открыт до <b>{until}</b>.\nТеперь можно генерировать претензии без лимита 30 дней.",
        reply_markup=main_menu_kb(),
    )


# ==========================
# Текстовые сообщения вне FSM
# ==========================

@router.message()
async def fallback(message: Message, bot: Bot, state: FSMContext):
    current = await state.get_state()
    if current is None:
        user = await DB.get_user(message.from_user.id)
        if user is None:
            await DB.create_user_if_missing(message.from_user.id, message.from_user.username or "", message.from_user.full_name)
            user = await DB.get_user(message.from_user.id)
        bot_username = await get_bot_username(bot)
        await message.answer(build_opening_text(user, bot_username), reply_markup=main_menu_kb())
        return
    await message.answer("Используйте кнопки или напишите /menu для возврата в главное меню.")


# ==========================
# Запуск
# ==========================

async def on_startup(bot: Bot):
    await DB.connect()
    await get_bot_username(bot)
    logger.info("Bot started")


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в переменных окружения")

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await on_startup(bot)
    try:
        await dp.start_polling(bot)
    finally:
        if DB.conn is not None:
            await DB.conn.close()


if __name__ == "__main__":
    asyncio.run(main())
