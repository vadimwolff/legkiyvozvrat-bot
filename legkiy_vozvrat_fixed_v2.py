
# -*- coding: utf-8 -*-
"""
ЛегкийВозврат Эксперт — версия с исправленной оплатой и кнопкой Help
aiogram 3.x
"""

import asyncio
import logging
import sqlite3
import os
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    PreCheckoutQuery
)
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

BOT_TOKEN = os.getenv("BOT_TOKEN")

FREE_LIMIT = 3
SUB_PRICE = 149
SUB_DAYS = 30

logging.basicConfig(level=logging.INFO)

# ===== DB =====
conn = sqlite3.connect("bot.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    free_used INTEGER DEFAULT 0,
    sub_until TEXT
)
""")
conn.commit()

def get_user(user_id):
    cursor.execute("SELECT free_used, sub_until FROM users WHERE user_id=?", (user_id,))
    row = cursor.fetchone()
    if not row:
        cursor.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
        conn.commit()
        return 0, None
    return row

def has_access(user_id):
    free_used, sub_until = get_user(user_id)
    if sub_until:
        if datetime.fromisoformat(sub_until) > datetime.now():
            return True
    return free_used < FREE_LIMIT

def use_free(user_id):
    cursor.execute("UPDATE users SET free_used = free_used + 1 WHERE user_id=?", (user_id,))
    conn.commit()

def activate_subscription(user_id):
    until = datetime.now() + timedelta(days=SUB_DAYS)
    cursor.execute("UPDATE users SET sub_until=? WHERE user_id=?", (until.isoformat(), user_id))
    conn.commit()

# ===== FSM =====
class Form(StatesGroup):
    amount = State()
    reason = State()

router = Router()

# ===== MAIN MENU =====
def main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Создать претензию", callback_data="start_form")],
        [InlineKeyboardButton(text="❓ Help", callback_data="help")]
    ])

@router.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "👋 Добро пожаловать в ЛегкийВозврат Эксперт\n\nВыбери действие:",
        reply_markup=main_menu()
    )

# ===== HELP =====
@router.callback_query(F.data == "help")
async def help_handler(callback: CallbackQuery):
    text = (
        "❓ *Как работает бот:*\n\n"
        "1. Нажми 'Создать претензию'\n"
        "2. Введи сумму и причину\n"
        "3. Получи готовый текст\n\n"
        "🎁 Первые 3 — бесплатно\n"
        "💎 Далее — подписка за 149 Stars\n\n"
        "📩 Поддержка: @your_support"
    )
    await callback.message.answer(text, parse_mode="Markdown")

# ===== FORM =====
@router.callback_query(F.data == "start_form")
async def start_form(callback: CallbackQuery, state: FSMContext):
    if not has_access(callback.from_user.id):
        await send_payment(callback.message)
        return

    await state.set_state(Form.amount)
    await callback.message.answer("💰 Введи сумму:")

@router.message(Form.amount)
async def get_amount(message: Message, state: FSMContext):
    await state.update_data(amount=message.text)
    await state.set_state(Form.reason)
    await message.answer("📄 Причина:")

@router.message(Form.reason)
async def get_reason(message: Message, state: FSMContext):
    data = await state.get_data()

    text = f"""
ПРЕТЕНЗИЯ

Прошу вернуть {data['amount']} руб.
Основание: {message.text}

Срок: 10 дней.
"""

    use_free(message.from_user.id)

    await message.answer(text, reply_markup=main_menu())
    await state.clear()

# ===== PAYMENT =====
async def send_payment(message: Message):
    bot = message.bot

    link = await bot.create_invoice_link(
        title="Безлимит на 30 дней",
        description="Доступ без ограничений",
        payload="sub30d",
        currency="XTR",
        prices=[{"label": "Подписка", "amount": SUB_PRICE}],
        subscription_period=SUB_DAYS * 24 * 60 * 60
    )

    print("PAYMENT LINK:", link)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Купить за 149 Stars", url=link)]
    ])

    await message.answer("❌ Лимит исчерпан. Купи подписку:", reply_markup=kb)

# ===== PAY HANDLERS =====
@router.pre_checkout_query()
async def pre_checkout(pre_checkout_query: PreCheckoutQuery, bot: Bot):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@router.message(F.successful_payment)
async def successful_payment(message: Message):
    activate_subscription(message.from_user.id)
    await message.answer("✅ Подписка активирована!", reply_markup=main_menu())

# ===== MAIN =====
async def main():
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(router)

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
