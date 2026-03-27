#!/usr/bin/env python3
"""
Easy Stars Bot - Telegram бот для заработка звезд через подписки на каналы
"""

import asyncio
import logging
import psycopg2
import psycopg2.extras
from typing import Optional, List, Tuple
from contextlib import contextmanager

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger('aiogram').setLevel(logging.WARNING)
logging.getLogger('aiogram.event').setLevel(logging.ERROR)
logging.getLogger('aiogram.dispatcher').setLevel(logging.ERROR)

# ==================== КОНФИГУРАЦИЯ ====================
import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
BOT_USERNAME = os.getenv("BOT_USERNAME", "EasyStarsBot")

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "123456789")
ADMIN_IDS = [int(x.strip()) for x in ADMIN_IDS_RAW.split(",") if x.strip()]

DATABASE_URL = os.getenv("DATABASE_URL", "easystars.db")

DEFAULT_REQUIRED_CHANNELS = [
    {"link": "https://t.me/AFA_RIT", "username": "@AFA_RIT"},
    {"link": "https://t.me/Piggy_egor_REAL", "username": "@Piggy_egor_REAL"},
    {"link": "https://t.me/nft_by_1111", "username": "@nft_by_1111"}
]

REFERRAL_BONUS = float(os.getenv("REFERRAL_BONUS", "2"))
MIN_WITHDRAW = float(os.getenv("MIN_WITHDRAW", "15"))
MAX_WITHDRAW = float(os.getenv("MAX_WITHDRAW", "100"))
PENALTY_PERCENT = float(os.getenv("PENALTY_PERCENT", "100"))
REPAY_PERCENT = float(os.getenv("REPAY_PERCENT", "50"))

TASK_REWARD = 0.5
REFERRAL_SUB_REWARD = 2.0

AVAILABLE_PRICES = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]


def get_required_channels() -> List[dict]:
    channels = db.get_required_channels()
    if not channels:
        for ch in DEFAULT_REQUIRED_CHANNELS:
            db.add_required_channel(ch['link'], ch['username'])
        return db.get_required_channels()
    return channels


# ==================== БАЗА ДАННЫХ ====================
class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self._init_tables()

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(self.db_url, cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_tables(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    balance FLOAT DEFAULT 0,
                    referred_by BIGINT DEFAULT NULL,
                    is_banned BOOLEAN DEFAULT FALSE,
                    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    total_earned FLOAT DEFAULT 0,
                    total_withdrawn FLOAT DEFAULT 0,
                    required_subs_completed BOOLEAN DEFAULT FALSE
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    link TEXT,
                    username TEXT,
                    price FLOAT DEFAULT 0.5,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS required_channels (
                    id SERIAL PRIMARY KEY,
                    link TEXT NOT NULL,
                    username TEXT NOT NULL,
                    position INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id BIGINT,
                    channel_id TEXT,
                    subscribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    rewarded BOOLEAN DEFAULT FALSE,
                    PRIMARY KEY (user_id, channel_id)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    amount FLOAT,
                    status TEXT DEFAULT 'pending',
                    wallet TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP,
                    processed_by BIGINT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS promocodes (
                    code TEXT PRIMARY KEY,
                    stars FLOAT,
                    max_uses INTEGER,
                    used INTEGER DEFAULT 0,
                    created_by BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS promocode_uses (
                    id SERIAL PRIMARY KEY,
                    code TEXT,
                    user_id BIGINT,
                    used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id SERIAL PRIMARY KEY,
                    referrer_id BIGINT,
                    referred_id BIGINT,
                    earned FLOAT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    bonus_given BOOLEAN DEFAULT FALSE
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS required_subscriptions (
                    user_id BIGINT,
                    channel_link TEXT,
                    channel_username TEXT,
                    subscribed BOOLEAN DEFAULT FALSE,
                    PRIMARY KEY (user_id, channel_link)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS referral_required_subs (
                    id SERIAL PRIMARY KEY,
                    referrer_id BIGINT,
                    referred_id BIGINT,
                    channel_username TEXT,
                    is_subscribed BOOLEAN DEFAULT TRUE,
                    last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    bonus_given BOOLEAN DEFAULT FALSE,
                    UNIQUE(referrer_id, referred_id, channel_username)
                )
            """)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status)")

            default_settings = {
                'referral_bonus': str(REFERRAL_BONUS),
                'min_withdraw': str(MIN_WITHDRAW),
                'max_withdraw': str(MAX_WITHDRAW),
                'penalty_percent': str(PENALTY_PERCENT),
                'repay_percent': str(REPAY_PERCENT),
                'maintenance_mode': '0',
                'maintenance_text': 'Бот на техническом обслуживании. Пожалуйста, зайдите позже.',
                'welcome_text': 'Добро пожаловать в Easy Stars Bot!\n\nВыполняйте задания и получайте звезды!'
            }

            for key, value in default_settings.items():
                cursor.execute(
                    "INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO NOTHING",
                    (key, value)
                )

            cursor.execute("SELECT COUNT(*) AS cnt FROM required_channels")
            row = cursor.fetchone()
            count = row['cnt'] if row else 0
            if count == 0:
                for i, ch in enumerate(DEFAULT_REQUIRED_CHANNELS):
                    cursor.execute(
                        "INSERT INTO required_channels (link, username, position) VALUES (%s, %s, %s)",
                        (ch['link'], ch['username'], i)
                    )
    def format_number(self, num: float) -> str:
        if num == int(num):
            return str(int(num))
        return f"{num:.2f}".rstrip('0').rstrip('.') if '.' in str(num) else str(num)

    def get_required_channels(self) -> List[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT link, username, position FROM required_channels WHERE is_active = TRUE ORDER BY position"
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_required_channel_by_position(self, position: int) -> Optional[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT link, username, position FROM required_channels WHERE position = %s AND is_active = TRUE",
                (position,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def add_required_channel(self, link: str, username: str, position: int = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if position is None:
                cursor.execute("SELECT MAX(position) FROM required_channels")
                row = cursor.fetchone()
                max_pos = list(row.values())[0] if row else None
                position = (max_pos + 1) if max_pos is not None else 0
            cursor.execute(
                "INSERT INTO required_channels (link, username, position, is_active) VALUES (%s, %s, %s, TRUE)",
                (link, username, position)
            )

    def update_required_channel(self, position: int, link: str, username: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE required_channels SET link = %s, username = %s WHERE position = %s",
                (link, username, position)
            )

    def delete_required_channel(self, position: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM required_channels WHERE position = %s", (position,))
            cursor.execute("SELECT position FROM required_channels ORDER BY position")
            positions = [row['position'] for row in cursor.fetchall()]
            for new_pos, old_pos in enumerate(positions):
                cursor.execute("UPDATE required_channels SET position = %s WHERE position = %s", (new_pos, old_pos))

    def get_user(self, user_id: int) -> Optional[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def create_user(self, user_id: int, username: str = None, full_name: str = None, referred_by: int = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO users (id, username, full_name, referred_by) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                (user_id, username, full_name, referred_by)
            )
            return cursor.rowcount > 0

    def set_required_subs_completed(self, user_id: int, completed: bool):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET required_subs_completed = %s WHERE id = %s",
                           (1 if completed else 0, user_id))

    def add_referral_bonus(self, referrer_id: int, referred_id: int, bonus: float):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE users SET balance = balance + %s, total_earned = total_earned + %s WHERE id = %s",
                (bonus, bonus, referrer_id)
            )
            cursor.execute(
                "INSERT INTO referrals (referrer_id, referred_id, earned, bonus_given) VALUES (%s, %s, %s, 1)",
                (referrer_id, referred_id, bonus)
            )

    def has_referral_bonus_given(self, referrer_id: int, referred_id: int) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT bonus_given FROM referrals WHERE referrer_id = %s AND referred_id = %s",
                (referrer_id, referred_id)
            )
            row = cursor.fetchone()
            return bool(row['bonus_given']) if row else False

    def get_referrals(self, user_id: int) -> List[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT u.id, u.username, u.full_name, r.earned, r.created_at, r.bonus_given
                FROM referrals r 
                JOIN users u ON r.referred_id = u.id 
                WHERE r.referrer_id = %s 
                ORDER BY r.created_at DESC
            """, (user_id,))
            return [dict(row) for row in cursor.fetchall()]

    def update_user_balance(self, user_id: int, amount: float, add_to_earned: bool = False):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if add_to_earned:
                cursor.execute(
                    "UPDATE users SET balance = balance + %s, total_earned = total_earned + %s WHERE id = %s",
                    (amount, amount, user_id)
                )
            else:
                cursor.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (amount, user_id))

    def set_user_balance(self, user_id: int, amount: float):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = %s WHERE id = %s", (amount, user_id))

    def ban_user(self, user_id: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = TRUE WHERE id = %s", (user_id,))

    def unban_user(self, user_id: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = FALSE WHERE id = %s", (user_id,))

    def add_channel(self, channel_id: str, name: str, link: str, username: str, price: float = TASK_REWARD):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO channels (id, name, link, username, price) VALUES (%s, %s, %s, %s, %s)",
                (channel_id, name, link, username, price)
            )

    def get_channel(self, channel_id: str) -> Optional[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM channels WHERE id = %s", (channel_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_channels(self, include_inactive: bool = False) -> List[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if include_inactive:
                cursor.execute("SELECT * FROM channels ORDER BY created_at")
            else:
                cursor.execute("SELECT * FROM channels WHERE is_active = TRUE ORDER BY created_at")
            return [dict(row) for row in cursor.fetchall()]

    def toggle_channel(self, channel_id: str) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE channels SET is_active = NOT is_active WHERE id = %s", (channel_id,))
            return cursor.rowcount > 0

    def delete_channel(self, channel_id: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM channels WHERE id = %s", (channel_id,))

    def add_subscription(self, user_id: int, channel_id: str, rewarded: bool = True):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO subscriptions (user_id, channel_id, rewarded) VALUES (%s, %s, %s) ON CONFLICT (user_id, channel_id) DO UPDATE SET rewarded=EXCLUDED.rewarded",
                (user_id, channel_id, 1 if rewarded else 0)
            )

    def is_channel_subscribed(self, user_id: int, channel_id: str) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM subscriptions WHERE user_id = %s AND channel_id = %s", (user_id, channel_id))
            return cursor.fetchone() is not None

    def add_required_subscription(self, user_id: int, channel_link: str, channel_username: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO required_subscriptions (user_id, channel_link, channel_username) VALUES (%s, %s, %s) ON CONFLICT (user_id, channel_link) DO NOTHING",
                (user_id, channel_link, channel_username)
            )

    def set_required_subscribed(self, user_id: int, channel_link: str, subscribed: bool):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE required_subscriptions SET subscribed = %s WHERE user_id = %s AND channel_link = %s",
                (1 if subscribed else 0, user_id, channel_link)
            )

    def create_withdrawal(self, user_id: int, amount: float, wallet: str) -> int:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO withdrawals (user_id, amount, wallet) VALUES (%s, %s, %s) RETURNING id",
                (user_id, amount, wallet)
            )
            row = cursor.fetchone()
            return row['id'] if row else None

    def get_pending_withdrawals(self) -> List[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM withdrawals WHERE status = 'pending' ORDER BY created_at")
            return [dict(row) for row in cursor.fetchall()]

    def update_withdrawal_status(self, withdrawal_id: int, status: str, processed_by: int = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE withdrawals SET status = %s, processed_at = CURRENT_TIMESTAMP, processed_by = %s WHERE id = %s",
                (status, processed_by, withdrawal_id)
            )
            if status == 'rejected':
                cursor.execute("SELECT user_id, amount FROM withdrawals WHERE id = %s", (withdrawal_id,))
                row = cursor.fetchone()
                if row:
                    cursor.execute("UPDATE users SET balance = balance + %s WHERE id = %s",
                                   (row['amount'], row['user_id']))

    def add_promocode(self, code: str, stars: float, max_uses: int, created_by: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO promocodes (code, stars, max_uses, created_by) VALUES (%s, %s, %s, %s)",
                (code.upper(), stars, max_uses, created_by)
            )

    def delete_promocode(self, code: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM promocodes WHERE code = %s", (code.upper(),))

    def get_promocode(self, code: str) -> Optional[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM promocodes WHERE code = %s", (code.upper(),))
            row = cursor.fetchone()
            return dict(row) if row else None

    def use_promocode(self, code: str, user_id: int) -> Tuple[bool, str]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM promocodes WHERE code = %s AND used < max_uses", (code.upper(),))
            promo = cursor.fetchone()
            if not promo:
                return False, "Промокод не найден или уже использован все активации"

            cursor.execute("SELECT * FROM promocode_uses WHERE code = %s AND user_id = %s", (code.upper(), user_id))
            if cursor.fetchone():
                return False, "Вы уже использовали этот промокод"

            cursor.execute("INSERT INTO promocode_uses (code, user_id) VALUES (%s, %s)", (code.upper(), user_id))
            cursor.execute("UPDATE promocodes SET used = used + 1 WHERE code = %s", (code.upper(),))
            cursor.execute(
                "UPDATE users SET balance = balance + %s, total_earned = total_earned + %s WHERE id = %s",
                (promo['stars'], promo['stars'], user_id)
            )
            return True, f"Вы активировали промокод и получили {self.format_number(promo['stars'])} звезд!"

    def get_all_promocodes(self) -> List[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM promocodes ORDER BY created_at DESC")
            return [dict(row) for row in cursor.fetchall()]

    def get_setting(self, key: str) -> str:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM settings WHERE key = %s", (key,))
            row = cursor.fetchone()
            return row['value'] if row else None

    def set_setting(self, key: str, value: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value", (key, value))

    def get_stats(self) -> dict:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) AS cnt FROM users")
            total_users = cursor.fetchone()['cnt']
            cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE is_banned = TRUE")
            banned_users = cursor.fetchone()['cnt']
            cursor.execute("SELECT COALESCE(SUM(balance), 0) AS s FROM users")
            total_balance = cursor.fetchone()['s']
            cursor.execute("SELECT COALESCE(SUM(total_earned), 0) AS s FROM users")
            total_earned = cursor.fetchone()['s']
            cursor.execute("SELECT COUNT(*) AS cnt FROM channels")
            total_channels = cursor.fetchone()['cnt']
            cursor.execute("SELECT COUNT(*) AS cnt FROM withdrawals WHERE status = 'pending'")
            pending_withdrawals = cursor.fetchone()['cnt']
            cursor.execute("SELECT COALESCE(SUM(amount), 0) AS s FROM withdrawals WHERE status = 'paid'")
            total_paid = cursor.fetchone()['s']
            return {
                'total_users': total_users,
                'banned_users': banned_users,
                'total_balance': total_balance,
                'total_earned': total_earned,
                'total_channels': total_channels,
                'pending_withdrawals': pending_withdrawals,
                'total_paid': total_paid
            }

    def get_all_users(self) -> List[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, username, full_name, balance, is_banned FROM users ORDER BY registered_at DESC")
            return [dict(row) for row in cursor.fetchall()]

    def get_all_users_for_broadcast(self) -> List[int]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM users WHERE is_banned = FALSE")
            return [row['id'] for row in cursor.fetchall()]

    def add_referral_required_sub(self, referrer_id: int, referred_id: int, channel_username: str,
                                  bonus_given: bool = False):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO referral_required_subs 
                (referrer_id, referred_id, channel_username, is_subscribed, bonus_given) 
                VALUES (?, ?, ?, 1, ?)
            """, (referrer_id, referred_id, channel_username, 1 if bonus_given else 0))

    def update_referral_required_sub_status(self, referrer_id: int, referred_id: int, channel_username: str,
                                            is_subscribed: bool):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE referral_required_subs 
                SET is_subscribed = %s, last_checked = CURRENT_TIMESTAMP 
                WHERE referrer_id = %s AND referred_id = %s AND channel_username = ?
            """, (1 if is_subscribed else 0, referrer_id, referred_id, channel_username))

    def get_referral_required_sub_status(self, referrer_id: int, referred_id: int, channel_username: str) -> Optional[dict]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT is_subscribed, bonus_given FROM referral_required_subs 
                WHERE referrer_id = %s AND referred_id = %s AND channel_username = ?
            """, (referrer_id, referred_id, channel_username))
            row = cursor.fetchone()
            return dict(row) if row else None

    def process_referral_required_sub_change(self, referrer_id: int, referred_id: int, channel_username: str,
                                             new_status: bool) -> Tuple[bool, float]:
        current = self.get_referral_required_sub_status(referrer_id, referred_id, channel_username)

        if current is None:
            self.add_referral_required_sub(referrer_id, referred_id, channel_username, False)
            return False, 0

        if current['is_subscribed'] == new_status:
            return False, 0

        self.update_referral_required_sub_status(referrer_id, referred_id, channel_username, new_status)

        if not current['bonus_given']:
            return False, 0

        amount_change = REFERRAL_SUB_REWARD if new_status else -REFERRAL_SUB_REWARD
        self.update_user_balance(referrer_id, amount_change)

        return True, amount_change

    def set_referral_channel_bonus_given(self, referrer_id: int, referred_id: int, channel_username: str):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE referral_required_subs 
                SET bonus_given = TRUE 
                WHERE referrer_id = %s AND referred_id = %s AND channel_username = ?
            """, (referrer_id, referred_id, channel_username))


db = Database(DATABASE_URL)


# ==================== FSM СОСТОЯНИЯ ====================
class WithdrawStates(StatesGroup):
    waiting_for_amount = State()
    waiting_for_wallet = State()


class AdminStates(StatesGroup):
    waiting_for_channel_link = State()
    waiting_for_channel_username = State()
    waiting_for_add_promocode = State()
    waiting_for_broadcast = State()
    waiting_for_user_action = State()
    waiting_for_required_channel_link = State()
    waiting_for_required_channel_username = State()
    waiting_for_channel_price = State()
    waiting_for_required_channel_edit_link = State()
    waiting_for_required_channel_edit_username = State()


class UserStates(StatesGroup):
    waiting_for_promo = State()


bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def check_subscription(bot_instance: Bot, user_id: int, channel_username: str) -> bool:
    try:
        if not channel_username.startswith("@"):
            channel_username = "@" + channel_username
        try:
            member = await bot_instance.get_chat_member(channel_username, user_id)
            return member.status in ['member', 'administrator', 'creator']
        except TelegramBadRequest as e:
            error_text = str(e).lower()
            if "participant_id_invalid" in error_text or "user not found" in error_text:
                return False
            if "chat not found" in error_text:
                return False
            return False
    except Exception:
        return False


async def check_referral_required_subscriptions(bot_instance: Bot, user_id: int):
    referrals = db.get_referrals(user_id)
    required_channels = get_required_channels()

    for ref in referrals:
        referred_id = ref['id']
        for ch in required_channels:
            channel_username = ch['username']
            try:
                is_subscribed = await check_subscription(bot_instance, referred_id, channel_username)
                changed, amount = db.process_referral_required_sub_change(
                    user_id, referred_id, channel_username, is_subscribed
                )
                if changed:
                    try:
                        status_text = "подписался" if is_subscribed else "отписался"
                        action_text = f"+{REFERRAL_SUB_REWARD}⭐" if is_subscribed else f"-{REFERRAL_SUB_REWARD}⭐"
                        await bot_instance.send_message(
                            user_id,
                            f"🔄 Реферал {ref.get('full_name', referred_id)} {status_text} "
                            f"на канал {channel_username}!\n"
                            f"Ваш баланс: {action_text}"
                        )
                    except Exception:
                        pass
            except Exception:
                continue


async def check_all_required_subscriptions(user_id: int) -> Tuple[bool, List[dict]]:
    required_channels = get_required_channels()
    not_subscribed = []

    for ch in required_channels:
        is_subscribed = await check_subscription(bot, user_id, ch['username'])
        db.set_required_subscribed(user_id, ch['link'], is_subscribed)
        if not is_subscribed:
            not_subscribed.append(ch)

    all_subscribed = len(not_subscribed) == 0

    if all_subscribed:
        user = db.get_user(user_id)
        if user and user.get('referred_by') and not user.get('required_subs_completed'):
            db.set_required_subs_completed(user_id, True)

            if not db.has_referral_bonus_given(user['referred_by'], user_id):
                bonus = float(db.get_setting('referral_bonus'))
                db.add_referral_bonus(user['referred_by'], user_id, bonus)

                for ch in required_channels:
                    db.add_referral_required_sub(user['referred_by'], user_id, ch['username'], True)

                try:
                    await bot.send_message(
                        user['referred_by'],
                        f"🎉 Ваш реферал {user.get('full_name', user_id)} выполнил обязательные подписки!\n"
                        f"Вы получили +{db.format_number(bonus)} звезд!\n\n"
                        f"Теперь вы будете получать +{REFERRAL_SUB_REWARD}⭐ когда реферал подписывается на обязательные каналы, "
                        f"и -{REFERRAL_SUB_REWARD}⭐ когда отписывается!"
                    )
                except Exception:
                    pass

    return all_subscribed, not_subscribed


async def safe_edit(message: Message, text: str, reply_markup=None):
    try:
        await message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest:
        try:
            await message.delete()
        except Exception:
            pass
        await message.answer(text, reply_markup=reply_markup)


async def periodic_referral_check():
    while True:
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT referrer_id FROM referrals")
                referrers = [row['referrer_id'] for row in cursor.fetchall()]
            for referrer_id in referrers:
                await check_referral_required_subscriptions(bot, referrer_id)
                await asyncio.sleep(1)
            await asyncio.sleep(120)
        except Exception:
            await asyncio.sleep(60)


# ==================== КЛАВИАТУРЫ ====================
class Keyboards:
    @staticmethod
    def required_channels_menu(not_subscribed_channels: List[dict] = None) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        channels_to_show = not_subscribed_channels if not_subscribed_channels else get_required_channels()
        for ch in channels_to_show:
            builder.button(text=f"📢 Подписаться на {ch['username']}", url=ch['link'])
        builder.button(text="✅ Проверить подписки", callback_data="check_required_subs")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def main_menu(user_id: int = None) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="💰 Баланс", callback_data="balance")
        builder.button(text="📋 Задания", callback_data="tasks")
        builder.button(text="💸 Вывод", callback_data="withdraw")
        builder.button(text="🎁 Промокод", callback_data="promo")
        builder.button(text="💎 Заработать", callback_data="earn")
        builder.button(text="ℹ️ Помощь", callback_data="help")
        if user_id and user_id in ADMIN_IDS:
            builder.button(text="🔐 Админ панель", callback_data="admin_panel")
        builder.adjust(2)
        return builder.as_markup()

    @staticmethod
    def admin_panel() -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="📡 Управление каналами", callback_data="admin_channels")
        builder.button(text="📢 Обязательные каналы", callback_data="admin_required_channels")
        builder.button(text="🎟 Промокоды", callback_data="admin_promocodes")
        builder.button(text="👥 Пользователи", callback_data="admin_users")
        builder.button(text="💸 Заявки на вывод", callback_data="admin_withdrawals")
        builder.button(text="📊 Статистика", callback_data="admin_stats")
        builder.button(text="📣 Рассылка", callback_data="admin_broadcast")
        builder.button(text="◀️ Назад", callback_data="back_to_menu")
        builder.adjust(2)
        return builder.as_markup()

    @staticmethod
    def required_channels_admin_menu(required_channels: List[dict]) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        for i, ch in enumerate(required_channels):
            builder.button(
                text=f"{i + 1}. {ch['username']}",
                callback_data=f"required_channel_{i}"
            )
        builder.button(text="➕ Добавить канал", callback_data="required_channel_add")
        builder.button(text="🗑 Удалить канал", callback_data="required_channel_delete_select")
        builder.button(text="◀️ Назад", callback_data="admin_panel")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def required_channel_actions(position: int, channel: dict) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="✏️ Изменить ссылку", callback_data=f"required_channel_edit_link_{position}")
        builder.button(text="🖊️ Изменить username", callback_data=f"required_channel_edit_username_{position}")
        builder.button(text="🗑 Удалить", callback_data=f"required_channel_delete_{position}")
        builder.button(text="◀️ Назад", callback_data="admin_required_channels")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def required_channels_delete_list(required_channels: List[dict]) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        for i, ch in enumerate(required_channels):
            builder.button(
                text=f"🗑 Удалить {i + 1}. {ch['username']}",
                callback_data=f"required_channel_confirm_delete_{i}"
            )
        builder.button(text="◀️ Назад", callback_data="admin_required_channels")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def price_selection_menu() -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        for price in AVAILABLE_PRICES:
            price_str = str(int(price)) if price == int(price) else str(price)
            builder.button(text=f"{price_str}⭐", callback_data=f"set_price_{price}")
        builder.button(text="◀️ Отмена", callback_data="admin_panel")
        builder.adjust(5)
        return builder.as_markup()

    @staticmethod
    def back_button(callback_data: str) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="◀️ Назад", callback_data=callback_data)
        return builder.as_markup()

    @staticmethod
    def cancel_button() -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="❌ Отмена", callback_data="admin_required_channels_cancel")
        return builder.as_markup()

    @staticmethod
    def users_list(users: List[dict], page: int = 0) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        start = page * 10
        end = min(start + 10, len(users))
        for user in users[start:end]:
            status = "🔴" if user['is_banned'] else "🟢"
            name = user.get('username') or user.get('full_name') or str(user['id'])
            builder.button(
                text=f"{status} {name[:20]} | {db.format_number(user['balance'])}⭐",
                callback_data=f"user_{user['id']}"
            )
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"users_page_{page - 1}"))
        if end < len(users):
            nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"users_page_{page + 1}"))
        if nav_buttons:
            builder.row(*nav_buttons)
        builder.button(text="◀️ Назад в админку", callback_data="admin_panel")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def user_actions(user_id: int, is_banned: bool) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="💰 Выдать звезды", callback_data=f"user_addstars_{user_id}")
        builder.button(text="🔨 Забрать звезды", callback_data=f"user_delstars_{user_id}")
        builder.button(text="🎯 Установить баланс", callback_data=f"user_setbalance_{user_id}")
        builder.button(text="👥 Рефералы", callback_data=f"user_referrals_{user_id}")
        if is_banned:
            builder.button(text="🔓 Разблокировать", callback_data=f"user_unban_{user_id}")
        else:
            builder.button(text="🔒 Заблокировать", callback_data=f"user_ban_{user_id}")
        builder.button(text="◀️ Назад", callback_data="admin_users")
        builder.adjust(2)
        return builder.as_markup()

    @staticmethod
    def channels_list(channels: List[dict]) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        for ch in channels:
            status = "✅" if ch['is_active'] else "❌"
            price_str = str(int(ch['price'])) if ch['price'] == int(ch['price']) else str(ch['price'])
            builder.button(
                text=f"{status} {ch.get('name', ch['id'][:15])} | {price_str}⭐",
                callback_data=f"channel_{ch['id']}"
            )
        builder.button(text="➕ Добавить канал", callback_data="admin_add_channel")
        builder.button(text="◀️ Назад", callback_data="admin_panel")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def channel_actions(channel_id: str, is_active: bool) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        action = "❌ Выключить" if is_active else "✅ Включить"
        builder.button(text=action, callback_data=f"channel_toggle_{channel_id}")
        builder.button(text="🗑 Удалить", callback_data=f"channel_delete_{channel_id}")
        builder.button(text="◀️ Назад", callback_data="admin_channels")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def task_channel(channel: dict) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="📢 Подписаться на канал", url=channel['link'])
        builder.button(text="✅ Проверить подписку", callback_data=f"check_task_{channel['id']}")
        builder.button(text="◀️ Назад", callback_data="tasks")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def withdrawals_list(withdrawals: List[dict]) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        for w in withdrawals:
            user = db.get_user(w['user_id'])
            name = user.get('full_name', user.get('username', str(w['user_id']))) if user else str(w['user_id'])
            builder.button(
                text=f"#{w['id']} {name} | {db.format_number(w['amount'])}⭐",
                callback_data=f"withdraw_{w['id']}"
            )
        builder.button(text="◀️ Назад", callback_data="admin_panel")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def withdrawal_actions(withdrawal_id: int) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="✅ Подтвердить", callback_data=f"withdraw_confirm_{withdrawal_id}")
        builder.button(text="💸 Оплачено", callback_data=f"withdraw_pay_{withdrawal_id}")
        builder.button(text="❌ Отклонить", callback_data=f"withdraw_reject_{withdrawal_id}")
        builder.button(text="◀️ Назад", callback_data="admin_withdrawals")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def promocodes_list(promocodes: List[dict]) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        for p in promocodes:
            builder.button(
                text=f"{p['code']} | {db.format_number(p['stars'])}⭐ | {p['used']}/{p['max_uses']}",
                callback_data=f"promocode_{p['code']}"
            )
        builder.button(text="➕ Добавить промокод", callback_data="admin_add_promocode")
        builder.button(text="◀️ Назад", callback_data="admin_panel")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def promocode_actions(code: str) -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="🗑 Удалить", callback_data=f"promocode_delete_{code}")
        builder.button(text="◀️ Назад", callback_data="admin_promocodes")
        builder.adjust(1)
        return builder.as_markup()

    @staticmethod
    def broadcast_cancel() -> InlineKeyboardMarkup:
        builder = InlineKeyboardBuilder()
        builder.button(text="◀️ Отмена", callback_data="admin_panel")
        return builder.as_markup()


# ==================== ХЭНДЛЕРЫ ПОЛЬЗОВАТЕЛЕЙ ====================
@dp.message(Command("start"))
async def cmd_start(message: Message, command: CommandObject):
    user = message.from_user
    referred_by = None

    if command.args and command.args.isdigit():
        referred_by = int(command.args)
        if referred_by == user.id:
            referred_by = None

    user_data = db.get_user(user.id)
    if not user_data:
        db.create_user(user.id, user.username, user.full_name, referred_by)
        required_channels = get_required_channels()
        for ch in required_channels:
            db.add_required_subscription(user.id, ch['link'], ch['username'])

        welcome_text = db.get_setting('welcome_text')
        bonus = float(db.get_setting('referral_bonus'))

        await message.answer(
            f"{welcome_text}\n\n"
            f"⚠️ Для начала работы необходимо подписаться на наши каналы!\n\n"
            f"После подписки на все каналы вы получите +{db.format_number(bonus)} звезд за реферала (если пришли по ссылке)!",
            reply_markup=Keyboards.required_channels_menu()
        )
    else:
        all_subscribed, not_subscribed = await check_all_required_subscriptions(user.id)

        if all_subscribed:
            await message.answer(
                "🌟 Добро пожаловать!\n\nИспользуйте кнопки ниже для навигации:",
                reply_markup=Keyboards.main_menu(user.id)
            )
        else:
            await message.answer(
                "⚠️ Для использования бота необходимо подписаться на обязательные каналы:",
                reply_markup=Keyboards.required_channels_menu(not_subscribed)
            )


@dp.callback_query(F.data == "check_required_subs")
async def check_required_subs(callback: CallbackQuery):
    user_id = callback.from_user.id
    all_subscribed, not_subscribed = await check_all_required_subscriptions(user_id)

    if all_subscribed:
        await safe_edit(
            callback.message,
            "✅ Все обязательные подписки подтверждены! Теперь вы можете пользоваться ботом.",
            reply_markup=Keyboards.main_menu(user_id)
        )
    else:
        await safe_edit(
            callback.message,
            "⚠️ Для использования бота необходимо подписаться на все обязательные каналы:",
            reply_markup=Keyboards.required_channels_menu(not_subscribed)
        )
    await callback.answer()


@dp.callback_query(F.data == "balance")
async def show_balance(callback: CallbackQuery):
    user = db.get_user(callback.from_user.id)
    if user:
        await safe_edit(
            callback.message,
            f"💰 Ваш баланс: {db.format_number(user['balance'])} звезд\n\n"
            f"📈 Всего заработано: {db.format_number(user['total_earned'])}\n"
            f"💸 Всего выведено: {db.format_number(user['total_withdrawn'])}\n\n"
            f"💡 Информация: Вы получаете +{REFERRAL_SUB_REWARD}⭐ когда ваши рефералы подписываются на обязательные каналы, "
            f"и -{REFERRAL_SUB_REWARD}⭐ когда они отписываются!",
            reply_markup=Keyboards.back_button("back_to_menu")
        )
    await callback.answer()


@dp.callback_query(F.data == "tasks")
async def show_tasks(callback: CallbackQuery):
    all_subscribed, _ = await check_all_required_subscriptions(callback.from_user.id)
    if not all_subscribed:
        await callback.answer("⚠️ Сначала подпишитесь на обязательные каналы!", show_alert=True)
        return

    channels = db.get_all_channels()
    if not channels:
        await safe_edit(
            callback.message,
            "📋 Пока нет доступных заданий.\n\nСледите за обновлениями!",
            reply_markup=Keyboards.back_button("back_to_menu")
        )
        await callback.answer()
        return

    text = "📋 Доступные задания:\n\n"
    buttons = []
    for i, ch in enumerate(channels, 1):
        reward = ch['price']
        reward_str = str(int(reward)) if reward == int(reward) else str(reward)
        text += f"{i}. {ch.get('name', ch['id'])} — +{reward_str}⭐\n"
        buttons.append([InlineKeyboardButton(text=f"📢 Задание {i}", callback_data=f"task_{ch['id']}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_menu")])

    await safe_edit(callback.message, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()


@dp.callback_query(F.data.startswith("task_"))
async def show_task_detail(callback: CallbackQuery):
    all_subscribed, _ = await check_all_required_subscriptions(callback.from_user.id)
    if not all_subscribed:
        await callback.answer("⚠️ Сначала подпишитесь на обязательные каналы!", show_alert=True)
        return

    channel_id = callback.data.split("_")[1]
    channel = db.get_channel(channel_id)

    if not channel:
        await callback.answer("❌ Задание не найдено", show_alert=True)
        return

    reward_str = str(int(channel['price'])) if channel['price'] == int(channel['price']) else str(channel['price'])
    text = (
        f"📋 Задание: {channel['name']}\n\n"
        f"💰 Награда: +{reward_str} звезд\n\n"
        f"📢 Чтобы получить награду, подпишитесь на канал и нажмите кнопку проверки."
    )

    await safe_edit(callback.message, text, reply_markup=Keyboards.task_channel(channel))
    await callback.answer()


@dp.callback_query(F.data.startswith("check_task_"))
async def check_task_subscription(callback: CallbackQuery):
    all_subscribed, _ = await check_all_required_subscriptions(callback.from_user.id)
    if not all_subscribed:
        await callback.answer("⚠️ Сначала подпишитесь на обязательные каналы!", show_alert=True)
        return

    channel_id = callback.data.split("_")[2]
    channel = db.get_channel(channel_id)

    if not channel:
        await callback.answer("❌ Задание не найдено", show_alert=True)
        return

    channel_username = channel.get('username') or channel['link']
    is_subscribed = await check_subscription(bot, callback.from_user.id, channel_username)

    if is_subscribed:
        if not db.is_channel_subscribed(callback.from_user.id, channel_id):
            db.add_subscription(callback.from_user.id, channel_id, True)
            db.update_user_balance(callback.from_user.id, channel['price'], add_to_earned=True)

            reward_str = str(int(channel['price'])) if channel['price'] == int(channel['price']) else str(
                channel['price'])
            await callback.answer(f"✅ Вы получили {reward_str} звезд!", show_alert=True)

            user = db.get_user(callback.from_user.id)
            if user and user.get('referred_by'):
                repay_percent = float(db.get_setting('repay_percent'))
                referrer_bonus = channel['price'] * repay_percent / 100
                db.update_user_balance(user['referred_by'], referrer_bonus)

            await show_balance(callback)
        else:
            await callback.answer("❌ Вы уже получали награду за это задание!", show_alert=True)
    else:
        await callback.answer("❌ Вы не подписаны на канал! Подпишитесь и попробуйте снова.", show_alert=True)


@dp.callback_query(F.data == "earn")
async def show_earn(callback: CallbackQuery):
    all_subscribed, _ = await check_all_required_subscriptions(callback.from_user.id)
    if not all_subscribed:
        await callback.answer("⚠️ Сначала подпишитесь на обязательные каналы!", show_alert=True)
        return

    bonus = float(db.get_setting('referral_bonus'))
    referrals = db.get_referrals(callback.from_user.id)

    text = "💎 Заработать звезды\n\n"
    text += f"1. Выполняйте задания в разделе 'Задания' — за каждое задание своя награда\n"
    text += f"2. Приглашайте друзей: +{db.format_number(bonus)}⭐ за каждого (после выполнения им обязательных подписок)\n"
    text += f"3. Получайте +{REFERRAL_SUB_REWARD}⭐ когда рефералы подписываются на обязательные каналы\n\n"
    text += f"Ваша реферальная ссылка:\n"
    text += f"https://t.me/{BOT_USERNAME}?start={callback.from_user.id}\n\n"

    if referrals:
        text += f"👥 Приглашенные друзья ({len(referrals)}):\n"
        for ref in referrals[:10]:
            text += f"• {ref.get('full_name', ref.get('username', ref['id']))} +{db.format_number(ref['earned'])}⭐\n"

    await safe_edit(callback.message, text, reply_markup=Keyboards.back_button("back_to_menu"))
    await callback.answer()


@dp.callback_query(F.data == "withdraw")
async def withdraw_start(callback: CallbackQuery, state: FSMContext):
    all_subscribed, _ = await check_all_required_subscriptions(callback.from_user.id)
    if not all_subscribed:
        await callback.answer("⚠️ Сначала подпишитесь на обязательные каналы!", show_alert=True)
        return

    user = db.get_user(callback.from_user.id)
    min_withdraw = float(db.get_setting('min_withdraw'))
    max_withdraw = float(db.get_setting('max_withdraw'))

    if user['balance'] < min_withdraw:
        await callback.answer(f"❌ Минимальная сумма вывода: {db.format_number(min_withdraw)} звезд", show_alert=True)
        return

    await safe_edit(
        callback.message,
        f"💸 Вывод звезд\n\n"
        f"Ваш баланс: {db.format_number(user['balance'])}⭐\n"
        f"Минимум: {db.format_number(min_withdraw)}⭐\n"
        f"Максимум: {db.format_number(max_withdraw)}⭐\n\n"
        f"Введите сумму для вывода:"
    )
    await state.set_state(WithdrawStates.waiting_for_amount)
    await callback.answer()


@dp.message(WithdrawStates.waiting_for_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.replace(',', '.'))
        min_withdraw = float(db.get_setting('min_withdraw'))
        max_withdraw = float(db.get_setting('max_withdraw'))

        user = db.get_user(message.from_user.id)
        if amount < min_withdraw:
            await message.answer(f"❌ Сумма меньше минимальной ({db.format_number(min_withdraw)}⭐)")
            return
        if amount > max_withdraw:
            await message.answer(f"❌ Сумма больше максимальной ({db.format_number(max_withdraw)}⭐)")
            return
        if amount > user['balance']:
            await message.answer(f"❌ Недостаточно звезд. Ваш баланс: {db.format_number(user['balance'])}⭐")
            return

        await state.update_data(amount=amount)
        await message.answer("💳 Введите номер кошелька/карты для вывода:")
        await state.set_state(WithdrawStates.waiting_for_wallet)
    except ValueError:
        await message.answer("❌ Введите число!")


@dp.message(WithdrawStates.waiting_for_wallet)
async def withdraw_wallet(message: Message, state: FSMContext):
    data = await state.get_data()
    amount = data['amount']
    wallet = message.text.strip()

    user = db.get_user(message.from_user.id)
    if amount > user['balance']:
        await message.answer("❌ Недостаточно звезд!")
        await state.clear()
        return

    db.update_user_balance(message.from_user.id, -amount)
    db.create_withdrawal(message.from_user.id, amount, wallet)

    await message.answer(
        f"✅ Заявка на вывод {db.format_number(amount)}⭐ создана!\n\n"
        f"Ожидайте подтверждения администратором.",
        reply_markup=Keyboards.main_menu(message.from_user.id)
    )
    await state.clear()

    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"🆕 Новая заявка на вывод!\n"
                f"👤 {message.from_user.full_name}\n"
                f"💰 Сумма: {db.format_number(amount)}⭐\n"
                f"💳 Кошелек: {wallet}"
            )
        except Exception:
            pass


@dp.callback_query(F.data == "promo")
async def promo_start(callback: CallbackQuery, state: FSMContext):
    all_subscribed, _ = await check_all_required_subscriptions(callback.from_user.id)
    if not all_subscribed:
        await callback.answer("⚠️ Сначала подпишитесь на обязательные каналы!", show_alert=True)
        return

    await safe_edit(
        callback.message,
        "🎁 Активация промокода\n\nВведите промокод:",
        reply_markup=Keyboards.back_button("back_to_menu")
    )
    await state.set_state(UserStates.waiting_for_promo)
    await callback.answer()


@dp.message(UserStates.waiting_for_promo)
async def process_promo(message: Message, state: FSMContext):
    code = message.text.strip()
    success, msg = db.use_promocode(code, message.from_user.id)

    if success:
        await message.answer(f"✅ {msg}", reply_markup=Keyboards.main_menu(message.from_user.id))
    else:
        await message.answer(f"❌ {msg}", reply_markup=Keyboards.back_button("back_to_menu"))

    await state.clear()


@dp.callback_query(F.data == "help")
async def show_help(callback: CallbackQuery):
    text = (
        "ℹ️ Помощь\n\n"
        "🤖 Easy Stars Bot позволяет зарабатывать звезды, выполняя задания.\n\n"
        "Как это работает:\n"
        "1. Сначала подпишитесь на обязательные каналы\n"
        "2. Выберите задания в разделе 📋 Задания\n"
        "3. Подпишитесь на канал\n"
        "4. Нажмите ✅ Проверить подписку\n"
        "5. Получите звезды на баланс\n\n"
        "Реферальная система:\n"
        f"• Приглашайте друзей и получайте +{REFERRAL_BONUS}⭐ после выполнения ими обязательных подписок\n"
        f"• Получайте +{REFERRAL_SUB_REWARD}⭐ когда рефералы подписываются на обязательные каналы\n"
        f"• Теряете -{REFERRAL_SUB_REWARD}⭐ когда рефералы отписываются от обязательных каналов\n\n"
        "Вывод средств:\n"
        "💰 Накопите минимальную сумму\n"
        "💸 Подайте заявку на вывод\n"
        "⏳ Дождитесь обработки администратором\n\n"
        "По всем вопросам обращайтесь к администратору."
    )
    await safe_edit(callback.message, text, reply_markup=Keyboards.back_button("back_to_menu"))
    await callback.answer()


@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(
        callback.message,
        "🌟 Главное меню:",
        reply_markup=Keyboards.main_menu(callback.from_user.id)
    )
    await callback.answer()


# ==================== ХЭНДЛЕРЫ АДМИНИСТРАТОРА ====================
@dp.message(Command("adminpanel"))
async def cmd_admin_panel(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        return
    await message.answer("🔐 Административная панель EASY STARS", reply_markup=Keyboards.admin_panel())


@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return
    await state.clear()
    await safe_edit(callback.message, "🔐 Административная панель", reply_markup=Keyboards.admin_panel())
    await callback.answer()


@dp.callback_query(F.data == "admin_channels")
async def admin_channels(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    channels = db.get_all_channels(include_inactive=True)
    if channels:
        await safe_edit(callback.message, "📡 Управление каналами", reply_markup=Keyboards.channels_list(channels))
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Добавить канал", callback_data="admin_add_channel")
        kb.button(text="◀️ Назад", callback_data="admin_panel")
        kb.adjust(1)
        await safe_edit(callback.message, "📡 Нет добавленных каналов.", reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data == "admin_add_channel")
async def add_channel_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    await safe_edit(
        callback.message,
        "➕ Добавление канала\n\n"
        "Отправьте ссылку на канал.\n"
        "Пример: https://t.me/channelname\n\n"
        "⚠️ Бот должен быть администратором в этом канале!"
    )
    await state.set_state(AdminStates.waiting_for_channel_link)
    await callback.answer()


@dp.message(AdminStates.waiting_for_channel_link)
async def process_channel_link(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        await state.clear()
        return

    link = message.text.strip()

    if "t.me/" in link:
        username_raw = link.split("t.me/")[-1].strip("/").split("?")[0]
    else:
        username_raw = link.lstrip("@")

    try:
        chat = await bot.get_chat(f"@{username_raw}")
        channel_id = str(chat.id)
        channel_name = chat.title or username_raw

        bot_info = await bot.get_me()
        bot_member = await bot.get_chat_member(chat.id, bot_info.id)
        if bot_member.status not in ['administrator', 'creator']:
            await message.answer(
                "❌ Бот не является администратором этого канала!\n"
                "Добавьте бота в администраторы и попробуйте снова."
            )
            return

        await state.update_data(
            channel_id=channel_id,
            channel_name=channel_name,
            channel_link=f"https://t.me/{username_raw}"
        )

        await message.answer(
            f"✅ Канал найден: <b>{channel_name}</b>\n\n"
            f"Теперь отправьте username канала с символом @\n"
            f"Пример: @{username_raw}",
            parse_mode="HTML"
        )
        await state.set_state(AdminStates.waiting_for_channel_username)

    except TelegramBadRequest as e:
        if "chat not found" in str(e).lower():
            await message.answer(
                "❌ Канал не найден.\n"
                "Проверьте правильность ссылки и убедитесь, что бот добавлен в канал."
            )
        else:
            await message.answer(f"❌ Ошибка Telegram: {e}")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(AdminStates.waiting_for_channel_username)
async def process_channel_username(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        await state.clear()
        return

    username = message.text.strip()
    if not username.startswith("@"):
        await message.answer(
            "❌ Username должен начинаться с @\n"
            f"Пример: @channelname\n\n"
            "Попробуйте снова:"
        )
        return

    data = await state.get_data()
    await state.update_data(channel_username=username)

    await message.answer(
        f"✅ Информация о канале:\n\n"
        f"📡 Название: {data['channel_name']}\n"
        f"🔗 Username: {username}\n\n"
        f"Теперь выберите награду за подписку:",
        reply_markup=Keyboards.price_selection_menu()
    )
    await state.set_state(AdminStates.waiting_for_channel_price)


@dp.callback_query(AdminStates.waiting_for_channel_price, F.data.startswith("set_price_"))
async def process_channel_price(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    price = float(callback.data.split("_")[2])
    data = await state.get_data()

    db.add_channel(
        channel_id=data['channel_id'],
        name=data['channel_name'],
        link=data['channel_link'],
        username=data['channel_username'],
        price=price
    )

    price_str = str(int(price)) if price == int(price) else str(price)
    await safe_edit(
        callback.message,
        f"✅ Канал успешно добавлен!\n\n"
        f"📡 Название: {data['channel_name']}\n"
        f"🔗 Username: {data['channel_username']}\n"
        f"💰 Награда за подписку: {price_str}⭐",
        reply_markup=Keyboards.admin_panel()
    )
    await state.clear()
    await callback.answer()


@dp.callback_query(F.data.startswith("channel_"))
async def channel_action(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    parts = callback.data.split("_")
    if len(parts) == 2:
        channel_id = parts[1]
        channel = db.get_channel(channel_id)
        if channel:
            price_str = str(int(channel['price'])) if channel['price'] == int(channel['price']) else str(
                channel['price'])
            await safe_edit(
                callback.message,
                f"Канал: {channel.get('name', channel_id)}\n"
                f"Username: {channel.get('username', 'не указан')}\n"
                f"Цена: {price_str}⭐\n"
                f"Статус: {'✅ Активен' if channel['is_active'] else '❌ Неактивен'}",
                reply_markup=Keyboards.channel_actions(channel_id, channel['is_active'])
            )
    elif len(parts) >= 3:
        action = parts[1]
        channel_id = "_".join(parts[2:])
        if action == "toggle":
            db.toggle_channel(channel_id)
            await admin_channels(callback)
        elif action == "delete":
            db.delete_channel(channel_id)
            await admin_channels(callback)
    await callback.answer()


# ==================== ХЭНДЛЕРЫ АДМИНИСТРАТОРА ДЛЯ ОБЯЗАТЕЛЬНЫХ КАНАЛОВ ====================
@dp.callback_query(F.data == "admin_required_channels")
async def admin_required_channels(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    required_channels = get_required_channels()
    text = "📢 Управление обязательными каналами\n\n"
    for i, ch in enumerate(required_channels, 1):
        text += f"{i}. {ch['username']}\n   Ссылка: {ch['link']}\n\n"

    if not required_channels:
        text = "📢 Нет обязательных каналов. Добавьте хотя бы один канал."

    await safe_edit(
        callback.message,
        text,
        reply_markup=Keyboards.required_channels_admin_menu(required_channels)
    )
    await callback.answer()


@dp.callback_query(
    F.data.startswith("required_channel_") &
    ~F.data.startswith("required_channel_edit_") &
    ~F.data.startswith("required_channel_delete_") &
    ~F.data.startswith("required_channel_confirm_delete_") &
    ~F.data.startswith("required_channel_add") &
    ~F.data.startswith("required_channel_delete_select")
)
async def required_channel_detail(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    parts = callback.data.split("_")
    try:
        position = int(parts[-1])
    except ValueError:
        await callback.answer("❌ Неверный формат данных")
        return

    channel = db.get_required_channel_by_position(position)

    if not channel:
        await callback.answer("❌ Канал не найден")
        return

    text = f"📢 Обязательный канал #{position + 1}\n\n"
    text += f"🔗 Ссылка: {channel['link']}\n"
    text += f"🆔 Username: {channel['username']}"

    await safe_edit(
        callback.message,
        text,
        reply_markup=Keyboards.required_channel_actions(position, channel)
    )
    await callback.answer()


@dp.callback_query(F.data == "required_channel_add")
async def required_channel_add_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    await safe_edit(
        callback.message,
        "➕ Добавление обязательного канала\n\n"
        "Отправьте ссылку на канал.\n"
        "Пример: https://t.me/channelname\n\n"
        "⚠️ Бот должен быть администратором в этом канале!",
        reply_markup=Keyboards.cancel_button()
    )
    await state.set_state(AdminStates.waiting_for_required_channel_link)
    await callback.answer()


@dp.message(AdminStates.waiting_for_required_channel_link)
async def process_required_channel_link(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        await state.clear()
        return

    link = message.text.strip()

    if "t.me/" in link:
        username_raw = link.split("t.me/")[-1].strip("/").split("?")[0]
    else:
        username_raw = link.lstrip("@")

    try:
        chat = await bot.get_chat(f"@{username_raw}")
        channel_name = chat.title or username_raw

        bot_info = await bot.get_me()
        bot_member = await bot.get_chat_member(chat.id, bot_info.id)
        if bot_member.status not in ['administrator', 'creator']:
            await message.answer(
                "❌ Бот не является администратором этого канала!\n"
                "Добавьте бота в администраторы и попробуйте снова.",
                reply_markup=Keyboards.cancel_button()
            )
            return

        await state.update_data(
            channel_link=f"https://t.me/{username_raw}",
            channel_name=channel_name
        )

        await message.answer(
            f"✅ Канал найден: <b>{channel_name}</b>\n\n"
            f"Теперь отправьте username канала с символом @\n"
            f"Пример: @{username_raw}",
            parse_mode="HTML",
            reply_markup=Keyboards.cancel_button()
        )
        await state.set_state(AdminStates.waiting_for_required_channel_username)

    except TelegramBadRequest as e:
        if "chat not found" in str(e).lower():
            await message.answer(
                "❌ Канал не найден.\n"
                "Проверьте правильность ссылки и убедитесь, что бот добавлен в канал.",
                reply_markup=Keyboards.cancel_button()
            )
        else:
            await message.answer(f"❌ Ошибка Telegram: {e}", reply_markup=Keyboards.cancel_button())
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}", reply_markup=Keyboards.cancel_button())


@dp.message(AdminStates.waiting_for_required_channel_username)
async def process_required_channel_username(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        await state.clear()
        return

    username = message.text.strip()
    if not username.startswith("@"):
        await message.answer(
            "❌ Username должен начинаться с @\n"
            f"Пример: @channelname\n\n"
            "Попробуйте снова:",
            reply_markup=Keyboards.cancel_button()
        )
        return

    data = await state.get_data()
    channel_link = data['channel_link']

    required_channels = get_required_channels()
    position = len(required_channels)
    db.add_required_channel(channel_link, username, position)

    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users")
        users = cursor.fetchall()
        for user in users:
            db.add_required_subscription(user[0], channel_link, username)

    await message.answer(
        f"✅ Обязательный канал успешно добавлен!\n\n"
        f"🔗 Ссылка: {channel_link}\n"
        f"🆔 Username: {username}",
        reply_markup=Keyboards.admin_panel()
    )
    await state.clear()


# Обработчик для изменения ссылки
@dp.callback_query(F.data.startswith("required_channel_edit_link_"))
async def required_channel_edit_link_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    # Формат: required_channel_edit_link_{position}
    parts = callback.data.split("_")
    try:
        position = int(parts[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат данных")
        return

    channel = db.get_required_channel_by_position(position)

    if not channel:
        await callback.answer("❌ Канал не найден")
        return

    await state.update_data(edit_position=position, edit_field="link")
    await safe_edit(
        callback.message,
        f"✏️ Изменение ссылки для канала #{position + 1}\n\n"
        f"Текущая ссылка: {channel['link']}\n\n"
        f"Отправьте новую ссылку на канал:",
        reply_markup=Keyboards.cancel_button()
    )
    await state.set_state(AdminStates.waiting_for_required_channel_link)
    await callback.answer()


# Обработчик для изменения username
@dp.callback_query(F.data.startswith("required_channel_edit_username_"))
async def required_channel_edit_username_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    # Формат: required_channel_edit_username_{position}
    parts = callback.data.split("_")
    try:
        position = int(parts[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат данных")
        return

    channel = db.get_required_channel_by_position(position)

    if not channel:
        await callback.answer("❌ Канал не найден")
        return

    await state.update_data(edit_position=position, edit_field="username")
    await safe_edit(
        callback.message,
        f"✏️ Изменение username для канала #{position + 1}\n\n"
        f"Текущий username: {channel['username']}\n\n"
        f"Отправьте новый username канала (с символом @):",
        reply_markup=Keyboards.cancel_button()
    )
    await state.set_state(AdminStates.waiting_for_required_channel_username)
    await callback.answer()


# Обработчик для удаления канала из списка действий
@dp.callback_query(F.data.startswith("required_channel_delete_") & ~F.data.startswith("required_channel_delete_select") & ~F.data.startswith("required_channel_confirm_delete_"))
async def required_channel_delete(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    # Формат: required_channel_delete_{position}
    parts = callback.data.split("_")
    try:
        position = int(parts[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат данных")
        return

    channel = db.get_required_channel_by_position(position)

    if not channel:
        await callback.answer("❌ Канал не найден")
        return

    db.delete_required_channel(position)

    # Обновляем подписки для всех пользователей
    required_channels = get_required_channels()
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users")
        users = cursor.fetchall()
        for user in users:
            # Удаляем старые подписки на удаленный канал
            cursor.execute(
                "DELETE FROM required_subscriptions WHERE user_id = %s AND channel_link = %s",
                (user[0], channel['link'])
            )
            # Добавляем новые подписки на текущие каналы
            for ch in required_channels:
                db.add_required_subscription(user[0], ch['link'], ch['username'])

    await callback.answer("✅ Канал удален")
    await admin_required_channels(callback)


# Обработчик для выбора канала для удаления (кнопка "Удалить канал" в админке)
@dp.callback_query(F.data == "required_channel_delete_select")
async def required_channel_delete_select(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    required_channels = get_required_channels()
    if not required_channels:
        await callback.answer("❌ Нет каналов для удаления")
        return

    await safe_edit(
        callback.message,
        "🗑 Выберите канал для удаления:",
        reply_markup=Keyboards.required_channels_delete_list(required_channels)
    )
    await callback.answer()


# Обработчик для подтверждения удаления из списка удаления
@dp.callback_query(F.data.startswith("required_channel_confirm_delete_"))
async def required_channel_confirm_delete(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    # Формат: required_channel_confirm_delete_{position}
    parts = callback.data.split("_")
    try:
        position = int(parts[-1])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат данных")
        return

    channel = db.get_required_channel_by_position(position)

    if not channel:
        await callback.answer("❌ Канал не найден")
        return

    db.delete_required_channel(position)

    # Обновляем подписки для всех пользователей
    required_channels = get_required_channels()
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM users")
        users = cursor.fetchall()
        for user in users:
            # Удаляем старые подписки на удаленный канал
            cursor.execute(
                "DELETE FROM required_subscriptions WHERE user_id = %s AND channel_link = %s",
                (user[0], channel['link'])
            )
            # Добавляем новые подписки на текущие каналы
            for ch in required_channels:
                db.add_required_subscription(user[0], ch['link'], ch['username'])

    await callback.answer("✅ Канал удален")
    await admin_required_channels(callback)


@dp.callback_query(F.data == "admin_required_channels_cancel")
async def admin_required_channels_cancel(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    await state.clear()
    await admin_required_channels(callback)


# ==================== ОСТАЛЬНЫЕ ХЭНДЛЕРЫ АДМИНИСТРАТОРА ====================
@dp.callback_query(F.data == "admin_promocodes")
async def admin_promocodes(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    promocodes = db.get_all_promocodes()
    if promocodes:
        await safe_edit(callback.message, "🎟 Управление промокодами",
                        reply_markup=Keyboards.promocodes_list(promocodes))
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Добавить промокод", callback_data="admin_add_promocode")
        kb.button(text="◀️ Назад", callback_data="admin_panel")
        kb.adjust(1)
        await safe_edit(callback.message, "🎟 Нет добавленных промокодов.", reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data == "admin_add_promocode")
async def add_promocode_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    await safe_edit(
        callback.message,
        "➕ Добавление промокода\n\n"
        "Введите промокод в формате:\n"
        "<b>КОД КОЛИЧЕСТВО_АКТИВАЦИЙ КОЛИЧЕСТВО_ЗВЕЗД</b>\n\n"
        "Пример: PROMO100 100 50\n\n"
        "Где:\n"
        "— КОД: слово или слово с цифрами\n"
        "— КОЛИЧЕСТВО_АКТИВАЦИЙ: сколько раз можно активировать\n"
        "— КОЛИЧЕСТВО_ЗВЕЗД: сколько звезд получит игрок",
        reply_markup=Keyboards.cancel_button()
    )
    await state.set_state(AdminStates.waiting_for_add_promocode)
    await callback.answer()


@dp.message(AdminStates.waiting_for_add_promocode)
async def process_add_promocode(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        await state.clear()
        return

    try:
        parts = message.text.strip().split()
        if len(parts) != 3:
            await message.answer(
                "❌ Неверный формат!\n"
                "Используйте: КОД КОЛИЧЕСТВО_АКТИВАЦИЙ КОЛИЧЕСТВО_ЗВЕЗД\n"
                "Пример: PROMO100 100 50"
            )
            return

        code = parts[0].upper()
        max_uses = int(parts[1])
        stars = float(parts[2].replace(',', '.'))

        if max_uses <= 0:
            await message.answer("❌ Количество активаций должно быть больше 0")
            return
        if stars <= 0:
            await message.answer("❌ Количество звезд должно быть больше 0")
            return

        db.add_promocode(code, stars, max_uses, message.from_user.id)

        await message.answer(
            f"✅ Промокод успешно добавлен!\n\n"
            f"🎟 Код: <b>{code}</b>\n"
            f"💰 Награда: {db.format_number(stars)}⭐\n"
            f"🔢 Лимит активаций: {max_uses}",
            parse_mode="HTML",
            reply_markup=Keyboards.admin_panel()
        )
    except ValueError:
        await message.answer("❌ Ошибка: количество активаций — целое число, звёзды — число.\nПример: PROMO 10 25")
        return
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
        return
    finally:
        await state.clear()


@dp.callback_query(F.data.startswith("promocode_"))
async def promocode_action(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    parts = callback.data.split("_")
    if len(parts) == 2:
        code = parts[1]
        promo = db.get_promocode(code)
        if promo:
            await safe_edit(
                callback.message,
                f"Промокод: <b>{promo['code']}</b>\n"
                f"💰 Награда: {db.format_number(promo['stars'])}⭐\n"
                f"📊 Использований: {promo['used']}/{promo['max_uses']}",
                reply_markup=Keyboards.promocode_actions(code)
            )
    elif len(parts) == 3 and parts[1] == "delete":
        code = parts[2]
        db.delete_promocode(code)
        await admin_promocodes(callback)
        await callback.answer("🗑 Промокод удалён")
        return

    await callback.answer()


@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    users = db.get_all_users()
    await safe_edit(callback.message, "👥 Список пользователей:", reply_markup=Keyboards.users_list(users, 0))
    await callback.answer()


@dp.callback_query(F.data.startswith("users_page_"))
async def users_page(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    page = int(callback.data.split("_")[2])
    users = db.get_all_users()
    await safe_edit(callback.message, "👥 Список пользователей:", reply_markup=Keyboards.users_list(users, page))
    await callback.answer()


@dp.callback_query(
    F.data.startswith("user_") &
    ~F.data.startswith("user_addstars_") &
    ~F.data.startswith("user_delstars_") &
    ~F.data.startswith("user_setbalance_") &
    ~F.data.startswith("user_referrals_") &
    ~F.data.startswith("user_ban_") &
    ~F.data.startswith("user_unban_")
)
async def user_detail(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    try:
        user_id = int(callback.data.split("_")[1])
    except (IndexError, ValueError):
        await callback.answer("❌ Неверный формат данных")
        return

    user = db.get_user(user_id)
    if not user:
        await callback.answer("❌ Пользователь не найден")
        return

    text = (
        f"👤 Информация о пользователе\n\n"
        f"🆔 ID: {user['id']}\n"
        f"👤 Имя: {user.get('full_name', 'Не указано')}\n"
        f"📝 Username: @{user.get('username', 'Нет')}\n"
        f"💰 Баланс: {db.format_number(user['balance'])}⭐\n"
        f"📈 Заработано: {db.format_number(user['total_earned'])}⭐\n"
        f"💸 Выведено: {db.format_number(user['total_withdrawn'])}⭐\n"
        f"🚫 Статус: {'🔴 Заблокирован' if user['is_banned'] else '🟢 Активен'}"
    )

    await safe_edit(callback.message, text, reply_markup=Keyboards.user_actions(user_id, user['is_banned']))
    await callback.answer()


@dp.callback_query(F.data.startswith("user_addstars_"))
async def user_add_stars(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    user_id = int(callback.data.split("_")[2])
    await safe_edit(callback.message, "💰 Введите сумму звезд для выдачи:")
    await state.update_data(action="addstars", user_id=user_id)
    await state.set_state(AdminStates.waiting_for_user_action)
    await callback.answer()


@dp.callback_query(F.data.startswith("user_delstars_"))
async def user_del_stars(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    user_id = int(callback.data.split("_")[2])
    await safe_edit(callback.message, "🔨 Введите сумму звезд для списания:")
    await state.update_data(action="delstars", user_id=user_id)
    await state.set_state(AdminStates.waiting_for_user_action)
    await callback.answer()


@dp.callback_query(F.data.startswith("user_setbalance_"))
async def user_set_balance(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    user_id = int(callback.data.split("_")[2])
    await safe_edit(callback.message, "🎯 Введите новую сумму баланса:")
    await state.update_data(action="setbalance", user_id=user_id)
    await state.set_state(AdminStates.waiting_for_user_action)
    await callback.answer()


@dp.callback_query(F.data.startswith("user_ban_"))
async def user_ban(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    user_id = int(callback.data.split("_")[2])
    db.ban_user(user_id)
    await callback.answer("✅ Пользователь заблокирован")
    await user_detail(callback)


@dp.callback_query(F.data.startswith("user_unban_"))
async def user_unban(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    user_id = int(callback.data.split("_")[2])
    db.unban_user(user_id)
    await callback.answer("✅ Пользователь разблокирован")
    await user_detail(callback)


@dp.message(AdminStates.waiting_for_user_action)
async def process_user_action(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        await state.clear()
        return

    try:
        data = await state.get_data()
        action = data.get('action')
        user_id = data.get('user_id')
        amount = float(message.text.replace(',', '.'))

        if action == "addstars":
            db.update_user_balance(user_id, amount, add_to_earned=True)
            await message.answer(f"✅ Пользователю выдано {db.format_number(amount)}⭐")
            try:
                await bot.send_message(user_id, f"🎉 Администратор выдал вам {db.format_number(amount)} звезд!")
            except Exception:
                pass
        elif action == "delstars":
            db.update_user_balance(user_id, -amount)
            await message.answer(f"✅ У пользователя списано {db.format_number(amount)}⭐")
            try:
                await bot.send_message(user_id, f"⚠️ Администратор списал {db.format_number(amount)} звезд.")
            except Exception:
                pass
        elif action == "setbalance":
            db.set_user_balance(user_id, amount)
            await message.answer(f"✅ Баланс установлен: {db.format_number(amount)}⭐")
            try:
                await bot.send_message(user_id,
                                       f"🎯 Администратор установил ваш баланс: {db.format_number(amount)}⭐")
            except Exception:
                pass

        await cmd_admin_panel(message)
    except ValueError:
        await message.answer("❌ Введите корректное число")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

    await state.clear()


@dp.callback_query(F.data.startswith("user_referrals_"))
async def user_referrals(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    user_id = int(callback.data.split("_")[2])
    referrals = db.get_referrals(user_id)

    if not referrals:
        await callback.answer("У этого пользователя нет рефералов", show_alert=True)
        return

    text = "👥 Список рефералов:\n\n"
    for ref in referrals:
        text += f"• {ref.get('full_name', ref.get('username', ref['id']))} +{db.format_number(ref['earned'])}⭐\n"

    kb = InlineKeyboardBuilder()
    kb.button(text="◀️ Назад", callback_data=f"user_{user_id}")

    await safe_edit(callback.message, text, reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data == "admin_withdrawals")
async def admin_withdrawals(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    withdrawals = db.get_pending_withdrawals()
    if withdrawals:
        await safe_edit(callback.message, "💸 Ожидающие заявки:",
                        reply_markup=Keyboards.withdrawals_list(withdrawals))
    else:
        kb = InlineKeyboardBuilder()
        kb.button(text="◀️ Назад", callback_data="admin_panel")
        await safe_edit(callback.message, "💸 Нет ожидающих заявок.", reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data.startswith("withdraw_"))
async def withdraw_action(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    parts = callback.data.split("_")
    if len(parts) == 2:
        withdrawal_id = int(parts[1])
        withdrawals = db.get_pending_withdrawals()
        withdrawal = next((w for w in withdrawals if w['id'] == withdrawal_id), None)

        if withdrawal:
            user = db.get_user(withdrawal['user_id'])
            await safe_edit(
                callback.message,
                f"Заявка #{withdrawal_id}\n\n"
                f"👤 {user.get('full_name', withdrawal['user_id']) if user else withdrawal['user_id']}\n"
                f"💰 {db.format_number(withdrawal['amount'])}⭐\n"
                f"💳 {withdrawal['wallet']}",
                reply_markup=Keyboards.withdrawal_actions(withdrawal_id)
            )
    elif len(parts) == 3:
        action = parts[1]
        withdrawal_id = int(parts[2])

        withdrawals = db.get_pending_withdrawals()
        withdrawal = next((w for w in withdrawals if w['id'] == withdrawal_id), None)

        if withdrawal:
            if action == "confirm":
                db.update_withdrawal_status(withdrawal_id, "confirmed", callback.from_user.id)
                try:
                    await bot.send_message(withdrawal['user_id'],
                                           f"✅ Заявка на вывод {db.format_number(withdrawal['amount'])}⭐ подтверждена!")
                except Exception:
                    pass
                await callback.answer("✅ Заявка подтверждена")
            elif action == "pay":
                db.update_withdrawal_status(withdrawal_id, "paid", callback.from_user.id)
                try:
                    await bot.send_message(withdrawal['user_id'],
                                           f"💸 Вывод {db.format_number(withdrawal['amount'])}⭐ выполнен!")
                except Exception:
                    pass
                await callback.answer("💸 Вывод отмечен как оплаченный")
            elif action == "reject":
                db.update_withdrawal_status(withdrawal_id, "rejected", callback.from_user.id)
                try:
                    await bot.send_message(withdrawal['user_id'],
                                           f"❌ Заявка на вывод {db.format_number(withdrawal['amount'])}⭐ отклонена.")
                except Exception:
                    pass
                await callback.answer("❌ Заявка отклонена")

        await admin_withdrawals(callback)
        return

    await callback.answer()


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    stats = db.get_stats()
    text = (
        f"📊 Статистика\n\n"
        f"👥 Пользователей: {stats['total_users']}\n"
        f"🚫 Заблокировано: {stats['banned_users']}\n"
        f"💰 Общий баланс: {db.format_number(stats['total_balance'])}⭐\n"
        f"📈 Заработано: {db.format_number(stats['total_earned'])}⭐\n"
        f"📡 Каналов: {stats['total_channels']}\n"
        f"💸 Ожидает вывод: {stats['pending_withdrawals']}\n"
        f"💵 Выплачено: {db.format_number(stats['total_paid'])}⭐"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="◀️ Назад", callback_data="admin_panel")
    await safe_edit(callback.message, text, reply_markup=kb.as_markup())
    await callback.answer()


@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа")
        return

    await safe_edit(
        callback.message,
        "📣 Рассылка\n\n"
        "Отправьте текст для рассылки.\n"
        "Текст будет отправлен всем пользователям бота.",
        reply_markup=Keyboards.broadcast_cancel()
    )
    await state.set_state(AdminStates.waiting_for_broadcast)
    await callback.answer()


@dp.message(AdminStates.waiting_for_broadcast)
async def process_broadcast(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа")
        await state.clear()
        return

    text = message.text
    users = db.get_all_users_for_broadcast()
    total = len(users)
    sent = 0
    failed = 0

    status_msg = await message.answer(f"📣 Рассылка начата...\nВсего: {total} пользователей")

    for user_id in users:
        try:
            await bot.send_message(user_id, f"📢 Новость:\n\n{text}")
            sent += 1
            if sent % 10 == 0:
                try:
                    await status_msg.edit_text(f"📣 Рассылка: {sent}/{total}")
                except Exception:
                    pass
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    try:
        await status_msg.edit_text(
            f"✅ Рассылка завершена!\n"
            f"📤 Отправлено: {sent}/{total}\n"
            f"❌ Ошибок: {failed}"
        )
    except Exception:
        await message.answer(
            f"✅ Рассылка завершена!\n"
            f"📤 Отправлено: {sent}/{total}\n"
            f"❌ Ошибок: {failed}"
        )

    await state.clear()
    await cmd_admin_panel(message)


# ==================== ЗАПУСК ====================
import aiohttp
from aiohttp import web
import asyncio

async def health(request):
    return web.Response(text="OK")

async def run_web():
    app = web.Application()
    app.router.add_get("/", health)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await asyncio.Event().wait()  # вечно ждать

async def main():
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        logger.info("✅ База данных подключена")
    except Exception as e:
        logger.error(f"❌ Ошибка БД: {e}")
        return

    logger.info("🚀 Easy Stars Bot запущен!")
    print("🚀 Easy Stars Bot запущен!")

    asyncio.create_task(periodic_referral_check())
    asyncio.create_task(run_web())
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())