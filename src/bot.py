"""Telegram bot command handlers for Kleinanzeigen monitor with constant search URL."""

import logging
from typing import Optional, Callable, Awaitable

from telegram import BotCommand, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

from .config import Config
from .database import Database

logger = logging.getLogger(__name__)


class KleinanzeigenBot:
    """Telegram bot for managing subscriptions to the constant Kleinanzeigen search."""
    
    def __init__(
        self, 
        database: Database, 
        on_test_callback: Optional[Callable[[int], Awaitable[dict]]] = None
    ):
        self.db = database
        self.on_test_callback = on_test_callback
        self.application: Optional[Application] = None
    
    async def send_message(self, chat_id: int, text: str) -> bool:
        """Send a message to a specific chat."""
        if not self.application:
            logger.error("Bot application not initialized")
            return False
        
        try:
            await self.application.bot.send_message(
                chat_id=chat_id,
                text=text,
                disable_web_page_preview=False
            )
            return True
        except Exception as exc:
            logger.error(f"Failed to send message to {chat_id}: {exc}")
            return False
    
    def setup_handlers(self, application: Application) -> None:
        """Set up minimal command handlers for subscription flow."""
        self.application = application
        
        application.add_handler(CommandHandler("start", self.cmd_start))
        application.add_handler(CommandHandler("status", self.cmd_status))
        application.add_handler(CommandHandler("test", self.cmd_test))
        application.add_handler(CommandHandler("stop", self.cmd_stop))
    
    async def setup_menu(self) -> None:
        """Configure the Telegram command menu (left-bottom button)."""
        if not self.application:
            logger.warning("Cannot set menu before application is initialized")
            return
        
        commands = [
            BotCommand("start", "Подписаться на поиск автомобилей"),
            BotCommand("status", "Показать текущую статистику"),
            BotCommand("test", "Запустить проверку прямо сейчас"),
            BotCommand("stop", "Отключиться от уведомлений"),
        ]
        
        await self.application.bot.set_my_commands(commands)
    
    def _register_user(self, update: Update) -> int:
        """Register user (or refresh data) and ensure default query bound."""
        chat_id = update.effective_chat.id
        user = update.effective_user
        username = user.username if user else None
        first_name = user.first_name if user else None
        
        self.db.register_user(chat_id, username, first_name)
        self.db.ensure_default_query(chat_id)
        return chat_id
    
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Subscribe the user to notifications from the constant search URL."""
        chat_id = self._register_user(update)
        
        await update.message.reply_text(
            "🤖� *Специально для вас — Харьковский Терминатор уже в деле!* 💥🤖\n\n"
            "🚗🔎 Он возьмёт на себя поиск свежих автомобилей на Kleinanzeigen.\n"
            f"📡 Слежу за этой ссылкой:\n{Config.SEARCH_URL}\n\n"
            "✨ Как только появится что-то новое, сразу пришлю уведомление.\n"
            "ℹ️ /status — статистика, /stop — отключить подписку.",
            parse_mode='Markdown'
        )
        logger.info("User %s subscribed to default feed", chat_id)
    
    async def cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Disable notifications for the user."""
        chat_id = update.effective_chat.id
        self.db.disable_user_queries(chat_id)
        await update.message.reply_text(
            "⏸ Уведомления приостановлены. Отправь /start, чтобы включить снова."
        )
        logger.info("User %s unsubscribed", chat_id)
    
    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show simple status for the user."""
        chat_id = update.effective_chat.id
        self.db.register_user(chat_id)  # refresh last_active
        
        subscribed = self.db.has_enabled_query(chat_id)
        seen_count = self.db.get_seen_listings_count(chat_id)
        last_check = self.db.get_last_check(chat_id)
        stats = self.db.get_stats_summary(chat_id)
        
        message = "📊 *Статус подписки*\n\n"
        message += f"🔗 Поиск: {Config.SEARCH_URL}\n"
        message += f"📬 Подписка: {'активна' if subscribed else 'выключена'}\n"
        message += f"📝 Отправлено объявлений: {seen_count}\n"
        
        if last_check:
            message += "\n*Последняя проверка:*\n"
            message += f"• Время: {last_check['check_time'][:19]}\n"
            message += f"• Найдено: {last_check['total_found']}\n"
            message += f"• Новых: {last_check['new_found']}\n"
        
        message += "\n*Всего за всё время:*\n"
        message += f"• Проверок: {stats['total_checks']}\n"
        message += f"• Новых объявлений: {stats['total_new_found']}\n"
        if stats['total_errors']:
            message += f"• Ошибок: {stats['total_errors']}"
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def cmd_test(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Trigger manual check for the user."""
        chat_id = self._register_user(update)
        
        if not self.on_test_callback:
            await update.message.reply_text("❌ Тестовая проверка недоступна.")
            return
        
        await update.message.reply_text(
            "🔄 Запускаю проверку постоянного поиска...\n"
            "Это может занять до минуты."
        )
        
        try:
            context.application.create_task(self._run_test_callback(chat_id))
        except Exception as exc:
            logger.error("Error running test: %s", exc)
            await update.message.reply_text(f"❌ Ошибка: {exc}")
    
    async def _run_test_callback(self, chat_id: int) -> None:
        """Execute /test callback and report results."""
        if not self.on_test_callback:
            return
        
        try:
            result = await self.on_test_callback(chat_id)
            if result:
                await self.send_message(
                    chat_id,
                    "✅ Проверка завершена.\n"
                    f"Найдено: {result.get('total', 0)}\n"
                    f"Новых: {result.get('new', 0)}"
                )
        except Exception as exc:
            logger.error("Test callback error for %s: %s", chat_id, exc)
            await self.send_message(chat_id, f"❌ Ошибка проверки: {exc}")
