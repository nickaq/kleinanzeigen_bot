"""Main entry point for Kleinanzeigen Telegram bot - Multi-user version."""

import logging
import sys
from typing import Optional

from telegram.ext import Application

from .bot import KleinanzeigenBot
from .config import Config
from .database import Database
from .scheduler import Scheduler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('kleinanzeigen_bot.log')
    ]
)

logger = logging.getLogger(__name__)


class BotApplication:
    """Main application class combining bot and scheduler - Multi-user."""
    
    def __init__(self) -> None:
        self.database: Optional[Database] = None
        self.bot: Optional[KleinanzeigenBot] = None
        self.scheduler: Optional[Scheduler] = None
        self.application: Optional[Application] = None
    
    def run(self) -> None:
        """Validate configuration and start polling loop."""
        errors = Config.validate()
        if errors:
            for error in errors:
                logger.error(f"Configuration error: {error}")
            sys.exit(1)
        
        Config.ensure_data_dir()
        logger.info("Initializing database...")
        self.database = Database()
        
        logger.info("Starting Telegram bot...")
        self.application = Application.builder().token(Config.TG_BOT_TOKEN).build()
        
        self.bot = KleinanzeigenBot(
            database=self.database,
            on_test_callback=self._on_test
        )
        self.bot.setup_handlers(self.application)
        
        self.scheduler = Scheduler(
            database=self.database,
            send_message=self.bot.send_message
        )
        
        # Register lifecycle hooks for scheduler control
        self.application.post_init = self._on_application_start
        self.application.post_shutdown = self._on_application_shutdown
        
        logger.info("Bot started in MULTI-USER mode. Press Ctrl+C to stop.")
        self.application.run_polling(drop_pending_updates=True)
    
    async def _on_application_start(self, app: Application) -> None:
        """Hook executed by PTB once the application is initialized."""
        del app  # unused but kept for signature compatibility
        if self.bot:
            await self.bot.setup_menu()
        if self.scheduler:
            self.scheduler.start()
            logger.info("Scheduler started via post_init hook")
    
    async def _on_application_shutdown(self, app: Application) -> None:
        """Hook executed by PTB after shutdown to stop scheduler."""
        del app  # unused but kept for signature compatibility
        if self.scheduler:
            self.scheduler.stop()
            logger.info("Scheduler stopped via post_shutdown hook")
    
    async def _on_test(self, chat_id: int) -> dict:
        """Handle /test command callback for a specific user."""
        if self.scheduler:
            return await self.scheduler.run_check_for_user(chat_id)
        return {"total": 0, "new": 0}


def main() -> None:
    """Main entry point."""
    app = BotApplication()
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")


if __name__ == "__main__":
    main()
