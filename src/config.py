"""Configuration module for Kleinanzeigen Telegram Bot - Multi-user version."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Application configuration from environment variables."""
    
    # Telegram settings - only BOT_TOKEN needed, chat IDs come from users dynamically
    TG_BOT_TOKEN: str = os.getenv("TG_BOT_TOKEN", "")
    
    # Monitoring settings (defaults, users can override)
    INTERVAL_MINUTES: int = int(os.getenv("INTERVAL_MINUTES", "5"))
    # 0 means "no limit" per cycle
    MAX_NEW_PER_CYCLE: int = int(os.getenv("MAX_NEW_PER_CYCLE", "0"))
    MAX_TEST_LISTINGS: int = int(os.getenv("MAX_TEST_LISTINGS", "10"))  # Limit for /test command
    MAX_LISTINGS_PER_QUERY: int = int(os.getenv("MAX_LISTINGS_PER_QUERY", "50"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "20"))
    
    # Monitoring URL (fixed search feed)
    SEARCH_URL: str = os.getenv(
        "SEARCH_URL",
        "https://www.kleinanzeigen.de/s-autos/chemnitz/c216l3869r150"
    )
    
    # HTTP settings
    USER_AGENT: str = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    
    # Database
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "data/kleinanzeigen.db")
    
    # Rate limiting
    MIN_DELAY_BETWEEN_REQUESTS: float = 2.0  # seconds
    MAX_DELAY_BETWEEN_REQUESTS: float = 5.0  # seconds
    MIN_DELAY_BETWEEN_MESSAGES: float = 0.3  # seconds
    MAX_DELAY_BETWEEN_MESSAGES: float = 1.0  # seconds
    
    # Retry settings
    MAX_RETRIES: int = 2
    RETRY_DELAY: float = 3.0  # seconds
    
    @classmethod
    def validate(cls) -> list[str]:
        """Validate required configuration. Returns list of errors."""
        errors = []
        if not cls.TG_BOT_TOKEN:
            errors.append("TG_BOT_TOKEN is required")
        if not cls.SEARCH_URL:
            errors.append("SEARCH_URL is required")
        return errors
    
    @classmethod
    def ensure_data_dir(cls) -> None:
        """Ensure the data directory exists."""
        db_path = Path(cls.DATABASE_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)


# Known car brands for title parsing fallback
KNOWN_BRANDS = [
    "Alfa Romeo", "Audi", "BMW", "Chevrolet", "Citroën", "Dacia", "Fiat",
    "Ford", "Honda", "Hyundai", "Jaguar", "Jeep", "Kia", "Land Rover",
    "Lexus", "Mazda", "Mercedes-Benz", "Mercedes", "Mini", "Mitsubishi",
    "Nissan", "Opel", "Peugeot", "Porsche", "Renault", "Seat", "Skoda",
    "Smart", "Subaru", "Suzuki", "Tesla", "Toyota", "Volkswagen", "VW",
    "Volvo"
]
