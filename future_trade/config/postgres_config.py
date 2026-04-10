"""
PostgreSQL Configuration for future_trade.
Uses pydantic-settings for type-safe configuration with .env support.
"""
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).parent.parent


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    # Tushare API
    tushare_token: str = "e3005d0c8df82146706128c35263800b863396c059201688e1fc1ace"
    
    # PostgreSQL
    pg_host: str = "127.0.0.1"
    pg_port: int = 5432
    pg_database: str = "future_trade"
    pg_user: str = "postgres"
    pg_password: str = "161514"
    
    # Sync settings
    sync_start_date: str = "20100101"
    api_rate_limit: int = 120  # requests per minute (Tushare 2000 points tier)
    
    # Futures exchanges
    exchanges: list[str] = ["CFFEX", "DCE", "CZCE", "SHFE", "INE", "GFEX"]
    
    @property
    def database_url(self) -> str:
        """Get PostgreSQL connection URL."""
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}"
    
    @property
    def api_interval(self) -> float:
        """Calculate interval between API calls in seconds."""
        return 60.0 / self.api_rate_limit


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
