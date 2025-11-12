from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PRODUCT_BASE_URL: str = "http://localhost:8002/products"
    DATABASE_URL: str = "sqlite:///./image.db"
    LOG_LEVEL: str = "INFO"
    HTTP_TIMEOUT: int = 5
    HTTP_RETRIES: int = 2

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
