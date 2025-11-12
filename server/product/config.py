from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    SUPPLIER_BASE_URL: str = "http://localhost:8001/suppliers"
    CATEGORY_BASE_URL: str = "http://localhost:8003/categories"
    IMAGE_BASE_URL: str = "http://localhost:8004/images"
    DATABASE_URL: str = "sqlite:///./product.db"
    LOG_LEVEL: str = "INFO"
    HTTP_TIMEOUT: int = 5
    HTTP_RETRIES: int = 2

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()
