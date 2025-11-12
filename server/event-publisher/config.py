from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    RABBITMQ_URL: str = "amqp://admin:admin@rabbitmq:5672/"
    EVENTS_DIR: str = "/app/events" 
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

settings = Settings()