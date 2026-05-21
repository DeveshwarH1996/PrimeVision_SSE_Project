from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_url: str = "redis://localhost:6379"
    enrichment_retry_count: int = 3
    enrichment_step_delay: float = 0.5
    confidence_threshold: float = 0.5
    staleness_threshold_seconds: int = 60

    model_config = {"env_file": ".env"}


settings = Settings()
