from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Note: All values defined here are defaults and will be overridden automatically
    by environment variables thanks to pydantic-settings.

    For example, the docker-compose.yml file sets environment variables to the following values
        REDIS_NODES=redis://redis-node-1:6379,redis://redis-node-2:6379
        DEBUG=true
    """

    # Redis Configuration
    REDIS_NODES: str = ""  # Comma-separated list of redis URLs

    # Consistent Hashing Config
    VIRTUAL_NODES: int = 100

    # Batch Processing Config
    BATCH_INTERVAL_SECONDS: float = 10.0

    # App Config
    DEBUG: bool = True
    API_PREFIX: str = "/api/v1"

    class Config:
        env_file = ".env"


settings = Settings()
