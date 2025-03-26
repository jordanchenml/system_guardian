import enum
import os
from pathlib import Path
from tempfile import gettempdir
from typing import List, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict
from yarl import URL

TEMP_DIR = Path(gettempdir())


class LogLevel(str, enum.Enum):  # noqa: WPS600
    """Possible log levels."""

    NOTSET = "NOTSET"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    FATAL = "FATAL"


class Settings(BaseSettings):
    """
    Application settings.

    These parameters can be configured
    with environment variables.
    """

    host: str = "localhost"
    port: int = 5566
    # quantity of workers for uvicorn
    workers_count: int = 1
    # Enable uvicorn reloading
    reload: bool = False

    # Current environment
    environment: str = "dev"

    log_level: LogLevel = LogLevel.INFO
    enable_file_logging: bool = False
    logs_dir: Optional[str] = None
    structured_logging: bool = False

    # Variables for the database
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "system_guardian"
    db_pass: str = "system_guardian"
    db_base: str = "system_guardian"
    db_echo: bool = False

    # Variables for RabbitMQ
    # rabbit_host: str = "system_guardian-rmq"
    rabbit_host: str = "localhost"
    rabbit_port: int = 5672
    rabbit_user: str = "guest"
    rabbit_pass: str = "guest"
    rabbit_vhost: str = "/"

    rabbit_pool_size: int = 2
    rabbit_channel_pool_size: int = 10

    # kafka_bootstrap_servers: List[str] = ["system_guardian-kafka:9092"]
    kafka_bootstrap_servers: List[str] = ["localhost:9092"]
    
    # Qdrant Vector DB settings
    qdrant_host: str = "localhost"
    qdrant_port: int = 6333
    qdrant_grpc_port: int = 6334
    qdrant_api_key: Optional[str] = None
    qdrant_timeout: int = 10  # Seconds
    
    # OpenAI settings
    openai_api_key: str = os.getenv("SYSTEM_GUARDIAN_OPENAI_API_KEY")
    openai_embedding_model: str = "text-embedding-ada-002"
    openai_completion_model: str = "gpt-3.5-turbo"
    
    # AI task-specific model settings
    ai_incident_detection_model: str = "gpt-3.5-turbo"  # 用於事件檢測和分析的模型
    ai_severity_classification_model: str = "gpt-3.5-turbo"  # 用於嚴重性分類的模型
    ai_similarity_search_model: str = "gpt-3.5-turbo"  # 用於相似度搜索的模型
    ai_resolution_generation_model: str = "gpt-4"  # 用於解決方案生成的模型
    ai_root_cause_analysis_model: str = "gpt-4"  # 用於根因分析的模型
    ai_report_generation_model: str = "gpt-4"  # 用於報告生成的模型
    ai_trend_analysis_model: str = "gpt-3.5-turbo"  # 用於趨勢分析的模型
    
    # 是否允許使用高級模型 (例如 GPT-4) 
    ai_allow_advanced_models: bool = True
    
    # 模型溫度設置
    ai_default_temperature: float = 0.3  # 低溫以獲得更一致的結果
    ai_creative_temperature: float = 0.7  # 高溫用於需要創造性的任務

    @property
    def db_url(self) -> URL:
        """
        Assemble database URL from settings.

        :return: database URL.
        """
        return URL.build(
            scheme="postgresql+asyncpg",
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pass,
            path=f"/{self.db_base}",
        )

    @property
    def rabbit_url(self) -> URL:
        """
        Assemble RabbitMQ URL from settings.

        :return: rabbit URL.
        """
        return URL.build(
            scheme="amqp",
            host=self.rabbit_host,
            port=self.rabbit_port,
            user=self.rabbit_user,
            password=self.rabbit_pass,
            path=self.rabbit_vhost,
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="SYSTEM_GUARDIAN_",
    )


settings = Settings()
