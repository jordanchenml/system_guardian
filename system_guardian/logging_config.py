"""Configure logging for the system guardian application."""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

from loguru import logger

from system_guardian.settings import settings, LogLevel


def configure_loguru(
    sink=sys.stdout,
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    rotation: str = "20 MB",
    retention: str = "1 week",
    format_string: Optional[str] = None,
    serialize: bool = False,
) -> None:
    """
    Configure Loguru logger with given parameters.

    :param sink: Output sink (default: stdout)
    :param level: Log level (default: from settings)
    :param log_file: Optional file path to write logs to
    :param rotation: When to rotate logs (size or time)
    :param retention: How long to keep logs
    :param format_string: Log format string
    :param serialize: Whether to serialize logs as JSON
    """
    # Remove default handlers
    logger.remove()

    # Use the level from settings if not provided
    if level is None:
        level = settings.log_level.value

    # Default format string for general use
    if format_string is None:
        format_string = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )

    # Define a filter for common loggers we want to suppress
    def filter_noisy_loggers(record):
        # 完全禁用這些命名空間的日誌
        blocked_loggers = [
            "sqlalchemy",
            "aio_pika",
            "asyncio",
            "httpx",
            "urllib3",
            "uvicorn.access",
        ]

        # 檢查記錄是否來自這些命名空間
        for logger_name in blocked_loggers:
            if logger_name in record["name"]:
                return False

        # 過濾掉警告訊息中的特定內容
        if (
            "Qdrant client version" in record["message"]
            and "incompatible with server version" in record["message"]
        ):
            return False

        # 允許其他所有記錄
        return True

    # Add stdout handler with filter
    logger.add(
        sink=sink,
        level=level,
        format=format_string,
        colorize=True,
        backtrace=True,
        diagnose=True,
        filter=filter_noisy_loggers,
    )

    # Add file handler if requested
    if log_file:
        # Ensure directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        logger.add(
            sink=log_file,
            level=level,
            format=format_string,
            rotation=rotation,
            retention=retention,
            compression="zip",
            serialize=serialize,
            backtrace=True,
            diagnose=True,
            filter=filter_noisy_loggers,
        )

    logger.debug(f"Configured Loguru with level: {level}")


def configure_sqlalchemy_logging():
    """特別處理 SQLAlchemy 的日誌配置，確保它完全禁用或使用正確的格式"""
    # 方法1：直接禁用所有 SQLAlchemy 記錄器
    for name in [
        "sqlalchemy",
        "sqlalchemy.engine",
        "sqlalchemy.pool",
        "sqlalchemy.orm",
        "sqlalchemy.dialects",
    ]:
        # 完全禁用這些記錄器
        sql_logger = logging.getLogger(name)
        sql_logger.setLevel(logging.CRITICAL + 10)  # 超過最高級別
        sql_logger.disabled = True
        sql_logger.propagate = False

        # 移除任何處理器
        for handler in sql_logger.handlers[:]:
            sql_logger.removeHandler(handler)

    # 方法2：修改默認 Python 日誌處理器
    logging.getLogger().handlers.clear()

    # 方法3：Monkey patch SQLAlchemy的日誌功能
    try:
        from sqlalchemy.engine import Engine
        from sqlalchemy.log import instance_logger, Identified

        # 保存原始方法
        original_instance_logger = instance_logger

        # 定義新的靜音方法
        def silent_instance_logger(cls):
            logger = original_instance_logger(cls)
            # 使記錄器靜音
            logger.disabled = True
            logger.setLevel(logging.CRITICAL + 10)
            return logger

        # 替換方法
        instance_logger = silent_instance_logger

        # 修改 Identified 類的 _should_log_debug 方法
        original_should_log_debug = Identified._should_log_debug

        def silent_should_log_debug(self):
            return False

        Identified._should_log_debug = silent_should_log_debug

    except ImportError:
        # SQLAlchemy可能未導入
        pass


def configure_logging():
    """Configures the application logging."""
    # 首先，確保 Python 的日誌系統不會輸出任何內容
    logging.basicConfig(handlers=[logging.NullHandler()])

    # 立即禁用 SQLAlchemy 日誌
    configure_sqlalchemy_logging()

    # 首先，完全關閉幾個我們不想看到日誌的模組
    for logger_name in [
        "sqlalchemy",
        "aio_pika",
        "asyncio",
        "httpx",
        "urllib3",
        "uvicorn.access",
        "fastapi",
    ]:
        module_logger = logging.getLogger(logger_name)
        module_logger.setLevel(logging.CRITICAL + 10)  # 高於CRITICAL
        module_logger.disabled = True
        # 移除任何可能的處理器
        if module_logger.handlers:
            for handler in module_logger.handlers:
                module_logger.removeHandler(handler)
        # 防止向上傳播日誌
        module_logger.propagate = False

    # 特別處理 sqlalchemy - 完全禁用其日誌
    logging.getLogger("sqlalchemy").disabled = True
    logging.getLogger("sqlalchemy.engine").disabled = True
    logging.getLogger("sqlalchemy.engine.base.Engine").disabled = True

    # Get log level from environment
    level = settings.log_level.value

    # Configure file log path if enabled
    log_file = None
    if settings.enable_file_logging:
        logs_dir = settings.logs_dir or "logs"
        os.makedirs(logs_dir, exist_ok=True)
        date_str = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(logs_dir, f"system_guardian_{date_str}.log")

    # Configure Loguru
    configure_loguru(
        level=level,
        log_file=log_file,
        serialize=settings.structured_logging,
    )

    # 設置一個 NULL 處理器作為回退
    null_handler = logging.NullHandler()
    logging.getLogger().addHandler(null_handler)

    # 為每個 SQLAlchemy 日誌器設置攔截處理器
    sql_interceptor = SQLAlchemyInterceptor()
    for name in [
        "sqlalchemy",
        "sqlalchemy.engine",
        "sqlalchemy.pool",
        "sqlalchemy.orm",
        "sqlalchemy.dialects",
    ]:
        logger = logging.getLogger(name)
        logger.handlers = [sql_interceptor]  # 替換所有處理器

    # 安裝攔截器 - 確保所有標準庫日誌都通過 loguru 重定向
    intercept_handler = InterceptHandler()
    intercept_handler.intercept_all_loggers()

    # Log startup message
    logger.info(f"Logging configured with level {level}")
    if log_file:
        logger.info(f"Logging to file: {log_file}")


# 針對 SQLAlchemy 的特殊攔截器
class SQLAlchemyInterceptor(logging.Handler):
    """特別處理 SQLAlchemy 日誌的處理器"""

    def __init__(self):
        super().__init__()
        self.level = logging.CRITICAL + 10  # 超過最高級別

    def emit(self, record):
        """永遠不發出日誌"""
        pass


# Intercept standard library logging
class InterceptHandler(logging.Handler):
    """
    Intercepts standard library logging and redirects to loguru.

    This handler is needed to capture logs from libraries that use
    the standard logging module.
    """

    def __init__(self):
        super().__init__()
        self.handled_loggers = set()

    def intercept_all_loggers(self):
        """Intercept all existing loggers."""
        # Get standard library root logger
        root_logger = logging.getLogger()

        # Remove any existing handlers
        if root_logger.handlers:
            for handler in root_logger.handlers[:]:
                if not isinstance(handler, logging.NullHandler):
                    root_logger.removeHandler(handler)

        # Add our handler to the root logger
        root_logger.addHandler(self)
        root_logger.setLevel(logging.WARNING)  # 只關注重要消息

        # 禁用特定的日誌logger - 使用更強的禁用方法
        for unwanted_logger in ["sqlalchemy", "aio_pika", "asyncio"]:
            for suffix in ["", ".engine", ".pool", ".orm", ".dialects"]:
                log_name = unwanted_logger + suffix
                logging.getLogger(log_name).setLevel(logging.CRITICAL + 10)
                logging.getLogger(log_name).disabled = True
                logging.getLogger(log_name).propagate = False
                # 確保沒有處理器
                for handler in logging.getLogger(log_name).handlers[:]:
                    logging.getLogger(log_name).removeHandler(handler)
                # 添加空處理器
                logging.getLogger(log_name).addHandler(logging.NullHandler())

        # Iterate through all loggers and configure them
        for logger_name in logging.root.manager.loggerDict:
            # 跳過不想處理的日誌
            if any(
                name in logger_name for name in ["sqlalchemy", "aio_pika", "asyncio"]
            ):
                continue
            self._intercept_logger(logger_name)

        # Special case for uvicorn loggers
        for logger_name in ("uvicorn", "uvicorn.access"):
            ulogger = logging.getLogger(logger_name)
            # Remove existing handlers
            if ulogger.handlers:
                for handler in ulogger.handlers[:]:
                    ulogger.removeHandler(handler)
            ulogger.addHandler(self)
            if logger_name == "uvicorn.access":
                ulogger.setLevel(logging.CRITICAL)
            else:
                ulogger.setLevel(logging.WARNING)

        logger.debug("Intercepted all standard library loggers")

    def _intercept_logger(self, logger_name: str):
        """Intercept a specific logger."""
        if logger_name in self.handled_loggers:
            return

        log = logging.getLogger(logger_name)

        # 完全禁用不想要的日誌
        if any(
            name in logger_name
            for name in ["sqlalchemy", "aio_pika", "asyncio", "httpx", "urllib3"]
        ):
            log.setLevel(logging.CRITICAL + 10)
            log.disabled = True
            log.propagate = False
            # 確保沒有處理器
            if log.handlers:
                for handler in log.handlers[:]:
                    log.removeHandler(handler)
            # 添加空處理器
            log.addHandler(logging.NullHandler())
            return

        # Remove existing handlers
        if log.handlers:
            for handler in log.handlers[:]:
                log.removeHandler(handler)
        log.addHandler(self)
        log.propagate = False
        self.handled_loggers.add(logger_name)

    def __call__(self, record):
        """Handler implementation that redirects to loguru."""
        self.emit(record)

    # Additional methods required for a proper Handler subclass
    def createLock(self):
        """No locking needed for this handler."""
        self.lock = None

    def acquire(self):
        """No lock to acquire."""
        pass

    def release(self):
        """No lock to release."""
        pass

    def setLevel(self, level):
        """Set the logging level."""
        self.level = level

    def setFormatter(self, formatter):
        """Set the formatter for this handler."""
        self.formatter = formatter

    def flush(self):
        """Ensure all logging output has been flushed."""
        pass

    def close(self):
        """Tidy up any resources used by the handler."""
        pass

    def emit(self, record):
        """
        Emit a record - standard logging Handler interface.

        This implements the logging.Handler interface method.
        """
        # 檢查記錄是否應該被完全忽略
        if any(
            name in record.name.lower()
            for name in ["sqlalchemy", "aio_pika", "httpx", "asyncio", "urllib3"]
        ):
            return

        # 檢查記錄的級別是否達到了對應日誌器設置的閾值
        logger_obj = logging.getLogger(record.name)
        if logger_obj.disabled or record.levelno < logger_obj.level:
            return  # 不處理低於設置閾值的日誌

        # 避免特定消息內容
        if record.getMessage() and (
            "raw sql" in record.getMessage()
            or "generated in" in record.getMessage()
            or "cached since" in record.getMessage()
            or "HTTP Request" in record.getMessage()
            or "Qdrant client version" in record.getMessage()
        ):
            return

        # Get corresponding Loguru level
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message
        frame = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        # 避免記錄uvicorn和fastapi的詳細日誌
        if (
            any(prefix in record.name for prefix in ["uvicorn", "fastapi"])
            and record.levelno < logging.WARNING
        ):
            return

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage(),
        )
