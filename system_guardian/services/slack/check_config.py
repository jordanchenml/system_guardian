#!/usr/bin/env python
"""
檢查 Slack 配置的腳本，確保所有必要的環境變數都已正確設置。
用法: python -m system_guardian.services.slack.check_config
"""

import os
import sys
from loguru import logger
from system_guardian.settings import settings


def check_slack_config():
    """檢查 Slack 配置是否正確設置"""

    logger.info("正在檢查 Slack 配置...")

    # 檢查是否啟用 Slack
    if not settings.slack_enabled:
        logger.error("❌ Slack 通知未啟用")
        logger.info("請設置環境變數: SYSTEM_GUARDIAN_SLACK_ENABLED=true")
    else:
        logger.success("✅ Slack 通知已啟用")

    # 檢查 Bot Token
    if not settings.slack_bot_token:
        logger.error("❌ 未設置 Slack Bot Token")
        logger.info("請設置環境變數: SYSTEM_GUARDIAN_SLACK_BOT_TOKEN=xoxb-...")
    else:
        masked_token = (
            f"{settings.slack_bot_token[:10]}...{settings.slack_bot_token[-4:]}"
        )
        logger.success(f"✅ 已設置 Slack Bot Token: {masked_token}")

    # 檢查頻道 ID
    if not settings.slack_channel_id:
        logger.error("❌ 未設置 Slack 頻道 ID")
        logger.info("請設置環境變數: SYSTEM_GUARDIAN_SLACK_CHANNEL_ID=C0123456789")
    else:
        logger.success(f"✅ 已設置 Slack 頻道 ID: {settings.slack_channel_id}")

    # 檢查其他選項設置
    logger.info(f"Slack 用戶名: {settings.slack_username}")
    logger.info(f"Slack 表情符號: {settings.slack_icon_emoji}")
    logger.info(f"Slack 超時: {settings.slack_timeout} 秒")

    # 總結
    if (
        settings.slack_enabled
        and settings.slack_bot_token
        and settings.slack_channel_id
    ):
        logger.success("✅ Slack 配置已正確設置，可以發送通知")
        return True
    else:
        logger.error("❌ Slack 配置不完整，無法發送通知")
        return False


def main():
    """主函數"""
    success = check_slack_config()

    if not success:
        logger.info(
            """
要啟用 Slack 通知，請設置以下環境變數:
export SYSTEM_GUARDIAN_SLACK_ENABLED=true
export SYSTEM_GUARDIAN_SLACK_BOT_TOKEN=xoxb-your-bot-token
export SYSTEM_GUARDIAN_SLACK_CHANNEL_ID=your-channel-id
        """,
        )
        sys.exit(1)
    else:
        logger.info("您可以運行以下命令測試 Slack 通知:")
        logger.info("python -m system_guardian.services.slack.test_notification")
        sys.exit(0)


if __name__ == "__main__":
    main()
