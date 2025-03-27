#!/usr/bin/env python
"""
Test script for sending incident notifications to Slack.
Usage: python -m system_guardian.services.slack.test_notification
"""

import asyncio
import logging
import sys
from datetime import datetime

from system_guardian.services.slack.client import SlackClient
from system_guardian.services.slack.templates import SlackMessageTemplate, AlertSeverity
from system_guardian.settings import settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


async def test_slack_notification():
    """Test Slack notification by sending a test incident message."""
    # Check if Slack is enabled and configured
    if not settings.slack_enabled:
        logger.error(
            "Slack notifications are disabled. Set SYSTEM_GUARDIAN_SLACK_ENABLED=true to enable.",
        )
        return False

    if not settings.slack_bot_token:
        logger.error(
            "Slack bot token is not configured. Set SYSTEM_GUARDIAN_SLACK_BOT_TOKEN.",
        )
        return False

    if not settings.slack_channel_id:
        logger.error(
            "Slack channel ID is not configured. Set SYSTEM_GUARDIAN_SLACK_CHANNEL_ID.",
        )
        return False

    try:
        # Initialize Slack client
        slack_client = SlackClient(
            token=settings.slack_bot_token,
            default_channel=settings.slack_channel_id,
            username=settings.slack_username,
            icon_emoji=settings.slack_icon_emoji,
        )

        # Create a test incident message using create_incident_notification
        template = SlackMessageTemplate.create_incident_notification(
            incident_id="TEST-999",
            title="Test Incident Notification",
            severity=AlertSeverity.MEDIUM,
            description="This is a test incident notification to verify Slack integration.",
            timestamp=datetime.now().isoformat(),
        )

        # Send the test message
        logger.info(
            f"Sending test incident notification to channel: {settings.slack_channel_id}",
        )
        response = await slack_client.send_template(template)

        if response.get("ok", False):
            logger.info("✅ Test notification sent successfully!")
            return True
        else:
            logger.error(
                f"❌ Failed to send test notification: {response.get('error', 'unknown error')}",
            )
            return False

    except Exception as e:
        logger.error(f"❌ Error sending test notification: {str(e)}")
        return False


async def main():
    """Run the test and exit with appropriate status code."""
    success = await test_slack_notification()
    if success:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
