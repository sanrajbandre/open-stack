"""
Slack Notifier
==============

This module provides a simple interface for posting messages to a Slack
channel via an incoming webhook.  Incoming webhooks are the
recommended mechanism for programmatically sending notifications into
Slack without requiring a full OAuth flow.

To configure the notifier, supply the ``slack_webhook_url`` field on
the Config object.  If the webhook is not set, calls to
``send_message`` will be logged as warnings and no message will be
sent.

For detailed instructions on creating Slack webhooks, refer to the
official documentation: https://api.slack.com/messaging/webhooks
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

import requests

from .config import Config

LOGGER = logging.getLogger(__name__)


class SlackNotifier:
    """Send notifications to Slack via webhook."""

    def __init__(self, config: Config, session: Optional[requests.Session] = None) -> None:
        self.webhook_url = config.slack_webhook_url
        self.session = session or requests.Session()

    def send_message(self, text: str, attachments: Optional[Dict[str, Any]] = None) -> bool:
        """Send a message to Slack.

        :param text: Primary text to send.  Slack will render this in
            the channel.
        :param attachments: Optional attachments payload.  Attachments
            provide richer formatting (see Slack documentation).
        :returns: ``True`` if the message was accepted by Slack,
            ``False`` otherwise.
        """
        if not self.webhook_url:
            LOGGER.warning("Slack webhook URL not configured; message not sent")
            return False
        payload = {"text": text}
        if attachments:
            payload["attachments"] = [attachments]
        try:
            resp = self.session.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if resp.status_code == 200:
                LOGGER.info("Slack message sent successfully")
                return True
            LOGGER.error(
                "Failed to send Slack message: status %s response %s", resp.status_code, resp.text
            )
        except Exception as exc:
            LOGGER.error("Error sending Slack message: %s", exc)
        return False
