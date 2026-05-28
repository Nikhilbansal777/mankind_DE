import requests
import os
import json

def send_slack_alert(message: str, webhook_url: str):
    """
    Sends a formatted error message to a Slack channel via a webhook.

    Args:
        message (str): The detailed error message and traceback.
        webhook_url (str): The Slack incoming webhook URL.
    """
    if not webhook_url:
        # Log this to the console if the URL isn't configured
        print("ERROR: SLACK_WEBHOOK_URL is not set. Cannot send alert.")
        return

    try:
        # We create a richly formatted message using Slack's "Blocks" feature
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "🚨 Data Pipeline Failure"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*A critical error was detected in the data pipeline. Please investigate.*"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f" детали ошибки (Error Details):\n```\n{message}\n```"
                    }
                }
            ]
        }
        # Send the POST request to the Slack webhook URL
        response = requests.post(webhook_url, data=json.dumps(payload), headers={'Content-Type': 'application/json'}, timeout=10)
        response.raise_for_status() # This will raise an error for bad responses (like 404 or 500)
        print("Slack alert sent successfully.")
    except requests.exceptions.RequestException as e:
        # If sending the alert fails, we print the error to the console/log
        print(f"FATAL: Could not send Slack alert. Reason: {e}")