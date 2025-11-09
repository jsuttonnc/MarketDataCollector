import os
import requests

def send_pushover_notification(message):
    pushover_token = os.getenv("PUSHOVER_TOKEN")  # Replace with your Pushover App Token or use environment variable
    pushover_user = os.getenv("PUSHOVER_USER")    # Replace with your Pushover User Key or use environment variable

    if not pushover_token or not pushover_user:
        print("Pushover credentials are not set. Skipping notification.")
        return

    # Pushover notification payload
    payload = {
        "token": pushover_token,
        "user": pushover_user,
        "message": message
    }

    # Send the notification
    response = requests.post("https://api.pushover.net/1/messages.json", data=payload)

    if response.status_code == 200:
        print("Pushover notification sent successfully.")
    else:
        print(f"Failed to send notification: {response.status_code} - {response.text}")
