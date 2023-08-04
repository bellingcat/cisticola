import argparse
from telethon.sync import TelegramClient
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cisticola command line tools")
    parser.add_argument("--telethon_session", type=str)

    args = parser.parse_args()

    api_id = os.environ["TELEGRAM_API_ID"]
    api_hash = os.environ["TELEGRAM_API_HASH"]
    phone = os.environ["TELEGRAM_PHONE"]
    telethon_session_name = args.telethon_session

    if telethon_session_name is None:
        telethon_session_name = phone

    client = TelegramClient(telethon_session_name, api_id, api_hash)
    client.start()

    client.disconnect()
