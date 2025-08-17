# Run this script once to authenticate:
# File: authenticate_telegram.py

import asyncio
import os
import getpass

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv

async def authenticate():
    load_dotenv()
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    phone = os.getenv("TELEGRAM_PHONE")

    client = TelegramClient('crypto_scraper_session', api_id, api_hash)

    await client.connect()

    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        code = input('Enter the code you received: ')

        try:
            await client.sign_in(phone, code)
        except SessionPasswordNeededError:
            # Two-factor authentication is enabled
            print("Two-factor authentication detected.")
            password = getpass.getpass('Enter your 2FA password: ')
            await client.sign_in(password=password)

    print("✅ Authentication successful!")
    print("Session saved. You can now use the scraper.")

    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(authenticate())