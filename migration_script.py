# migration_script.py
import asyncio
from main import CryptoScraperApp

async def migrate_data():
    """Migrate existing data to new schema"""
    app = CryptoScraperApp()
    await app.initialize()
    print("Migration complete!")

if __name__ == "__main__":
    asyncio.run(migrate_data())