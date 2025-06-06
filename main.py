# main.py

import asyncio
import os
import logging
import sys
from telethon import TelegramClient

# --- KONFIGURASI LOGGING ---
# Ini akan menangkap semua log dari semua modul dan menampilkannya di terminal
log_format = '%(asctime)s - %(name)-18s - %(levelname)-8s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, stream=sys.stdout)
logger = logging.getLogger(__name__)

# Atur level log Telethon agar tidak terlalu 'berisik' dengan pesan DEBUG-nya
logging.getLogger('telethon').setLevel(level=logging.WARNING)

# --- Impor dari modul kita ---
from shared.config import BOT_TOKEN, SESSIONS_DIR, DOWNLOADS_DIR, API_ID, API_HASH
from shared.database import init_db
from bot.handlers import setup_handlers
from user.manager import startup_all_workers, shutdown_all_workers

# Gunakan API ID dan Hash asli untuk bot juga
bot = TelegramClient('bot_controller_session', API_ID, API_HASH)

async def main():
    logger.info("Starting up the bot...")
    # Buat direktori yang dibutuhkan jika belum ada
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    
    # Inisialisasi Database
    await init_db()
    
    # Hubungkan dan jalankan bot controller
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Bot Controller is connected and running.")
    
    # Pasang semua handler (perintah, callback, dll)
    setup_handlers(bot)
    logger.info("All bot event handlers have been set up.")
    
    # Jalankan kembali worker yang sebelumnya berstatus 'running'
    await startup_all_workers(bot)
    
    logger.info("===== Bot is fully operational. Press Ctrl+C to stop. =====")
    await bot.run_until_disconnected()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        logger.info("Initiating graceful shutdown...")
        if bot.is_connected():
            # Beri kesempatan untuk proses shutdown internal
            loop.run_until_complete(shutdown_all_workers())
            logger.info("Disconnecting bot controller...")
            loop.run_until_complete(bot.disconnect())
        logger.info("Shutdown complete. Goodbye!")