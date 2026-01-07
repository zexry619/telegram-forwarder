import asyncio
import os
import logging
import sys
from telethon import TelegramClient
from shared.config import BOT_TOKEN, SESSIONS_DIR, API_ID, API_HASH, get_telethon_proxy, DOWNLOADS_DIR, CACHE_TTL_HOURS, MAX_CACHE_DISK_MB

# --- KONFIGURASI LOGGING ---
log_format = '%(asctime)s - %(name)-18s - %(levelname)-8s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format, stream=sys.stdout)
logger = logging.getLogger(__name__)

logging.getLogger('telethon').setLevel(logging.WARNING)

# --- Import dari modul proyek ---
from shared.database import init_db
from bot.handlers import setup_handlers
from user.manager import startup_all_workers, shutdown_all_workers, schedule_monitor
# --- Inisialisasi bot utama ---
proxy = get_telethon_proxy()
bot = TelegramClient('bot_controller_session', API_ID, API_HASH, proxy=proxy)

async def main():
    logger.info("Starting up the bot...")
    os.makedirs(SESSIONS_DIR, exist_ok=True)
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)

    # Inisialisasi Database
    await init_db()

    # Start bot
    await bot.start(bot_token=BOT_TOKEN)
    logger.info("Bot Controller is connected and running.")

    # Setup handler
    setup_handlers(bot)
    logger.info("All bot event handlers have been set up.")

    # Startup all workers
    await startup_all_workers(bot)

    # Start background tasks
    asyncio.create_task(schedule_monitor(bot))
    # Periodic cache cleanup
    async def cache_cleanup_loop():
        from utils.cleanup import cleanup_download_folder
        while True:
            try:
                await cleanup_download_folder(
                    DOWNLOADS_DIR,
                    max_age_hours=CACHE_TTL_HOURS,
                    max_total_mb=MAX_CACHE_DISK_MB,
                )
            except Exception as e:
                logger.error(f"Cache cleanup error: {e}")
            await asyncio.sleep(3600)
    asyncio.create_task(cache_cleanup_loop())

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
