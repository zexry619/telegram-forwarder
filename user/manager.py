# user/manager.py

import asyncio
import os
import logging
from telethon import TelegramClient
from shared.database import get_user_config, update_user_config, get_all_running_users, get_user_routes
from .worker import UserWorker
from shared.config import API_ID, API_HASH, SESSIONS_DIR, get_telethon_proxy

logger = logging.getLogger(__name__)

ACTIVE_SESSIONS = {}

proxy = get_telethon_proxy()

async def get_client_for_user(user_id: int):
    if user_id in ACTIVE_SESSIONS:
        session_obj = ACTIVE_SESSIONS[user_id]
        client = session_obj.client if isinstance(session_obj, UserWorker) else session_obj
        if not client.is_connected():
            try:
                client.set_proxy(proxy)
                await client.connect()
                if not client.is_connected(): return None # Gagal connect
                client.me = await client.get_me()
            except Exception as e:
                logger.error(f"Failed to reconnect client for user {user_id}: {e}")
                return None
        return client

    session_path = os.path.join(SESSIONS_DIR, str(user_id))
    if os.path.exists(f"{session_path}.session"):
        try:
            client = TelegramClient(session_path, API_ID, API_HASH, proxy=proxy)
            await client.connect()
            if await client.is_user_authorized():
                client.me = await client.get_me()
                ACTIVE_SESSIONS[user_id] = client
                return client
            else:
                await client.disconnect()
        except Exception as e:
            logger.error(f"Failed to create client from session for {user_id}: {e}")
    return None

def get_new_client(user_id: int):
    session_path = os.path.join(SESSIONS_DIR, str(user_id))
    return TelegramClient(session_path, API_ID, API_HASH, proxy=proxy)

async def add_new_client(user_id: int, client: TelegramClient):
    if client and client.is_connected() and await client.is_user_authorized():
        client.me = await client.get_me()
        ACTIVE_SESSIONS[user_id] = client
        logger.info(f"New client for user {user_id} ({client.me.first_name}) has been registered in memory.")

async def get_worker_status(user_id: int) -> str:
    if user_id in ACTIVE_SESSIONS and isinstance(ACTIVE_SESSIONS[user_id], UserWorker):
        return ACTIVE_SESSIONS[user_id].status
    config = await get_user_config(user_id)
    return config.get('status', 'stopped')

async def start_user_worker(user_id: int, bot_client):
    logger.info(f"Attempting to start worker for user {user_id}...")
    
    # 1. Cek status di memori dulu
    if user_id in ACTIVE_SESSIONS and isinstance(ACTIVE_SESSIONS[user_id], UserWorker):
        logger.warning(f"User {user_id} worker is already running in memory. No action taken.")
        return False, "Worker sudah berjalan."
        
    # 2. Persiapan
    config = await get_user_config(user_id)
    routes = await get_user_routes(user_id, enabled_only=True)
    routes_with_target = [route for route in routes if route.get('target_chat_id')]
    if not routes_with_target:
        return False, "❌ Gagal: Belum ada route aktif yang punya target."
        
    client = await get_client_for_user(user_id)
    if not client:
        await update_user_config(user_id, 'status', 'login_required')
        return False, "❌ Gagal: Sesi tidak valid. Silakan login ulang."
        
    # 3. Buat dan jalankan worker
    try:
        worker = UserWorker(user_id, client, config, routes, bot_client)
        ACTIVE_SESSIONS[user_id] = worker
        
        await worker.start() # Tunggu start selesai
        await update_user_config(user_id, 'status', 'running')
        
        logger.info(f"Successfully started worker for user {user_id}.")
        return True, "✅ Worker berhasil dimulai."
    except Exception as e:
        error_msg = f"Gagal memulai worker: {e}"
        logger.error(f"Error starting worker for {user_id}: {error_msg}", exc_info=True)
        await update_user_config(user_id, 'status', 'error')
        await update_user_config(user_id, 'last_error', str(e))
        # Pastikan state di memori bersih jika gagal
        if user_id in ACTIVE_SESSIONS:
            ACTIVE_SESSIONS[user_id] = client
        return False, error_msg


async def refresh_user_worker_routes(user_id: int):
    if user_id not in ACTIVE_SESSIONS or not isinstance(ACTIVE_SESSIONS[user_id], UserWorker):
        return
    worker = ACTIVE_SESSIONS[user_id]
    worker.config = await get_user_config(user_id)
    await worker.reload_routes(await get_user_routes(user_id, enabled_only=True))

async def stop_user_worker(user_id: int):
    logger.info(f"Attempting to stop worker for user {user_id}...")
    
    if user_id not in ACTIVE_SESSIONS or not isinstance(ACTIVE_SESSIONS[user_id], UserWorker):
        logger.warning(f"User {user_id} worker is not running in memory. No action taken.")
        # Sinkronkan DB untuk keamanan
        await update_user_config(user_id, 'status', 'stopped')
        return False, "Worker tidak sedang berjalan."
    
    worker = ACTIVE_SESSIONS[user_id]
    client = worker.client
    
    await worker.stop()
    
    # Gantikan worker dengan client di memori
    ACTIVE_SESSIONS[user_id] = client
    
    await update_user_config(user_id, 'status', 'stopped')
    logger.info(f"Successfully stopped worker for user {user_id}.")
    return True, "✅ Worker berhasil dihentikan."

async def logout_user(user_id: int):
    logger.info(f"Processing logout for user {user_id}...")
    # Hentikan worker dulu
    if user_id in ACTIVE_SESSIONS and isinstance(ACTIVE_SESSIONS[user_id], UserWorker):
        await stop_user_worker(user_id)
    
    if user_id in ACTIVE_SESSIONS:
        client = ACTIVE_SESSIONS.pop(user_id)
        if client.is_connected():
            await client.disconnect()
            
    session_file = os.path.join(SESSIONS_DIR, f"{user_id}.session")
    journal_file = os.path.join(SESSIONS_DIR, f"{user_id}.session-journal")
    if os.path.exists(session_file): os.remove(session_file)
    if os.path.exists(journal_file): os.remove(journal_file)
    
    await update_user_config(user_id, 'status', 'stopped')
    logger.info(f"User {user_id} has been logged out and session file deleted.")
    return True, "✅ Anda berhasil logout."

async def startup_all_workers(bot_client):
    logger.info("Checking for users with 'running' status to restart...")
    user_ids = await get_all_running_users()
    if not user_ids:
        logger.info("No workers to restart.")
        return
    for user_id in user_ids:
        await start_user_worker(user_id, bot_client)

async def shutdown_all_workers():
    logger.info(f"Shutting down {len(ACTIVE_SESSIONS)} active session(s)...")
    for user_id in list(ACTIVE_SESSIONS.keys()):
        if isinstance(ACTIVE_SESSIONS[user_id], UserWorker):
            await stop_user_worker(user_id)
        else:
            client = ACTIVE_SESSIONS.pop(user_id)
            if client.is_connected():
                await client.disconnect()
    logger.info("All active sessions have been shut down.")

async def schedule_monitor(bot_client):
    import datetime
    from shared.database import get_allowed_users

    while True:
        try:
            user_ids = await get_allowed_users()
            now = datetime.datetime.now().time()
            for user_id in user_ids:
                config = await get_user_config(user_id)
                start_t = config.get('start_time')
                stop_t = config.get('stop_time')
                if not start_t or not stop_t:
                    continue
                try:
                    s_start = datetime.datetime.strptime(start_t, "%H:%M").time()
                    s_stop = datetime.datetime.strptime(stop_t, "%H:%M").time()
                except ValueError:
                    continue

                if s_start < s_stop:
                    active = s_start <= now < s_stop
                else:
                    active = now >= s_start or now < s_stop

                status = await get_worker_status(user_id)
                if active and status != 'running':
                    await start_user_worker(user_id, bot_client)
                elif not active and status == 'running':
                    await stop_user_worker(user_id)
        except Exception as e:
            logger.error(f"Schedule monitor error: {e}")
        await asyncio.sleep(60)
