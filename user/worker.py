# user/worker.py

import asyncio
import os
import hashlib
import imagehash
import logging
from PIL import Image, UnidentifiedImageError
from telethon import events
from telethon.errors import ChatForwardsRestrictedError, FloodWaitError
from telethon.tl.types import (
    DocumentAttributeSticker, MessageMediaPhoto, MessageMediaDocument,
    DocumentAttributeVideo
)
from shared.database import (
    db_record_message, db_check_message_exists, db_check_duplicate_by_fingerprint,
    db_check_duplicate_by_thumbnail_hash, db_check_duplicate_by_image_hash,
    db_check_duplicate_by_content_hash
)
from shared.config import DOWNLOADS_DIR
from shared.config import ADMIN_USER_IDS

logger = logging.getLogger(__name__)

async def calculate_md5_hash(fp):
    try:
        h = hashlib.md5(); f=open(fp,'rb'); h.update(f.read()); f.close(); return h.hexdigest()
    except: return None
async def calculate_image_hash(fp):
    try: return f"p_{str(imagehash.phash(Image.open(fp)))}_d_{str(imagehash.dhash(Image.open(fp)))}"
    except: return None
def get_media_type_string(m):
    if isinstance(m, MessageMediaPhoto): return "photo"
    if isinstance(m, MessageMediaDocument):
        if hasattr(m.document, 'mime_type') and m.document.mime_type.startswith('video/'): return "video"
    return "document"
def is_valid_media(m):
    if not m or (hasattr(m, 'document') and any(isinstance(a, DocumentAttributeSticker) for a in m.document.attributes)): return False
    return isinstance(m, (MessageMediaPhoto, MessageMediaDocument))
async def get_media_fingerprint(m):
    if isinstance(m, MessageMediaPhoto) and hasattr(m, 'photo'): return f"photo_{m.photo.id}_{m.photo.access_hash}"
    if isinstance(m, MessageMediaDocument) and hasattr(m, 'document'):
        d = m.document; fp = [f"doc_{d.id}_{d.access_hash}",f"s{d.size}"]; [fp.extend([f"d{a.duration}",f"w{a.w}",f"h{a.h}"]) for a in d.attributes if isinstance(a,DocumentAttributeVideo)]; return "_".join(fp)
    return None

async def send_admin_notification(bot_client, msg):
    for admin_id in ADMIN_USER_IDS:
        try:
            await bot_client.send_message(admin_id, f"🚨 [BOT ERROR]:\n{msg}")
        except Exception:
            pass
class UserWorker:
    def __init__(self, user_id: int, client, config: dict, bot_client):
        self.user_id = user_id
        self.client = client
        self.config = config
        self.bot_client = bot_client
        self.status = "initializing"
        self._tasks = []
        self._queue = asyncio.Queue(maxsize=50)
        self.download_path = os.path.join(DOWNLOADS_DIR, str(self.user_id))
        os.makedirs(self.download_path, exist_ok=True)
        # client.me seharusnya sudah diisi oleh manager sebelum worker dibuat
        if not hasattr(self.client, 'me') or not self.client.me:
            raise ValueError("Client 'me' attribute not set before creating worker.")

    async def start(self):
        # Tambahkan event handler HANYA saat worker benar-benar dimulai
        self.client.add_event_handler(self._new_message_handler, events.NewMessage(incoming=True))
        
        # Mulai task-task latar belakang
        for i in range(3):
            self._tasks.append(asyncio.create_task(self._process_queue()))
        
        # Ubah status menjadi running
        self.status = "running"
        
        # Kirim feedback ke pengguna
        logger.info(f"[USER_ID: {self.user_id}] Worker started with {len(self._tasks)} queue processors.")
        await self.send_feedback("✅ Worker berhasil dimulai.")

    async def stop(self):
        self.status = "stopped"
        if self.client.is_connected():
            self.client.remove_event_handler(self._new_message_handler)
        for task in self._tasks: task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info(f"[USER_ID: {self.user_id}] Worker stopped.")

    async def send_feedback(self, message: str):
        try:
            await self.bot_client.send_message(self.user_id, f"ℹ️ **Notifikasi Worker:**\n{message}")
        except: pass

    async def _new_message_handler(self, event):
        if self.status != 'running' or not is_valid_media(event.message.media): return
        if event.out or event.chat_id in self.config['excluded_chat_ids'] or event.chat_id == self.config['target_chat_id']: return

        try:
            chat = await event.get_chat()
            chat_name = getattr(chat, 'title', getattr(chat, 'first_name', f"Chat {event.chat_id}"))
            media_type = get_media_type_string(event.message.media)
            logger.info(f"[USER_ID: {self.user_id}] 🆕 New {media_type} from '{chat_name}' (MsgID: {event.message.id})")

            message_key = f"{event.chat_id}_{event.message.id}"
            if await db_check_message_exists(self.user_id, message_key):
                logger.info(f"[USER_ID: {self.user_id}] ⏩ Skip (MsgID: {message_key}): Already in DB.")
                return

            fp = await get_media_fingerprint(event.message)
            if await db_check_duplicate_by_fingerprint(self.user_id, fp):
                logger.info(f"[USER_ID: {self.user_id}] ⏩ Skip (MsgID: {message_key}): Duplicate fingerprint.")
                return

            logger.info(f"[USER_ID: {self.user_id}] ➡️ Attempting direct forward for MsgID: {event.message.id}")
            await self.client.forward_messages(self.config['target_chat_id'], event.message)
            logger.info(f"[USER_ID: {self.user_id}] ✅ Directly forwarded MsgID: {event.message.id}")
            await db_record_message(self.user_id, message_key, {'chat_id': event.chat_id, 'message_id': event.message.id, 'chat_name': chat_name, 'media_type': media_type, 'fingerprint': fp, 'status': 'forwarded_directly'})
        except ChatForwardsRestrictedError:
            logger.info(f"[USER_ID: {self.user_id}] 🚫 Direct forward restricted. Queuing for download.")
            await self._queue.put(event)
        except Exception as e:
            if "target peer" in str(e).lower():
                logger.error(f"[USER_ID: {self.user_id}] ⛔️ CRITICAL ERROR: Target chat ID {self.config['target_chat_id']} is invalid or I don't have access. Stopping worker.")
                await self.send_feedback(f"⛔️ Worker dihentikan! Target chat `{self.config['target_chat_id']}` tidak valid atau saya tidak punya akses.")
                from user.manager import stop_user_worker; await stop_user_worker(self.user_id)
            else:
                logger.warning(f"[USER_ID: {self.user_id}] ⚠️ Direct forward failed ({type(e).__name__}), queuing...")
                await self._queue.put(event)
            await send_admin_notification(self.bot_client, f"User {self.user_id} mengalami error: {e}")
    async def _process_queue(self):
        while self.status == 'running':
            try:
                event = await self._queue.get()
                message_key = f"{event.chat_id}_{event.message.id}"
                file_path, db_data = None, {}
                chat = await event.get_chat(); chat_name = getattr(chat,'title', f"Chat {event.chat_id}")
                media_type = get_media_type_string(event.media)
                logger.info(f"[USER_ID: {self.user_id}] 📥 Processing MsgID {event.message.id} from queue. Downloading...")
                try:
                    db_data = {'chat_id': event.chat_id, 'message_id': event.message.id, 'chat_name': chat_name, 'media_type': media_type, 'fingerprint': await get_media_fingerprint(event.message)}
                    file_path = await self.client.download_media(event.message, file=self.download_path)
                    if not file_path: raise ValueError("Download failed")
                    logger.info(f"[USER_ID: {self.user_id}] 📥 Downloaded: {os.path.basename(file_path)}")

                    content_hash = await calculate_md5_hash(file_path)
                    if await db_check_duplicate_by_content_hash(self.user_id, content_hash):
                        logger.info(f"[USER_ID: {self.user_id}] ⏩ Skip (MsgID: {event.message.id}): Duplicate content hash.")
                        db_data.update({'status': 'duplicate_content_hash', 'content_hash': content_hash}); await db_record_message(self.user_id, message_key, db_data)
                        continue
                    db_data['content_hash'] = content_hash
                    
                    if media_type == 'photo':
                        image_hash = await calculate_image_hash(file_path)
                        if await db_check_duplicate_by_image_hash(self.user_id, image_hash):
                            logger.info(f"[USER_ID: {self.user_id}] ⏩ Skip (MsgID: {event.message.id}): Duplicate image hash.")
                            db_data.update({'status': 'duplicate_image_hash', 'image_hash': image_hash}); await db_record_message(self.user_id, message_key, db_data)
                            continue
                        db_data['image_hash'] = image_hash

                    caption = f"Forwarded from: **{chat_name}**\n\n{event.message.text or ''}".strip()
                    logger.info(f"[USER_ID: {self.user_id}] 📤 Uploading MsgID {event.message.id}...")
                    await self.client.send_file(self.config['target_chat_id'], file_path, caption=caption, parse_mode='md')
                    logger.info(f"[USER_ID: {self.user_id}] ✅ Uploaded successfully.")
                    db_data['status'] = 'forwarded_uploaded'; await db_record_message(self.user_id, message_key, db_data)

                except FloodWaitError as e:
                    logger.warning(f"[USER_ID: {self.user_id}] ⏱️ FloodWait in queue: {e.seconds}s. Re-queueing.")
                    await asyncio.sleep(e.seconds); await self._queue.put(event)
                except Exception as e:
                    error_msg = f"Failed to process MsgID {message_key}: {type(e).__name__}"; logger.error(f"[USER_ID: {self.user_id}] ❌ {error_msg}", exc_info=True)
                    await self.send_feedback(error_msg); db_data['status'] = f"error: {str(e)[:100]}"; await db_record_message(self.user_id, message_key, db_data)
                    await send_admin_notification(self.bot_client, f"User {self.user_id} mengalami error: {e}")
                finally:
                    if file_path and os.path.exists(file_path): os.remove(file_path)
                    self._queue.task_done()
            except asyncio.CancelledError: break