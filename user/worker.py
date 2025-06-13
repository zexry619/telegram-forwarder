# user/worker.py

import logging
from telethon import events
from telethon.errors import ChatForwardsRestrictedError
from telethon.tl.types import (
    DocumentAttributeSticker, MessageMediaPhoto, MessageMediaDocument,
    DocumentAttributeVideo
)
from shared.database import (
    db_record_message, db_check_message_exists, db_check_duplicate_by_fingerprint
)

logger = logging.getLogger(__name__)

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

class UserWorker:
    def __init__(self, user_id: int, client, config: dict, bot_client):
        self.user_id = user_id
        self.client = client
        self.config = config
        self.bot_client = bot_client
        self.status = "initializing"
        # client.me seharusnya sudah diisi oleh manager sebelum worker dibuat
        if not hasattr(self.client, 'me') or not self.client.me:
            raise ValueError("Client 'me' attribute not set before creating worker.")

    async def start(self):
        # Tambahkan event handler HANYA saat worker benar-benar dimulai
        self.client.add_event_handler(self._new_message_handler, events.NewMessage(incoming=True))
        
        # Ubah status menjadi running
        self.status = "running"

        # Kirim feedback ke pengguna
        logger.info(f"[USER_ID: {self.user_id}] Worker started.")
        await self.send_feedback("✅ Worker berhasil dimulai.")

    async def stop(self):
        self.status = "stopped"
        if self.client.is_connected():
            self.client.remove_event_handler(self._new_message_handler)
        logger.info(f"[USER_ID: {self.user_id}] Worker stopped.")

    async def send_feedback(self, message: str):
        try:
            await self.bot_client.send_message(self.user_id, f"ℹ️ **Notifikasi Worker:**\n{message}")
        except: pass

    async def _new_message_handler(self, event):
        if self.status != 'running' or not is_valid_media(event.message.media): return
        if event.out or event.chat_id in self.config['excluded_chat_ids'] or event.chat_id == self.config['target_chat_id']: return

        media_type = get_media_type_string(event.message.media)
        allowed = self.config.get('allowed_media_types', set())
        if allowed and media_type not in allowed:
            logger.info(f"[USER_ID: {self.user_id}] ⏩ Skip {media_type} not allowed.")
            return

        try:
            chat = await event.get_chat()
            chat_name = getattr(chat, 'title', getattr(chat, 'first_name', f"Chat {event.chat_id}"))
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
            logger.info(f"[USER_ID: {self.user_id}] 🚫 Direct forward restricted. Skipping.")
            await db_record_message(self.user_id, message_key, {
                'chat_id': event.chat_id,
                'message_id': event.message.id,
                'chat_name': chat_name,
                'media_type': media_type,
                'fingerprint': fp,
                'status': 'skipped_forward_restricted'
            })
        except Exception as e:
            if "target peer" in str(e).lower():
                logger.error(f"[USER_ID: {self.user_id}] ⛔️ CRITICAL ERROR: Target chat ID {self.config['target_chat_id']} is invalid or I don't have access. Stopping worker.")
                await self.send_feedback(f"⛔️ Worker dihentikan! Target chat `{self.config['target_chat_id']}` tidak valid atau saya tidak punya akses.")
                from user.manager import stop_user_worker; await stop_user_worker(self.user_id)
            else:
                logger.warning(f"[USER_ID: {self.user_id}] ⚠️ Direct forward failed ({type(e).__name__}). Skipping.")
                await db_record_message(self.user_id, message_key, {
                    'chat_id': event.chat_id,
                    'message_id': event.message.id,
                    'chat_name': chat_name,
                    'media_type': media_type,
                    'fingerprint': fp,
                    'status': f'error: {type(e).__name__}'
                })
