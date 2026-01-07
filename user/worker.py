# user/worker.py

import logging
import hashlib
from io import BytesIO
import os
from PIL import Image, UnidentifiedImageError
import imagehash
from telethon import events
from telethon.errors import ChatForwardsRestrictedError
from shared.config import MAX_UPLOAD_SIZE_BYTES, DOWNLOADS_DIR
from PIL import Image
from telethon.tl.types import (
    DocumentAttributeSticker, MessageMediaPhoto, MessageMediaDocument,
    DocumentAttributeVideo, DocumentAttributeFilename
)
from telethon import utils as tg_utils

try:
    from FastTelethon import download_file as ft_download_file, upload_file as ft_upload_file
    HAS_FAST = True
except Exception:
    HAS_FAST = False
from shared.database import (
    db_record_message,
    db_check_message_exists,
    db_check_duplicate_by_fingerprint,
    db_check_duplicate_by_thumbnail_hash,
    db_check_duplicate_by_image_hash,
    db_check_duplicate_by_content_hash,
    db_record_message, db_check_message_exists, db_check_duplicate_by_fingerprint
)

logger = logging.getLogger(__name__)

def _prepare_thumb_jpeg(data: bytes) -> bytes | None:
    if not data:
        return None
    try:
        img = Image.open(BytesIO(data)).convert('RGB')
        img.thumbnail((320, 320))
        for q in (85, 75, 65, 55):
            buf = BytesIO()
            img.save(buf, format='JPEG', quality=q, optimize=True)
            b = buf.getvalue()
            if len(b) <= 200 * 1024:
                return b
        return b
    except Exception:
        return None

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

async def calculate_thumbnail_hash_bytes(data: bytes) -> tuple[str | None, str | None]:
    """Hitung MD5 dan image hash dari data thumbnail."""
    if not data:
        return None, None
    md5 = hashlib.md5(data).hexdigest()
    try:
        img = Image.open(BytesIO(data))
        ih = f"p_{imagehash.phash(img)}_d_{imagehash.dhash(img)}"
    except Exception:
        ih = None
    return md5, ih

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

    def _try_delete_cache(self, cached_path: str | None):
        if not cached_path:
            return
        try:
            base_dir = os.path.abspath(os.path.join(DOWNLOADS_DIR, str(self.user_id)))
            p = os.path.abspath(cached_path)
            if p.startswith(base_dir) and os.path.exists(p):
                os.remove(p)
                logger.info(f"[USER_ID: {self.user_id}] 🗑️ Deleted cache {p}")
        except Exception:
            pass

    def _sha256_file(self, path: str) -> str | None:
        try:
            h = hashlib.sha256()
            with open(path, 'rb') as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b''):
                    h.update(chunk)
            return h.hexdigest()
        except Exception:
            return None

    def _sha256_bytes(self, data: bytes) -> str | None:
        try:
            return hashlib.sha256(data).hexdigest()
        except Exception:
            return None

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

            # Ambil thumbnail terkecil untuk deteksi duplikasi
            thumb_bytes = await self.client.download_media(event.message, file=bytes, thumb=0)
            thumb_md5, img_hash = await calculate_thumbnail_hash_bytes(thumb_bytes)
            dup_key = await db_check_duplicate_by_thumbnail_hash(self.user_id, thumb_md5)
            if dup_key:
                logger.info(f"[USER_ID: {self.user_id}] ⏩ Skip (MsgID: {message_key}): Duplicate thumbnail hash.")
                await db_record_message(self.user_id, message_key, {
                    'chat_id': event.chat_id,
                    'message_id': event.message.id,
                    'chat_name': chat_name,
                    'media_type': media_type,
                    'fingerprint': fp,
                    'thumbnail_md5_hash': thumb_md5,
                    'image_hash': img_hash,
                    'status': 'duplicate_thumbnail_hash',
                    'is_duplicate_of_key': dup_key,
                })
                return
            if img_hash:
                dup_key = await db_check_duplicate_by_image_hash(self.user_id, img_hash)
                if dup_key:
                    logger.info(f"[USER_ID: {self.user_id}] ⏩ Skip (MsgID: {message_key}): Duplicate image hash.")
                    await db_record_message(self.user_id, message_key, {
                        'chat_id': event.chat_id,
                        'message_id': event.message.id,
                        'chat_name': chat_name,
                        'media_type': media_type,
                        'fingerprint': fp,
                        'thumbnail_md5_hash': thumb_md5,
                        'image_hash': img_hash,
                        'status': 'duplicate_image_hash',
                        'is_duplicate_of_key': dup_key,
                    })
                    return

            # Do not pre-download to avoid delaying direct forward
            cached_path = None

            logger.info(f"[USER_ID: {self.user_id}] ➡️ Attempting direct forward for MsgID: {event.message.id}")
            await self.client.forward_messages(self.config['target_chat_id'], event.message)
            logger.info(f"[USER_ID: {self.user_id}] ✅ Directly forwarded MsgID: {event.message.id}")
            # No pre-cache used on success path; nothing to delete
            content_hash = None
            await db_record_message(self.user_id, message_key, {
                'chat_id': event.chat_id,
                'message_id': event.message.id,
                'chat_name': chat_name,
                'media_type': media_type,
                'fingerprint': fp,
                'thumbnail_md5_hash': thumb_md5,
                'image_hash': img_hash,
                'content_hash': content_hash,
                'status': 'forwarded_directly'
            })
        except ChatForwardsRestrictedError:
            if self.config.get('reupload_on_restricted'):
                logger.info(f"[USER_ID: {self.user_id}] 🚧 Forward restricted. Trying download & re-upload...")
                try:
                    # Ukuran kasar bila tersedia (untuk dokumen/video); jika melebihi limit, skip
                    raw_size = None
                    if hasattr(event.message, 'file') and getattr(event.message, 'file', None):
                        try:
                            raw_size = event.message.file.size
                        except Exception:
                            raw_size = None
                    if raw_size and MAX_UPLOAD_SIZE_BYTES and raw_size > MAX_UPLOAD_SIZE_BYTES:
                        logger.info(f"[USER_ID: {self.user_id}] ⏭️ Skip re-upload: file too large {raw_size} bytes.")
                        await db_record_message(self.user_id, message_key, {
                            'chat_id': event.chat_id,
                            'message_id': event.message.id,
                            'chat_name': chat_name,
                            'media_type': media_type,
                            'fingerprint': fp,
                            'thumbnail_md5_hash': thumb_md5,
                            'image_hash': img_hash,
                            'status': 'skipped_reupload_file_too_large'
                        })
                        return
                    caption = (event.message.message or '').strip() or None
                    user_dir = os.path.join(DOWNLOADS_DIR, str(self.user_id))
                    os.makedirs(user_dir, exist_ok=True)

                    # Parallel download if possible
                    try:
                        cached_path = None
                        if HAS_FAST and isinstance(event.message.media, MessageMediaDocument) and getattr(event.message.media, 'document', None):
                            try:
                                fname = None
                                for attr in (event.message.media.document.attributes or []):
                                    if isinstance(attr, DocumentAttributeFilename):
                                        fname = attr.file_name
                                        break
                                if not fname:
                                    ext = '.mp4' if media_type == 'video' else ''
                                    fname = f"{event.message.id}{ext}"
                                temp_path = os.path.join(user_dir, fname)
                                with open(temp_path, 'wb') as out:
                                    await ft_download_file(self.client, event.message.media.document, out)
                                cached_path = temp_path
                            except Exception as e:
                                logger.warning(f"[USER_ID: {self.user_id}] Fast download failed: {e}. Falling back to standard.")
                                cached_path = None
                        
                        if not cached_path:
                            cached_path = await self.client.download_media(event.message, file=user_dir)
                        file_to_send = cached_path
                    except Exception:
                        data = await self.client.download_media(event.message, file=bytes)
                        if not data:
                            logger.info(f"[USER_ID: {self.user_id}] ❌ Download failed (probably protected content). Skipping.")
                            await db_record_message(self.user_id, message_key, {
                                'chat_id': event.chat_id,
                                'message_id': event.message.id,
                                'chat_name': chat_name,
                                'media_type': media_type,
                                'fingerprint': fp,
                                'thumbnail_md5_hash': thumb_md5,
                                'image_hash': img_hash,
                                'status': 'skipped_protected_content'
                            })
                            return
                        if MAX_UPLOAD_SIZE_BYTES and len(data) > MAX_UPLOAD_SIZE_BYTES:
                            logger.info(f"[USER_ID: {self.user_id}] ⏭️ Data exceeds MAX_UPLOAD_SIZE. Skipping.")
                            await db_record_message(self.user_id, message_key, {
                                'chat_id': event.chat_id,
                                'message_id': event.message.id,
                                'chat_name': chat_name,
                                'media_type': media_type,
                                'fingerprint': fp,
                                'thumbnail_md5_hash': thumb_md5,
                                'image_hash': img_hash,
                                'status': 'skipped_reupload_over_limit'
                            })
                            return
                        # Write bytes to disk so we can use fast upload
                        fname = f"{event.message.id}.bin"
                        cached_path = os.path.join(user_dir, fname)
                        with open(cached_path, 'wb') as f:
                            f.write(data)
                        file_to_send = cached_path

                    # Calculate strong content hash (to avoid duplicate re-uploads)
                    content_hash = None
                    if isinstance(file_to_send, (bytes, bytearray)):
                        content_hash = self._sha256_bytes(file_to_send)
                    elif isinstance(file_to_send, str):
                        content_hash = self._sha256_file(file_to_send)

                    if content_hash:
                        dup_key = await db_check_duplicate_by_content_hash(self.user_id, content_hash)
                        if dup_key:
                            logger.info(f"[USER_ID: {self.user_id}] ⏩ Skip: duplicate by content hash.")
                            self._try_delete_cache(cached_path)
                            await db_record_message(self.user_id, message_key, {
                                'chat_id': event.chat_id,
                                'message_id': event.message.id,
                                'chat_name': chat_name,
                                'media_type': media_type,
                                'fingerprint': fp,
                                'thumbnail_md5_hash': thumb_md5,
                                'image_hash': img_hash,
                                'content_hash': content_hash,
                                'status': 'duplicate_content_hash',
                                'is_duplicate_of_key': dup_key,
                            })
                            return

                    # Prepare send options to preserve video playability
                    send_kwargs = {
                        'caption': caption,
                        'force_document': (media_type == 'document')
                    }
                    try:
                        thumb_data = await self.client.download_media(event.message, file=bytes, thumb=0)
                    except Exception:
                        thumb_data = None
                    # For videos, let Telegram generate its own preview; use thumb only for non-video
                    if thumb_data and media_type != 'video':
                        norm_thumb = _prepare_thumb_jpeg(thumb_data)
                        if norm_thumb:
                            send_kwargs['thumb'] = norm_thumb

                    # Fast parallel upload when possible
                    input_file = None
                    if HAS_FAST and isinstance(file_to_send, str) and os.path.exists(file_to_send):
                        try:
                            with open(file_to_send, 'rb') as f:
                                input_file = await ft_upload_file(self.client, f)
                        except Exception:
                            input_file = None

                    if input_file is not None:
                        # Prefer original attributes/mime for correct video handling
                        attrs = []
                        mime_type = None
                        out_name = None
                        if isinstance(event.message.media, MessageMediaDocument) and getattr(event.message.media, 'document', None):
                            mime_type = event.message.media.document.mime_type
                            for attr in (event.message.media.document.attributes or []):
                                if isinstance(attr, DocumentAttributeFilename):
                                    out_name = attr.file_name
                                if isinstance(attr, DocumentAttributeVideo):
                                    attrs.append(DocumentAttributeVideo(
                                        duration=getattr(attr, 'duration', None),
                                        w=getattr(attr, 'w', None),
                                        h=getattr(attr, 'h', None),
                                        supports_streaming=True
                                    ))
                            if out_name:
                                attrs.append(DocumentAttributeFilename(out_name))
                        if media_type == 'video' and (not mime_type or not mime_type.startswith('video/')):
                            mime_type = 'video/mp4'
                        if not attrs:
                            inferred_attrs, inferred_mime = tg_utils.get_attributes(file_to_send)
                            attrs = inferred_attrs
                            mime_type = mime_type or inferred_mime
                        await self.client.send_file(
                            self.config['target_chat_id'],
                            file=input_file,
                            attributes=attrs,
                            mime_type=mime_type,
                            force_document=(media_type == 'document'),
                            **send_kwargs
                        )
                    else:
                        # Build explicit attributes/mime from original for correct orientation
                        attrs = []
                        mime_type = None
                        out_name = None
                        if isinstance(event.message.media, MessageMediaDocument) and getattr(event.message.media, 'document', None):
                            mime_type = event.message.media.document.mime_type
                            for attr in (event.message.media.document.attributes or []):
                                if isinstance(attr, DocumentAttributeFilename):
                                    out_name = attr.file_name
                                if isinstance(attr, DocumentAttributeVideo):
                                    attrs.append(DocumentAttributeVideo(
                                        duration=getattr(attr, 'duration', None),
                                        w=getattr(attr, 'w', None),
                                        h=getattr(attr, 'h', None),
                                        supports_streaming=True
                                    ))
                            if out_name:
                                attrs.append(DocumentAttributeFilename(out_name))
                        if media_type == 'video' and (not mime_type or not mime_type.startswith('video/')):
                            mime_type = 'video/mp4'
                        await self.client.send_file(
                            self.config['target_chat_id'],
                            file=file_to_send,
                            attributes=attrs or None,
                            mime_type=mime_type,
                            **send_kwargs
                        )
                    logger.info(f"[USER_ID: {self.user_id}] ✅ Re-uploaded MsgID: {event.message.id}")
                    # Hapus cache jika kita membuatnya tadi
                    self._try_delete_cache(cached_path)
                    await db_record_message(self.user_id, message_key, {
                        'chat_id': event.chat_id,
                        'message_id': event.message.id,
                        'chat_name': chat_name,
                        'media_type': media_type,
                        'fingerprint': fp,
                        'thumbnail_md5_hash': thumb_md5,
                        'image_hash': img_hash,
                        'content_hash': content_hash,
                        'status': 'reuploaded_due_to_forward_restricted'
                    })
                except Exception as e:
                    logger.warning(f"[USER_ID: {self.user_id}] ❌ Re-upload failed: {type(e).__name__}: {e}")
                    await db_record_message(self.user_id, message_key, {
                        'chat_id': event.chat_id,
                        'message_id': event.message.id,
                        'chat_name': chat_name,
                        'media_type': media_type,
                        'fingerprint': fp,
                        'thumbnail_md5_hash': thumb_md5,
                        'image_hash': img_hash,
                        'status': f'reupload_error: {type(e).__name__}'
                    })
            else:
                logger.info(f"[USER_ID: {self.user_id}] 🚫 Direct forward restricted. Skipping.")
                await db_record_message(self.user_id, message_key, {
                    'chat_id': event.chat_id,
                    'message_id': event.message.id,
                    'chat_name': chat_name,
                    'media_type': media_type,
                    'fingerprint': fp,
                    'thumbnail_md5_hash': thumb_md5,
                    'image_hash': img_hash,
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
                    'thumbnail_md5_hash': thumb_md5,
                    'image_hash': img_hash,
                    'status': f'error: {type(e).__name__}'
                })
