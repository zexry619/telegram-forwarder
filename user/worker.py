# user/worker.py

import asyncio
import hashlib
import logging
import os
from io import BytesIO

import imagehash
from PIL import Image
from telethon import events
from telethon import utils as tg_utils
from telethon.errors import ChatForwardsRestrictedError
from telethon.tl.types import (
    DocumentAttributeFilename,
    DocumentAttributeSticker,
    DocumentAttributeVideo,
    MessageMediaDocument,
    MessageMediaPhoto,
)

from shared.config import DOWNLOADS_DIR, LIVE_REUPLOAD_CONCURRENCY, MAX_UPLOAD_SIZE_BYTES
from shared.database import (
    db_check_duplicate_by_content_hash,
    db_check_duplicate_by_fingerprint,
    db_check_duplicate_by_image_hash,
    db_check_duplicate_by_thumbnail_hash,
    db_check_message_exists,
    db_record_message,
)
from shared.telegram import resolve_chat_peer

try:
    from FastTelethon import download_file as ft_download_file, upload_file as ft_upload_file
    HAS_FAST = True
except Exception:
    HAS_FAST = False

logger = logging.getLogger(__name__)


def _prepare_thumb_jpeg(data: bytes) -> bytes | None:
    if not data:
        return None
    try:
        img = Image.open(BytesIO(data)).convert('RGB')
        img.thumbnail((320, 320))
        for quality in (85, 75, 65, 55):
            buf = BytesIO()
            img.save(buf, format='JPEG', quality=quality, optimize=True)
            payload = buf.getvalue()
            if len(payload) <= 200 * 1024:
                return payload
        return payload
    except Exception:
        return None


def _finalize_media_send_metadata(
    media_type: str,
    cached_path: str | None,
    attrs: list,
    mime_type: str | None,
    out_name: str | None,
) -> tuple[list | None, str | None]:
    attrs = list(attrs or [])
    inferred_attrs = []
    inferred_mime = None
    if cached_path:
        try:
            inferred_attrs, inferred_mime = tg_utils.get_attributes(
                cached_path,
                mime_type=mime_type,
                force_document=(media_type == 'document'),
                supports_streaming=(media_type == 'video'),
            )
        except TypeError:
            inferred_attrs, inferred_mime = tg_utils.get_attributes(cached_path)

    if media_type == 'video':
        if not mime_type or not mime_type.startswith('video/'):
            mime_type = inferred_mime or 'video/mp4'
        if not any(isinstance(attr, DocumentAttributeVideo) for attr in attrs):
            inferred_video = next((attr for attr in inferred_attrs if isinstance(attr, DocumentAttributeVideo)), None)
            if inferred_video:
                attrs.insert(0, inferred_video)
        if not any(isinstance(attr, DocumentAttributeFilename) for attr in attrs):
            filename = out_name or (os.path.basename(cached_path) if cached_path else "video.mp4")
            attrs.append(DocumentAttributeFilename(filename))
    elif not attrs and inferred_attrs:
        attrs = inferred_attrs
        mime_type = mime_type or inferred_mime

    return attrs or None, mime_type


def get_media_type_string(media):
    if isinstance(media, MessageMediaPhoto):
        return "photo"
    if isinstance(media, MessageMediaDocument):
        if hasattr(media.document, 'mime_type') and media.document.mime_type.startswith('video/'):
            return "video"
    return "document"


def is_valid_media(media):
    if not media:
        return False
    if hasattr(media, 'document') and any(isinstance(attr, DocumentAttributeSticker) for attr in media.document.attributes):
        return False
    return isinstance(media, (MessageMediaPhoto, MessageMediaDocument))


async def get_media_fingerprint(media):
    if isinstance(media, MessageMediaPhoto) and hasattr(media, 'photo'):
        return f"photo_{media.photo.id}_{media.photo.access_hash}"
    if isinstance(media, MessageMediaDocument) and hasattr(media, 'document'):
        doc = media.document
        fingerprint = [f"doc_{doc.id}_{doc.access_hash}", f"s{doc.size}"]
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeVideo):
                fingerprint.extend([f"d{attr.duration}", f"w{attr.w}", f"h{attr.h}"])
        return "_".join(fingerprint)
    return None


async def calculate_thumbnail_hash_bytes(data: bytes) -> tuple[str | None, str | None]:
    if not data:
        return None, None
    md5 = hashlib.md5(data).hexdigest()
    try:
        img = Image.open(BytesIO(data))
        image_hash = f"p_{imagehash.phash(img)}_d_{imagehash.dhash(img)}"
    except Exception:
        image_hash = None
    return md5, image_hash


class UserWorker:
    def __init__(self, user_id: int, client, config: dict, routes: list[dict], bot_client):
        self.user_id = user_id
        self.client = client
        self.config = config
        self.routes = routes
        self.bot_client = bot_client
        self.status = "initializing"
        self.reupload_semaphore = asyncio.Semaphore(max(1, int(LIVE_REUPLOAD_CONCURRENCY or 1)))
        if not hasattr(self.client, 'me') or not self.client.me:
            raise ValueError("Client 'me' attribute not set before creating worker.")

    async def reload_routes(self, routes: list[dict]):
        self.routes = routes
        logger.info(f"[USER_ID: {self.user_id}] Reloaded {len(routes)} route(s).")

    def _try_delete_cache(self, cached_path: str | None):
        if not cached_path:
            return
        try:
            base_dir = os.path.abspath(os.path.join(DOWNLOADS_DIR, str(self.user_id)))
            path = os.path.abspath(cached_path)
            if path.startswith(base_dir) and os.path.exists(path):
                os.remove(path)
                logger.info(f"[USER_ID: {self.user_id}] Deleted cache {path}")
        except Exception:
            pass

    def _sha256_file(self, path: str) -> str | None:
        try:
            digest = hashlib.sha256()
            with open(path, 'rb') as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                    digest.update(chunk)
            return digest.hexdigest()
        except Exception:
            return None

    def _sha256_bytes(self, data: bytes) -> str | None:
        try:
            return hashlib.sha256(data).hexdigest()
        except Exception:
            return None

    def _build_unique_cache_name(self, event, preferred_name: str | None = None, fallback_ext: str = "") -> str:
        safe_name = os.path.basename(preferred_name) if preferred_name else ""
        if os.altsep:
            safe_name = safe_name.replace(os.altsep, "_")
        safe_name = safe_name.replace(os.sep, "_")
        if not safe_name:
            safe_name = f"media{fallback_ext}"
        return f"{event.chat_id}_{event.message.id}_{safe_name}"

    async def start(self):
        self.client.add_event_handler(self._new_message_handler, events.NewMessage(incoming=True))
        self.status = "running"
        logger.info(
            f"[USER_ID: {self.user_id}] Worker started with {len(self.routes)} route(s), reupload concurrency={self.reupload_semaphore._value}."
        )
        await self.send_feedback("✅ Worker berhasil dimulai.")

    async def stop(self):
        self.status = "stopped"
        if self.client.is_connected():
            self.client.remove_event_handler(self._new_message_handler)
        logger.info(f"[USER_ID: {self.user_id}] Worker stopped.")

    async def send_feedback(self, message: str):
        try:
            await self.bot_client.send_message(self.user_id, f"ℹ️ **Notifikasi Worker:**\n{message}")
        except Exception:
            pass

    def _matching_routes(self, event) -> list[dict]:
        media_type = get_media_type_string(event.message.media)
        matched = []
        for route in self.routes:
            if not route.get('enabled') or not route.get('target_chat_id'):
                continue
            source_chat_id = route.get('source_chat_id')
            if source_chat_id is not None and event.chat_id != source_chat_id:
                continue
            if source_chat_id is None and event.chat_id in route.get('excluded_chat_ids', set()):
                continue
            if event.chat_id == route.get('target_chat_id'):
                continue
            allowed = route.get('allowed_media_types', set())
            if allowed and media_type not in allowed:
                continue
            matched.append(route)
        return matched

    async def _build_shared_message_context(self, event) -> dict:
        media_type = get_media_type_string(event.message.media)
        chat = await event.get_chat()
        chat_name = getattr(chat, 'title', getattr(chat, 'first_name', f"Chat {event.chat_id}"))
        message_key = f"{event.chat_id}_{event.message.id}"
        fingerprint = await get_media_fingerprint(event.message.media)
        try:
            thumb_bytes = await self.client.download_media(event.message, file=bytes, thumb=0)
        except Exception:
            thumb_bytes = None
        thumb_md5, img_hash = await calculate_thumbnail_hash_bytes(thumb_bytes)
        return {
            'media_type': media_type,
            'chat_name': chat_name,
            'message_key': message_key,
            'fingerprint': fingerprint,
            'thumb_bytes': thumb_bytes,
            'thumb_md5': thumb_md5,
            'img_hash': img_hash,
            'reupload_payload': None,
        }

    async def _cleanup_shared_context(self, shared_ctx: dict):
        payload = (shared_ctx or {}).get('reupload_payload') or {}
        self._try_delete_cache(payload.get('cached_path'))

    async def _prepare_reupload_payload(self, event, media_type: str, shared_ctx: dict) -> dict:
        if shared_ctx.get('reupload_payload') is not None:
            return shared_ctx['reupload_payload']

        raw_size = None
        if hasattr(event.message, 'file') and getattr(event.message, 'file', None):
            try:
                raw_size = event.message.file.size
            except Exception:
                raw_size = None
        if raw_size and MAX_UPLOAD_SIZE_BYTES and raw_size > MAX_UPLOAD_SIZE_BYTES:
            payload = {'status': 'skipped_reupload_file_too_large'}
            shared_ctx['reupload_payload'] = payload
            return payload

        caption = (event.message.message or '').strip() or None
        user_dir = os.path.join(DOWNLOADS_DIR, str(self.user_id))
        os.makedirs(user_dir, exist_ok=True)

        cached_path = None
        try:
            if HAS_FAST and isinstance(event.message.media, MessageMediaDocument) and getattr(event.message.media, 'document', None):
                try:
                    original_name = None
                    for attr in (event.message.media.document.attributes or []):
                        if isinstance(attr, DocumentAttributeFilename):
                            original_name = attr.file_name
                            break
                    filename = self._build_unique_cache_name(
                        event,
                        preferred_name=original_name,
                        fallback_ext='.mp4' if media_type == 'video' else '',
                    )
                    temp_path = os.path.join(user_dir, filename)
                    with open(temp_path, 'wb') as out:
                        await ft_download_file(self.client, event.message.media.document, out)
                    cached_path = temp_path
                except Exception as e:
                    logger.warning(f"[USER_ID: {self.user_id}] Fast download failed: {e}")
                    cached_path = None
            if not cached_path:
                downloaded = await self.client.download_media(event.message, file=user_dir)
                if isinstance(downloaded, str):
                    target_name = self._build_unique_cache_name(event, preferred_name=os.path.basename(downloaded))
                    target_path = os.path.join(user_dir, target_name)
                    if os.path.abspath(downloaded) != os.path.abspath(target_path):
                        try:
                            os.replace(downloaded, target_path)
                            downloaded = target_path
                        except Exception:
                            pass
                cached_path = downloaded
        except Exception:
            data = await self.client.download_media(event.message, file=bytes)
            if not data:
                payload = {'status': 'skipped_protected_content'}
                shared_ctx['reupload_payload'] = payload
                return payload
            if MAX_UPLOAD_SIZE_BYTES and len(data) > MAX_UPLOAD_SIZE_BYTES:
                payload = {'status': 'skipped_reupload_over_limit'}
                shared_ctx['reupload_payload'] = payload
                return payload
            filename = self._build_unique_cache_name(event, fallback_ext='.bin')
            cached_path = os.path.join(user_dir, filename)
            with open(cached_path, 'wb') as handle:
                handle.write(data)

        file_to_send = cached_path
        content_hash = self._sha256_file(file_to_send) if isinstance(file_to_send, str) else None

        send_kwargs = {'caption': caption}
        if shared_ctx.get('thumb_bytes') and media_type != 'video':
            normalized_thumb = _prepare_thumb_jpeg(shared_ctx['thumb_bytes'])
            if normalized_thumb:
                send_kwargs['thumb'] = normalized_thumb

        attrs = []
        mime_type = None
        out_name = None
        if isinstance(event.message.media, MessageMediaDocument) and getattr(event.message.media, 'document', None):
            mime_type = event.message.media.document.mime_type
            for attr in (event.message.media.document.attributes or []):
                if isinstance(attr, DocumentAttributeFilename):
                    out_name = attr.file_name
                if isinstance(attr, DocumentAttributeVideo):
                    attrs.append(
                        DocumentAttributeVideo(
                            duration=getattr(attr, 'duration', None),
                            w=getattr(attr, 'w', None),
                            h=getattr(attr, 'h', None),
                            supports_streaming=True,
                        )
                    )
            if out_name:
                attrs.append(DocumentAttributeFilename(out_name))
        attrs, mime_type = _finalize_media_send_metadata(
            media_type,
            file_to_send if isinstance(file_to_send, str) else None,
            attrs,
            mime_type,
            out_name,
        )

        payload = {
            'status': 'ready',
            'cached_path': cached_path,
            'file_to_send': file_to_send,
            'content_hash': content_hash,
            'send_kwargs': send_kwargs,
            'attrs': attrs,
            'mime_type': mime_type,
            'force_document': (media_type == 'document'),
        }
        shared_ctx['reupload_payload'] = payload
        return payload

    async def _new_message_handler(self, event):
        if self.status != 'running' or event.out or not is_valid_media(event.message.media):
            return

        routes = self._matching_routes(event)
        if not routes:
            return

        shared_ctx = None
        try:
            shared_ctx = await self._build_shared_message_context(event)
            for route in routes:
                try:
                    await self._process_route(event, route, shared_ctx)
                except Exception as e:
                    logger.warning(
                        f"[USER_ID: {self.user_id}][route:{route.get('id')}] Unexpected route error: {type(e).__name__}: {e}"
                    )
        finally:
            if shared_ctx:
                await self._cleanup_shared_context(shared_ctx)

    async def _process_route(self, event, route: dict, shared_ctx: dict):
        media_type = shared_ctx['media_type']
        route_id = route['id']
        route_name = route.get('name') or f"Route {route_id}"
        target_peer = resolve_chat_peer(self.client, route['target_chat_id'])
        chat_name = shared_ctx['chat_name']
        message_key = shared_ctx['message_key']

        logger.info(
            f"[USER_ID: {self.user_id}][route:{route_id}] New {media_type} from '{chat_name}' (MsgID: {event.message.id})"
        )

        if await db_check_message_exists(self.user_id, message_key, route_id=route_id):
            logger.info(f"[USER_ID: {self.user_id}][route:{route_id}] Skip {message_key}: already processed.")
            return

        fingerprint = shared_ctx['fingerprint']
        if await db_check_duplicate_by_fingerprint(self.user_id, fingerprint, route_id=route_id):
            logger.info(f"[USER_ID: {self.user_id}][route:{route_id}] Skip {message_key}: duplicate fingerprint.")
            return

        thumb_md5 = shared_ctx['thumb_md5']
        img_hash = shared_ctx['img_hash']
        dup_key = await db_check_duplicate_by_thumbnail_hash(self.user_id, thumb_md5, route_id=route_id)
        if dup_key:
            logger.info(f"[USER_ID: {self.user_id}][route:{route_id}] Skip {message_key}: duplicate thumbnail hash.")
            await db_record_message(self.user_id, message_key, {
                'route_id': route_id,
                'chat_id': event.chat_id,
                'message_id': event.message.id,
                'chat_name': chat_name,
                'media_type': media_type,
                'fingerprint': fingerprint,
                'thumbnail_md5_hash': thumb_md5,
                'image_hash': img_hash,
                'status': 'duplicate_thumbnail_hash',
                'is_duplicate_of_key': dup_key,
            })
            return
        if img_hash:
            dup_key = await db_check_duplicate_by_image_hash(self.user_id, img_hash, route_id=route_id)
            if dup_key:
                logger.info(f"[USER_ID: {self.user_id}][route:{route_id}] Skip {message_key}: duplicate image hash.")
                await db_record_message(self.user_id, message_key, {
                    'route_id': route_id,
                    'chat_id': event.chat_id,
                    'message_id': event.message.id,
                    'chat_name': chat_name,
                    'media_type': media_type,
                    'fingerprint': fingerprint,
                    'thumbnail_md5_hash': thumb_md5,
                    'image_hash': img_hash,
                    'status': 'duplicate_image_hash',
                    'is_duplicate_of_key': dup_key,
                })
                return

        try:
            logger.info(f"[USER_ID: {self.user_id}][route:{route_id}] Forwarding MsgID: {event.message.id} -> {route_name}")
            await self.client.forward_messages(target_peer, event.message)
            await db_record_message(self.user_id, message_key, {
                'route_id': route_id,
                'chat_id': event.chat_id,
                'message_id': event.message.id,
                'chat_name': chat_name,
                'media_type': media_type,
                'fingerprint': fingerprint,
                'thumbnail_md5_hash': thumb_md5,
                'image_hash': img_hash,
                'content_hash': None,
                'status': 'forwarded_directly',
            })
            return
        except ChatForwardsRestrictedError:
            if not route.get('reupload_on_restricted'):
                logger.info(f"[USER_ID: {self.user_id}][route:{route_id}] Forward restricted. Reupload OFF.")
                await db_record_message(self.user_id, message_key, {
                    'route_id': route_id,
                    'chat_id': event.chat_id,
                    'message_id': event.message.id,
                    'chat_name': chat_name,
                    'media_type': media_type,
                    'fingerprint': fingerprint,
                    'thumbnail_md5_hash': thumb_md5,
                    'image_hash': img_hash,
                    'status': 'skipped_forward_restricted',
                })
                return
        except Exception as e:
            if "target peer" in str(e).lower():
                logger.error(f"[USER_ID: {self.user_id}][route:{route_id}] Invalid target peer for route '{route_name}'.")
                await self.send_feedback(
                    f"⛔️ Route `{route_name}` gagal dipakai karena target `{route.get('target_chat_id')}` tidak valid."
                )
            else:
                logger.warning(f"[USER_ID: {self.user_id}][route:{route_id}] Forward failed: {type(e).__name__}")
            await db_record_message(self.user_id, message_key, {
                'route_id': route_id,
                'chat_id': event.chat_id,
                'message_id': event.message.id,
                'chat_name': chat_name,
                'media_type': media_type,
                'fingerprint': fingerprint,
                'thumbnail_md5_hash': thumb_md5,
                'image_hash': img_hash,
                'status': f'error: {type(e).__name__}',
            })
            return

        await self._reupload_message(
            event=event,
            route_name=route_name,
            route_id=route_id,
            media_type=media_type,
            chat_name=chat_name,
            message_key=message_key,
            fingerprint=fingerprint,
            thumb_md5=thumb_md5,
            img_hash=img_hash,
            target_peer=target_peer,
            shared_ctx=shared_ctx,
        )

    async def _reupload_message(
        self,
        *,
        event,
        route_name: str,
        route_id: int,
        media_type: str,
        chat_name: str,
        message_key: str,
        fingerprint: str | None,
        thumb_md5: str | None,
        img_hash: str | None,
        target_peer,
        shared_ctx: dict,
    ):
        try:
            async with self.reupload_semaphore:
                payload = await self._prepare_reupload_payload(event, media_type, shared_ctx)
                status = payload.get('status')
                if status != 'ready':
                    await db_record_message(self.user_id, message_key, {
                        'route_id': route_id,
                        'chat_id': event.chat_id,
                        'message_id': event.message.id,
                        'chat_name': chat_name,
                        'media_type': media_type,
                        'fingerprint': fingerprint,
                        'thumbnail_md5_hash': thumb_md5,
                        'image_hash': img_hash,
                        'status': status,
                    })
                    return

                content_hash = payload.get('content_hash')
                if content_hash:
                    dup_key = await db_check_duplicate_by_content_hash(self.user_id, content_hash, route_id=route_id)
                    if dup_key:
                        await db_record_message(self.user_id, message_key, {
                            'route_id': route_id,
                            'chat_id': event.chat_id,
                            'message_id': event.message.id,
                            'chat_name': chat_name,
                            'media_type': media_type,
                            'fingerprint': fingerprint,
                            'thumbnail_md5_hash': thumb_md5,
                            'image_hash': img_hash,
                            'content_hash': content_hash,
                            'status': 'duplicate_content_hash',
                            'is_duplicate_of_key': dup_key,
                        })
                        return

                input_file = None
                file_to_send = payload.get('file_to_send')
                if HAS_FAST and isinstance(file_to_send, str) and os.path.exists(file_to_send):
                    try:
                        with open(file_to_send, 'rb') as handle:
                            input_file = await ft_upload_file(self.client, handle)
                    except Exception:
                        input_file = None

                send_kwargs = dict(payload.get('send_kwargs') or {})
                attrs = payload.get('attrs')
                mime_type = payload.get('mime_type')
                force_document = bool(payload.get('force_document'))

                if input_file is not None:
                    await self.client.send_file(
                        target_peer,
                        file=input_file,
                        attributes=attrs,
                        mime_type=mime_type,
                        force_document=force_document,
                        **send_kwargs,
                    )
                else:
                    await self.client.send_file(
                        target_peer,
                        file=file_to_send,
                        attributes=attrs,
                        mime_type=mime_type,
                        force_document=force_document,
                        **send_kwargs,
                    )

                logger.info(f"[USER_ID: {self.user_id}][route:{route_id}] Reuploaded MsgID: {event.message.id} -> {route_name}")
                await db_record_message(self.user_id, message_key, {
                    'route_id': route_id,
                    'chat_id': event.chat_id,
                    'message_id': event.message.id,
                    'chat_name': chat_name,
                    'media_type': media_type,
                    'fingerprint': fingerprint,
                    'thumbnail_md5_hash': thumb_md5,
                    'image_hash': img_hash,
                    'content_hash': content_hash,
                    'status': 'reuploaded_due_to_forward_restricted',
                })
        except Exception as e:
            logger.warning(
                f"[USER_ID: {self.user_id}][route:{route_id}] Reupload failed for route '{route_name}': {type(e).__name__}: {e}"
            )
            await db_record_message(self.user_id, message_key, {
                'route_id': route_id,
                'chat_id': event.chat_id,
                'message_id': event.message.id,
                'chat_name': chat_name,
                'media_type': media_type,
                'fingerprint': fingerprint,
                'thumbnail_md5_hash': thumb_md5,
                'image_hash': img_hash,
                'status': f'reupload_error: {type(e).__name__}',
            })
