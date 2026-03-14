import asyncio
import logging
import os
import hashlib
import time
from io import BytesIO
from typing import Optional

from telethon.errors import (
    ChatForwardsRestrictedError,
    FloodWaitError,
    RpcCallFailError,
    ServerError,
    TimedOutError,
)
from telethon.tl.types import MessageMediaDocument, DocumentAttributeFilename, DocumentAttributeVideo
from telethon.tl.types import KeyboardButtonCallback
from PIL import Image
from telethon import utils as tg_utils

try:
    from FastTelethon import download_file as ft_download_file, upload_file as ft_upload_file
    HAS_FAST = True
except Exception:
    HAS_FAST = False

from shared.config import MAX_UPLOAD_SIZE_BYTES, DOWNLOADS_DIR
from shared.database import (
    db_record_message,
    db_check_duplicate_by_fingerprint,
    db_check_duplicate_by_thumbnail_hash,
    db_check_duplicate_by_image_hash,
    db_check_duplicate_by_content_hash,
)
from shared.telegram import resolve_chat_peer

# Reuse helpers from worker
from user.worker import (
    is_valid_media,
    get_media_type_string,
    get_media_fingerprint,
    calculate_thumbnail_hash_bytes,
)

logger = logging.getLogger(__name__)


class MigrationCancelled(Exception):
    pass

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


async def _sha256_file(path: str) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _build_unique_cache_name(source_chat_id: int, msg_id: int, preferred_name: str | None = None, fallback_ext: str = "") -> str:
    safe_name = os.path.basename(preferred_name) if preferred_name else ""
    if os.altsep:
        safe_name = safe_name.replace(os.altsep, "_")
    safe_name = safe_name.replace(os.sep, "_")
    if not safe_name:
        safe_name = f"media{fallback_ext}"
    return f"{source_chat_id}_{msg_id}_{safe_name}"


def _migration_status_buttons(active: bool):
    if not active:
        return None
    return [[KeyboardButtonCallback("⛔ Batalkan", b'mig_cancel')]]


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


async def _sleep_with_cancel(delay: float, stop_event: asyncio.Event | None):
    if delay <= 0:
        return
    if not stop_event:
        await asyncio.sleep(delay)
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay)
    except asyncio.TimeoutError:
        return
    raise MigrationCancelled()


async def _run_with_retry(coro_factory, *, stop_event: asyncio.Event | None = None, retries: int = 2):
    attempt = 0
    while True:
        if stop_event and stop_event.is_set():
            raise MigrationCancelled()
        try:
            return await coro_factory()
        except MigrationCancelled:
            raise
        except FloodWaitError as e:
            if attempt >= retries or getattr(e, 'seconds', 0) > 10:
                raise
            attempt += 1
            await _sleep_with_cancel(float(getattr(e, 'seconds', 1)), stop_event)
        except (TimedOutError, RpcCallFailError, ServerError):
            if attempt >= retries:
                raise
            attempt += 1
            await _sleep_with_cancel(float(attempt), stop_event)


def _is_retryable_rpc_error(exc: Exception) -> bool:
    return isinstance(exc, (TimedOutError, RpcCallFailError, ServerError, FloodWaitError))


async def run_migration(
    user_id: int,
    client,
    bot_client,
    source_chat_id: int,
    dest_chat_id: int,
    *,
    limit: Optional[int] = None,
    respect_media_filter: bool = True,
    dedupe_mode: str = 'loose',  # 'none' | 'loose' | 'strict'
    concurrency: int = 1,
    stop_event: asyncio.Event | None = None,
    status_msg_id: int | None = None,
):
    """
    Migrate media messages from `source_chat_id` to `dest_chat_id` using user's account.
    - Try direct forward first; if forward-restricted and size-permits, reupload.
    - Records statuses in DB with prefix 'migrated_...'.
    - Sends progress updates via bot_client.
    """
    # Guard: source != dest
    if source_chat_id == dest_chat_id:
        try:
            await bot_client.send_message(user_id, "❌ Sumber dan tujuan tidak boleh sama.")
        except Exception:
            pass
        return

    # Load user config for media filter and reupload toggle
    from shared.database import get_user_config
    config = await get_user_config(user_id)
    allowed = set(config.get('allowed_media_types', set())) if respect_media_filter else set()
    reupload_on_restricted = bool(config.get('reupload_on_restricted'))
    source_peer = resolve_chat_peer(client, source_chat_id)
    dest_peer = resolve_chat_peer(client, dest_chat_id)

    processed = 0
    succeeded = 0
    reuploaded = 0
    skipped = 0
    failed = 0

    # Resolve names for clearer terminal logs
    try:
        src_ent = await client.get_entity(source_peer)
        src_name = getattr(src_ent, 'title', getattr(src_ent, 'first_name', str(source_chat_id)))
    except Exception:
        src_name = str(source_chat_id)
    try:
        dst_ent = await client.get_entity(dest_peer)
        dst_name = getattr(dst_ent, 'title', getattr(dst_ent, 'first_name', str(dest_chat_id)))
    except Exception:
        dst_name = str(dest_chat_id)

    async def notify(msg: str):
        try:
            await bot_client.send_message(user_id, msg)
        except Exception:
            pass

    last_status_edit_ts = 0.0
    migration_done = False
    async def update_status():
        if not status_msg_id:
            return
        nonlocal last_status_edit_ts
        # Throttle edits to at most once every 1.5s
        now = time.time()
        if now - last_status_edit_ts < 1.5:
            return
        last_status_edit_ts = now
        try:
            text = (
                "📈 Status Migrasi\n"
                f"Sumber: `{src_name}` → Tujuan: `{dst_name}`\n"
                f"Dedupe: `{dedupe_mode}` | Paralel: `{concurrency}`\n"
                f"Diproses: `{processed}` | Sukses: `{succeeded}` | Reupload: `{reuploaded}`\n"
                f"Skip: `{skipped}` | Gagal: `{failed}`"
            )
            # Show up to 3 active per-item progresses
            active_lines = []
            try:
                for (mid, info) in list(progress_map.items())[:3]:
                    phase, pct = info
                    active_lines.append(f"• Msg `{mid}` {phase}: {pct}%")
            except Exception:
                pass
            if active_lines:
                text += "\n" + "\n".join(active_lines)
            await bot_client.edit_message(
                user_id,
                status_msg_id,
                text,
                buttons=_migration_status_buttons(not migration_done and not (stop_event and stop_event.is_set()))
            )
        except Exception:
            pass

    # Track per-item progress here
    progress_map = {}

    async def status_pump():
        while not migration_done:
            await asyncio.sleep(1.5)
            try:
                await update_status()
            except Exception:
                pass

    def make_progress_cb(msg_id: int, phase: str):
        last_pct = {'v': -1}

        def cb(current: int, total: int):
            if stop_event and stop_event.is_set():
                raise MigrationCancelled()
            try:
                if not total:
                    return
                pct = int(current * 100 / total)
                if pct >= last_pct['v'] + 5 or pct in (100,):
                    last_pct['v'] = pct
                    progress_map[msg_id] = (phase, pct)
            except Exception:
                pass
        return cb

    logger.info(
        f"[MIG][user:{user_id}] Start migration | Source: {src_name} ({source_chat_id}) → Dest: {dst_name} ({dest_chat_id}) | "
        f"Limit: {limit if limit else 'all'} | Dedupe: {dedupe_mode} | ReuploadOnRestricted: {reupload_on_restricted}"
    )
    await notify(
        f"🚚 Memulai migrasi media\nSumber: `{source_chat_id}` → Tujuan: `{dest_chat_id}`\n"
        + (f"Limit: `{limit}`" if limit else "Limit: semua")
    )
    await update_status()
    status_task = asyncio.create_task(status_pump()) if status_msg_id else None

    # Concurrency setup
    sem = asyncio.Semaphore(max(1, int(concurrency or 1)))
    counter_lock = asyncio.Lock()

    async def process_message(m):
        nonlocal processed, succeeded, reuploaded, skipped, failed
        media_type = 'unknown'

        if stop_event and stop_event.is_set():
            raise MigrationCancelled()
        if not is_valid_media(m.media):
            return

        media_type = get_media_type_string(m.media)
        if allowed and media_type not in allowed:
            return

        message_key = f"{source_chat_id}_{m.id}"
        try:
            fp = await get_media_fingerprint(m.media)
            thumb_md5 = None
            img_hash = None
            if dedupe_mode != 'none':
                if await db_check_duplicate_by_fingerprint(user_id, fp, route_id=None):
                    skipped += 1
                    logger.info(f"[MIG][user:{user_id}] Skip msg {m.id} ({media_type}) due to duplicate fingerprint")
                    await db_record_message(user_id, message_key, {
                        'chat_id': source_chat_id,
                        'message_id': m.id,
                        'chat_name': None,
                        'media_type': media_type,
                        'fingerprint': fp,
                        'thumbnail_md5_hash': None,
                        'image_hash': None,
                        'status': 'migrated_skip_duplicate_fingerprint',
                    })
                    return

                if dedupe_mode == 'strict':
                    thumb_bytes = await _run_with_retry(
                        lambda: client.download_media(m, file=bytes, thumb=0),
                        stop_event=stop_event,
                    )
                    thumb_md5, img_hash = await calculate_thumbnail_hash_bytes(thumb_bytes)
                    dup_key = await db_check_duplicate_by_thumbnail_hash(user_id, thumb_md5, route_id=None)
                    if dup_key:
                        skipped += 1
                        logger.info(f"[MIG][user:{user_id}] Skip msg {m.id} ({media_type}) due to duplicate thumbnail hash")
                        await db_record_message(user_id, message_key, {
                            'chat_id': source_chat_id,
                            'message_id': m.id,
                            'chat_name': None,
                            'media_type': media_type,
                            'fingerprint': fp,
                            'thumbnail_md5_hash': thumb_md5,
                            'image_hash': img_hash,
                            'status': 'migrated_skip_duplicate_thumbnail',
                            'is_duplicate_of_key': dup_key,
                        })
                        return
                    if img_hash:
                        dup_key = await db_check_duplicate_by_image_hash(user_id, img_hash, route_id=None)
                        if dup_key:
                            skipped += 1
                            logger.info(f"[MIG][user:{user_id}] Skip msg {m.id} ({media_type}) due to duplicate image hash")
                            await db_record_message(user_id, message_key, {
                                'chat_id': source_chat_id,
                                'message_id': m.id,
                                'chat_name': None,
                                'media_type': media_type,
                                'fingerprint': fp,
                                'thumbnail_md5_hash': thumb_md5,
                                'image_hash': img_hash,
                                'status': 'migrated_skip_duplicate_image',
                                'is_duplicate_of_key': dup_key,
                            })
                            return

            try:
                logger.info(f"[MIG][user:{user_id}] Forward msg {m.id} ({media_type}) → {dst_name} ({dest_chat_id})")
                await _run_with_retry(
                    lambda: client.forward_messages(dest_peer, m),
                    stop_event=stop_event,
                )
                succeeded += 1
                await db_record_message(user_id, message_key, {
                    'chat_id': source_chat_id,
                    'message_id': m.id,
                    'chat_name': None,
                    'media_type': media_type,
                    'fingerprint': fp,
                    'thumbnail_md5_hash': thumb_md5,
                    'image_hash': img_hash,
                    'status': 'migrated_forwarded',
                })
            except ChatForwardsRestrictedError:
                if not reupload_on_restricted:
                    skipped += 1
                    logger.info(f"[MIG][user:{user_id}] Restricted msg {m.id}. Reupload OFF → skip")
                    await db_record_message(user_id, message_key, {
                        'chat_id': source_chat_id,
                        'message_id': m.id,
                        'chat_name': None,
                        'media_type': media_type,
                        'fingerprint': fp,
                        'thumbnail_md5_hash': thumb_md5,
                        'image_hash': img_hash,
                        'status': 'migrated_skipped_forward_restricted',
                    })
                    return

                try:
                    logger.info(f"[MIG][user:{user_id}] Restricted msg {m.id}. Trying reupload…")
                    raw_size = None
                    if hasattr(m, 'file') and getattr(m, 'file', None):
                        try:
                            raw_size = m.file.size
                        except Exception:
                            raw_size = None
                    if raw_size and MAX_UPLOAD_SIZE_BYTES and raw_size > MAX_UPLOAD_SIZE_BYTES:
                        skipped += 1
                        logger.info(f"[MIG][user:{user_id}] Skip reupload msg {m.id}: size {raw_size} > limit {MAX_UPLOAD_SIZE_BYTES}")
                        await db_record_message(user_id, message_key, {
                            'chat_id': source_chat_id,
                            'message_id': m.id,
                            'chat_name': None,
                            'media_type': media_type,
                            'fingerprint': fp,
                            'thumbnail_md5_hash': thumb_md5,
                            'image_hash': img_hash,
                            'status': 'migrated_skip_over_limit',
                        })
                        return

                    caption = (m.message or '').strip() or None
                    user_dir = os.path.join(DOWNLOADS_DIR, str(user_id))
                    os.makedirs(user_dir, exist_ok=True)

                    cached_path = None
                    # Prefer parallel downloader if available and we have a Document (with size)
                    if HAS_FAST and isinstance(m.media, MessageMediaDocument) and getattr(m.media, 'document', None):
                        try:
                            # Choose a filename
                            fname = None
                            for attr in (m.media.document.attributes or []):
                                if isinstance(attr, DocumentAttributeFilename):
                                    fname = attr.file_name
                                    break
                            if not fname:
                                ext = '.mp4' if media_type == 'video' else ''
                                fname = f"media{ext}"
                            fname = _build_unique_cache_name(source_chat_id, m.id, preferred_name=fname, fallback_ext='.mp4' if media_type == 'video' else '')
                            cached_path = os.path.join(user_dir, fname)
                            with open(cached_path, 'wb') as out:
                                await _run_with_retry(
                                    lambda: ft_download_file(
                                        client,
                                        m.media.document,
                                        out,
                                        progress_callback=make_progress_cb(m.id, 'downloading')
                                    ),
                                    stop_event=stop_event,
                                )
                        except Exception as e:
                            cached_path = None
                            logger.info(f"[MIG][user:{user_id}] Fast download failed for msg {m.id}: {type(e).__name__}")
                    if not cached_path:
                        try:
                            downloaded = await _run_with_retry(
                                lambda: client.download_media(
                                    m,
                                    file=user_dir,
                                    progress_callback=make_progress_cb(m.id, 'downloading')
                                ),
                                stop_event=stop_event,
                            )
                            if isinstance(downloaded, str):
                                unique_name = _build_unique_cache_name(source_chat_id, m.id, preferred_name=os.path.basename(downloaded))
                                unique_path = os.path.join(user_dir, unique_name)
                                if os.path.abspath(downloaded) != os.path.abspath(unique_path):
                                    try:
                                        os.replace(downloaded, unique_path)
                                        downloaded = unique_path
                                    except Exception:
                                        pass
                            cached_path = downloaded
                        except Exception as download_exc:
                            if _is_retryable_rpc_error(download_exc):
                                raise
                            data = await _run_with_retry(
                                lambda: client.download_media(
                                    m,
                                    file=bytes,
                                    progress_callback=make_progress_cb(m.id, 'downloading')
                                ),
                                stop_event=stop_event,
                            )
                            if not data:
                                skipped += 1
                                logger.info(f"[MIG][user:{user_id}] Skip reupload msg {m.id}: protected content (no bytes)")
                                await db_record_message(user_id, message_key, {
                                    'chat_id': source_chat_id,
                                    'message_id': m.id,
                                    'chat_name': None,
                                    'media_type': media_type,
                                    'fingerprint': fp,
                                    'thumbnail_md5_hash': thumb_md5,
                                    'image_hash': img_hash,
                                    'status': 'migrated_skip_protected_content',
                                })
                                return
                            if MAX_UPLOAD_SIZE_BYTES and len(data) > MAX_UPLOAD_SIZE_BYTES:
                                skipped += 1
                                logger.info(f"[MIG][user:{user_id}] Skip reupload msg {m.id}: downloaded bytes exceed limit {MAX_UPLOAD_SIZE_BYTES}")
                                await db_record_message(user_id, message_key, {
                                    'chat_id': source_chat_id,
                                    'message_id': m.id,
                                    'chat_name': None,
                                    'media_type': media_type,
                                    'fingerprint': fp,
                                    'thumbnail_md5_hash': thumb_md5,
                                    'image_hash': img_hash,
                                    'status': 'migrated_skip_over_limit',
                                })
                                return
                            # Write bytes to disk to use fast upload
                            fname = _build_unique_cache_name(source_chat_id, m.id, fallback_ext='.bin')
                            cached_path = os.path.join(user_dir, fname)
                            with open(cached_path, 'wb') as f:
                                f.write(data)

                    # Calculate strong content hash from cached file (preferred)
                    content_hash = None
                    try:
                        if cached_path and os.path.exists(cached_path):
                            content_hash = await _sha256_file(cached_path)
                    except Exception:
                        content_hash = None

                    if dedupe_mode == 'strict' and content_hash:
                        dup_key = await db_check_duplicate_by_content_hash(user_id, content_hash, route_id=None)
                        if dup_key:
                            skipped += 1
                            logger.info(f"[MIG][user:{user_id}] Skip reupload msg {m.id}: duplicate by content hash")
                            try:
                                if cached_path and os.path.exists(cached_path):
                                    os.remove(cached_path)
                            except Exception:
                                pass
                            await db_record_message(user_id, message_key, {
                                'chat_id': source_chat_id,
                                'message_id': m.id,
                                'chat_name': None,
                                'media_type': media_type,
                                'fingerprint': fp,
                                'thumbnail_md5_hash': thumb_md5,
                                'image_hash': img_hash,
                                'content_hash': content_hash,
                                'status': 'migrated_skip_duplicate_content',
                                'is_duplicate_of_key': dup_key,
                            })
                            return

                    # Build send args and upload via FastTelethon if possible for speed
                    send_kwargs = {
                        'caption': caption,
                        'force_document': (media_type == 'document')
                    }
                    # Thumb optional
                    thumb_data = thumb_bytes if thumb_md5 else None
                    if thumb_data is None and media_type != 'video':
                        try:
                            thumb_data = await _run_with_retry(
                                lambda: client.download_media(m, file=bytes, thumb=0),
                                stop_event=stop_event,
                            )
                        except Exception:
                            thumb_data = None
                    if thumb_data and media_type != 'video':
                        norm_thumb = _prepare_thumb_jpeg(thumb_data)
                        if norm_thumb:
                            send_kwargs['thumb'] = norm_thumb

                    if stop_event and stop_event.is_set():
                        return

                    input_file = None
                    if HAS_FAST and cached_path and os.path.exists(cached_path):
                        try:
                            with open(cached_path, 'rb') as f:
                                input_file = await _run_with_retry(
                                    lambda: ft_upload_file(
                                        client,
                                        f,
                                        progress_callback=make_progress_cb(m.id, 'uploading')
                                    ),
                                    stop_event=stop_event,
                                )
                        except Exception as e:
                            input_file = None
                            logger.info(f"[MIG][user:{user_id}] Fast upload failed for msg {m.id}: {type(e).__name__}")

                    if input_file is not None:
                        # Build attributes/mime from original message to ensure streamable video
                        attrs = []
                        mime_type = None
                        out_name = None
                        if isinstance(m.media, MessageMediaDocument) and getattr(m.media, 'document', None):
                            mime_type = m.media.document.mime_type
                            for attr in (m.media.document.attributes or []):
                                if isinstance(attr, DocumentAttributeFilename):
                                    out_name = attr.file_name
                                if isinstance(attr, DocumentAttributeVideo):
                                    attrs.append(DocumentAttributeVideo(
                                        duration=getattr(attr, 'duration', None),
                                        w=getattr(attr, 'w', None),
                                        h=getattr(attr, 'h', None),
                                        supports_streaming=True
                                    ))
                            # Ensure filename present
                            if out_name:
                                attrs.append(DocumentAttributeFilename(out_name))
                        attrs, mime_type = _finalize_media_send_metadata(
                            media_type,
                            cached_path,
                            attrs,
                            mime_type,
                            out_name,
                        )

                        force_doc = bool(send_kwargs.pop('force_document', media_type == 'document'))
                        await _run_with_retry(
                            lambda: client.send_file(
                                dest_peer,
                                file=input_file,
                                attributes=attrs,
                                mime_type=mime_type,
                                force_document=force_doc,
                                **send_kwargs
                            ),
                            stop_event=stop_event,
                        )
                    else:
                        # Fallback to standard send_file using cached_path, with explicit attributes
                        attrs = []
                        mime_type = None
                        out_name = None
                        if isinstance(m.media, MessageMediaDocument) and getattr(m.media, 'document', None):
                            mime_type = m.media.document.mime_type
                            for attr in (m.media.document.attributes or []):
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
                        attrs, mime_type = _finalize_media_send_metadata(
                            media_type,
                            cached_path,
                            attrs,
                            mime_type,
                            out_name,
                        )

                        force_doc = bool(send_kwargs.pop('force_document', media_type == 'document'))
                        await _run_with_retry(
                            lambda: client.send_file(
                                dest_peer,
                                file=cached_path,
                                attributes=attrs or None,
                                mime_type=mime_type,
                                progress_callback=make_progress_cb(m.id, 'uploading'),
                                force_document=force_doc,
                                **send_kwargs
                            ),
                            stop_event=stop_event,
                        )
                    reuploaded += 1
                    logger.info(f"[MIG][user:{user_id}] Reuploaded msg {m.id} ({media_type}) → {dst_name} ({dest_chat_id})")
                    await db_record_message(user_id, message_key, {
                        'chat_id': source_chat_id,
                        'message_id': m.id,
                        'chat_name': None,
                        'media_type': media_type,
                        'fingerprint': fp,
                        'thumbnail_md5_hash': thumb_md5,
                        'image_hash': img_hash,
                        'content_hash': content_hash,
                        'status': 'migrated_reuploaded',
                    })
                    try:
                        if cached_path and os.path.exists(cached_path):
                            os.remove(cached_path)
                    except Exception:
                        pass
                except MigrationCancelled:
                    raise
                except Exception as e:
                    failed += 1
                    logger.warning(f"[MIG][user:{user_id}] Reupload failed for msg {m.id}: {type(e).__name__}: {e}")
                    await db_record_message(user_id, message_key, {
                        'chat_id': source_chat_id,
                        'message_id': m.id,
                        'chat_name': None,
                        'media_type': media_type,
                        'fingerprint': fp,
                        'thumbnail_md5_hash': None,
                        'image_hash': None,
                        'status': f'migrated_error:{type(e).__name__}',
                    })
                    try:
                        await notify(f"❌ Reupload gagal untuk msg `{m.id}`: {type(e).__name__}")
                    except Exception:
                        pass
            except MigrationCancelled:
                raise
            except Exception as e:
                failed += 1
                logger.warning(f"[MIG][user:{user_id}] Forward failed for msg {m.id}: {type(e).__name__}: {e}")
                await db_record_message(user_id, message_key, {
                    'chat_id': source_chat_id,
                    'message_id': m.id,
                    'chat_name': None,
                    'media_type': media_type,
                    'fingerprint': None,
                    'thumbnail_md5_hash': None,
                    'image_hash': None,
                    'status': f'migrated_error:{type(e).__name__}',
                })
                try:
                    await notify(f"❌ Forward gagal untuk msg `{m.id}`: {type(e).__name__}")
                except Exception:
                    pass
        except MigrationCancelled:
            raise
        except Exception as e:
            failed += 1
            await db_record_message(user_id, message_key, {
                'chat_id': source_chat_id,
                'message_id': getattr(m, 'id', None),
                'chat_name': None,
                'media_type': media_type,
                'fingerprint': None,
                'thumbnail_md5_hash': None,
                'image_hash': None,
                'status': f'migrated_error_outer:{type(e).__name__}',
            })
        finally:
            async with counter_lock:
                processed += 1
                if processed % 25 == 0:
                    logger.info(
                        f"[MIG][user:{user_id}] Progress | processed={processed} success={succeeded} reupload={reuploaded} skip={skipped} fail={failed}"
                    )
                    await notify(
                        f"📦 Migrasi berjalan: diproses `{processed}` | sukses `{succeeded}` | reupload `{reuploaded}` | skip `{skipped}` | gagal `{failed}`"
                    )
                # Update status message every item
                await update_status()
            # Clear entry for this message
            progress_map.pop(m.id, None)

    async def worker_wrapper(m):
        async with sem:
            await process_message(m)

    pending = set()
    max_pending = max(1, int(concurrency or 1))
    cancelled = False
    try:
        async for m in client.iter_messages(source_peer, limit=limit, reverse=True):
            if stop_event and stop_event.is_set():
                cancelled = True
                break
            pending.add(asyncio.create_task(worker_wrapper(m)))
            if len(pending) >= max_pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    try:
                        await task
                    except MigrationCancelled:
                        cancelled = True

        if cancelled and pending:
            for queued in pending:
                queued.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            pending = set()

        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    await task
                except MigrationCancelled:
                    cancelled = True
                    for queued in pending:
                        queued.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                        pending = set()
                    break
            if cancelled:
                break
    except asyncio.CancelledError:
        cancelled = True
    finally:
        migration_done = True
        if status_task:
            status_task.cancel()
            await asyncio.gather(status_task, return_exceptions=True)
        await update_status()

    if cancelled or (stop_event and stop_event.is_set()):
        try:
            await bot_client.send_message(user_id, "⛔ Migrasi dibatalkan oleh pengguna.")
        except Exception:
            pass
        logger.info(
            f"[MIG][user:{user_id}] Cancelled | processed={processed} success={succeeded} reupload={reuploaded} skip={skipped} fail={failed}"
        )
        return

    logger.info(f"[MIG][user:{user_id}] Done | processed={processed} success={succeeded} reupload={reuploaded} skip={skipped} fail={failed}")
    await notify(
        f"✅ Migrasi selesai. Total diproses `{processed}`, sukses `{succeeded}`, reupload `{reuploaded}`, skip `{skipped}`, gagal `{failed}`"
    )
