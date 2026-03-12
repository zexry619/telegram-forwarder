import logging
import asyncio
from telethon import events
from telethon.errors import MessageNotModifiedError
from telethon.tl.types import KeyboardButtonCallback

from shared.config import ADMIN_USER_IDS
from shared.database import (
    allow_user,
    create_user_route,
    delete_user_route,
    disallow_user,
    get_allowed_users,
    get_route_by_id,
    get_user_config,
    get_user_routes,
    is_user_allowed,
    update_user_config,
    update_user_route,
)
from shared.telegram import get_dialog_display_name, is_selectable_target_dialog
from .keyboards import (
    admin_user_management_keyboard,
    auth_menu_keyboard,
    back_to_main_menu_button,
    dynamic_chat_list_keyboard,
    exclude_menu_keyboard,
    main_menu_keyboard,
    onboarding_keyboard,
    media_filter_keyboard,
    route_detail_keyboard,
    route_exclude_menu_keyboard,
    route_media_filter_keyboard,
    routes_menu_keyboard,
    schedule_menu_keyboard,
    settings_menu_keyboard,
)
from .conversations import setup_conversation_handlers
from user.manager import (
    get_client_for_user,
    get_worker_status,
    logout_user,
    refresh_user_worker_routes,
    start_user_worker,
    stop_user_worker,
)

logger = logging.getLogger(__name__)

def request_access_keyboard():
    return [[KeyboardButtonCallback("🚩 Request Access", b'request_access')]]

def setup_handlers(bot):
    # State sementara untuk sesi migrasi per user
    MIGRATION_STATE = {}

    # --- AUTH DECORATOR ---
    def authorized_only(func):
        async def wrapper(event):
            if event.sender_id in ADMIN_USER_IDS or await is_user_allowed(event.sender_id):
                return await func(event)
            else:
                # Susun daftar kontak admin dari ADMIN_USER_IDS (.env)
                try:
                    admin_mentions = []
                    for admin_id in ADMIN_USER_IDS:
                        # Mention berbasis ID: tg://user?id=<id>
                        admin_mentions.append(f"[Admin](tg://user?id={admin_id})")
                    admins_text = ", ".join(admin_mentions) if admin_mentions else "admin"
                except Exception:
                    admins_text = "admin"

                warning_text = (
                    "❗ Kamu belum diizinkan menggunakan bot ini.\n"
                    f"Silakan hubungi {admins_text} untuk approval,\n"
                    "atau klik tombol di bawah untuk request akses."
                )

                await event.reply(warning_text, buttons=request_access_keyboard())
        return wrapper

    async def try_edit(event, text, **kwargs):
        try:
            await event.edit(text, **kwargs)
        except MessageNotModifiedError:
            await event.answer()

    async def show_login_onboarding(event, *, via_edit: bool):
        text = (
            "**Login Dulu Sebelum Pakai Bot**\n\n"
            "Akun Telegram Anda belum terhubung ke bot ini.\n\n"
            "**Langkah pakai:**\n"
            "1. Klik `Login Sekarang`\n"
            "2. Masukkan nomor Telegram Anda, contoh `+62812xxxx`\n"
            "3. Masukkan kode verifikasi dari Telegram dengan format `1_2_3_4_5`\n"
            "4. Jika akun memakai 2FA, masukkan password Anda\n"
            "5. Setelah login berhasil, baru pilih target lalu jalankan worker\n\n"
            "Bot belum bisa dipakai sebelum proses login selesai."
        )
        if via_edit:
            await try_edit(event, text, buttons=onboarding_keyboard())
        else:
            await event.reply(text, buttons=onboarding_keyboard())

    async def refresh_live_worker(user_id: int):
        try:
            await refresh_user_worker_routes(user_id)
        except Exception as e:
            logger.warning(f"Failed to refresh live worker routes for {user_id}: {e}")

    async def chat_label(client, chat_id):
        if chat_id is None:
            return "Semua Chat"
        try:
            entity = await client.get_entity('me' if chat_id == getattr(client.me, 'id', None) else chat_id)
            return getattr(entity, 'title', getattr(entity, 'first_name', str(chat_id)))
        except Exception:
            return str(chat_id)

    async def render_route_detail(event, route: dict):
        client = await get_client_for_user(event.sender_id)
        source_label = await chat_label(client, route.get('source_chat_id')) if client else str(route.get('source_chat_id'))
        target_label = await chat_label(client, route.get('target_chat_id')) if client and route.get('target_chat_id') else (str(route.get('target_chat_id')) if route.get('target_chat_id') else 'Belum diatur')
        media_types = route.get('allowed_media_types') or set()
        media_label = "semua" if not media_types else ", ".join(sorted(media_types))
        exclude_count = len(route.get('excluded_chat_ids') or set())
        text = (
            f"🧩 **Route: {route.get('name')}**\n\n"
            f"ID: `{route.get('id')}`\n"
            f"Status: `{ 'AKTIF' if route.get('enabled') else 'NONAKTIF' }`\n"
            f"Tipe: `{ 'DEFAULT' if route.get('is_default') else 'CUSTOM' }`\n"
            f"Sumber: `{source_label}`\n"
            f"Tujuan: `{target_label}`\n"
            f"Media: `{media_label}`\n"
            f"Pengecualian: `{exclude_count} chat`\n"
            f"Re-upload saat forward diblokir: `{ 'ON' if route.get('reupload_on_restricted') else 'OFF' }`"
        )
        await try_edit(
            event,
            text,
            buttons=route_detail_keyboard(
                route['id'],
                route.get('enabled', True),
                route.get('is_default', False),
                route.get('reupload_on_restricted', False),
            ),
        )

    def login_required(func):
        async def wrapper(event):
            client = await get_client_for_user(event.sender_id)
            if client:
                return await func(event)
            if hasattr(event, 'answer') and hasattr(event, 'edit'):
                await event.answer("Login akun Telegram dulu.", alert=True)
                await show_login_onboarding(event, via_edit=True)
            else:
                await show_login_onboarding(event, via_edit=False)
        return wrapper

    # --- HANDLER MENU UTAMA & DASAR ---
    @bot.on(events.NewMessage(pattern='/start'))
    @authorized_only
    async def start_handler(event):
        client = await get_client_for_user(event.sender_id)
        if not client:
            await show_login_onboarding(event, via_edit=False)
            return
        buttons = main_menu_keyboard()
        if event.sender_id in ADMIN_USER_IDS:
            buttons.append([KeyboardButtonCallback("⚙️ Admin Panel", b'admin_user_mgmt')])
        await event.reply(
            "**Selamat Datang di Bot Forwarder!**\n\nGunakan tombol di bawah untuk mengelola akun Anda.",
            buttons=buttons
        )

    @bot.on(events.CallbackQuery(data=b'main_menu'))
    @authorized_only
    async def main_menu_handler(event):
        client = await get_client_for_user(event.sender_id)
        if not client:
            await show_login_onboarding(event, via_edit=True)
            return
        buttons = main_menu_keyboard()
        if event.sender_id in ADMIN_USER_IDS:
            buttons.append([KeyboardButtonCallback("⚙️ Admin Panel", b'admin_user_mgmt')])
        await try_edit(event, "**Menu Utama**\n\nSilakan pilih salah satu opsi:", buttons=buttons)

    @bot.on(events.CallbackQuery(data=b'status'))
    @authorized_only
    @login_required
    async def status_handler(event):
        await event.answer("Memuat status...")
        user_id = event.sender_id
        config = await get_user_config(user_id)
        routes = await get_user_routes(user_id)
        client = await get_client_for_user(user_id)
        worker_status = await get_worker_status(user_id)
        target = await chat_label(client, config.get('target_chat_id')) if client and config.get('target_chat_id') else "Belum diatur"
        exclude_count = len(config.get('excluded_chat_ids', set()))
        active_routes = len([route for route in routes if route.get('enabled')])
        route_lines = []
        for route in routes[:5]:
            src = await chat_label(client, route.get('source_chat_id')) if client else str(route.get('source_chat_id'))
            dst = await chat_label(client, route.get('target_chat_id')) if client and route.get('target_chat_id') else "Belum diatur"
            route_lines.append(
                f"• `{route.get('name')}`: `{src}` → `{dst}` {'✅' if route.get('enabled') else '⏸️'}"
            )
        status_text = (
            f"📊 **Status Akun Anda**\n\n"
            f"**Status Worker:** `{worker_status.upper()}`\n"
            f"**Target Default:** `{target}`\n"
            f"**Chat Dikecualikan (default):** `{exclude_count} chat`\n"
            f"**Total Routes:** `{len(routes)}`\n"
            f"**Routes Aktif:** `{active_routes}`\n"
            f"**Re-upload saat forward diblokir:** `{ 'ON' if config.get('reupload_on_restricted') else 'OFF' }`\n"
            f"**Eager download cache:** `{ 'ON' if config.get('eager_cache_enabled') else 'OFF' }`"
        )
        if route_lines:
            status_text += "\n\n**Ringkasan Routes:**\n" + "\n".join(route_lines)
        await try_edit(event, status_text, buttons=main_menu_keyboard())

    # --- HANDLER WORKER (START/STOP) ---
    @bot.on(events.CallbackQuery(data=b'start_worker'))
    @authorized_only
    @login_required
    async def start_worker_handler(event):
        await event.answer("Memulai worker...", alert=False)
        success, message = await start_user_worker(event.sender_id, bot)
        await event.answer(message, alert=True)
        await status_handler(event)

    @bot.on(events.CallbackQuery(data=b'stop_worker'))
    @authorized_only
    @login_required
    async def stop_worker_handler(event):
        await event.answer("Menghentikan worker...", alert=False)
        success, message = await stop_user_worker(event.sender_id)
        await event.answer(message, alert=True)
        await status_handler(event)

    # --- HANDLER PEMILIHAN TARGET (INTERAKTIF DENGAN OPSI HAPUS) ---
    @bot.on(events.CallbackQuery(data=b'list_chats'))
    @authorized_only
    @login_required
    async def list_chats_for_target_handler(event):
        user_id = event.sender_id
        await event.answer("Mengambil daftar chat...")
        client = await get_client_for_user(user_id)
        if not client:
            return await event.answer("Anda harus login terlebih dahulu.", alert=True)
        try:
            config = await get_user_config(user_id)
            current_target = config.get('target_chat_id')
            dialogs = await client.get_dialogs(limit=50)
            buttons = []
            if current_target:
                buttons.append([KeyboardButtonCallback("🗑️ Hapus Target Saat Ini", b'delete_target')])
            for dialog in dialogs:
                if not is_selectable_target_dialog(dialog, include_saved_messages=True):
                    continue
                btn_text = get_dialog_display_name(dialog)[:40]
                if dialog.id == current_target:
                    btn_text = f"🎯 {btn_text}"
                callback_data = f"pick_target_{dialog.id}".encode('utf-8')
                buttons.append([KeyboardButtonCallback(btn_text, callback_data)])
            buttons.append([KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')])
            prompt_text = (
                "**Pilih atau Ubah Target Default Route**\n\n"
                f"**Target saat ini:** `{current_target or 'Belum diatur'}`\n\n"
                "Klik pada grup/channel atau `Saved Messages` di bawah untuk menjadikannya target default."
            )
            await try_edit(event, prompt_text, buttons=buttons)
        except Exception as e:
            logger.error(f"Error listing chats for target for user {user_id}: {e}", exc_info=True)
            await event.answer(f"Error: {type(e).__name__}", alert=True)

    @bot.on(events.CallbackQuery(pattern=b"pick_target_(-?\\d+)"))
    @authorized_only
    @login_required
    async def pick_target_handler(event):
        user_id = event.sender_id
        try:
            target_id = int(event.pattern_match.group(1).decode('utf-8'))
        except (IndexError, ValueError):
            return await event.answer("Callback data tidak valid.", alert=True)
        await event.answer(f"Mengatur target ke ID: {target_id}...")
        await update_user_config(user_id, 'target_chat_id', target_id)
        await refresh_live_worker(user_id)
        await event.edit(f"✅ **Target berhasil diatur ke:** `{target_id}`", buttons=main_menu_keyboard())

    @bot.on(events.CallbackQuery(data=b'delete_target'))
    @authorized_only
    @login_required
    async def delete_target_handler(event):
        user_id = event.sender_id
        await event.answer("Menghapus target...", alert=False)
        await update_user_config(user_id, 'target_chat_id', None)
        await refresh_live_worker(user_id)
        await event.answer("✅ Target berhasil dihapus!", alert=True)
        await list_chats_for_target_handler(event)

    # --- HANDLER MANAJEMEN PENGECUALIAN (INTERAKTIF) ---
    @bot.on(events.CallbackQuery(data=b'set_exclude'))
    @authorized_only
    @login_required
    async def set_exclude_menu_handler(event):
        await event.answer()
        config = await get_user_config(event.sender_id)
        count = len(config.get('excluded_chat_ids', set()))
        text = (f"🚫 **Menu Pengecualian**\n\n"
                f"Pesan dari chat yang dikecualikan akan diabaikan oleh `Default Route`.\n"
                f"**Jumlah saat ini:** `{count} chat`")
        await try_edit(event, text, buttons=exclude_menu_keyboard())

    @bot.on(events.CallbackQuery(data=b'exclude_add_list'))
    @authorized_only
    @login_required
    async def exclude_add_list_handler(event):
        user_id = event.sender_id
        await event.answer("Memuat daftar chat untuk ditambahkan...")
        client = await get_client_for_user(user_id)
        if not client: return await event.answer("Harap login.", alert=True)
        try:
            dialogs = await client.get_dialogs(limit=50)
            config = await get_user_config(user_id)
            buttons = dynamic_chat_list_keyboard(dialogs, "excl_add", config.get('excluded_chat_ids', set()), show_all=True)
            buttons.append([KeyboardButtonCallback("⬅️ Kembali ke Menu Pengecualian", b'set_exclude')])
            await try_edit(event, "Pilih chat untuk **ditambahkan** ke pengecualian (yang sudah ada ditandai ✅):", buttons=buttons)
        except Exception as e: 
            logger.error(f"Error listing chats for exclusion (add) for user {user_id}: {e}")
            await event.answer(f"Error: {type(e).__name__}", alert=True)

    @bot.on(events.CallbackQuery(data=b'exclude_remove_list'))
    @authorized_only
    @login_required
    async def exclude_remove_list_handler(event):
        user_id = event.sender_id
        await event.answer("Memuat daftar pengecualian...")
        client = await get_client_for_user(user_id)
        if not client: return await event.answer("Harap login.", alert=True)
        try:
            config = await get_user_config(user_id)
            existing_ids = config.get('excluded_chat_ids', set())
            if not existing_ids: return await event.answer("Daftar pengecualian Anda kosong.", alert=True)
            dialogs = await client.get_dialogs(limit=100)
            buttons = dynamic_chat_list_keyboard(dialogs, "excl_rem", existing_ids, show_all=False)
            buttons.append([KeyboardButtonCallback("⬅️ Kembali ke Menu Pengecualian", b'set_exclude')])
            await try_edit(event, "Pilih chat untuk **dihapus** dari pengecualian:", buttons=buttons)
        except Exception as e: 
            logger.error(f"Error listing chats for exclusion (remove) for user {user_id}: {e}")
            await event.answer(f"Error: {type(e).__name__}", alert=True)
            
    @bot.on(events.CallbackQuery(pattern=b"excl_add_(-?\\d+)"))
    @authorized_only
    @login_required
    async def exclude_add_handler(event):
        user_id, chat_id = event.sender_id, int(event.pattern_match.group(1))
        config = await get_user_config(user_id)
        excluded_ids = config.get('excluded_chat_ids', set())
        if chat_id in excluded_ids: return await event.answer("Sudah ada di daftar.")
        excluded_ids.add(chat_id)
        await update_user_config(user_id, 'excluded_chat_ids', excluded_ids)
        await refresh_live_worker(user_id)
        await event.answer(f"ID {chat_id} ditambahkan.", alert=False)
        await exclude_add_list_handler(event) # Refresh

    @bot.on(events.CallbackQuery(pattern=b"excl_rem_(-?\\d+)"))
    @authorized_only
    @login_required
    async def exclude_remove_handler(event):
        user_id, chat_id = event.sender_id, int(event.pattern_match.group(1))
        config = await get_user_config(user_id)
        excluded_ids = config.get('excluded_chat_ids', set())
        excluded_ids.discard(chat_id)
        await update_user_config(user_id, 'excluded_chat_ids', excluded_ids)
        await refresh_live_worker(user_id)
        await event.answer(f"ID {chat_id} dihapus.", alert=False)
        await exclude_remove_list_handler(event) # Refresh

    # --- HANDLER MEDIA FILTER ---
    @bot.on(events.CallbackQuery(data=b'set_media_filter'))
    @authorized_only
    @login_required
    async def media_filter_menu_handler(event):
        config = await get_user_config(event.sender_id)
        current = config.get('allowed_media_types', set())
        text = ("🗂 **Filter Media**\n\n"
                "Pilih tipe media yang akan diteruskan oleh `Default Route`:")
        await try_edit(event, text, buttons=media_filter_keyboard(current))

    @bot.on(events.CallbackQuery(data=b'media_filter_all'))
    @authorized_only
    @login_required
    async def media_filter_all_handler(event):
        await update_user_config(event.sender_id, 'allowed_media_types', set())
        await refresh_live_worker(event.sender_id)
        await event.answer('Mengatur filter ke: semua media')
        await media_filter_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'media_filter_photo'))
    @authorized_only
    @login_required
    async def media_filter_photo_handler(event):
        await update_user_config(event.sender_id, 'allowed_media_types', {'photo'})
        await refresh_live_worker(event.sender_id)
        await event.answer('Mengatur filter ke: hanya foto')
        await media_filter_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'media_filter_video'))
    @authorized_only
    @login_required
    async def media_filter_video_handler(event):
        await update_user_config(event.sender_id, 'allowed_media_types', {'video'})
        await refresh_live_worker(event.sender_id)
        await event.answer('Mengatur filter ke: hanya video')
        await media_filter_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'media_filter_document'))
    @authorized_only
    @login_required
    async def media_filter_document_handler(event):
        await update_user_config(event.sender_id, 'allowed_media_types', {'document'})
        await refresh_live_worker(event.sender_id)
        await event.answer('Mengatur filter ke: hanya dokumen')
        await media_filter_menu_handler(event)

    # --- HANDLER JADWAL OTOMATIS ---
    @bot.on(events.CallbackQuery(data=b'schedule_menu'))
    @authorized_only
    @login_required
    async def schedule_menu_handler(event):
        config = await get_user_config(event.sender_id)
        start = config.get('start_time') or '-'
        stop = config.get('stop_time') or '-'
        text = f"⏰ **Jadwal Otomatis**\n\nWaktu mulai: `{start}`\nWaktu stop: `{stop}`"
        await try_edit(event, text, buttons=schedule_menu_keyboard())

    @bot.on(events.CallbackQuery(data=b'set_start_time'))
    @authorized_only
    @login_required
    async def set_start_time_handler(event):
        async with bot.conversation(event.sender_id, timeout=60) as conv:
            await conv.send_message("Kirim jam mulai (HH:MM), atau ketik batal:")
            resp = await conv.get_response()
            t = resp.text.strip()
            if t.lower() == 'batal':
                await conv.send_message("Dibatalkan.")
            else:
                try:
                    import datetime
                    datetime.datetime.strptime(t, "%H:%M")
                    await update_user_config(event.sender_id, 'start_time', t)
                    await conv.send_message(f"Jam mulai diatur ke {t}")
                except ValueError:
                    await conv.send_message("Format waktu salah. Gunakan HH:MM")
        await schedule_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'set_stop_time'))
    @authorized_only
    @login_required
    async def set_stop_time_handler(event):
        async with bot.conversation(event.sender_id, timeout=60) as conv:
            await conv.send_message("Kirim jam stop (HH:MM), atau ketik batal:")
            resp = await conv.get_response()
            t = resp.text.strip()
            if t.lower() == 'batal':
                await conv.send_message("Dibatalkan.")
            else:
                try:
                    import datetime
                    datetime.datetime.strptime(t, "%H:%M")
                    await update_user_config(event.sender_id, 'stop_time', t)
                    await conv.send_message(f"Jam stop diatur ke {t}")
                except ValueError:
                    await conv.send_message("Format waktu salah. Gunakan HH:MM")
        await schedule_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'clear_schedule'))
    @authorized_only
    @login_required
    async def clear_schedule_handler(event):
        await update_user_config(event.sender_id, 'start_time', None)
        await update_user_config(event.sender_id, 'stop_time', None)
        await event.answer('Jadwal dinonaktifkan', alert=True)
        await schedule_menu_handler(event)

    # --- HANDLER PENGATURAN ---
    @bot.on(events.CallbackQuery(data=b'settings_menu'))
    @authorized_only
    @login_required
    async def settings_menu_handler(event):
        config = await get_user_config(event.sender_id)
        text = (
            "⚙️ **Pengaturan Default Route**\n\n"
            "- Re-upload saat forward diblokir: aktifkan untuk mengunduh dan mengunggah ulang media jika forward langsung ditolak.\n"
            "  Catatan: Hormati aturan sumber dan hak cipta.\n\n"
            "- Eager download cache: tidak menunda forward; cache hanya dipakai untuk re-upload jika forward diblokir.\n"
            "  Cache dibersihkan otomatis berdasarkan TTL."
        )
        await try_edit(event, text, buttons=settings_menu_keyboard(config.get('reupload_on_restricted'), config.get('eager_cache_enabled')))

    @bot.on(events.CallbackQuery(data=b'toggle_reupload_on_restricted'))
    @authorized_only
    @login_required
    async def toggle_reupload_on_restricted_handler(event):
        user_id = event.sender_id
        config = await get_user_config(user_id)
        new_val = not bool(config.get('reupload_on_restricted'))
        await update_user_config(user_id, 'reupload_on_restricted', new_val)
        await refresh_live_worker(user_id)
        await event.answer(f"Re-upload saat forward diblokir: {'ON' if new_val else 'OFF'}", alert=True)
        await settings_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'toggle_eager_cache'))
    @authorized_only
    @login_required
    async def toggle_eager_cache_handler(event):
        user_id = event.sender_id
        config = await get_user_config(user_id)
        new_val = not bool(config.get('eager_cache_enabled'))
        await update_user_config(user_id, 'eager_cache_enabled', new_val)
        await refresh_live_worker(user_id)
        await event.answer(f"Eager download cache: {'ON' if new_val else 'OFF'}", alert=True)
        await settings_menu_handler(event)

    # --- HANDLER OTENTIKASI & BANTUAN ---
    @bot.on(events.CallbackQuery(data=b'auth_menu'))
    @authorized_only
    async def auth_menu_handler(event):
        client = await get_client_for_user(event.sender_id)
        if client:
            text = "🔐 **Login / Logout**\n\nAkun Telegram Anda sudah terhubung. Jika ingin ganti akun, logout dulu."
        else:
            text = (
                "🔐 **Login / Logout**\n\n"
                "Anda belum login.\n"
                "Klik `Login dengan Akun`, lalu masukkan nomor Telegram, kode verifikasi, dan password 2FA jika diminta."
            )
        await try_edit(event, text, buttons=auth_menu_keyboard())

    @bot.on(events.CallbackQuery(data=b'logout'))
    @authorized_only
    async def logout_handler(event):
        await event.answer("Memproses logout...", alert=False)
        success, message = await logout_user(event.sender_id)
        await event.answer(message, alert=True)
        await main_menu_handler(event)
        
    @bot.on(events.CallbackQuery(data=b'help'))
    @authorized_only
    async def help_handler(event):
        help_text = """
        **❓ Bantuan & Informasi**

        Bot ini memungkinkan Anda untuk meneruskan media dari semua chat (kecuali yang dikecualikan) ke satu chat target secara otomatis.

        **Alur Kerja:**
        1.  **Login:** Klik `Login Sekarang` atau buka menu `Login/Logout`, lalu masukkan nomor Telegram Anda.
        2.  **Verifikasi:** Masukkan kode dari Telegram dengan format `1_2_3_4_5`. Jika akun memakai 2FA, masukkan password Anda.
        3.  **Pilih Target:** Gunakan menu `Pilih Target dari Daftar` untuk memilih/mengubah/menghapus grup/channel atau `Saved Messages` tujuan.
        4.  **Atur Pengecualian:** Masuk ke menu `Atur Pengecualian` untuk menambah atau menghapus chat yang pesannya akan diabaikan.
        5.  **Migrasi Media:** Jika ingin salin media antar chat, gunakan tombol `Migrasi Media` di menu utama.
        6.  **Jalankan:** Klik `Jalankan` untuk memulai proses forwarding.
        7.  **Status:** Cek konfigurasi dan status worker Anda (berjalan/berhenti) kapan saja.
        """
        await try_edit(event, help_text, buttons=back_to_main_menu_button())

    # --- HANDLER ROUTES ---
    @bot.on(events.CallbackQuery(data=b'routes_menu'))
    @authorized_only
    @login_required
    async def routes_menu_handler(event):
        text = (
            "🧩 **Kelola Routes**\n\n"
            "Route adalah aturan forwarding terpisah.\n"
            "Contoh:\n"
            "- Semua video -> Channel A\n"
            "- Grup B -> Channel C\n"
            "- Saved Messages -> Channel D\n\n"
            "Menu lama seperti Target/Pengecualian/Filter/Pengaturan tetap mengedit `Default Route`."
        )
        await try_edit(event, text, buttons=routes_menu_keyboard())

    @bot.on(events.CallbackQuery(data=b'route_list'))
    @authorized_only
    @login_required
    async def route_list_handler(event):
        routes = await get_user_routes(event.sender_id)
        buttons = []
        for route in routes:
            marker = "✅" if route.get('enabled') else "⏸️"
            suffix = " (Default)" if route.get('is_default') else ""
            buttons.append([
                KeyboardButtonCallback(f"{marker} {route.get('name')[:28]}{suffix}", f"route_view_{route['id']}".encode())
            ])
        buttons.append([KeyboardButtonCallback("➕ Tambah Route Baru", b'route_add')])
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", b'routes_menu')])
        await try_edit(event, "📋 **Daftar Routes**\n\nPilih route untuk melihat atau mengubah.", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"route_view_(\\d+)"))
    @authorized_only
    @login_required
    async def route_view_handler(event):
        route = await get_route_by_id(event.sender_id, int(event.pattern_match.group(1)))
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        await render_route_detail(event, route)

    @bot.on(events.CallbackQuery(pattern=b"route_rename_(\\d+)"))
    @authorized_only
    @login_required
    async def route_rename_handler(event):
        route_id = int(event.pattern_match.group(1))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        async with bot.conversation(event.sender_id, timeout=120) as conv:
            await conv.send_message(
                f"Nama route saat ini: `{route.get('name')}`\nKirim nama baru. Ketik `batal` untuk membatalkan."
            )
            resp = await conv.get_response()
            name = (resp.text or '').strip()
            if not name or name.lower() == 'batal':
                await conv.send_message("Ubah nama dibatalkan.")
                return await render_route_detail(event, route)
            route = await update_user_route(event.sender_id, route_id, name=name[:80])
        await refresh_live_worker(event.sender_id)
        await event.answer("Nama route diperbarui.")
        await render_route_detail(event, route)

    @bot.on(events.CallbackQuery(data=b'route_add'))
    @authorized_only
    @login_required
    async def route_add_handler(event):
        user_id = event.sender_id
        async with bot.conversation(user_id, timeout=120) as conv:
            await conv.send_message("Kirim nama route baru. Contoh: `Video Grup A -> Channel B`. Ketik `batal` untuk membatalkan.")
            resp = await conv.get_response()
            name = (resp.text or '').strip()
            if not name or name.lower() == 'batal':
                await conv.send_message("Pembuatan route dibatalkan.", buttons=routes_menu_keyboard())
                return
            config = await get_user_config(user_id)
            route = await create_user_route(
                user_id,
                name[:80],
                reupload_on_restricted=bool(config.get('reupload_on_restricted')),
            )
        await refresh_live_worker(user_id)
        await event.edit(
            f"✅ Route `{route['name']}` dibuat.\nSekarang pilih sumber untuk route ini.",
            buttons=[[KeyboardButtonCallback("📥 Pilih Sumber", f"route_pick_source_menu_{route['id']}".encode())],
                     [KeyboardButtonCallback("⬅️ Kembali ke Routes", b'route_list')]]
        )

    @bot.on(events.CallbackQuery(pattern=b"route_pick_source_menu_(\\d+)"))
    @authorized_only
    @login_required
    async def route_pick_source_menu_handler(event):
        route_id = int(event.pattern_match.group(1))
        client = await get_client_for_user(event.sender_id)
        dialogs = await client.get_dialogs(limit=100)
        buttons = [[KeyboardButtonCallback("🌐 Semua Chat", f"route_pick_source_any_{route_id}".encode())]]
        buttons.extend(dynamic_chat_list_keyboard(
            dialogs, f"route_pick_source_{route_id}", set(), show_all=True, include_saved_messages=True
        ))
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", f"route_view_{route_id}".encode())])
        await try_edit(event, "Pilih sumber route. Gunakan `Semua Chat` jika route ini berlaku global.", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"route_pick_source_any_(\\d+)"))
    @authorized_only
    @login_required
    async def route_pick_source_any_handler(event):
        route_id = int(event.pattern_match.group(1))
        route = await update_user_route(event.sender_id, route_id, source_chat_id=None)
        await refresh_live_worker(event.sender_id)
        await event.answer("Sumber route diatur ke semua chat.")
        await render_route_detail(event, route)

    @bot.on(events.CallbackQuery(pattern=b"route_pick_source_(\\d+)_(-?\\d+)"))
    @authorized_only
    @login_required
    async def route_pick_source_handler(event):
        route_id = int(event.pattern_match.group(1))
        chat_id = int(event.pattern_match.group(2))
        route = await update_user_route(event.sender_id, route_id, source_chat_id=chat_id)
        await refresh_live_worker(event.sender_id)
        await event.answer("Sumber route berhasil diatur.")
        await render_route_detail(event, route)

    @bot.on(events.CallbackQuery(pattern=b"route_pick_target_menu_(\\d+)"))
    @authorized_only
    @login_required
    async def route_pick_target_menu_handler(event):
        route_id = int(event.pattern_match.group(1))
        client = await get_client_for_user(event.sender_id)
        dialogs = await client.get_dialogs(limit=100)
        buttons = dynamic_chat_list_keyboard(
            dialogs, f"route_pick_target_{route_id}", set(), show_all=True, include_saved_messages=True
        )
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", f"route_view_{route_id}".encode())])
        await try_edit(event, "Pilih tujuan route ini:", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"route_pick_target_(\\d+)_(-?\\d+)"))
    @authorized_only
    @login_required
    async def route_pick_target_handler(event):
        route_id = int(event.pattern_match.group(1))
        chat_id = int(event.pattern_match.group(2))
        route = await update_user_route(event.sender_id, route_id, target_chat_id=chat_id)
        await refresh_live_worker(event.sender_id)
        await event.answer("Tujuan route berhasil diatur.")
        await render_route_detail(event, route)

    @bot.on(events.CallbackQuery(pattern=b"route_toggle_(\\d+)"))
    @authorized_only
    @login_required
    async def route_toggle_handler(event):
        route_id = int(event.pattern_match.group(1))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        route = await update_user_route(event.sender_id, route_id, enabled=not route.get('enabled'))
        await refresh_live_worker(event.sender_id)
        await event.answer(f"Route {'diaktifkan' if route.get('enabled') else 'dinonaktifkan'}.", alert=True)
        await render_route_detail(event, route)

    @bot.on(events.CallbackQuery(pattern=b"route_delete_(\\d+)"))
    @authorized_only
    @login_required
    async def route_delete_handler(event):
        route_id = int(event.pattern_match.group(1))
        try:
            deleted = await delete_user_route(event.sender_id, route_id)
        except ValueError as e:
            return await event.answer(str(e), alert=True)
        if not deleted:
            return await event.answer("Route tidak ditemukan.", alert=True)
        await refresh_live_worker(event.sender_id)
        await event.answer("Route berhasil dihapus.", alert=True)
        await route_list_handler(event)

    @bot.on(events.CallbackQuery(pattern=b"route_filter_menu_(\\d+)"))
    @authorized_only
    @login_required
    async def route_filter_menu_handler(event):
        route_id = int(event.pattern_match.group(1))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        current = route.get('allowed_media_types', set())
        selected = "semua media" if not current else ", ".join(sorted(current))
        await try_edit(
            event,
            f"🗂 **Filter Media Route**\n\nRoute: `{route.get('name')}`\nAktif: `{selected}`\n\nTekan tombol untuk toggle pilihan.",
            buttons=route_media_filter_keyboard(route_id, current),
        )

    async def _update_route_media_filter(event, route_id: int, media_types: set):
        await update_user_route(event.sender_id, route_id, allowed_media_types=media_types)
        await refresh_live_worker(event.sender_id)
        await event.answer("Filter route diperbarui.")
        await route_filter_menu_handler(event)

    async def _toggle_route_media_type(event, route_id: int, media_type: str):
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        media_types = set(route.get('allowed_media_types') or set())
        if media_type in media_types:
            media_types.remove(media_type)
        else:
            media_types.add(media_type)
        await _update_route_media_filter(event, route_id, media_types)

    @bot.on(events.CallbackQuery(pattern=b"route_filter_all_(\\d+)"))
    @authorized_only
    @login_required
    async def route_filter_all_handler(event):
        await _update_route_media_filter(event, int(event.pattern_match.group(1)), set())

    @bot.on(events.CallbackQuery(pattern=b"route_filter_photo_(\\d+)"))
    @authorized_only
    @login_required
    async def route_filter_photo_handler(event):
        await _toggle_route_media_type(event, int(event.pattern_match.group(1)), 'photo')

    @bot.on(events.CallbackQuery(pattern=b"route_filter_video_(\\d+)"))
    @authorized_only
    @login_required
    async def route_filter_video_handler(event):
        await _toggle_route_media_type(event, int(event.pattern_match.group(1)), 'video')

    @bot.on(events.CallbackQuery(pattern=b"route_filter_document_(\\d+)"))
    @authorized_only
    @login_required
    async def route_filter_document_handler(event):
        await _toggle_route_media_type(event, int(event.pattern_match.group(1)), 'document')

    @bot.on(events.CallbackQuery(pattern=b"route_exclude_menu_(\\d+)"))
    @authorized_only
    @login_required
    async def route_exclude_menu_handler(event):
        route_id = int(event.pattern_match.group(1))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        count = len(route.get('excluded_chat_ids') or set())
        text = (
            f"🚫 **Pengecualian Route**\n\n"
            f"Route: `{route.get('name')}`\n"
            f"Jumlah pengecualian: `{count} chat`\n\n"
            "Catatan: jika sumber route spesifik ke satu chat, daftar pengecualian biasanya tidak terpakai."
        )
        await try_edit(event, text, buttons=route_exclude_menu_keyboard(route_id))

    @bot.on(events.CallbackQuery(pattern=b"route_exclude_add_list_(\\d+)"))
    @authorized_only
    @login_required
    async def route_exclude_add_list_handler(event):
        route_id = int(event.pattern_match.group(1))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        client = await get_client_for_user(event.sender_id)
        dialogs = await client.get_dialogs(limit=100)
        buttons = dynamic_chat_list_keyboard(
            dialogs,
            f"route_exclude_add_{route_id}",
            route.get('excluded_chat_ids', set()),
            show_all=True,
            include_saved_messages=True,
        )
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", f"route_exclude_menu_{route_id}".encode())])
        await try_edit(event, f"Pilih chat untuk ditambahkan ke pengecualian route `{route.get('name')}`:", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"route_exclude_remove_list_(\\d+)"))
    @authorized_only
    @login_required
    async def route_exclude_remove_list_handler(event):
        route_id = int(event.pattern_match.group(1))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        existing_ids = route.get('excluded_chat_ids', set())
        if not existing_ids:
            return await event.answer("Daftar pengecualian route kosong.", alert=True)
        client = await get_client_for_user(event.sender_id)
        dialogs = await client.get_dialogs(limit=100)
        buttons = dynamic_chat_list_keyboard(
            dialogs,
            f"route_exclude_remove_{route_id}",
            existing_ids,
            show_all=False,
            include_saved_messages=True,
        )
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", f"route_exclude_menu_{route_id}".encode())])
        await try_edit(event, f"Pilih chat untuk dihapus dari pengecualian route `{route.get('name')}`:", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"route_exclude_add_(\\d+)_(-?\\d+)"))
    @authorized_only
    @login_required
    async def route_exclude_add_handler(event):
        route_id = int(event.pattern_match.group(1))
        chat_id = int(event.pattern_match.group(2))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        excluded = set(route.get('excluded_chat_ids') or set())
        if chat_id in excluded:
            return await event.answer("Chat sudah ada di pengecualian route.", alert=True)
        excluded.add(chat_id)
        await update_user_route(event.sender_id, route_id, excluded_chat_ids=excluded)
        await refresh_live_worker(event.sender_id)
        await event.answer("Pengecualian route ditambahkan.")
        await route_exclude_add_list_handler(event)

    @bot.on(events.CallbackQuery(pattern=b"route_exclude_remove_(\\d+)_(-?\\d+)"))
    @authorized_only
    @login_required
    async def route_exclude_remove_handler(event):
        route_id = int(event.pattern_match.group(1))
        chat_id = int(event.pattern_match.group(2))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        excluded = set(route.get('excluded_chat_ids') or set())
        excluded.discard(chat_id)
        await update_user_route(event.sender_id, route_id, excluded_chat_ids=excluded)
        await refresh_live_worker(event.sender_id)
        await event.answer("Pengecualian route dihapus.")
        await route_exclude_remove_list_handler(event)

    @bot.on(events.CallbackQuery(pattern=b"route_reupload_toggle_(\\d+)"))
    @authorized_only
    @login_required
    async def route_reupload_toggle_handler(event):
        route_id = int(event.pattern_match.group(1))
        route = await get_route_by_id(event.sender_id, route_id)
        if not route:
            return await event.answer("Route tidak ditemukan.", alert=True)
        route = await update_user_route(
            event.sender_id,
            route_id,
            reupload_on_restricted=not route.get('reupload_on_restricted'),
        )
        await refresh_live_worker(event.sender_id)
        await event.answer(
            f"Re-upload route: {'ON' if route.get('reupload_on_restricted') else 'OFF'}",
            alert=True,
        )
        await render_route_detail(event, route)

    async def open_migration_menu(event, *, via_edit: bool):
        user_id = event.sender_id
        from user.manager import get_client_for_user
        client = await get_client_for_user(user_id)
        if not client:
            if via_edit:
                await event.answer("Harap login terlebih dahulu.", alert=True)
                await show_login_onboarding(event, via_edit=True)
            else:
                await show_login_onboarding(event, via_edit=False)
            return

        MIGRATION_STATE[user_id] = {'src': None, 'dst': None, 'limit': None}
        text = (
            "🚚 **Migrasi Media**\n\n"
            "Gunakan menu ini untuk menyalin media dari satu chat ke chat lain.\n\n"
            "**Alur singkat:**\n"
            "1. Pilih sumber\n"
            "2. Pilih tujuan\n"
            "3. Masukkan limit pesan\n"
            "4. Pilih mode duplikasi\n"
            "5. Pilih paralel upload\n\n"
            "Progress migrasi akan dikirim realtime."
        )
        buttons = [
            [KeyboardButtonCallback("📥 Pilih Sumber", b'mig_src_list')],
            [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]
        ]
        if via_edit:
            await try_edit(event, text, buttons=buttons)
        else:
            await event.reply(text, buttons=buttons)

    # --- FITUR MIGRASI MEDIA ---
    @bot.on(events.CallbackQuery(data=b'migration_menu'))
    @authorized_only
    @login_required
    async def migration_menu_handler(event):
        await open_migration_menu(event, via_edit=True)

    @bot.on(events.NewMessage(pattern='/migrasi'))
    @authorized_only
    @login_required
    async def migrasi_entry(event):
        await open_migration_menu(event, via_edit=False)

    @bot.on(events.CallbackQuery(data=b'mig_src_list'))
    @authorized_only
    @login_required
    async def mig_src_list(event):
        user_id = event.sender_id
        from user.manager import get_client_for_user
        client = await get_client_for_user(user_id)
        if not client:
            return await event.answer("Harap login terlebih dahulu.", alert=True)
        dialogs = await client.get_dialogs(limit=100)
        buttons = dynamic_chat_list_keyboard(
            dialogs, "mig_pick_src", set(), show_all=True, include_saved_messages=True
        )
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", b'main_menu')])
        await try_edit(event, "Pilih Grup/Channel/Saved Messages Sumber:", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"mig_pick_src_(-?\\d+)"))
    @authorized_only
    @login_required
    async def mig_pick_src(event):
        user_id = event.sender_id
        chat_id = int(event.pattern_match.group(1))
        state = MIGRATION_STATE.get(user_id) or {'src': None, 'dst': None}
        state['src'] = chat_id
        MIGRATION_STATE[user_id] = state
        await event.answer("Sumber dipilih.")
        await event.edit(
            f"✅ Sumber terpilih: `{chat_id}`\nSekarang pilih tujuan.",
            buttons=[[KeyboardButtonCallback("🎯 Pilih Tujuan", b'mig_dst_list')],
                     [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]]
        )

    @bot.on(events.CallbackQuery(data=b'mig_dst_list'))
    @authorized_only
    @login_required
    async def mig_dst_list(event):
        user_id = event.sender_id
        from user.manager import get_client_for_user
        client = await get_client_for_user(user_id)
        if not client:
            return await event.answer("Harap login terlebih dahulu.", alert=True)
        dialogs = await client.get_dialogs(limit=100)
        buttons = dynamic_chat_list_keyboard(
            dialogs, "mig_pick_dst", set(), show_all=True, include_saved_messages=True
        )
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", b'main_menu')])
        await try_edit(event, "Pilih Grup/Channel/Saved Messages Tujuan:", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"mig_pick_dst_(-?\\d+)"))
    @authorized_only
    @login_required
    async def mig_pick_dst(event):
        user_id = event.sender_id
        chat_id = int(event.pattern_match.group(1))
        state = MIGRATION_STATE.get(user_id)
        if not state or not state.get('src'):
            return await event.answer("Pilih sumber dulu dari menu Migrasi Media.", alert=True)
        state['dst'] = chat_id
        MIGRATION_STATE[user_id] = state

        # Ask for limit using conversation
        async with bot.conversation(user_id, timeout=120) as conv:
            await conv.send_message(
                f"✅ Tujuan terpilih: `{chat_id}`\nKetik jumlah pesan yang ingin disalin (hanya yang ada media).\nContoh: `100` atau ketik `semua`:")
            resp = await conv.get_response()
            txt = (resp.text or '').strip().lower()
            limit = None
            if txt != 'semua':
                try:
                    limit = int(txt)
                    if limit <= 0:
                        limit = None
                except Exception:
                    limit = None
            state['limit'] = limit
            MIGRATION_STATE[user_id] = state

            # Ask dedupe mode via buttons
            await conv.send_message(
                "Pilih mode deteksi duplikasi:",
                buttons=[
                    [KeyboardButtonCallback("🚫 Nonaktif", b'mig_dedupe_none')],
                    [KeyboardButtonCallback("🙂 Longgar", b'mig_dedupe_loose')],
                    [KeyboardButtonCallback("🔒 Ketat", b'mig_dedupe_strict')],
                ]
            )

        # Menunggu pilihan mode duplikasi pada tombol
        await event.answer("Pilih mode duplikasi dari tombol yang muncul.")

    @bot.on(events.CallbackQuery(data=b'mig_dedupe_none'))
    @authorized_only
    @login_required
    async def mig_dedupe_none(event):
        await _start_migration_with_mode(event, 'none')

    @bot.on(events.CallbackQuery(data=b'mig_dedupe_loose'))
    @authorized_only
    @login_required
    async def mig_dedupe_loose(event):
        await _start_migration_with_mode(event, 'loose')

    @bot.on(events.CallbackQuery(data=b'mig_dedupe_strict'))
    @authorized_only
    @login_required
    async def mig_dedupe_strict(event):
        await _start_migration_with_mode(event, 'strict')

    async def _start_migration_with_mode(event, mode: str):
        user_id = event.sender_id
        state = MIGRATION_STATE.get(user_id)
        if not state or not state.get('src') or not state.get('dst'):
            return await event.edit("Sesi migrasi tidak lengkap. Buka lagi menu `Migrasi Media` dari menu utama.", buttons=back_to_main_menu_button())
        state['dedupe_mode'] = mode
        MIGRATION_STATE[user_id] = state

        # Prompt concurrency options
        buttons = [
            [KeyboardButtonCallback("⚡ Paralel 1", b'mig_conc_1')],
            [KeyboardButtonCallback("⚡⚡ Paralel 2", b'mig_conc_2')],
            [KeyboardButtonCallback("⚡⚡⚡ Paralel 3", b'mig_conc_3')],
        ]
        await event.edit("Pilih tingkat paralelisme re-upload:", buttons=buttons)

    @bot.on(events.CallbackQuery(data=b'mig_conc_1'))
    @authorized_only
    @login_required
    async def mig_conc_1(event):
        await _start_migration_now(event, 1)

    @bot.on(events.CallbackQuery(data=b'mig_conc_2'))
    @authorized_only
    @login_required
    async def mig_conc_2(event):
        await _start_migration_now(event, 2)

    @bot.on(events.CallbackQuery(data=b'mig_conc_3'))
    @authorized_only
    @login_required
    async def mig_conc_3(event):
        await _start_migration_now(event, 3)

    async def _start_migration_now(event, concurrency: int):
        user_id = event.sender_id
        state = MIGRATION_STATE.get(user_id)
        if not state or not state.get('src') or not state.get('dst'):
            return await event.edit("Sesi migrasi tidak lengkap. Buka lagi menu `Migrasi Media` dari menu utama.", buttons=back_to_main_menu_button())
        state['concurrency'] = concurrency
        MIGRATION_STATE[user_id] = state

        from user.manager import get_client_for_user
        client = await get_client_for_user(user_id)
        if not client:
            return await event.edit("❌ Sesi tidak valid. Silakan login ulang.", buttons=back_to_main_menu_button())

        from user.migrator import run_migration
        src = state['src']
        dst = state['dst']
        lim = state.get('limit')
        dmode = state.get('dedupe_mode', 'loose')
        conc = state.get('concurrency', 1)

        await event.edit(
            f"🚀 Migrasi akan dimulai. Sumber `{src}` → Tujuan `{dst}`\nLimit: `{lim if lim else 'semua'}`\nMode duplikasi: `{dmode}`\nParalel: `{conc}`\nProgress realtime akan muncul di bawah ini.",
            buttons=back_to_main_menu_button()
        )

        # Create a status message with Cancel button
        status = await bot.send_message(
            user_id,
            "📈 Status Migrasi\nMenyiapkan…",
            buttons=[[KeyboardButtonCallback("⛔ Batalkan", b'mig_cancel')]]
        )

        # Prepare stop event and task in state
        import asyncio as _asyncio
        stop_event = _asyncio.Event()
        MIGRATION_STATE[user_id]['stop_event'] = stop_event
        MIGRATION_STATE[user_id]['status_msg_id'] = status.id

        task = _asyncio.get_event_loop().create_task(
            run_migration(
                user_id, client, bot, src, dst,
                limit=lim, dedupe_mode=dmode, concurrency=conc,
                stop_event=stop_event, status_msg_id=status.id
            )
        )
        MIGRATION_STATE[user_id]['task'] = task

    @bot.on(events.CallbackQuery(data=b'mig_cancel'))
    @authorized_only
    @login_required
    async def mig_cancel(event):
        user_id = event.sender_id
        state = MIGRATION_STATE.get(user_id)
        if not state or not state.get('task'):
            return await event.answer("Tidak ada migrasi yang berjalan.", alert=True)
        stop_event = state.get('stop_event')
        if stop_event:
            stop_event.set()
        await event.answer("Sedang membatalkan migrasi…", alert=False)
        try:
            await bot.edit_message(user_id, state.get('status_msg_id'), "⛔ Membatalkan migrasi…")
        except Exception:
            pass

    # --- ADMIN PANEL ---
    @bot.on(events.CallbackQuery(data=b'admin_user_mgmt'))
    async def admin_user_mgmt_menu(event):
        if event.sender_id not in ADMIN_USER_IDS:
            await event.answer("Kamu bukan admin!", alert=True)
            return
        await event.edit("Kelola Allowed Users:", buttons=admin_user_management_keyboard())

    @bot.on(events.CallbackQuery(data=b'admin_list_users'))
    async def admin_list_users(event):
        if event.sender_id not in ADMIN_USER_IDS:
            await event.answer("Kamu bukan admin!", alert=True)
            return
        users = await get_allowed_users()
        text = "Allowed users:\n\n" + "\n".join([f"`{uid}`" for uid in users]) if users else "Belum ada user yang diizinkan."
        await event.edit(text, buttons=admin_user_management_keyboard())

    @bot.on(events.CallbackQuery(data=b'admin_add_user'))
    async def admin_add_user(event):
        if event.sender_id not in ADMIN_USER_IDS:
            await event.answer("Kamu bukan admin!", alert=True)
            return
        async with bot.conversation(event.sender_id, timeout=60) as conv:
            await conv.send_message("Kirimkan USER ID Telegram yang ingin diizinkan:")
            resp = await conv.get_response()
            try:
                uid = int(resp.text.strip())
                await allow_user(uid)
                await conv.send_message(f"✅ User `{uid}` berhasil diizinkan.", buttons=admin_user_management_keyboard())
            except:
                await conv.send_message("❌ Format USER ID tidak valid.", buttons=admin_user_management_keyboard())

    @bot.on(events.CallbackQuery(data=b'admin_remove_user'))
    async def admin_remove_user(event):
        if event.sender_id not in ADMIN_USER_IDS:
            await event.answer("Kamu bukan admin!", alert=True)
            return
        async with bot.conversation(event.sender_id, timeout=60) as conv:
            await conv.send_message("Kirimkan USER ID Telegram yang ingin dihapus dari allowed user:")
            resp = await conv.get_response()
            try:
                uid = int(resp.text.strip())
                await disallow_user(uid)
                await conv.send_message(f"✅ User `{uid}` sudah tidak diizinkan lagi.", buttons=admin_user_management_keyboard())
            except:
                await conv.send_message("❌ Format USER ID tidak valid.", buttons=admin_user_management_keyboard())

    # --- HANDLER REQUEST & APPROVE USER BARU ---
    @bot.on(events.CallbackQuery(data=b'request_access'))
    async def request_access_handler(event):
        for admin_id in ADMIN_USER_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🚩 User `{event.sender_id}` meminta akses ke bot.\nApprove user ini?",
                    buttons=[
                        [KeyboardButtonCallback(f"✅ Approve {event.sender_id}", f'approve_user_{event.sender_id}'.encode())]
                    ]
                )
            except Exception:
                pass
        await event.edit("✅ Request akses sudah dikirim ke admin. Silakan tunggu approval.")

    @bot.on(events.CallbackQuery(pattern=r'approve_user_(\d+)'))
    async def approve_user_handler(event):
        if event.sender_id not in ADMIN_USER_IDS:
            await event.answer("Kamu bukan admin!", alert=True)
            return
        new_user_id = int(event.pattern_match.group(1).decode())
        await allow_user(new_user_id)
        try:
            await bot.send_message(new_user_id, "✅ Kamu sudah di-approve admin, silakan gunakan bot.")
        except Exception:
            pass
        await event.edit(f"User `{new_user_id}` sudah di-approve dan kini bisa akses bot.")

    # --- DAFTARKAN CONVERSATION HANDLERS DARI FILE LAIN (UNTUK LOGIN) ---
    setup_conversation_handlers(bot)
