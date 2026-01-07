import logging
import asyncio
from telethon import events
from telethon.errors import MessageNotModifiedError
from telethon.tl.types import KeyboardButtonCallback

from shared.config import ADMIN_USER_IDS
from shared.database import (
    get_user_config, update_user_config,
    allow_user, disallow_user, get_allowed_users, is_user_allowed
)
from .keyboards import (
    main_menu_keyboard, auth_menu_keyboard, back_to_main_menu_button,
    exclude_menu_keyboard, dynamic_chat_list_keyboard, admin_user_management_keyboard,
    media_filter_keyboard, schedule_menu_keyboard, settings_menu_keyboard
)
from .conversations import setup_conversation_handlers
from user.manager import (
    get_worker_status, start_user_worker, stop_user_worker, logout_user, 
    get_client_for_user
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

    # --- HANDLER MENU UTAMA & DASAR ---
    @bot.on(events.NewMessage(pattern='/start'))
    @authorized_only
    async def start_handler(event):
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
        buttons = main_menu_keyboard()
        if event.sender_id in ADMIN_USER_IDS:
            buttons.append([KeyboardButtonCallback("⚙️ Admin Panel", b'admin_user_mgmt')])
        await try_edit(event, "**Menu Utama**\n\nSilakan pilih salah satu opsi:", buttons=buttons)

    @bot.on(events.CallbackQuery(data=b'status'))
    @authorized_only
    async def status_handler(event):
        await event.answer("Memuat status...")
        user_id = event.sender_id
        config = await get_user_config(user_id)
        worker_status = await get_worker_status(user_id)
        target = config.get('target_chat_id', "Belum diatur")
        exclude_count = len(config.get('excluded_chat_ids', set()))
        status_text = (
            f"📊 **Status Akun Anda**\n\n"
            f"**Status Worker:** `{worker_status.upper()}`\n"
            f"**ID Chat Target:** `{target}`\n"
            f"**Chat Dikecualikan:** `{exclude_count} chat`\n"
            f"**Re-upload saat forward diblokir:** `{ 'ON' if config.get('reupload_on_restricted') else 'OFF' }`\n"
            f"**Eager download cache:** `{ 'ON' if config.get('eager_cache_enabled') else 'OFF' }`"
        )
        await try_edit(event, status_text, buttons=main_menu_keyboard())

    # --- HANDLER WORKER (START/STOP) ---
    @bot.on(events.CallbackQuery(data=b'start_worker'))
    @authorized_only
    async def start_worker_handler(event):
        await event.answer("Memulai worker...", alert=False)
        success, message = await start_user_worker(event.sender_id, bot)
        await event.answer(message, alert=True)
        await status_handler(event)

    @bot.on(events.CallbackQuery(data=b'stop_worker'))
    @authorized_only
    async def stop_worker_handler(event):
        await event.answer("Menghentikan worker...", alert=False)
        success, message = await stop_user_worker(event.sender_id)
        await event.answer(message, alert=True)
        await status_handler(event)

    # --- HANDLER PEMILIHAN TARGET (INTERAKTIF DENGAN OPSI HAPUS) ---
    @bot.on(events.CallbackQuery(data=b'list_chats'))
    @authorized_only
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
                if dialog.is_group or dialog.is_channel:
                    btn_text = dialog.name[:40]
                    if dialog.id == current_target:
                        btn_text = f"🎯 {btn_text}"
                    callback_data = f"pick_target_{dialog.id}".encode('utf-8')
                    buttons.append([KeyboardButtonCallback(btn_text, callback_data)])
            buttons.append([KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')])
            prompt_text = (
                "**Pilih atau Ubah Chat Target**\n\n"
                f"**Target saat ini:** `{current_target or 'Belum diatur'}`\n\n"
                "Klik pada grup/channel di bawah untuk menjadikannya target baru."
            )
            await try_edit(event, prompt_text, buttons=buttons)
        except Exception as e:
            logger.error(f"Error listing chats for target for user {user_id}: {e}", exc_info=True)
            await event.answer(f"Error: {type(e).__name__}", alert=True)

    @bot.on(events.CallbackQuery(pattern=b"pick_target_(-?\\d+)"))
    @authorized_only
    async def pick_target_handler(event):
        user_id = event.sender_id
        try:
            target_id = int(event.pattern_match.group(1).decode('utf-8'))
        except (IndexError, ValueError):
            return await event.answer("Callback data tidak valid.", alert=True)
        await event.answer(f"Mengatur target ke ID: {target_id}...")
        await update_user_config(user_id, 'target_chat_id', target_id)
        await event.edit(f"✅ **Target berhasil diatur ke:** `{target_id}`", buttons=main_menu_keyboard())

    @bot.on(events.CallbackQuery(data=b'delete_target'))
    @authorized_only
    async def delete_target_handler(event):
        user_id = event.sender_id
        await event.answer("Menghapus target...", alert=False)
        await update_user_config(user_id, 'target_chat_id', None)
        await event.answer("✅ Target berhasil dihapus!", alert=True)
        await list_chats_for_target_handler(event)

    # --- HANDLER MANAJEMEN PENGECUALIAN (INTERAKTIF) ---
    @bot.on(events.CallbackQuery(data=b'set_exclude'))
    @authorized_only
    async def set_exclude_menu_handler(event):
        await event.answer()
        config = await get_user_config(event.sender_id)
        count = len(config.get('excluded_chat_ids', set()))
        text = (f"🚫 **Menu Pengecualian**\n\n"
                f"Pesan dari chat yang dikecualikan akan diabaikan.\n"
                f"**Jumlah saat ini:** `{count} chat`")
        await try_edit(event, text, buttons=exclude_menu_keyboard())

    @bot.on(events.CallbackQuery(data=b'exclude_add_list'))
    @authorized_only
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
    async def exclude_add_handler(event):
        user_id, chat_id = event.sender_id, int(event.pattern_match.group(1))
        config = await get_user_config(user_id)
        excluded_ids = config.get('excluded_chat_ids', set())
        if chat_id in excluded_ids: return await event.answer("Sudah ada di daftar.")
        excluded_ids.add(chat_id)
        await update_user_config(user_id, 'excluded_chat_ids', excluded_ids)
        await event.answer(f"ID {chat_id} ditambahkan.", alert=False)
        await exclude_add_list_handler(event) # Refresh

    @bot.on(events.CallbackQuery(pattern=b"excl_rem_(-?\\d+)"))
    @authorized_only
    async def exclude_remove_handler(event):
        user_id, chat_id = event.sender_id, int(event.pattern_match.group(1))
        config = await get_user_config(user_id)
        excluded_ids = config.get('excluded_chat_ids', set())
        excluded_ids.discard(chat_id)
        await update_user_config(user_id, 'excluded_chat_ids', excluded_ids)
        await event.answer(f"ID {chat_id} dihapus.", alert=False)
        await exclude_remove_list_handler(event) # Refresh

    # --- HANDLER MEDIA FILTER ---
    @bot.on(events.CallbackQuery(data=b'set_media_filter'))
    @authorized_only
    async def media_filter_menu_handler(event):
        config = await get_user_config(event.sender_id)
        current = config.get('allowed_media_types', set())
        text = ("🗂 **Filter Media**\n\n"
                "Pilih tipe media yang akan diteruskan:")
        await try_edit(event, text, buttons=media_filter_keyboard(current))

    @bot.on(events.CallbackQuery(data=b'media_filter_all'))
    @authorized_only
    async def media_filter_all_handler(event):
        await update_user_config(event.sender_id, 'allowed_media_types', set())
        await event.answer('Mengatur filter ke: semua media')
        await media_filter_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'media_filter_photo'))
    @authorized_only
    async def media_filter_photo_handler(event):
        await update_user_config(event.sender_id, 'allowed_media_types', {'photo'})
        await event.answer('Mengatur filter ke: hanya foto')
        await media_filter_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'media_filter_video'))
    @authorized_only
    async def media_filter_video_handler(event):
        await update_user_config(event.sender_id, 'allowed_media_types', {'video'})
        await event.answer('Mengatur filter ke: hanya video')
        await media_filter_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'media_filter_document'))
    @authorized_only
    async def media_filter_document_handler(event):
        await update_user_config(event.sender_id, 'allowed_media_types', {'document'})
        await event.answer('Mengatur filter ke: hanya dokumen')
        await media_filter_menu_handler(event)

    # --- HANDLER JADWAL OTOMATIS ---
    @bot.on(events.CallbackQuery(data=b'schedule_menu'))
    @authorized_only
    async def schedule_menu_handler(event):
        config = await get_user_config(event.sender_id)
        start = config.get('start_time') or '-'
        stop = config.get('stop_time') or '-'
        text = f"⏰ **Jadwal Otomatis**\n\nWaktu mulai: `{start}`\nWaktu stop: `{stop}`"
        await try_edit(event, text, buttons=schedule_menu_keyboard())

    @bot.on(events.CallbackQuery(data=b'set_start_time'))
    @authorized_only
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
    async def clear_schedule_handler(event):
        await update_user_config(event.sender_id, 'start_time', None)
        await update_user_config(event.sender_id, 'stop_time', None)
        await event.answer('Jadwal dinonaktifkan', alert=True)
        await schedule_menu_handler(event)

    # --- HANDLER PENGATURAN ---
    @bot.on(events.CallbackQuery(data=b'settings_menu'))
    @authorized_only
    async def settings_menu_handler(event):
        config = await get_user_config(event.sender_id)
        text = (
            "⚙️ **Pengaturan**\n\n"
            "- Re-upload saat forward diblokir: aktifkan untuk mengunduh dan mengunggah ulang media jika forward langsung ditolak.\n"
            "  Catatan: Hormati aturan sumber dan hak cipta.\n\n"
            "- Eager download cache: tidak menunda forward; cache hanya dipakai untuk re-upload jika forward diblokir.\n"
            "  Cache dibersihkan otomatis berdasarkan TTL."
        )
        await try_edit(event, text, buttons=settings_menu_keyboard(config.get('reupload_on_restricted'), config.get('eager_cache_enabled')))

    @bot.on(events.CallbackQuery(data=b'toggle_reupload_on_restricted'))
    @authorized_only
    async def toggle_reupload_on_restricted_handler(event):
        user_id = event.sender_id
        config = await get_user_config(user_id)
        new_val = not bool(config.get('reupload_on_restricted'))
        await update_user_config(user_id, 'reupload_on_restricted', new_val)
        # Jika worker sedang berjalan, update config in-memory agar efek langsung
        try:
            from user.manager import ACTIVE_SESSIONS
            sess = ACTIVE_SESSIONS.get(user_id)
            if hasattr(sess, 'config'):
                sess.config['reupload_on_restricted'] = new_val
        except Exception:
            pass
        await event.answer(f"Re-upload saat forward diblokir: {'ON' if new_val else 'OFF'}", alert=True)
        await settings_menu_handler(event)

    @bot.on(events.CallbackQuery(data=b'toggle_eager_cache'))
    @authorized_only
    async def toggle_eager_cache_handler(event):
        user_id = event.sender_id
        config = await get_user_config(user_id)
        new_val = not bool(config.get('eager_cache_enabled'))
        await update_user_config(user_id, 'eager_cache_enabled', new_val)
        try:
            from user.manager import ACTIVE_SESSIONS
            sess = ACTIVE_SESSIONS.get(user_id)
            if hasattr(sess, 'config'):
                sess.config['eager_cache_enabled'] = new_val
        except Exception:
            pass
        await event.answer(f"Eager download cache: {'ON' if new_val else 'OFF'}", alert=True)
        await settings_menu_handler(event)

    # --- HANDLER OTENTIKASI & BANTUAN ---
    @bot.on(events.CallbackQuery(data=b'auth_menu'))
    @authorized_only
    async def auth_menu_handler(event):
        await try_edit(event, "Menu Login/Logout", buttons=auth_menu_keyboard())

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
        1.  **Login:** Masuk dengan akun Telegram Anda melalui menu `Login/Logout`.
        2.  **Pilih Target:** Gunakan menu `Pilih Target dari Daftar` untuk memilih/mengubah/menghapus grup/channel tujuan.
        3.  **Atur Pengecualian:** Masuk ke menu `Atur Pengecualian` untuk menambah atau menghapus chat yang pesannya akan diabaikan.
        4.  **Jalankan:** Klik `Jalankan` untuk memulai proses forwarding.
        5.  **Status:** Cek konfigurasi dan status worker Anda (berjalan/berhenti) kapan saja.
        """
        await try_edit(event, help_text, buttons=back_to_main_menu_button())

    # --- FITUR MIGRASI MEDIA (/migrasi) ---
    @bot.on(events.NewMessage(pattern='/migrasi'))
    @authorized_only
    async def migrasi_entry(event):
        user_id = event.sender_id
        from user.manager import get_client_for_user
        client = await get_client_for_user(user_id)
        if not client:
            return await event.reply("❌ Anda harus login terlebih dahulu di menu Login/Logout.")

        MIGRATION_STATE[user_id] = {'src': None, 'dst': None, 'limit': None}
        await event.reply(
            "🧭 Mode Migrasi: Pilih sumber media yang akan disalin.",
            buttons=[[KeyboardButtonCallback("📥 Pilih Sumber", b'mig_src_list')],
                     [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]]
        )

    @bot.on(events.CallbackQuery(data=b'mig_src_list'))
    @authorized_only
    async def mig_src_list(event):
        user_id = event.sender_id
        from user.manager import get_client_for_user
        client = await get_client_for_user(user_id)
        if not client:
            return await event.answer("Harap login terlebih dahulu.", alert=True)
        dialogs = await client.get_dialogs(limit=100)
        buttons = dynamic_chat_list_keyboard(dialogs, "mig_pick_src", set(), show_all=True)
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", b'main_menu')])
        await try_edit(event, "Pilih Grup/Channel Sumber:", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"mig_pick_src_(-?\\d+)"))
    @authorized_only
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
    async def mig_dst_list(event):
        user_id = event.sender_id
        from user.manager import get_client_for_user
        client = await get_client_for_user(user_id)
        if not client:
            return await event.answer("Harap login terlebih dahulu.", alert=True)
        dialogs = await client.get_dialogs(limit=100)
        buttons = dynamic_chat_list_keyboard(dialogs, "mig_pick_dst", set(), show_all=True)
        buttons.append([KeyboardButtonCallback("⬅️ Kembali", b'main_menu')])
        await try_edit(event, "Pilih Grup/Channel Tujuan:", buttons=buttons)

    @bot.on(events.CallbackQuery(pattern=b"mig_pick_dst_(-?\\d+)"))
    @authorized_only
    async def mig_pick_dst(event):
        user_id = event.sender_id
        chat_id = int(event.pattern_match.group(1))
        state = MIGRATION_STATE.get(user_id)
        if not state or not state.get('src'):
            return await event.answer("Pilih sumber dulu via /migrasi.", alert=True)
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
    async def mig_dedupe_none(event):
        await _start_migration_with_mode(event, 'none')

    @bot.on(events.CallbackQuery(data=b'mig_dedupe_loose'))
    @authorized_only
    async def mig_dedupe_loose(event):
        await _start_migration_with_mode(event, 'loose')

    @bot.on(events.CallbackQuery(data=b'mig_dedupe_strict'))
    @authorized_only
    async def mig_dedupe_strict(event):
        await _start_migration_with_mode(event, 'strict')

    async def _start_migration_with_mode(event, mode: str):
        user_id = event.sender_id
        state = MIGRATION_STATE.get(user_id)
        if not state or not state.get('src') or not state.get('dst'):
            return await event.edit("Sesi migrasi tidak lengkap. Jalankan /migrasi lagi.", buttons=back_to_main_menu_button())
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
    async def mig_conc_1(event):
        await _start_migration_now(event, 1)

    @bot.on(events.CallbackQuery(data=b'mig_conc_2'))
    @authorized_only
    async def mig_conc_2(event):
        await _start_migration_now(event, 2)

    @bot.on(events.CallbackQuery(data=b'mig_conc_3'))
    @authorized_only
    async def mig_conc_3(event):
        await _start_migration_now(event, 3)

    async def _start_migration_now(event, concurrency: int):
        user_id = event.sender_id
        state = MIGRATION_STATE.get(user_id)
        if not state or not state.get('src') or not state.get('dst'):
            return await event.edit("Sesi migrasi tidak lengkap. Jalankan /migrasi lagi.", buttons=back_to_main_menu_button())
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
