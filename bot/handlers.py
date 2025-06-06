import logging
from telethon import events
from telethon.errors import MessageNotModifiedError
from telethon.tl.types import KeyboardButtonCallback

# Impor dari modul lokal
from shared.config import AUTHORIZED_USERS
from shared.database import get_user_config, update_user_config
from .keyboards import (
    main_menu_keyboard, auth_menu_keyboard, back_to_main_menu_button, 
    exclude_menu_keyboard, dynamic_chat_list_keyboard
)
from .conversations import setup_conversation_handlers
from user.manager import (
    get_worker_status, start_user_worker, stop_user_worker, logout_user, 
    get_client_for_user
)

logger = logging.getLogger(__name__)

# --- DECORATOR DAN HELPER ---

def authorized_only(func):
    async def wrapper(event):
        if AUTHORIZED_USERS and event.sender_id not in AUTHORIZED_USERS:
            await event.answer("Akses ditolak.", alert=True)
            return
        return await func(event)
    return wrapper

async def try_edit(event, text, **kwargs):
    try:
        await event.edit(text, **kwargs)
    except MessageNotModifiedError:
        await event.answer()

# --- SETUP HANDLER UTAMA ---

def setup_handlers(bot):
    
    # --- HANDLER MENU UTAMA & DASAR ---

    @bot.on(events.NewMessage(pattern='/start'))
    @authorized_only
    async def start_handler(event):
        await event.reply(
            "**Selamat Datang di Bot Forwarder!**\n\nGunakan tombol di bawah untuk mengelola akun Anda.",
            buttons=main_menu_keyboard()
        )

    @bot.on(events.CallbackQuery(data=b'main_menu'))
    @authorized_only
    async def main_menu_handler(event):
        await try_edit(event, "**Menu Utama**\n\nSilakan pilih salah satu opsi:", buttons=main_menu_keyboard())

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
            f"**Chat Dikecualikan:** `{exclude_count} chat`"
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

    # Daftarkan conversation handler dari file lain (untuk login)
    setup_conversation_handlers(bot)