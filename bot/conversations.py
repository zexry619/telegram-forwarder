# bot/conversations.py

import asyncio
import logging
from telethon import events
from telethon.errors import SessionPasswordNeededError
from shared.database import update_user_config, get_user_config
from .keyboards import main_menu_keyboard
from user.manager import get_client_for_user, add_new_client, get_new_client

logger = logging.getLogger(__name__)

def format_login_code(code: str) -> str:
    """Mengembalikan format kode login yang mungkin bisa melewati deteksi bot."""
    cleaned_code = "".join(filter(str.isdigit, code))
    if cleaned_code:
        # Trik paling umum: menyisipkan '_' di antara digit.
        return "_".join(list(cleaned_code))
    return code

def setup_conversation_handlers(bot):
    
    @bot.on(events.CallbackQuery(data=b'login'))
    async def login_handler(event):
        user_id = event.sender_id
        await event.edit("Memulai proses login...")
        client_for_login = None
        
        try:
            # Pengecekan awal, jika sudah ada client aktif, jangan lanjutkan.
            client = await get_client_for_user(user_id)
            if client:
                await event.edit("Anda sudah login. Untuk ganti akun, silakan 'Logout' dahulu.", buttons=main_menu_keyboard())
                return

            async with bot.conversation(user_id, timeout=300, exclusive=True) as conv:
                client_for_login = get_new_client(user_id)
                await client_for_login.connect()

                await conv.send_message("Silakan masukkan nomor telepon Anda (format +62...):")
                phone = await conv.get_response()
                
                try:
                    res = await client_for_login.send_code_request(phone.text)
                    await conv.send_message(
                        "Kode verifikasi telah dikirim ke Telegram Anda.\n\n"
                        "**PENTING:** Masukkan kode dengan format `1_2_3_4_5` (gunakan garis bawah di antara setiap angka)."
                    )
                    code_response = await conv.get_response()
                    # Terapkan trik format kode
                    formatted_code = format_login_code(code_response.text)

                    await client_for_login.sign_in(phone.text, formatted_code, phone_code_hash=res.phone_code_hash)
                except SessionPasswordNeededError:
                    await conv.send_message("Akun Anda dilindungi 2FA. Masukkan password:")
                    pw = await conv.get_response()
                    await client_for_login.sign_in(password=pw.text)
                
                await conv.send_message("✅ Login berhasil!")
                await add_new_client(user_id, client_for_login)
                # Setelah berhasil, jangan disconnect client-nya.
                client_for_login = None 

        except asyncio.TimeoutError:
            await event.respond("Waktu habis. Proses login dibatalkan.")
        except Exception as e:
            logger.error(f"Login failed for user {user_id}", exc_info=True)
            await event.respond(f"❌ Terjadi error saat login: {type(e).__name__}")
        finally:
            # Hanya disconnect jika login GAGAL dan client masih terkoneksi
            if client_for_login and client_for_login.is_connected():
                await client_for_login.disconnect()

    @bot.on(events.CallbackQuery(data=b'set_target_id'))
    async def set_target_id_handler(event):
        await event.delete()
        async with bot.conversation(event.sender_id, timeout=120) as conv:
            await conv.send_message("Kirim **ID Chat Target** baru. Kirim 'batal' untuk keluar.")
            try:
                r = await conv.get_response(); t = r.text.strip()
                if t.lower() == 'batal': 
                    await conv.send_message("Dibatalkan.")
                else:
                    await update_user_config(event.sender_id, 'target_chat_id', int(t))
                    await conv.send_message(f"✅ Target diatur ke: `{t}`")
            except (ValueError, TypeError): 
                await conv.send_message("❌ Format ID tidak valid.")
            except asyncio.TimeoutError: 
                await conv.send_message("Waktu habis.")
            finally:
                await bot.send_message(event.sender_id, "Kembali ke menu utama.", buttons=main_menu_keyboard())