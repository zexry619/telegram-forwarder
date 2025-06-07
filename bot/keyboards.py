from telethon.tl.types import KeyboardButtonCallback

def main_menu_keyboard():
    """
    Menghasilkan keyboard untuk menu utama bot.
    Ini adalah navigasi sentral bagi pengguna.
    """
    return [
        [
            KeyboardButtonCallback("▶️ Jalankan", b'start_worker'), 
            KeyboardButtonCallback("⏹️ Hentikan", b'stop_worker')
        ],
        [
            KeyboardButtonCallback("📊 Status", b'status')
        ],
        [
            # Tombol ini sekarang langsung membawa ke daftar pilihan target
            KeyboardButtonCallback("🎯 Pilih Target dari Daftar", b'list_chats'),
            KeyboardButtonCallback("🚫 Atur Pengecualian", b'set_exclude')
        ],
        [
            KeyboardButtonCallback("🗂 Filter Media", b'set_media_filter')
        ],
        [
            KeyboardButtonCallback("🔐 Login / Logout", b'auth_menu'),
            KeyboardButtonCallback("❓ Bantuan", b'help')
        ]
    ]

def auth_menu_keyboard():
    """
    Menampilkan keyboard untuk sub-menu Login/Logout.
    """
    return [
        [KeyboardButtonCallback("🔑 Login dengan Akun", b'login')],
        [KeyboardButtonCallback("📤 Logout & Hapus Sesi", b'logout')],
        [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]
    ]

def exclude_menu_keyboard():
    """
    Menampilkan keyboard untuk sub-menu manajemen pengecualian.
    """
    return [
        [KeyboardButtonCallback("➕ Tambah ke Daftar Pengecualian", b'exclude_add_list')],
        [KeyboardButtonCallback("➖ Hapus dari Daftar Pengecualian", b'exclude_remove_list')],
        [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]
    ]

def dynamic_chat_list_keyboard(dialogs, base_callback_prefix: str, existing_ids: set = None, show_all=True):
    """
    Membuat keyboard dinamis dari daftar chat untuk pemilihan interaktif.
    Ini adalah fungsi 'pintar' yang digunakan untuk memilih target dan pengecualian.

    Args:
        dialogs: Daftar objek dialog dari Telethon.
        base_callback_prefix (str): Awalan untuk callback data (cth: 'excl_add').
        existing_ids (set, optional): Set dari ID yang sudah ada di daftar. Defaults to None.
        show_all (bool, optional): Jika True, tampilkan semua chat. Jika False, tampilkan hanya chat yang ID-nya ada di `existing_ids`. Defaults to True.

    Returns:
        list: Sebuah list of lists dari KeyboardButtonCallback, siap digunakan di pesan.
    """
    if existing_ids is None:
        existing_ids = set()
        
    chat_buttons = []
    for dialog in dialogs:
        # Hanya proses grup dan channel
        if not (dialog.is_group or dialog.is_channel):
            continue

        dialog_id = dialog.id
        is_existing = dialog_id in existing_ids

        # Logika untuk menampilkan tombol:
        # Jika kita hanya ingin menampilkan yang sudah ada (menu Hapus), lewati yang lain.
        if not show_all and not is_existing:
            continue

        # Format teks tombol
        button_text = dialog.name[:40]  # Potong nama jika terlalu panjang
        if is_existing and show_all:  # Tandai tombol di menu 'Tambah' jika sudah ada
            button_text = f"✅ {button_text}"

        # Buat callback data yang unik, e.g., "excl_add_-100123456"
        callback_data = f"{base_callback_prefix}_{dialog_id}".encode('utf-8')
        chat_buttons.append([KeyboardButtonCallback(button_text, callback_data)])

    return chat_buttons

def back_to_main_menu_button():
    """
    Menghasilkan satu tombol untuk kembali ke menu utama.
    Berguna untuk pesan-pesan informasional.
    """
    return [[KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]]

def admin_user_management_keyboard():
    return [
        [KeyboardButtonCallback("👥 Lihat Allowed Users", b'admin_list_users')],
        [KeyboardButtonCallback("➕ Tambah Allowed User", b'admin_add_user')],
        [KeyboardButtonCallback("➖ Hapus Allowed User", b'admin_remove_user')],
        [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]
    ]

def media_filter_keyboard(current: set):
    def mark(text, cond):
        return f"✅ {text}" if cond else text

    return [
        [KeyboardButtonCallback(mark("Semua Media", not current), b'media_filter_all')],
        [KeyboardButtonCallback(mark("Hanya Foto", current == {'photo'}), b'media_filter_photo')],
        [KeyboardButtonCallback(mark("Hanya Video", current == {'video'}), b'media_filter_video')],
        [KeyboardButtonCallback(mark("Hanya Dokumen", current == {'document'}), b'media_filter_document')],
        [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]
    ]
