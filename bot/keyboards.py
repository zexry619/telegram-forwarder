from telethon.tl.types import KeyboardButtonCallback
from shared.telegram import get_dialog_display_name, is_selectable_target_dialog

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
            KeyboardButtonCallback("📊 Status", b'status'),
            KeyboardButtonCallback("🚚 Migrasi Media", b'migration_menu')
        ],
        [
            KeyboardButtonCallback("🎯 Target Route Default", b'list_chats'),
            KeyboardButtonCallback("🧩 Kelola Routes", b'routes_menu')
        ],
        [
            KeyboardButtonCallback("🚫 Pengecualian Default", b'set_exclude'),
            KeyboardButtonCallback("🗂 Filter Default", b'set_media_filter')
        ],
        [
            KeyboardButtonCallback("⏰ Jadwal Otomatis", b'schedule_menu'),
            KeyboardButtonCallback("⚙️ Pengaturan Default", b'settings_menu')
        ],
        [
            KeyboardButtonCallback("🔐 Login / Logout", b'auth_menu'),
            KeyboardButtonCallback("❓ Bantuan", b'help')
        ]
    ]

def onboarding_keyboard():
    return [
        [KeyboardButtonCallback("🔑 Login Sekarang", b'login')],
        [KeyboardButtonCallback("❓ Panduan Login", b'help')],
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

def dynamic_chat_list_keyboard(
    dialogs,
    base_callback_prefix: str,
    existing_ids: set = None,
    show_all=True,
    include_saved_messages: bool = False,
):
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
        if not is_selectable_target_dialog(dialog, include_saved_messages=include_saved_messages):
            continue

        dialog_id = dialog.id
        is_existing = dialog_id in existing_ids

        # Logika untuk menampilkan tombol:
        # Jika kita hanya ingin menampilkan yang sudah ada (menu Hapus), lewati yang lain.
        if not show_all and not is_existing:
            continue

        # Format teks tombol
        button_text = get_dialog_display_name(dialog)[:40]  # Potong nama jika terlalu panjang
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


def routes_menu_keyboard():
    return [
        [KeyboardButtonCallback("📋 Lihat Semua Routes", b'route_list')],
        [KeyboardButtonCallback("➕ Tambah Route Baru", b'route_add')],
        [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')],
    ]


def route_detail_keyboard(route_id: int, enabled: bool, is_default: bool, reupload_on: bool, allow_exclusions: bool = True):
    buttons = [
        [KeyboardButtonCallback("✏️ Ubah Nama", f'route_rename_{route_id}'.encode())],
        [KeyboardButtonCallback("📥 Atur Sumber", f'route_pick_source_menu_{route_id}'.encode())],
        [KeyboardButtonCallback("🎯 Atur Tujuan", f'route_pick_target_menu_{route_id}'.encode())],
        [KeyboardButtonCallback("🗂 Filter Media", f'route_filter_menu_{route_id}'.encode())],
        [KeyboardButtonCallback(("✅ " if reupload_on else "") + "♻️ Re-upload", f'route_reupload_toggle_{route_id}'.encode())],
        [KeyboardButtonCallback("✅ Nonaktifkan" if enabled else "▶️ Aktifkan", f'route_toggle_{route_id}'.encode())],
    ]
    if allow_exclusions:
        buttons.insert(4, [KeyboardButtonCallback("🚫 Pengecualian", f'route_exclude_menu_{route_id}'.encode())])
    if not is_default:
        buttons.append([KeyboardButtonCallback("🗑️ Hapus Route", f'route_delete_{route_id}'.encode())])
    buttons.append([KeyboardButtonCallback("⬅️ Kembali ke Routes", b'route_list')])
    return buttons


def route_media_filter_keyboard(route_id: int, current: set):
    def mark(text, cond):
        return f"✅ {text}" if cond else text

    return [
        [KeyboardButtonCallback(mark("Semua Media", not current), f'route_filter_all_{route_id}'.encode())],
        [KeyboardButtonCallback(mark("Foto", 'photo' in current), f'route_filter_photo_{route_id}'.encode())],
        [KeyboardButtonCallback(mark("Video", 'video' in current), f'route_filter_video_{route_id}'.encode())],
        [KeyboardButtonCallback(mark("Dokumen", 'document' in current), f'route_filter_document_{route_id}'.encode())],
        [KeyboardButtonCallback("⬅️ Kembali", f'route_view_{route_id}'.encode())],
    ]


def route_exclude_menu_keyboard(route_id: int):
    return [
        [KeyboardButtonCallback("➕ Tambah Pengecualian", f'route_exclude_add_list_{route_id}'.encode())],
        [KeyboardButtonCallback("➖ Hapus Pengecualian", f'route_exclude_remove_list_{route_id}'.encode())],
        [KeyboardButtonCallback("⬅️ Kembali", f'route_view_{route_id}'.encode())],
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

def schedule_menu_keyboard():
    return [
        [KeyboardButtonCallback("Atur Jam Mulai", b'set_start_time')],
        [KeyboardButtonCallback("Atur Jam Stop", b'set_stop_time')],
        [KeyboardButtonCallback("Nonaktifkan Jadwal", b'clear_schedule')],
        [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]
    ]

def settings_menu_keyboard(reupload_on: bool, eager_cache_on: bool):
    def mark(text, cond):
        return f"✅ {text}" if cond else text

    return [
        [KeyboardButtonCallback(mark("Re-upload saat forward diblokir", reupload_on), b'toggle_reupload_on_restricted')],
        [KeyboardButtonCallback(mark("Eager download cache", eager_cache_on), b'toggle_eager_cache')],
        [KeyboardButtonCallback("⬅️ Kembali ke Menu Utama", b'main_menu')]
    ]
