import os
from dotenv import load_dotenv

# Muat variabel dari file .env
load_dotenv()

def parse_int_env(var, default=None):
    val = os.getenv(var, None)
    try:
        if val is None or val.strip() == "":
            return default
        return int(val)
    except Exception:
        return default

# --- ADMIN ---
ADMIN_USER_IDS = {int(uid) for uid in os.getenv("ADMIN_USER_IDS", "").split(",") if uid}

# --- Telegram API ---
API_ID = int(os.getenv('TELEGRAM_API_ID', 0))
API_HASH = os.getenv('TELEGRAM_API_HASH', '')

# --- Bot Controller ---
BOT_TOKEN = os.getenv('BOT_TOKEN', '')

# --- Security ---
AUTH_USERS_STR = os.getenv('AUTHORIZED_USERS', '')
AUTHORIZED_USERS = {int(user_id.strip()) for user_id in AUTH_USERS_STR.split(',') if user_id.strip()}

# --- Database ---
DB_PATH = os.getenv('DB_PATH', 'forwarder_data.sqlite')

# --- Paths ---
SESSIONS_DIR = "sessions"
DOWNLOADS_DIR = "downloads"

# --- Proxy ---
PROXY_TYPE = os.getenv("PROXY_TYPE", "").lower() or None
PROXY_HOST = os.getenv("PROXY_HOST", None) or None
PROXY_PORT = parse_int_env("PROXY_PORT")    # <- Ini sudah aman!
PROXY_USER = os.getenv("PROXY_USER", None) or None
PROXY_PASS = os.getenv("PROXY_PASS", None) or None

def get_telethon_proxy():
    if not PROXY_TYPE or not PROXY_HOST or not PROXY_PORT:
        return None
    proxy = (PROXY_TYPE, PROXY_HOST, PROXY_PORT)
    if PROXY_USER and PROXY_PASS:
        proxy += (True, PROXY_USER, PROXY_PASS)
    return proxy
