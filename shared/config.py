import os
from dotenv import load_dotenv

# Muat variabel dari file .env
load_dotenv()

# Di bagian paling atas
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