import aiosqlite
import json
import os
from .config import DB_PATH

async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA busy_timeout = 5000;")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                target_chat_id INTEGER,
                excluded_chat_ids TEXT,
                status TEXT DEFAULT 'stopped' CHECK(status IN ('running', 'stopped', 'error', 'login_required')),
                last_error TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS forwarded_messages (
                user_id INTEGER NOT NULL,
                message_key TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                chat_name TEXT, media_type TEXT, fingerprint TEXT,
                thumbnail_md5_hash TEXT, content_hash TEXT, image_hash TEXT, status TEXT,
                is_duplicate_of_key TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, message_key),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON forwarded_messages (user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_fingerprint ON forwarded_messages (user_id, fingerprint)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_content_hash ON forwarded_messages (user_id, content_hash)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_image_hash ON forwarded_messages (user_id, image_hash)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_thumb_hash ON forwarded_messages (user_id, thumbnail_md5_hash)")
        await db.commit()
    print("Database initialized successfully.")

async def get_user_config(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT target_chat_id, excluded_chat_ids, status FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                await db.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
                await db.commit()
                return {'target_chat_id': None, 'excluded_chat_ids': set(), 'status': 'stopped'}
            target, excluded_json, status = row
            excluded = set(json.loads(excluded_json)) if excluded_json else set()
            return {'target_chat_id': target, 'excluded_chat_ids': excluded, 'status': status}

async def get_all_running_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE status = 'running'") as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

async def update_user_config(user_id: int, key: str, value):
    async with aiosqlite.connect(DB_PATH) as db:
        if key == 'excluded_chat_ids':
            value = json.dumps(list(value))
        allowed_keys = ['target_chat_id', 'excluded_chat_ids', 'status', 'last_error']
        if key not in allowed_keys: raise ValueError(f"Invalid config key: {key}")
        await db.execute(f"UPDATE users SET {key} = ? WHERE user_id = ?", (value, user_id))
        await db.commit()

async def db_record_message(user_id: int, message_key: str, data: dict):
    sql = """
        INSERT INTO forwarded_messages (user_id, message_key, chat_id, message_id, chat_name, media_type,
                                      fingerprint, thumbnail_md5_hash, content_hash, image_hash,
                                      status, is_duplicate_of_key)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, message_key) DO UPDATE SET
            chat_name=excluded.chat_name, media_type=excluded.media_type, fingerprint=excluded.fingerprint,
            thumbnail_md5_hash=excluded.thumbnail_md5_hash, content_hash=excluded.content_hash,
            image_hash=excluded.image_hash, status=excluded.status,
            is_duplicate_of_key=excluded.is_duplicate_of_key, timestamp=CURRENT_TIMESTAMP
    """
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(sql, (
                user_id, message_key, data.get('chat_id'), data.get('message_id'), data.get('chat_name'),
                data.get('media_type'), data.get('fingerprint'), data.get('thumbnail_md5_hash'),
                data.get('content_hash'), data.get('image_hash'), data.get('status'),
                data.get('is_duplicate_of_key')
            ))
            await db.commit()
        except Exception as e:
            print(f"DB Record Error for user {user_id}: {e}")

async def db_check_message_exists(user_id: int, message_key: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM forwarded_messages WHERE user_id = ? AND message_key = ? LIMIT 1", (user_id, message_key)) as c:
            return await c.fetchone() is not None

async def db_check_duplicate_by_fingerprint(user_id: int, fp: str) -> str | None:
    if not fp: return None
    sql = "SELECT message_key FROM forwarded_messages WHERE user_id = ? AND fingerprint = ? AND (status LIKE 'forwarded%' OR status LIKE 'duplicate%') LIMIT 1"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, (user_id, fp)) as c:
            r = await c.fetchone()
            return r[0] if r else None

async def db_check_duplicate_by_thumbnail_hash(user_id: int, thumb_hash: str) -> str | None:
    if not thumb_hash: return None
    sql = "SELECT message_key FROM forwarded_messages WHERE user_id = ? AND thumbnail_md5_hash = ? AND (status LIKE 'forwarded%' OR status LIKE 'duplicate%') LIMIT 1"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, (user_id, thumb_hash)) as c:
            r = await c.fetchone()
            return r[0] if r else None

async def db_check_duplicate_by_image_hash(user_id: int, ih: str) -> str | None:
    if not ih: return None
    sql = "SELECT message_key FROM forwarded_messages WHERE user_id = ? AND image_hash = ? AND (status LIKE 'forwarded%' OR status LIKE 'duplicate%') LIMIT 1"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, (user_id, ih)) as c:
            r = await c.fetchone()
            return r[0] if r else None

async def db_check_duplicate_by_content_hash(user_id: int, ch: str) -> str | None:
    if not ch: return None
    sql = "SELECT message_key FROM forwarded_messages WHERE user_id = ? AND content_hash = ? AND (status LIKE 'forwarded%' OR status LIKE 'duplicate%') LIMIT 1"
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, (user_id, ch)) as c:
            r = await c.fetchone()
            return r[0] if r else None
async def allow_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, status) VALUES (?, 'stopped')", (user_id,))
        await db.commit()

async def disallow_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        await db.commit()

async def is_user_allowed(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone() is not None

async def get_allowed_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            return [row[0] for row in await cursor.fetchall()]
