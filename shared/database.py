import aiosqlite
import json
import os
from .config import DB_PATH


def _serialize_set(value) -> str:
    return json.dumps(sorted(list(value or [])))


def _deserialize_set(value) -> set:
    return set(json.loads(value)) if value else set()


def _route_from_row(row) -> dict:
    return {
        'id': row[0],
        'user_id': row[1],
        'name': row[2],
        'source_chat_id': row[3],
        'target_chat_id': row[4],
        'excluded_chat_ids': _deserialize_set(row[5]),
        'allowed_media_types': _deserialize_set(row[6]),
        'reupload_on_restricted': bool(row[7] or 0),
        'enabled': bool(row[8] or 0),
        'is_default': bool(row[9] or 0),
    }


async def _ensure_default_route(db, user_id: int):
    async with db.execute(
        """
        SELECT id, user_id, name, source_chat_id, target_chat_id, excluded_chat_ids,
               allowed_media_types, reupload_on_restricted, enabled, is_default
        FROM routes
        WHERE user_id = ? AND is_default = 1
        LIMIT 1
        """,
        (user_id,),
    ) as cursor:
        row = await cursor.fetchone()
        if row:
            return _route_from_row(row)

    async with db.execute(
        """
        SELECT target_chat_id, excluded_chat_ids, allowed_media_types, reupload_on_restricted
        FROM users
        WHERE user_id = ?
        """,
        (user_id,),
    ) as cursor:
        user_row = await cursor.fetchone()

    target = user_row[0] if user_row else None
    excluded_json = user_row[1] if user_row else None
    allowed_json = user_row[2] if user_row else None
    reupload_flag = user_row[3] if user_row else 0

    await db.execute(
        """
        INSERT INTO routes (
            user_id, name, source_chat_id, target_chat_id, excluded_chat_ids,
            allowed_media_types, reupload_on_restricted, enabled, is_default
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1)
        """,
        (
            user_id,
            'Default Route',
            None,
            target,
            excluded_json or _serialize_set(set()),
            allowed_json or _serialize_set(set()),
            int(bool(reupload_flag or 0)),
        ),
    )
    async with db.execute(
        """
        SELECT id, user_id, name, source_chat_id, target_chat_id, excluded_chat_ids,
               allowed_media_types, reupload_on_restricted, enabled, is_default
        FROM routes
        WHERE user_id = ? AND is_default = 1
        LIMIT 1
        """,
        (user_id,),
    ) as cursor:
        row = await cursor.fetchone()
        return _route_from_row(row)


async def _migrate_forwarded_messages_table(db):
    async with db.execute("PRAGMA table_info(forwarded_messages)") as cursor:
        cols = await cursor.fetchall()
    col_names = [row[1] for row in cols]

    if not cols:
        await db.execute(
            """
            CREATE TABLE forwarded_messages (
                user_id INTEGER NOT NULL,
                route_id INTEGER NOT NULL DEFAULT 0,
                message_key TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                chat_name TEXT,
                media_type TEXT,
                fingerprint TEXT,
                thumbnail_md5_hash TEXT,
                content_hash TEXT,
                image_hash TEXT,
                status TEXT,
                is_duplicate_of_key TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, route_id, message_key),
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            """
        )
        return

    if 'route_id' in col_names:
        return

    await db.execute("ALTER TABLE forwarded_messages RENAME TO forwarded_messages_legacy")
    await db.execute(
        """
        CREATE TABLE forwarded_messages (
            user_id INTEGER NOT NULL,
            route_id INTEGER NOT NULL DEFAULT 0,
            message_key TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            chat_name TEXT,
            media_type TEXT,
            fingerprint TEXT,
            thumbnail_md5_hash TEXT,
            content_hash TEXT,
            image_hash TEXT,
            status TEXT,
            is_duplicate_of_key TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, route_id, message_key),
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )
        """
    )
    await db.execute(
        """
        INSERT INTO forwarded_messages (
            user_id, route_id, message_key, chat_id, message_id, chat_name, media_type,
            fingerprint, thumbnail_md5_hash, content_hash, image_hash, status,
            is_duplicate_of_key, timestamp
        )
        SELECT
            user_id, 0, message_key, chat_id, message_id, chat_name, media_type,
            fingerprint, thumbnail_md5_hash, content_hash, image_hash, status,
            is_duplicate_of_key, timestamp
        FROM forwarded_messages_legacy
        """
    )
    await db.execute("DROP TABLE forwarded_messages_legacy")


async def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA busy_timeout = 5000;")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                target_chat_id INTEGER,
                excluded_chat_ids TEXT,
                allowed_media_types TEXT,
                start_time TEXT,
                stop_time TEXT,
                reupload_on_restricted INTEGER DEFAULT 0,
                eager_cache_enabled INTEGER DEFAULT 0,
                status TEXT DEFAULT 'stopped' CHECK(status IN ('running', 'stopped', 'error', 'login_required')),
                last_error TEXT
            )
            """
        )
        async with db.execute("PRAGMA table_info(users)") as cursor:
            cols = [row[1] for row in await cursor.fetchall()]
            if 'allowed_media_types' not in cols:
                await db.execute("ALTER TABLE users ADD COLUMN allowed_media_types TEXT")
            if 'start_time' not in cols:
                await db.execute("ALTER TABLE users ADD COLUMN start_time TEXT")
            if 'stop_time' not in cols:
                await db.execute("ALTER TABLE users ADD COLUMN stop_time TEXT")
            if 'reupload_on_restricted' not in cols:
                await db.execute("ALTER TABLE users ADD COLUMN reupload_on_restricted INTEGER DEFAULT 0")
            if 'eager_cache_enabled' not in cols:
                await db.execute("ALTER TABLE users ADD COLUMN eager_cache_enabled INTEGER DEFAULT 0")

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS routes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                source_chat_id INTEGER,
                target_chat_id INTEGER,
                excluded_chat_ids TEXT,
                allowed_media_types TEXT,
                reupload_on_restricted INTEGER DEFAULT 0,
                enabled INTEGER DEFAULT 1,
                is_default INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
            )
            """
        )

        await _migrate_forwarded_messages_table(db)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON forwarded_messages (user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_route_message ON forwarded_messages (user_id, route_id, message_key)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_route_fingerprint ON forwarded_messages (user_id, route_id, fingerprint)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_route_content_hash ON forwarded_messages (user_id, route_id, content_hash)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_route_image_hash ON forwarded_messages (user_id, route_id, image_hash)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_user_route_thumb_hash ON forwarded_messages (user_id, route_id, thumbnail_md5_hash)")
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_routes_user_default ON routes (user_id, is_default) WHERE is_default = 1")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_routes_user_enabled ON routes (user_id, enabled)")

        async with db.execute("SELECT user_id FROM users") as cursor:
            user_ids = [row[0] for row in await cursor.fetchall()]
        for user_id in user_ids:
            await _ensure_default_route(db, user_id)

        await db.commit()
    print("Database initialized successfully.")


async def get_user_config(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT start_time, stop_time, eager_cache_enabled, status
            FROM users
            WHERE user_id = ?
            """,
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                await db.execute("INSERT INTO users (user_id) VALUES (?)", (user_id,))
                await db.commit()
                row = (None, None, 0, 'stopped')

        default_route = await _ensure_default_route(db, user_id)
        await db.commit()
        start_time, stop_time, eager_cache_flag, status = row
        return {
            'target_chat_id': default_route.get('target_chat_id'),
            'excluded_chat_ids': default_route.get('excluded_chat_ids', set()),
            'allowed_media_types': default_route.get('allowed_media_types', set()),
            'start_time': start_time,
            'stop_time': stop_time,
            'reupload_on_restricted': default_route.get('reupload_on_restricted', False),
            'eager_cache_enabled': bool(eager_cache_flag or 0),
            'status': status,
            'default_route_id': default_route.get('id'),
        }


async def get_all_running_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE status = 'running'") as cursor:
            return [row[0] for row in await cursor.fetchall()]


async def get_user_routes(user_id: int, enabled_only: bool = False):
    async with aiosqlite.connect(DB_PATH) as db:
        await _ensure_default_route(db, user_id)
        await db.commit()
        sql = (
            """
            SELECT id, user_id, name, source_chat_id, target_chat_id, excluded_chat_ids,
                   allowed_media_types, reupload_on_restricted, enabled, is_default
            FROM routes
            WHERE user_id = ?
            """
            + (" AND enabled = 1" if enabled_only else "")
            + " ORDER BY is_default DESC, id ASC"
        )
        async with db.execute(sql, (user_id,)) as cursor:
            rows = await cursor.fetchall()
        return [_route_from_row(row) for row in rows]


async def get_route_by_id(user_id: int, route_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT id, user_id, name, source_chat_id, target_chat_id, excluded_chat_ids,
                   allowed_media_types, reupload_on_restricted, enabled, is_default
            FROM routes
            WHERE user_id = ? AND id = ?
            LIMIT 1
            """,
            (user_id, route_id),
        ) as cursor:
            row = await cursor.fetchone()
        return _route_from_row(row) if row else None


async def create_user_route(
    user_id: int,
    name: str,
    *,
    source_chat_id=None,
    target_chat_id=None,
    excluded_chat_ids=None,
    allowed_media_types=None,
    reupload_on_restricted: bool = False,
    enabled: bool = True,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await db.execute(
            """
            INSERT INTO routes (
                user_id, name, source_chat_id, target_chat_id, excluded_chat_ids,
                allowed_media_types, reupload_on_restricted, enabled, is_default
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                user_id,
                name,
                source_chat_id,
                target_chat_id,
                _serialize_set(excluded_chat_ids or set()),
                _serialize_set(allowed_media_types or set()),
                int(bool(reupload_on_restricted)),
                int(bool(enabled)),
            ),
        )
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            route_id = (await cursor.fetchone())[0]
        await db.commit()
    return await get_route_by_id(user_id, route_id)


async def update_user_route(user_id: int, route_id: int, **fields):
    allowed_keys = {
        'name',
        'source_chat_id',
        'target_chat_id',
        'excluded_chat_ids',
        'allowed_media_types',
        'reupload_on_restricted',
        'enabled',
    }
    payload = []
    values = []
    for key, value in fields.items():
        if key not in allowed_keys:
            raise ValueError(f"Invalid route key: {key}")
        if key in {'excluded_chat_ids', 'allowed_media_types'}:
            value = _serialize_set(value or set())
        if key in {'reupload_on_restricted', 'enabled'}:
            value = 1 if value else 0
        payload.append(f"{key} = ?")
        values.append(value)
    if not payload:
        return await get_route_by_id(user_id, route_id)
    payload.append("updated_at = CURRENT_TIMESTAMP")
    values.extend([user_id, route_id])

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE routes SET {', '.join(payload)} WHERE user_id = ? AND id = ?",
            values,
        )
        await db.commit()
    return await get_route_by_id(user_id, route_id)


async def delete_user_route(user_id: int, route_id: int):
    route = await get_route_by_id(user_id, route_id)
    if not route:
        return False
    if route.get('is_default'):
        raise ValueError("Default route tidak bisa dihapus.")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM routes WHERE user_id = ? AND id = ?", (user_id, route_id))
        await db.commit()
    return True


async def update_user_config(user_id: int, key: str, value):
    user_keys = {'start_time', 'stop_time', 'eager_cache_enabled', 'status', 'last_error'}
    default_route_keys = {
        'target_chat_id',
        'excluded_chat_ids',
        'allowed_media_types',
        'reupload_on_restricted',
    }
    if key not in user_keys | default_route_keys:
        raise ValueError(f"Invalid config key: {key}")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        default_route = await _ensure_default_route(db, user_id)

        if key in default_route_keys:
            route_value = value
            if key in {'excluded_chat_ids', 'allowed_media_types'}:
                route_value = _serialize_set(value or set())
                user_value = route_value
            elif key == 'reupload_on_restricted':
                route_value = 1 if value else 0
                user_value = route_value
            else:
                user_value = value
            await db.execute(
                f"UPDATE routes SET {key} = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND id = ?",
                (route_value, user_id, default_route['id']),
            )
            await db.execute(f"UPDATE users SET {key} = ? WHERE user_id = ?", (user_value, user_id))
        else:
            user_value = value
            if key == 'eager_cache_enabled':
                user_value = 1 if value else 0
            await db.execute(f"UPDATE users SET {key} = ? WHERE user_id = ?", (user_value, user_id))

        await db.commit()


async def db_record_message(user_id: int, message_key: str, data: dict):
    route_id = int(data.get('route_id') or 0)
    sql = """
        INSERT INTO forwarded_messages (
            user_id, route_id, message_key, chat_id, message_id, chat_name, media_type,
            fingerprint, thumbnail_md5_hash, content_hash, image_hash, status, is_duplicate_of_key
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id, route_id, message_key) DO UPDATE SET
            chat_name=excluded.chat_name,
            media_type=excluded.media_type,
            fingerprint=excluded.fingerprint,
            thumbnail_md5_hash=excluded.thumbnail_md5_hash,
            content_hash=excluded.content_hash,
            image_hash=excluded.image_hash,
            status=excluded.status,
            is_duplicate_of_key=excluded.is_duplicate_of_key,
            timestamp=CURRENT_TIMESTAMP
    """
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                sql,
                (
                    user_id,
                    route_id,
                    message_key,
                    data.get('chat_id'),
                    data.get('message_id'),
                    data.get('chat_name'),
                    data.get('media_type'),
                    data.get('fingerprint'),
                    data.get('thumbnail_md5_hash'),
                    data.get('content_hash'),
                    data.get('image_hash'),
                    data.get('status'),
                    data.get('is_duplicate_of_key'),
                ),
            )
            await db.commit()
        except Exception as e:
            print(f"DB Record Error for user {user_id}: {e}")


async def db_check_message_exists(user_id: int, message_key: str, route_id: int = 0) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            """
            SELECT 1
            FROM forwarded_messages
            WHERE user_id = ? AND route_id = ? AND message_key = ?
            LIMIT 1
            """,
            (user_id, int(route_id or 0), message_key),
        ) as cursor:
            return await cursor.fetchone() is not None


async def db_check_duplicate_by_fingerprint(user_id: int, fp: str, route_id: int = 0) -> str | None:
    if not fp:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        if route_id is None:
            sql = """
                SELECT message_key
                FROM forwarded_messages
                WHERE user_id = ? AND fingerprint = ?
                  AND (
                      status LIKE 'forwarded%'
                      OR status LIKE 'reuploaded%'
                      OR status LIKE 'duplicate%'
                      OR status LIKE 'migrated_forwarded%'
                      OR status LIKE 'migrated_reuploaded%'
                  )
                LIMIT 1
            """
            params = (user_id, fp)
        else:
            sql = """
                SELECT message_key
                FROM forwarded_messages
                WHERE user_id = ? AND route_id = ? AND fingerprint = ?
                  AND (
                      status LIKE 'forwarded%'
                      OR status LIKE 'reuploaded%'
                      OR status LIKE 'duplicate%'
                      OR status LIKE 'migrated_forwarded%'
                      OR status LIKE 'migrated_reuploaded%'
                  )
                LIMIT 1
            """
            params = (user_id, int(route_id or 0), fp)
        async with db.execute(sql, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def db_check_duplicate_by_thumbnail_hash(user_id: int, thumb_hash: str, route_id: int = 0) -> str | None:
    if not thumb_hash:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        if route_id is None:
            sql = """
                SELECT message_key
                FROM forwarded_messages
                WHERE user_id = ? AND thumbnail_md5_hash = ?
                  AND (
                      status LIKE 'forwarded%'
                      OR status LIKE 'reuploaded%'
                      OR status LIKE 'duplicate%'
                      OR status LIKE 'migrated_forwarded%'
                      OR status LIKE 'migrated_reuploaded%'
                  )
                LIMIT 1
            """
            params = (user_id, thumb_hash)
        else:
            sql = """
                SELECT message_key
                FROM forwarded_messages
                WHERE user_id = ? AND route_id = ? AND thumbnail_md5_hash = ?
                  AND (
                      status LIKE 'forwarded%'
                      OR status LIKE 'reuploaded%'
                      OR status LIKE 'duplicate%'
                      OR status LIKE 'migrated_forwarded%'
                      OR status LIKE 'migrated_reuploaded%'
                  )
                LIMIT 1
            """
            params = (user_id, int(route_id or 0), thumb_hash)
        async with db.execute(sql, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def db_check_duplicate_by_image_hash(user_id: int, image_hash: str, route_id: int = 0) -> str | None:
    if not image_hash:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        if route_id is None:
            sql = """
                SELECT message_key
                FROM forwarded_messages
                WHERE user_id = ? AND image_hash = ?
                  AND (
                      status LIKE 'forwarded%'
                      OR status LIKE 'reuploaded%'
                      OR status LIKE 'duplicate%'
                      OR status LIKE 'migrated_forwarded%'
                      OR status LIKE 'migrated_reuploaded%'
                  )
                LIMIT 1
            """
            params = (user_id, image_hash)
        else:
            sql = """
                SELECT message_key
                FROM forwarded_messages
                WHERE user_id = ? AND route_id = ? AND image_hash = ?
                  AND (
                      status LIKE 'forwarded%'
                      OR status LIKE 'reuploaded%'
                      OR status LIKE 'duplicate%'
                      OR status LIKE 'migrated_forwarded%'
                      OR status LIKE 'migrated_reuploaded%'
                  )
                LIMIT 1
            """
            params = (user_id, int(route_id or 0), image_hash)
        async with db.execute(sql, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def db_check_duplicate_by_content_hash(user_id: int, content_hash: str, route_id: int = 0) -> str | None:
    if not content_hash:
        return None
    async with aiosqlite.connect(DB_PATH) as db:
        if route_id is None:
            sql = """
                SELECT message_key
                FROM forwarded_messages
                WHERE user_id = ? AND content_hash = ?
                  AND (
                      status LIKE 'forwarded%'
                      OR status LIKE 'reuploaded%'
                      OR status LIKE 'duplicate%'
                      OR status LIKE 'migrated_forwarded%'
                      OR status LIKE 'migrated_reuploaded%'
                  )
                LIMIT 1
            """
            params = (user_id, content_hash)
        else:
            sql = """
                SELECT message_key
                FROM forwarded_messages
                WHERE user_id = ? AND route_id = ? AND content_hash = ?
                  AND (
                      status LIKE 'forwarded%'
                      OR status LIKE 'reuploaded%'
                      OR status LIKE 'duplicate%'
                      OR status LIKE 'migrated_forwarded%'
                      OR status LIKE 'migrated_reuploaded%'
                  )
                LIMIT 1
            """
            params = (user_id, int(route_id or 0), content_hash)
        async with db.execute(sql, params) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def allow_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users (user_id, status) VALUES (?, 'stopped')", (user_id,))
        await _ensure_default_route(db, user_id)
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
