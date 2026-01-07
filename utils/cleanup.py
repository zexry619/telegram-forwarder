import os
import time

def _iter_files(root_dir: str):
    for r, _dirs, files in os.walk(root_dir):
        for fn in files:
            yield os.path.join(r, fn)

async def cleanup_download_folder(download_dir: str, max_age_hours: int = 24, max_total_mb: int | None = None):
    """
    Remove cached files older than TTL and optionally enforce a total size cap.

    - max_age_hours: delete files older than this age.
    - max_total_mb: if set, delete oldest files until usage <= cap.
    """
    try:
        now = time.time()
        cutoff = now - max_age_hours * 3600 if max_age_hours is not None else None

        # 1) Delete by age
        if cutoff is not None:
            for path in list(_iter_files(download_dir)):
                try:
                    if os.path.getmtime(path) < cutoff:
                        os.remove(path)
                except Exception:
                    pass

        # 2) Enforce size cap (delete oldest first)
        if max_total_mb and max_total_mb > 0:
            files = []
            total = 0
            for path in _iter_files(download_dir):
                try:
                    sz = os.path.getsize(path)
                    mt = os.path.getmtime(path)
                    files.append((mt, path, sz))
                    total += sz
                except Exception:
                    continue
            cap = max_total_mb * 1024 * 1024
            if total > cap:
                files.sort(key=lambda x: x[0])  # oldest first
                for _mt, path, sz in files:
                    try:
                        os.remove(path)
                        total -= sz
                    except Exception:
                        pass
                    if total <= cap:
                        break

        # 3) Remove empty directories
        for r, dirs, _ in os.walk(download_dir, topdown=False):
            for d in dirs:
                dp = os.path.join(r, d)
                try:
                    if not os.listdir(dp):
                        os.rmdir(dp)
                except Exception:
                    pass
    except Exception:
        # Never raise in background cleanup
        pass

if __name__ == "__main__":
    # Allow manual cleanup: `python utils/cleanup.py`
    from shared.config import DOWNLOADS_DIR, CACHE_TTL_HOURS
    # Optional env: MAX_CACHE_DISK_MB
    try:
        max_cache_mb = int(os.getenv("MAX_CACHE_DISK_MB", "0")) or None
    except Exception:
        max_cache_mb = None
    import asyncio
    asyncio.run(cleanup_download_folder(DOWNLOADS_DIR, max_age_hours=CACHE_TTL_HOURS, max_total_mb=max_cache_mb))
