import os
import time

async def cleanup_download_folder(download_dir, max_age_hours=24):
    now = time.time()
    cutoff = now - max_age_hours * 3600
    for root, dirs, files in os.walk(download_dir):
        for file in files:
            path = os.path.join(root, file)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except Exception:
                pass
