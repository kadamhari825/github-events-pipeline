import os
import shutil
from datetime import datetime, timedelta

import requests

from gh_events import config


def get_current_time():
    return config.FAKE_NOW if config.FAKE_NOW else datetime.now()


def get_expected_available_time(file_name):
    # "2026-08-30-16.json.gz" -> "2026-08-30-16"
    hour_str = file_name.replace(".json.gz", "")
    hour_start = datetime.strptime(hour_str, "%Y-%m-%d-%H")
    # hour 16 covers 16:00 to 16:59, so the file can only exist from 17:00
    return hour_start + timedelta(hours=1)


def run_extract_stage():
    os.makedirs(config.RAW_DIR, exist_ok=True)
    os.makedirs(config.LATE_DIR, exist_ok=True)

    hours_to_download = [f"{config.DATE}-{h}" for h in range(24)]

    for hour_str in hours_to_download:
        file_name = f"{hour_str}.json.gz"
        url = f"{config.BASE_URL}/{file_name}"
        local_path = os.path.join(config.RAW_DIR, file_name)
        late_path = os.path.join(config.LATE_DIR, file_name)

        if os.path.exists(local_path) or os.path.exists(late_path):
            print(f"already have {local_path}, skipping")
            continue

        hours_late = (get_current_time() - get_expected_available_time(file_name)).total_seconds() / 3600
        too_late = hours_late > config.LATE_WINDOW_HOURS and not config.BACKFILL_MODE
        save_path = late_path if too_late else local_path

        print(f"Downloading {url}")
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
            if too_late:
                print(f"{file_name}: {hours_late:.1f}h late, outside the {config.LATE_WINDOW_HOURS}h window, saved to late_arrivals only")
            elif hours_late > 0 and not config.BACKFILL_MODE:
                shutil.copy2(local_path, late_path)
                print(f"{file_name}: {hours_late:.1f}h late, inside the window, saved to raw and copied to late_arrivals")
            else:
                print(f"saved {file_name}")

        else:
            print(f'failed to download {file_name}: status {response.status_code}')