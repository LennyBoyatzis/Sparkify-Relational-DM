import os
import glob
from datetime import datetime
from typing import Dict, List


def get_timestamp_attrs(ts: datetime) -> Dict:
    """Split Datetime timestamp into its individual components"""

    return {
        'timestamp': ts.timestamp(),
        'hour': ts.hour,
        'day': ts.day,
        'weekofyear': ts.weekofyear,
        'month': ts.month,
        'year': ts.year,
        'weekday': ts.weekday()
    }


def get_files_in_dir(dir_path: str) -> List:
    """Get paths of all json files in a specified directory"""

    all_files = []
    for root, dirs, files in os.walk(dir_path):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files
