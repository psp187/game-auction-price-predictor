from __future__ import annotations
import base64, gzip, io
import nbtlib
from numba.core.cgutils import false_bit

from logger_config import setup_logger
import json
import datetime
from pathlib import Path
import shutil
from logging import Logger

logger = setup_logger("utils", "utils.log")

def decode_nbt(b64: str) -> nbtlib.File:
    raw = base64.b64decode(b64)
    nbt_bytes = gzip.decompress(raw) if raw[:2] == b'\x1f\x8b' else raw

    with io.BytesIO(nbt_bytes) as dec:
        return nbtlib.File.parse(dec)


def nbt_to_python(value):
    if hasattr(value, "unpack"):
        return value.unpack()
    if isinstance(value, (list, tuple)):
        return [nbt_to_python(x) for x in value]
    if isinstance(value, dict):
        return {str(k): nbt_to_python(v) for k, v in value.items()}
    return str(value)

def nbt_base64_to_dict(b64: str) -> dict:
    nbt_obj = decode_nbt(b64)
    py_obj = nbt_to_python(nbt_obj)

    return py_obj

def export_to_json(data, file_dir_path: Path, file_name: str, *, create_folder: bool = False, time_stamp=False):
    if not data:
        logger.info("No data provided to export_to_json.")
        return None

    final_path = file_dir_path / f'{file_name}.json'

    if time_stamp:
        t = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name_new = f'{file_name}_{t}.json'
        final_path = file_dir_path / file_name_new

    if create_folder:
        final_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Exporting data to {final_path}")
    try:
        with final_path.open("w", encoding='utf8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info("Data exported successfully.")
        return final_path
    except Exception as e:
        logger.error(f"Failed to save data to {final_path}. Error: {e}")
        return None


def archive(source_path: Path, output_path: Path, logger: Logger, retries=5, delay=0.5):
    if not source_path.exists():
        logger.warning(f"Plik źródłowy do archiwizacji nie istnieje: {source_path}")
        return

    destination_file = output_path / source_path.name
    output_path.mkdir(parents=True, exist_ok=True)

    for attempt in range(retries):
        try:
            shutil.move(str(source_path), str(destination_file))
            logger.info(f"Pomyślnie zarchiwizowano {source_path} do {destination_file}")
            return
        except Exception as e:
            logger.error(f"Nie udało się zarchiwizować {source_path}. Błąd krytyczny: {e}")
            break

def tax(price: float):
    """Taxes from game wiki"""

    if price < 1_000_000:
        return 0.0
    elif price < 10_000_000:
        return price * 0.01
    elif price < 100_000_000:
        return price * 0.02
    else:  # 2.5% for 100M+
        return price * 0.025

