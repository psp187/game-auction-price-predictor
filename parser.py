import sys
from contextlib import nullcontext
from typing import Union
from logging.handlers import RotatingFileHandler
import nbt_utils as utils
from pathlib import Path
import json
import logging

log_dir = Path(".") / "logs"
log_file = log_dir / f"parser.log"
log_dir.mkdir(parents=True, exist_ok=True)
handler = RotatingFileHandler(log_file, maxBytes=5242880, backupCount=5, encoding="utf-8")

log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(log_format)

logger = logging.getLogger("parser")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def dfs_collect_values(root, key: str):
    stack = [root]

    while stack:
        node = stack.pop()

        if isinstance(node, dict):
            if key in node:
                return node[key]
            for value in node.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            for item in node:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return None

def save_to_archive(file):
    folder_path = Path("archive")
    folder_path.mkdir(parents=True, exist_ok=True)
    file_path = folder_path / file.name

    if file_path.exists():
        print(f"File {file.name} already exists.")
    else:
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(file, f, ensure_ascii=False, indent=4)


def parse(source_path: Union[str, Path], output_path=Path("parsed_jsons")) -> bool:
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        source = Path(source_path)
        output = output_path / f"{source.name}"

        with source.open("r", encoding="utf-8") as f:
            data = json.load(f)

        auctions = data.get("auctions", [])
        cleaned = []

        for auction in auctions:
            if not auction.get('bin'):
                continue

            keys_remove = ['seller', 'seller_profile', 'buyer', 'buyer_profile', 'timestamp']
            for key in keys_remove:
                if key in auction:
                    del auction[key]

            keys = [
                'Name',
                'rarity_upgrades',
                'hot_potato_count',
                'gems',
                'modifier',
                'dungeon_item_level',
                'upgrade_level',
                'enchantments',
                'id',
                'dye_item',
                'item_tier',
                'hecatomb_s_runs',
                'eman_kills',
                'baseStatBoostPercentage',
                'boosters',
                'petInfo',
                'runes'

            ]
            try:
                NBT_DATA = auction['item_bytes']
                decoded = utils.nbt_base64_to_dict(NBT_DATA)
                extra = dfs_collect_values(decoded, 'ExtraAttributes') or {}
                display = dfs_collect_values(decoded, 'display') or {}

                try:
                    for key in keys:
                        if key in extra:
                            auction[key] = extra.get(key) or None
                        else:
                            auction[key] = display.get(key) or None
                except Exception as e:
                    logger.warning(f"Something happend to extra and display - 100 line: {e}")

                del auction['item_bytes']
            except Exception as e:
                logger.warning(f"Could not parse NBT for auction {auction.get('auction_id')}. Error: {e}")

            cleaned.append(auction)


        with output.open("w", encoding="utf-8") as f:
            json.dump(cleaned, f, ensure_ascii=False, indent=4)

        logger.info(f"Successfully parsed {source.name} -> {len(cleaned)} items.")
        remove_from_source(source)
        return True

    except FileNotFoundError:
        logger.error(f"File {source_path} not found.")
        return False
    except json.decoder.JSONDecodeError:
        logger.error(f"File {source_path} could not be decoded.")
        return False
    except KeyError:
        logger.error(f"File {source_path} has no key.")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred while parsing {source_path}: {e}")
        return False


def remove_from_source(source_path):
    source = Path(source_path)
    source.unlink(missing_ok=True)

parse(r"raw_data/auctions_2025-09-14_22-09-16.json")