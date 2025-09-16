from typing import Union, Any
from logger_config import setup_logger
import nbt_utils as utils
from pathlib import Path
import json
import shutil
import re

JSON = Any
logger = setup_logger("parser", "parser.log")

def decode_petinfo(petinfo: str | None) -> tuple:
    if not petinfo:
        return None, None, None, None
    try:
        raw_info = json.loads(petinfo)
        type = raw_info.get('type', None)
        tier = raw_info.get('tier', None)
        candy = raw_info.get('candyUsed')
        item = raw_info.get('heldItem', None)
        return type, tier, candy, item
    except json.JSONDecodeError:
        return None, None, None, None

def decode_name(txt: str | None) -> tuple:
    if not txt:
        return None, None

    colorless = re.sub(r'§.', '', txt)
    match = re.search(r'\[Lvl (\d+)\]', colorless)
    level = None
    clean = colorless.strip()

    if match:
        level = int(match.group(1))
        clean = colorless.replace(match.group(0), '').strip()
    return level, clean


def dfs_collect_value(root, key: str):
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

def parse(source_path: Union[str, Path], output_path=Path("parsed_jsons")) -> bool:
    source = Path(source_path)
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        output = output_path / source.name

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

            try:
                nbt_data = auction['item_bytes']
                decoded = utils.nbt_base64_to_dict(nbt_data)
                extra = dfs_collect_value(decoded, 'ExtraAttributes') or {}
                display = dfs_collect_value(decoded, 'display') or {}

                level, cleaned_name = decode_name(display.get('Name'))
                base_id = extra.get('id')

                auction['Name'] = cleaned_name
                auction['rarity_upgrades'] = extra.get('rarity_upgrades', 0)
                auction['hot_potato_count'] = extra.get('hot_potato_count', 0)
                auction['gems'] = extra.get('gems')
                auction['modifier'] = extra.get('modifier')
                auction['dungeon_item_level'] = extra.get('dungeon_item_level')
                auction['upgrade_level'] = extra.get('upgrade_level')
                auction['enchantments'] = extra.get('enchantments')
                if base_id == "PET":
                    pet_type, tier, candy, held_item = decode_petinfo(extra.get('petInfo'))
                    auction['id'] = pet_type
                else:
                    auction['id'] = base_id
                    tier, candy, held_item = None, None, None
                auction['dye_item'] = extra.get('dye_item')
                auction['item_tier'] = extra.get('item_tier')
                auction['hecatomb_s_runs'] = extra.get('hecatomb_s_runs')
                auction['eman_kills'] = extra.get('eman_kills')
                auction['baseStatBoostPercentage'] = extra.get('baseStatBoostPercentage')
                auction['boosters'] = extra.get('boosters')
                auction['pet_info'] = {'level': level, 'tier': tier, 'candy_used': candy, 'held_item': held_item}
                auction['runes'] = extra.get('runes')

                del auction['item_bytes']
            except Exception as e:
                logger.warning(f"Could not parse NBT for auction {auction.get('auction_id')}. Error: {e}")
                error_auction(auction, source.name)

            cleaned.append(auction)

        with output.open("w", encoding="utf-8") as f:
            json.dump(cleaned, f, ensure_ascii=False, indent=4)

        logger.info(f"Successfully parsed {source.name} -> {len(cleaned)} items.")
        archive(source)
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

def archive(source_path: Path) -> bool:
    try:
        output_path = Path("archive")
        output_path.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), output_path)
        logger.info(f"Archived {source_path.name} to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to archive {source_path.name}. Error: {e}")
    return False

def error_auction(data: dict, filename: str) -> bool:
    error_dir = Path("bugged_auctions")
    error_dir.mkdir(parents=True, exist_ok=True)
    auction_id = data.get("auction_id")
    error_filename = f"{auction_id}_from_{filename}"
    file_path = error_dir / error_filename

    try:
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.warning(f"Saved bugged auction {auction_id} to {error_filename}.")
        return True
    except Exception as e:
        logger.error(f"Failed to save bugged auction {auction_id}: {e}")
        return False