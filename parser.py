from typing import Union, Any
from logger_config import setup_logger
import nbt_utils as utils
from pathlib import Path
import json
import shutil
import re

# jaki sens ma ustawianie JSON = Any?
logger = setup_logger("parser", "parser.log")
OUTPUT_ARCHIVE_PATH_DIR = Path(r"archive\raw_archive")

# ogolnie staraj sie projektowac logike w taki sposob zeby funkcja zwracala ci "coś" albo None
# tzn. w przypadku sukcesu zwroc tuple (a, b, c, d), w przypadku faila zwróć None, a nie tuple (None, None, None, None)
# to jest taka standardowa praktyka
# wtedy w type hinting robisz -> tuple | None i user wie ze moze ci wyjsc gowno
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


def dfs_collect_value(root: Any, key: str) -> Any | None:
    stack = [root]

    while stack:
        node = stack.pop()

        if isinstance(node, dict):
            if key in node:
                return node[key]
            node = node.values()
        [stack.append(value) for value in node] # tutaj mozna sobie taki shortcut zrobic
    return None

def parse(source_path: Union[str, Path], output_path=Path("parsed_jsons")) -> bool:
    key_extra = [
        'gems', 'modifier', 'dungeon_item_level', 'upgrade_level', 'enchantments', 'dye_item', 'item_tier', 'hecatomb_s_runs', 'eman_kills',
        'baseStatBoostPercentage', 'boosters', 'runes', 'stats_book', 'power_ability_scroll', 'divan_powder_coating', 'thunder_charge', 'tuned_transmission',
        'ethermerge', 'farming_for_dummies_count', 'talisman_enrichment', 'polarvoid', 'mined_crops', 'bookworm_books', 'ability_scroll', 'ability_scroll',
        'drill_part_fuel_tank', 'boss_tier', 'blood_god_kills', 'winning_bid', 'collected_coins', 'wet_book_count', 'sinker', 'line', 'hook', 'pelts_earned',
        'gilded_gifted_coins', 'additional_coins', 'skin', 'mana_disintegrator_count', 'jalapeno_count', 'art_of_war_count', 'magma_cube_absorber', 'spider_kills',
        'chimera_found', 'logs_cut', 'absorb_logs_chopped', 'wood_singularity_count', 'artOfPeaceApplied', 'zombie_kills', 'drill_part_engine', 'pandora_rarity',
        'necromancer_souls', 'rarity_upgrades', 'hot_potato_count', 'drill_part_upgrade_module'
    ]

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
            # tak mozna zrobic lepiej, bez niepotrzebnego ifa drugiego
            for key in keys_remove:
                auction.pop(key, None)

            try:
                nbt_data = auction['item_bytes']
                decoded = utils.nbt_base64_to_dict(nbt_data)
                extra = dfs_collect_value(decoded, 'ExtraAttributes') or {}
                display = dfs_collect_value(decoded, 'display') or {}

                level, cleaned_name = decode_name(display.get('Name'))
                auction['name'] = cleaned_name

                base_id = extra.get('id')
                if base_id == "PET":
                    pet_type, tier, candy, held_item = decode_petinfo(extra.get('petInfo'))
                    auction['item_id'] = pet_type
                else:
                    auction['item_id'] = base_id
                    tier, candy, held_item = None, None, None

                auction['pet_info'] = {'level': level, 'tier': tier, 'candy_used': candy, 'held_item': held_item}

                for key in key_extra:
                    auction[key] = extra.get(key, None)

                auction.pop('item_bytes', None)  # del ci wyjebie wyjatek, a tego staramy sie uniknac
                # jak zalezy ci zeby miec info czy klucz zostal usuniety to sobie przypisz to wyrazenie wyzej do zmiennej i sprawdz
            except Exception as e:
                logger.warning(f"Could not parse NBT for auction {auction.get('auction_id')}. Error: {e}")
                error_auction(auction, source.name)

            cleaned.append(auction)

        with output.open("w", encoding="utf-8") as f:
            json.dump(cleaned, f, ensure_ascii=False, indent=4)

        logger.info(f"Successfully parsed {source.name} -> {len(cleaned)} items.")
        archive(source, OUTPUT_ARCHIVE_PATH_DIR)
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

# nie ma potrzeby zwracania czegokolwiek, jesli potem tego nie uzywasz
def archive(source_path: Path, output_path: Path):
    try:
        output_path.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), output_path)
        logger.info(f"Archived {source_path.name} to {output_path}")
    except Exception as e:
        logger.error(f"Failed to archive {source_path.name}. Error: {e}")

# tak samo tutaj - po co zwracac jak to do niczego nie sluzy
def error_auction(data: dict, filename: str):
    error_dir = Path("bugged_auctions")
    error_dir.mkdir(parents=True, exist_ok=True)
    auction_id = data.get("auction_id")
    error_filename = f"{auction_id}_from_{filename}"
    file_path = error_dir / error_filename

    try:
        with file_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.warning(f"Saved bugged auction {auction_id} to {error_filename}.")
    except Exception as e:
        logger.error(f"Failed to save bugged auction {auction_id}: {e}")

