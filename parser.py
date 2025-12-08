from logger_config import setup_logger
import utils
from pathlib import Path
import json
import re
from constants import key_extra, gem_types

class Parser:
    def __init__(self, logger_conf:tuple, output_parsed_path: Path, archive_path: Path):
        self.logger = setup_logger(*logger_conf)
        self.output_parsed_path = output_parsed_path
        self.archive_path = archive_path

    @staticmethod
    def decode_petinfo(petinfo: str | None) -> tuple:
        if not petinfo:
            return None, None, None, None

        try:
            raw_info = json.loads(petinfo)
            type = raw_info.get('type', None)
            tier = raw_info.get('tier', None)
            candy = raw_info.get('candyUsed')
            item = raw_info.get('heldItem', None)
            skin = raw_info.get('skin', None)
            return type, tier, candy, item, skin
        except json.JSONDecodeError:
            return None, None, None, None, None

    @staticmethod
    def decode_name(txt: str | None):
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

    @staticmethod
    def dfs_collect_value(root, key: str):
        stack = [root]

        while stack:
            node = stack.pop()

            if isinstance(node, dict):
                if key in node:
                    return node[key]
                for value in node.values():
                    stack.append(value)
            elif isinstance(node, list):
                for item in node:
                    stack.append(item)
        return None


    def _parse_gems(self, data):
        gems = {}
        if not data:
            return None

        for slot_name, slot_info in data.items():
            if slot_name == "unlocked_slots" or slot_name.endswith('_gem'):
                continue

            gem_quality = None
            if isinstance(slot_info, str):
                gem_quality = slot_info
            elif isinstance(slot_info, dict):
                gem_quality = slot_info.get('quality')

            if not gem_quality:
                continue

            gem_type = None

            known_type = data.get(f"{slot_name}_gem")
            if known_type:
                gem_type = known_type
            else:
                potential_type = slot_name.split('_')[0]
                if potential_type in gem_types:
                    gem_type = potential_type

            if gem_type and gem_quality:
                gems[f'{slot_name}'] = {'type': gem_type, 'quality': gem_quality}

        return gems if gems else None

    def _parse_fishing_parts(self, hook, line, sinker):

        hook_info = hook
        line_info = line
        sinker_info = sinker

        fishing_parts = {
            'hook': hook_info.get('part'),
            'line': line_info.get('part'),
            'sinker': sinker_info.get('part'),
        }

        return fishing_parts


    def _parse_souls(self, data):
        if not data:
            return None
        souls = []

        for soul in data:
            mob_name = soul.get('mob_id')
            location = soul.get('dropped_mode_id')
            instance = None
            if location == 'dungeon':
                instance = soul.get('dropped_instance_id')

            souls.append({
                'mob_name': mob_name,
                'location': location,
                'instance': instance,
            })

        return souls if souls else None

    def filter_single_auction(self, auction, key_extra_list: set):

        result = {}
        #or statement for future dataframe where auction_id is changed to uuid etc.
        auction_id = auction.get('auction_id') or auction.get('uuid')
        price = auction.get('price') or auction.get('starting_bid')

        result['auction_id'] = auction_id
        result['price'] = price

        nbt_data = auction['item_bytes']
        decoded = utils.nbt_base64_to_dict(nbt_data)
        extra = self.dfs_collect_value(decoded, 'ExtraAttributes') or {}
        display = self.dfs_collect_value(decoded, 'display') or {}

        level, cleaned_name = self.decode_name(display.get('Name'))
        result['name'] = cleaned_name

        base_id = extra.get('id')
        if base_id == "PET":
            pet_type, tier, candy, held_item, skin = self.decode_petinfo(extra.get('petInfo'))
            result['item_id'] = pet_type
        else:
            result['item_id'] = base_id
            tier, candy, held_item, skin = None, None, None, None

        result['pet_info'] = {'level': level, 'tier': tier, 'candy_used': candy, 'held_item': held_item, 'pet_skin':skin}
        result['gems'] = self._parse_gems(extra.get('gems'))

        hook_data = extra.get('hook', {})
        line_data = extra.get('line', {})
        sinker_data = extra.get('sinker', {})

        result['fishing_parts'] = self._parse_fishing_parts(hook_data, line_data, sinker_data)
        result['necromancer_souls'] = self._parse_souls(extra.get('necromancer_souls'))

        to_drop = ['necromancer_souls', 'hook', 'line', 'sinker', 'gems']
        temp = key_extra_list.copy()
        for key in to_drop:
            temp.remove(key)

        for key in temp:
            result[key] = extra.get(key, None)

        return result

    def parse(self, source_path):
        output_path = self.output_parsed_path
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

                try:
                    cleaned.append(self.filter_single_auction(auction, key_extra))

                except Exception as e:
                    self.logger.warning(f"Could not parse NBT for auction {auction.get('auction_id')}. Error: {e}")
                    auction_id = auction.get('auction_id')
                    error_filename = f"{auction_id}_from_{source.name}"
                    utils.export_to_json(data, Path('bugged_auctions'), file_name=error_filename, create_folder=True)

            with output.open("w", encoding="utf-8") as f:
                json.dump(cleaned, f, ensure_ascii=False, indent=4)

            self.logger.info(f"Successfully parsed {source.name} -> {len(cleaned)} items.")
            utils.archive(source, self.archive_path, logger=self.logger)
            return True

        except FileNotFoundError:
            self.logger.error(f"File {source_path} not found.")
            return False
        except json.decoder.JSONDecodeError:
            self.logger.error(f"File {source_path} could not be decoded.")
            return False
        except KeyError:
            self.logger.error(f"File {source_path} has no key.")
            return False
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while parsing {source_path}: {e}")
            return False

