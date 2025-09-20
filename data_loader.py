from pathlib import Path
import json
import sqlite3
from logger_config import setup_logger
from parser import archive
import re

logger = setup_logger('data_loader', 'data_loader.log')

INPUT_DIR = Path('parsed_jsons')
DATABASE = Path('hypixel_auctions.db')
PARSED_ARCHIVE_DIR = Path(r'archive\parsed_archive')


GEM_TYPES = {'RUBY', 'AMBER', 'TOPAZ', 'JADE', 'SAPPHIRE', 'AMETHYST', 'JASPER', 'OPAL', 'AQUAMARINE', 'CITRINE', 'ONYX', 'PERIDOT'}

def insert_auction(cur: sqlite3.Cursor, auction_data: dict) -> dict:
    sql_auctions = """INSERT INTO auctions (auction_id, price, bin, name, rarity_upgrades, hot_potato_count, modifier, dungeon_item_level, upgrade_level, 
                                            item_id, dye_item, item_tier, hecatomb_s_runs, eman_kills, baseStatBoostPercentage, stats_book, power_ability_scroll,
    divan_powder_coating, thunder_charge, tuned_transmission, ethermerge, farming_for_dummies_count, drill_part_upgrade_module, talisman_enrichment, polarvoid, mined_crops,
    bookworm_books, drill_part_fuel_tank, boss_tier, blood_god_kills, winning_bid, collected_coins, wet_book_count, pelts_earned, gilded_gifted_coins, additional_coins, skin, mana_disintegrator_count,
    jalapeno_count, art_of_war_count, magma_cube_absorber, spider_kills, chimera_found, logs_cut, absorb_logs_chopped, wood_singularity_count, artOfPeaceApplied, zombie_kills, drill_part_engine, pandora_rarity) 
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""" #auctions table

    rows_added = {
        'auctions': 0, 'enchantments': 0, 'gems': 0,
        'boosters': 0, 'pet_info': 0, 'runes': 0,
        'hooks': 0, 'lines': 0, 'sinkers': 0,
        'scrolls': 0, 'souls': 0
    }

    column_names = []
    match = re.search(r'\((.*?)\)', sql_auctions, re.DOTALL)
    if match:
        columns_str = match.group(1)
        column_names = [col.strip() for col in columns_str.split(',')]

    main_data_queue = []

    for column_name in column_names:
        if column_name == 'bin':
            main_data_queue.append(1 if auction_data.get('bin') else 0)
        else:
            main_data_queue.append(auction_data.get(column_name))

    main_data_tuple = tuple(main_data_queue)

    try:
        cur.execute(sql_auctions, main_data_tuple)
        auction_id = auction_data.get('auction_id')
        rows_added['auctions'] = 1
    except sqlite3.IntegrityError:
        logger.error(f"Auction already exists: {auction_data.get('auction_id')}")
        return {}
    except Exception as e:
        logger.warning(f"Failed to insert auction {auction_data.get('auction_id')}: {e}")
        return {}

    enchantments_data = auction_data.get('enchantments') #enchantments table

    if enchantments_data:
        enchants_queue = []

        for name, level in enchantments_data.items():
            enchants_queue.append((auction_id, name, level))

        if enchants_queue:
            try:
                sql_enchantments = """INSERT INTO enchantments (auction_id, name, level) VALUES (?, ?, ?);"""
                cur.executemany(sql_enchantments, enchants_queue)
                rows_added['enchantments'] = len(enchants_queue)
            except Exception as e:
                logger.error(f"Failed to insert enchantments for auction {auction_id}: {e}")

    gems_data = auction_data.get('gems')  #gems data

    if gems_data:
        gems_queue = []

        for slot_name, slot_info in gems_data.items():
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

            known_type = gems_data.get(f"{slot_name}_gem")
            if known_type:
                gem_type = known_type
            else:
                potential_type = slot_name.split('_')[0]
                if potential_type in GEM_TYPES:
                    gem_type = potential_type

            gems_queue.append((auction_id, slot_name, gem_type, gem_quality ))

        if gems_queue:
            try:
                sql_gems = """INSERT INTO gems (auction_id, gem_slot, gem_type, QUALITY) VALUES (?, ?, ?, ?);"""
                cur.executemany(sql_gems, gems_queue)
                rows_added['gems'] = len(gems_queue)
            except Exception as e:
                logger.error(f"Failed to insert gems for auction {auction_id}: {e}")

    boosters_data = auction_data.get('boosters')  #boosters table
    if boosters_data:
        boosters_queue = []

        if isinstance(boosters_data, list):
            for name in boosters_data:
                boosters_queue.append((auction_id, name))
        else:
            boosters_queue.append((auction_id, boosters_data))

        if boosters_queue:
            try:
                sql_boosters = """INSERT INTO boosters (auction_id, name) VALUES (?, ?);"""
                cur.executemany(sql_boosters, boosters_queue)
                rows_added['boosters'] = len(boosters_queue)
            except Exception as e:
                logger.error(f"Failed to insert boosters for auction {auction_id}: {e}")

    pet_data = auction_data.get('pet_info') #pet_info table
    if pet_data and pet_data.get('level') is not None:
        pet_queue = (
            auction_id,
            pet_data.get('level'),
            pet_data.get('tier'),
            pet_data.get('candy_used'),
            pet_data.get('held_item')
        )

        if pet_queue:
            try:
                sql_pet = """INSERT INTO pet_info (auction_id, level, tier, candy_used, held_item) VALUES (?, ?, ?, ?, ?);"""
                cur.execute(sql_pet, pet_queue)
                rows_added['pet_info'] = 1
            except Exception as e:
                logger.error(f"Failed to insert pet info for auction {auction_id}: {e}")

    runes_data = auction_data.get('runes') #runes table
    if runes_data:
        runes_queue = []
        for key, value in runes_data.items():
            runes_queue.append((auction_id, key, value))

        if runes_queue:
            try:
                sql_rune = """INSERT INTO runes (auction_id, name, level) VALUES (?, ?, ?);"""
                cur.executemany(sql_rune, runes_queue)
                rows_added['runes'] = len(runes_queue)
            except Exception as e:
                logger.error(f"Failed to insert runes for auction {auction_id}: {e}")

    hooks_data = auction_data.get('hook') #hooks table
    if hooks_data:
        hooks_queue = []
        hooks_queue.append((auction_id, hooks_data.get('part')))

        if hooks_queue:
            try:
                sql_hook = """INSERT INTO hooks (auction_id, name) VALUES (?, ?);"""
                cur.executemany(sql_hook, hooks_queue)
                rows_added['hooks'] = len(hooks_queue)
            except Exception as e:
                logger.error(f"Failed to insert hooks for auction {auction_id}: {e}")

    lines_data = auction_data.get('line')  # lines table
    if lines_data:
        lines_queue = []
        lines_queue.append((auction_id, lines_data.get('part')))

        if lines_queue:
            try:
                sql_line = """INSERT INTO lines (auction_id, name) VALUES (?, ?);"""
                cur.executemany(sql_line, lines_queue)
                rows_added['lines'] = len(lines_queue)
            except Exception as e:
                logger.error(f"Failed to insert lines for auction {auction_id}: {e}")

    sinkers_data = auction_data.get('sinker')  # sinkers table
    if sinkers_data:
        sinkers_queue = []
        sinkers_queue.append((auction_id, sinkers_data.get('part')))


        if sinkers_queue:
            try:
                sql_sinker = """INSERT INTO sinkers (auction_id, name) VALUES (?, ?);"""
                cur.executemany(sql_sinker, sinkers_queue)
                rows_added['sinkers'] = len(sinkers_queue)
            except Exception as e:
                logger.error(f"Failed to insert sinkers for auction {auction_id}: {e}")

    scrolls_data = auction_data.get('ability_scroll')  #scrolls table
    if scrolls_data:
        scrolls_queue = []

        if isinstance(scrolls_data, list):
            for name in scrolls_data:
                scrolls_queue.append((auction_id, name))
        else:
            scrolls_queue.append((auction_id, boosters_data))

        if scrolls_queue:
            try:
                sql_scrolls = """INSERT INTO hyp_scrolls (auction_id, name) VALUES (?, ?);"""
                cur.executemany(sql_scrolls, scrolls_queue)
                rows_added['scrolls'] = len(scrolls_queue)
            except Exception as e:
                logger.error(f"Failed to insert scrolls for auction {auction_id}: {e}")

    souls_data = auction_data.get('necromancer_souls')
    if souls_data:
        souls_queue = []
        for soul in souls_data:
            mob_name = soul.get('mob_id')
            location = soul.get('dropped_mode_id')
            instance = None
            if location == 'dungeon':
                instance = soul.get('dropped_instance_id')

            souls_queue.append((auction_id, mob_name, location, instance))

        if souls_queue:
            try:
                sql_souls = """INSERT INTO souls (auction_id, name, location, instance) VALUES (?, ?, ?, ?);"""
                cur.executemany(sql_souls, souls_queue)
                rows_added['souls'] = len(souls_queue)
            except Exception as e:
                logger.error(f"Failed to insert souls for auction {auction_id}: {e}")

    return rows_added

def load_data(input_path: Path = INPUT_DIR) -> None:
    try:
        with sqlite3.connect(DATABASE) as conn:
            cur = conn.cursor()
            for file in input_path.iterdir():
                with file.open('r', encoding='utf8') as f:
                    auctions = json.load(f)

                file_summary = {'auctions': 0, 'enchantments': 0, 'gems': 0,
                                'boosters': 0, 'pet_info': 0, 'runes': 0,
                                'hooks': 0, 'lines': 0, 'sinkers': 0,
                                'scrolls': 0, 'souls': 0}

                flag = True
                for auction in auctions:
                    single_auction = insert_auction(cur, auction)
                    if not single_auction:
                        flag = False
                    for table_name, count in single_auction.items():
                        file_summary[table_name] += count

                if flag:
                    archive(file, PARSED_ARCHIVE_DIR)
                else:
                    logger.warning(f'{file} was not archived!')

                for table_name, count in file_summary.items():
                    if count > 0:
                        logger.info(f"Inserted rows into {table_name}: {count}")
    except Exception as e:
        logger.error(f"Failed to load data from {INPUT_DIR}: {e}")

if __name__ == '__main__':
    load_data(INPUT_DIR)
