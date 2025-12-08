from pathlib import Path
import json
import sqlite3
from logger_config import setup_logger
import re
from utils import archive


class DataLoader:
    def __init__(self, logger_conf:tuple, input_dir_path, db_path, parsed_archive_dir):
        self.logger = setup_logger(*logger_conf)
        self.input_dir_path = input_dir_path
        self.db_path = db_path
        self.parsed_archive_dir = parsed_archive_dir
        self._reset_counter()

    def _reset_counter(self):
        self.rows_added_counter = {
            'auctions': 0, 'enchantments': 0, 'gems': 0,
            'boosters': 0, 'pet_info': 0, 'runes': 0,
            'hooks': 0, 'lines': 0, 'sinkers': 0,
            'hyp_scrolls': 0, 'souls': 0
        }


    #needs to be executed first, in order to get auction_id
    def _one_to_one_insert(self, auction_data, cur: sqlite3.Cursor):
            self.auction_id = auction_data.get('auction_id')
            auctions_table_query = """INSERT INTO auctions (
            auction_id, price, bin, name, rarity_upgrades, hot_potato_count, modifier, dungeon_item_level, upgrade_level,
            item_id, dye_item, item_tier, hecatomb_s_runs, eman_kills, baseStatBoostPercentage, stats_book, power_ability_scroll,
            divan_powder_coating, thunder_charge, tuned_transmission, ethermerge, farming_for_dummies_count, 
            drill_part_upgrade_module, talisman_enrichment, polarvoid, mined_crops,
            bookworm_books, drill_part_fuel_tank, boss_tier, blood_god_kills, winning_bid, collected_coins, 
            wet_book_count, pelts_earned, gilded_gifted_coins, additional_coins, skin, mana_disintegrator_count,
            jalapeno_count, art_of_war_count, magma_cube_absorber, spider_kills, chimera_found, logs_cut, absorb_logs_chopped, 
            wood_singularity_count, artOfPeaceApplied, zombie_kills, drill_part_engine, pandora_rarity, blaze_consumer, handles_found
            )
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""  # auctions table query

            column_names = []
            match = re.search(r'\((.*?)\)', auctions_table_query, re.DOTALL)
            if match:
                columns_str = match.group(1)
                column_names = [col.strip() for col in columns_str.split(',')]

            main_data_queue = []

            for column_name in column_names:
                main_data_queue.append(auction_data.get(column_name))

            main_data_tuple = tuple(main_data_queue)

            try:
                cur.execute(auctions_table_query, main_data_tuple)
                self.rows_added_counter['auctions'] += 1
                return True
            except sqlite3.IntegrityError:
                self.logger.error(f"Auction already exists: {self.auction_id}")
                return False
            except Exception as e:
                self.logger.warning(f"Failed to insert auction {self.auction_id}: {e}")
                return False

    def _insert_enchantments(self, data, cur: sqlite3.Cursor):
        if data:
            enchants_queue = []

            for name, level in data.items():
                enchants_queue.append((self.auction_id, name, level))

            if enchants_queue:
                try:
                    sql_enchantments = """INSERT INTO enchantments (auction_id, name, level) VALUES (?, ?, ?);"""
                    cur.executemany(sql_enchantments, enchants_queue)
                    self.rows_added_counter['enchantments'] += len(enchants_queue)
                except Exception as e:
                    self.logger.error(f"Failed to insert enchantments for auction {self.auction_id}: {e}")

    def _insert_gems(self, data, cur: sqlite3.Cursor):
        if data:
            gems_queue = []

            for gem_slot, type_quality in data.items():
                gems_queue.append((self.auction_id, gem_slot, type_quality.get('type'), type_quality.get('quality')))

            if gems_queue:
                try:
                    sql_gems = """INSERT INTO gems (auction_id, gem_slot, gem_type, QUALITY) VALUES (?, ?, ?, ?);"""
                    cur.executemany(sql_gems, gems_queue)
                    self.rows_added_counter['gems'] += len(gems_queue)
                except Exception as e:
                    self.logger.error(f"Failed to insert gems for auction {self.auction_id}: {e}")


    def _insert_boosterlike(self, data, cur: sqlite3.Cursor, table_name:str): #boosters or hyp_scrolls
        if data:
            boosterlike_queue = []

            if isinstance(data, list):
                for name in data:
                    boosterlike_queue.append((self.auction_id, name))
            else:
                boosterlike_queue.append((self.auction_id, data))

            if boosterlike_queue:
                try:
                    boosterlike_query = f"""INSERT INTO {table_name} (auction_id, name) VALUES (?, ?);"""
                    cur.executemany(boosterlike_query, boosterlike_queue)
                    self.rows_added_counter[f'{table_name}'] += len(boosterlike_queue)
                except Exception as e:
                    self.logger.error(f"Failed to insert data ({table_name}) for auction {self.auction_id}: {e}")

    def _insert_pet_data(self, data, cur: sqlite3.Cursor):
        if data and data.get('level') is not None:
            pet_queue = (
                self.auction_id,
                data.get('level'),
                data.get('tier'),
                data.get('candy_used'),
                data.get('held_item'),
                data.get('pet_skin')
            )

            if pet_queue:
                try:
                    sql_pet = """INSERT INTO pet_info (auction_id, level, tier, candy_used, held_item, pet_skin) VALUES (?, ?, ?, ?, ?, ?);"""
                    cur.execute(sql_pet, pet_queue)
                    self.rows_added_counter['pet_info'] += 1
                except Exception as e:
                    self.logger.error(f"Failed to insert pet info for auction {self.auction_id}: {e}")

    def _insert_runes(self, data, cur: sqlite3.Cursor):
        if data:
            runes_queue = []
            for key, value in data.items():
                runes_queue.append((self.auction_id, key, value))

            if runes_queue:
                try:
                    sql_rune = """INSERT INTO runes (auction_id, name, level) VALUES (?, ?, ?);"""
                    cur.executemany(sql_rune, runes_queue)
                    self.rows_added_counter['runes'] += len(runes_queue)
                except Exception as e:
                    self.logger.error(f"Failed to insert runes for auction {self.auction_id}: {e}")

    def _insert_fishing_part(self, data, cur: sqlite3.Cursor):
        if data:
            for part, part_name in data.items():
                fishing_part_queue = (self.auction_id, part_name)

                if part_name:
                    try:
                        table_name = f"{part}s"
                        sql_query = f"""INSERT INTO {table_name} (auction_id, name) VALUES (?, ?);"""
                        cur.execute(sql_query, fishing_part_queue)
                        self.rows_added_counter[f'{table_name}'] += 1
                    except Exception as e:
                        self.logger.error(f"Failed to insert fishing part ({table_name}) for auction {self.auction_id}: {e}")


    def _insert_souls(self, data, cur: sqlite3.Cursor):
        if data:
            souls_queue = []
            for soul in data:
                mob_name = soul.get('mob_name')
                location = soul.get('location')
                instance = soul.get('instance')
                souls_queue.append((self.auction_id, mob_name, location, instance))

            if souls_queue:
                try:
                    sql_souls = """INSERT INTO souls (auction_id, name, location, instance) VALUES (?, ?, ?, ?);"""
                    cur.executemany(sql_souls, souls_queue)
                    self.rows_added_counter['souls'] += len(souls_queue)
                except Exception as e:
                    self.logger.error(f"Failed to insert souls for auction {self.auction_id}: {e}")


    def _insert_single_auction(self, auction, cur: sqlite3.Cursor):
        if not self._one_to_one_insert(auction, cur):
            return
        self._insert_enchantments(auction.get('enchantments'), cur)
        self._insert_gems(auction.get('gems'), cur)
        self._insert_boosterlike(auction.get('boosters'), cur, 'boosters')
        self._insert_boosterlike(auction.get('ability_scroll'), cur, 'hyp_scrolls')
        self._insert_pet_data(auction.get('pet_info'), cur)
        self._insert_runes(auction.get('runes'), cur)
        self._insert_fishing_part(auction.get('fishing_parts'), cur)
        self._insert_souls(auction.get('necromancer_souls'), cur)
        return True


    def load_from_dir(self):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            for file in self.input_dir_path.glob('*.json'):
                self._reset_counter()

                with file.open('r', encoding='utf-8') as json_file:
                    auctions = json.load(json_file)

                for auction in auctions:
                    self._insert_single_auction(auction, cur)

                for table, count in self.rows_added_counter.items():
                    if count > 0:
                        self.logger.info(f"Successfully inserted {table}: {count} rows")

                archive(file, self.parsed_archive_dir, logger=self.logger)





