import sqlite3
from pathlib import Path
from logger_config import setup_logger


logger = setup_logger("database_setup", "database_setup.log")
DB_FILE = Path("hypixel_auctions.db")

DDL_STATEMENTS = [
"""PRAGMA foreign_keys = ON;""",
"""PRAGMA journal_mode = WAL;""",
"""PRAGMA synchronous = NORMAL;""",

"""
CREATE TABLE IF NOT EXISTS auctions (
    auction_id TEXT PRIMARY KEY,
    price REAL,
    bin INTEGER CHECK (bin IN (0, 1)),
    name TEXT,
    rarity_upgrades INTEGER,
    hot_potato_count INTEGER,
    modifier TEXT,
    dungeon_item_level INTEGER,
    upgrade_level INTEGER,
    item_id TEXT,
    dye_item TEXT,
    item_tier INTEGER,
    hecatomb_s_runs INTEGER,
    eman_kills INTEGER,
    baseStatBoostPercentage INTEGER,
    stats_book INTEGER,
    power_ability_scroll TEXT,
    divan_powder_coating INTEGER,
    thunder_charge INTEGER,
    tuned_transmission INTEGER,
    ethermerge INTEGER,
    farming_for_dummies_count INTEGER,
    drill_part_upgrade_module TEXT,
    talisman_enrichment TEXT,
    polarvoid INTEGER,
    mined_crops INTEGER,
    bookworm_books INTEGER,
    drill_part_fuel_tank TEXT, 
    boss_tier INTEGER,
    blood_god_kills INTEGER,
    winning_bid INTEGER,
    collected_coins INTEGER,
    wet_book_count INTEGER,
    pelts_earned INTEGER,
    gilded_gifted_coins INTEGER,
    additional_coins INTEGER,
    skin TEXT,
    mana_disintegrator_count INTEGER,
    jalapeno_count INTEGER,
    art_of_war_count INTEGER,
    magma_cube_absorber INTEGER,
    spider_kills INTEGER,
    chimera_found INTEGER,
    logs_cut INTEGER,
    absorb_logs_chopped INTEGER,
    wood_singularity_count INTEGER,
    artOfPeaceApplied INTEGER,
    zombie_kills INTEGER,
    drill_part_engine TEXT,
    pandora_rarity TEXT,
    blaze_consumer INTEGER,
    handles_found INTEGER
    );
""",  #main_table

"""
CREATE TABLE IF NOT EXISTS enchantments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id TEXT,
    name TEXT,
    level INTEGER,
    FOREIGN KEY (auction_id) REFERENCES auctions(auction_id) ON DELETE CASCADE
);
""",  #enchantments

"""
CREATE TABLE IF NOT EXISTS gems (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id TEXT,
    gem_slot TEXT,
    gem_type TEXT,
    QUALITY TEXT,
    FOREIGN KEY (auction_id) REFERENCES auctions(auction_id) ON DELETE CASCADE
);
""",  #gems

"""
CREATE TABLE IF NOT EXISTS runes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id TEXT,
    name TEXT,
    level INTEGER,
    FOREIGN KEY (auction_id) REFERENCES auctions (auction_id) ON DELETE CASCADE
);
""",  #runes

"""
CREATE TABLE IF NOT EXISTS boosters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id TEXT,
    name TEXT,
    FOREIGN KEY (auction_id) REFERENCES auctions (auction_id) ON DELETE CASCADE
);
""",  #boosters

"""
CREATE TABLE IF NOT EXISTS pet_info (
    auction_id TEXT PRIMARY KEY,
    level INTEGER,
    tier TEXT,
    candy_used INTEGER,
    held_item TEXT,
    pet_skin TEXT,
    FOREIGN KEY (auction_id) REFERENCES auctions (auction_id) ON DELETE CASCADE
);    
""",   #petInfo
    
"""
CREATE TABLE IF NOT EXISTS hyp_scrolls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id TEXT,
    name TEXT,
    FOREIGN KEY (auction_id) REFERENCES auctions (auction_id) ON DELETE CASCADE
);
""",  #hyperion_ability_scrolls

"""
CREATE TABLE IF NOT EXISTS sinkers (
    auction_id TEXT PRIMARY KEY,
    name TEXT,
    FOREIGN KEY (auction_id) REFERENCES auctions(auction_id) ON DELETE CASCADE
);
""",  #sinkers

"""
CREATE TABLE IF NOT EXISTS lines (
    auction_id TEXT PRIMARY KEY,
    name TEXT,
    FOREIGN KEY (auction_id) REFERENCES auctions(auction_id) ON DELETE CASCADE
);
""",  #lines

"""
CREATE TABLE IF NOT EXISTS hooks (
    auction_id TEXT PRIMARY KEY,
    name TEXT,
    FOREIGN KEY (auction_id) REFERENCES auctions(auction_id) ON DELETE CASCADE
);
""",  #hooks

"""
CREATE TABLE IF NOT EXISTS souls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    auction_id TEXT,
    name TEXT,
    location TEXT,
    instance TEXT,
    FOREIGN KEY (auction_id) REFERENCES auctions(auction_id) ON DELETE CASCADE
);
"""   #souls on item
]

def db_setup():
    try:
        if DB_FILE.exists():
            logger.info("Database already exists")
        else:
            with sqlite3.connect(DB_FILE) as conn:
                cur = conn.cursor()
                for ddl in DDL_STATEMENTS:
                    cur.execute(ddl)
            logger.info('Database setup complete.')
    except Exception as e:
        logger.error(f"Database setup failed. {e}")



if __name__ == '__main__':
    db_setup()