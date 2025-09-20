import pandas as pd
import pathlib
import numpy as np
import sqlite3
import pickle
from sklearn.preprocessing import OneHotEncoder

important_modifiers = [
    'withered', 'renowned', 'blessed', 'ancient', 'necrotic', 'stellar', 'snowy', 'groovy', 'spiritual', 'jaded', 'rooted', 'fabled', 'waxed', 'loving', 'strengthened',
    'squeaky', 'aote_stone', 'fleet', 'festive', 'mossy', 'treacherous', 'salty', 'suspicious', 'dimensional', 'auspicious', 'spiked', 'scraped', 'submerged', 'chomp',
    'unpleasant', 'bountiful', 'warped', 'fanged', 'lustrous', 'heated', 'blood_shot', 'lucky', 'ambered'
]
foundation = """
             SELECT auctions.*,
                    hooks.name          as hook_name,
                    lines.name          as line_name,
                    sinkers.name        as sinker_name,
                    pet_info.level      as pet_level,
                    pet_info.tier       as pet_tier,
                    pet_info.candy_used as pet_candy_used,
                    pet_info.held_item  as pet_held_item

             FROM auctions
                      LEFT JOIN hooks on auctions.auction_id = hooks.auction_id
                      LEFT JOIN lines on auctions.auction_id = lines.auction_id
                      LEFT JOIN sinkers on auctions.auction_id = sinkers.auction_id
                      LEFT JOIN pet_info on auctions.auction_id = pet_info.auction_id;"""

def onehot_column(frame, column_name: str, important_keys: list):
    df = frame.copy()
    temporary = df[column_name].to_numpy()
    for i, item in enumerate(temporary):
        if item not in important_keys:
            temporary[i] = 'Other'

    df[column_name] = temporary

    enc = OneHotEncoder(handle_unknown='ignore', sparse_output=False).set_output(transform='pandas')
    enctransform = enc.fit_transform(df[[column_name]])
    df = pd.concat([df, enctransform], axis=1).drop(column_name, axis=1)
    return df



df = sqlite3.connect('hypixel_auctions.db')
df_main = pd.read_sql_query(foundation, df)
df_main = onehot_column(df_main, 'modifier', important_modifiers)
df_main.info()




