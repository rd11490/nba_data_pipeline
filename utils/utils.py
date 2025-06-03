def check_duplicate_ids(df, id_col="id"):
    """
    Checks for duplicate values in the specified id column of a DataFrame.
    Prints the duplicate ids and raises an Exception if any are found.
    """
    dupes = df[df.index.duplicated(keep=False)]
    if not dupes.empty:
        print(f"Duplicate IDs found in index '{id_col}':")
        print(dupes.index.unique())
        raise Exception(f"Duplicate IDs found in index '{id_col}'!")

import time
import pandas as pd
from api.smart import SeasonType
from database.db_constants import Columns

SLEEP_TIME = 0.01

def convert_time_to_seconds(period, time_str):
    """
    Converts 'MM:SS' to seconds from game start for a given period.
    """
    minutes, seconds = map(int, time_str.split(':'))
    if int(period) <= 4:
        return (int(period) - 1) * 12 * 60 + (12 * 60 - (minutes * 60 + seconds))
    else:
        return 4 * 12 * 60 + (int(period) - 5) * 5 * 60 + (5 * 60 - (minutes * 60 + seconds))
def fill_nulls(df):
    """
    Fill NaN/nulls in a DataFrame: numeric columns get 0.0, others get None.
    """
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0.0)
        else:
            df[col] = df[col].where(df[col].notnull(), None)
    return df


def add_field(df, name, value):
    df[name] = value
    return df


def add_season(df, season):
    return add_field(df, Columns.SEASON, season)


def add_season_type(df, season_type):
    return add_field(df, Columns.SEASON_TYPE, season_type)


def add_season_and_type(df, season, season_type):
    return add_season_type(add_season(df, season), season_type)


def add_id(df, cols):
    df['id'] = df[cols].astype(str).agg('-'.join, axis=1)
    df = df.set_index('id')
    return df

def api_rate_limit():
    time.sleep(SLEEP_TIME)


def extract_season_from_game_id(game_id):
    season_start = int(game_id[3:5])
    season_end = season_start + 1
    return '20{}-{}'.format(season_start, season_end)


def extract_season_type_from_game_id(gameid):
    season_type_ind = gameid[2]
    if season_type_ind == '1':
        return SeasonType.Preseason
    elif season_type_ind == '2':
        return SeasonType.RegularSeason
    elif season_type_ind == '4':
        return SeasonType.Playoffs

