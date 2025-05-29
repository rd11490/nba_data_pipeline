import argparse
import pandas as pd
from api.smart import smart
from database.db_client import database_client
from database.db_constants import Tables, Columns
from utils.arg_parser import season_arg, season_type_arg, game_id_arg, delta_arg
from utils.utils import add_id, fill_nulls, extract_season_from_game_id, extract_season_type_from_game_id
import json

def agg_stints(grp):
    # Build a single row for each group
    row = {}
    row[Columns.STINTS] = json.dumps(list(grp[[Columns.IN_TIME_REAL, Columns.OUT_TIME_REAL]].to_dict('records')))
    return pd.Series(row)

def fetch_rotation(game_id, season, season_type):
    # Fetch rotation data from NBA API
    data = smart.game_rotation(game_id)
    home_df = data['HomeTeam']
    away_df = data['AwayTeam']


    if home_df.empty and away_df.empty:
        return None

    df = pd.concat([home_df, away_df], ignore_index=True)

    # Add season and season_type columns
    df[Columns.SEASON] = season
    df[Columns.SEASON_TYPE] = season_type
    df[Columns.GAME_ID] = game_id
    # Limit columns
    keep_cols = [
        Columns.GAME_ID,
        Columns.TEAM_ID,
        Columns.TEAM_NAME,
        Columns.PLAYER_ID,
        Columns.PLAYER_FIRST_NAME,
        Columns.PLAYER_LAST_NAME,
        Columns.IN_TIME_REAL,
        Columns.OUT_TIME_REAL,
        Columns.SEASON,
        Columns.SEASON_TYPE
    ]
    # Rename PERSON_ID to PLAYER_ID for consistency
    if 'PERSON_ID' in df.columns:
        df = df.rename(columns={'PERSON_ID': Columns.PLAYER_ID})
    df = df[keep_cols]
    # Group by player and aggregate stints
    
    # Group by player and aggregate stints into a single row per player
    group_cols = [
        Columns.GAME_ID,
        Columns.TEAM_ID,
        Columns.TEAM_NAME,
        Columns.PLAYER_ID,
        Columns.PLAYER_FIRST_NAME,
        Columns.PLAYER_LAST_NAME,
        Columns.SEASON,
        Columns.SEASON_TYPE
    ]

    result = df.groupby(group_cols)[[Columns.IN_TIME_REAL, Columns.OUT_TIME_REAL]].apply(agg_stints).reset_index()
    # Add id
    result = add_id(result, [Columns.GAME_ID, Columns.PLAYER_ID])
    # Fill nulls
    result = fill_nulls(result)
    return result

def filter_game_ids_delta(db, game_ids, season, season_type):
    q_delta = f'SELECT DISTINCT "{Columns.GAME_ID}" FROM {Tables.ROTATIONS} WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
    result_delta = db.read(q_delta, params={'season': season, 'stype': season_type})
    if result_delta is not None and not result_delta.empty:
        existing_game_ids = set(result_delta[Columns.GAME_ID].tolist())
        before_count = len(game_ids)
        filtered_game_ids = [gid for gid in game_ids if gid not in existing_game_ids]
        after_count = len(filtered_game_ids)
        print(f"Delta mode: {before_count - after_count} games already exist in rotations, {after_count} remaining to process for season {season}.")
        return filtered_game_ids
    else:
        print(f"Delta mode: No games found in rotations for season {season} and type {season_type}.")
        return game_ids

def get_game_ids(season, season_type, db):
    q = f'SELECT DISTINCT "{Columns.GAME_ID}" FROM {Tables.TEAM_GAME_LOG} WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
    result = db.read(q, params={'season': season, 'stype': season_type})
    game_ids = result[Columns.GAME_ID].tolist()
    return game_ids

def write_frames(dfs, db, games_to_process, i):
    all_df = pd.concat(dfs)
    db.write(all_df, Tables.ROTATIONS)
    print(f"Wrote {i}/{games_to_process} games to {Tables.ROTATIONS}")

def main():
    parser = argparse.ArgumentParser(description='Pull NBA rotations for given seasons and season type.')
    season_arg(parser)
    season_type_arg(parser)
    game_id_arg(parser)
    delta_arg(parser)
    args = parser.parse_args()

    has_game_id = args.game_id is not None
    has_season_and_type = args.season is not None and args.season_type is not None
    if has_game_id and has_season_and_type:
        raise Exception("You must provide either --game_id or both --season and --season_type, but not both.")
    if not has_game_id and not has_season_and_type:
        raise Exception("You must provide either --game_id or both --season and --season_type.")

    if args.game_id:
        # Determine season and season_type from game_id using utils
        season = extract_season_from_game_id(args.game_id)
        season_type = extract_season_type_from_game_id(args.game_id)
        df = fetch_rotation(args.game_id, season, season_type)
        if df is None or df.empty:
            print(f"No rotation data found for game {args.game_id}.")
            return
        database_client.write(df, Tables.ROTATIONS)
        print(f"Processed game {args.game_id}")
    else:
        seasons = [s.strip() for s in args.season.split(',') if s.strip()]
        for season in seasons:
            game_ids = get_game_ids(season, args.season_type, database_client)
            if getattr(args, 'delta', False):
                game_ids = filter_game_ids_delta(database_client, game_ids, season, args.season_type)
            dfs = []
            games_to_process = len(game_ids)
            for i, gid in enumerate(game_ids, 1):
                try:
                    df = fetch_rotation(gid, season, args.season_type)
                    if df is None or df.empty:
                        print(f"No rotation data found for game {args.game_id}.")
                    else:
                        dfs.append(df)
                    print(f"Processed game {gid}")
                except Exception as e:
                    print(f"Failed for game {gid}: {e}")
                if i%10 == 0 and len(dfs) > 0:
                    write_frames(dfs, database_client, games_to_process, i)
                    dfs = []
            if dfs:
                write_frames(dfs, database_client, games_to_process, i)
    database_client.close()

if __name__ == '__main__':
    main()
