import argparse
import pandas as pd

from api.smart import smart
from database.db_client import PostgresClient
from database.db_constants import Tables, Columns
from database.creds import creds
from utils.arg_parser import (
    season_arg,
    season_type_arg,
    game_id_arg,
    delta_arg,
)
from utils.utils import (
    extract_season_from_game_id,
    extract_season_type_from_game_id,
    add_season_and_type,
    add_id,
    fill_nulls,
)

def fetch_play_by_play_by_game_id(game_id):
    """
    Fetch play-by-play DataFrame for a single game_id, with SEASON and SEASON_TYPE columns added.
    """
    df = smart.play_by_play(game_id)
    season = extract_season_from_game_id(game_id)
    season_type = extract_season_type_from_game_id(game_id)
    df = add_season_and_type(df, season, season_type)
    df[Columns.GAME_ID] = game_id  # Add game_id column for traceability
    # Add 'id' column using GAME_ID and EVENTNUM (always present)
    df = add_id(df, [Columns.GAME_ID, Columns.EVENTNUM])
    # Fill NaN/nulls
    df = fill_nulls(df)
    df = df.drop_duplicates()
    print(f"Fetched play-by-play for game_id {game_id} with {len(df)} rows.")
    return df

def play_by_play_exists(db, game_id):
    """
    Returns True if the play-by-play data for the given game_id already exists in the play_by_play table.
    """
    check_query = f'''
        SELECT 1 FROM {Tables.PLAY_BY_PLAY}
        WHERE "{Columns.GAME_ID}" = :game_id
        LIMIT 1
    '''
    exists = db.read(check_query, params={'game_id': game_id})
    return exists is not None and not exists.empty

def get_existing_play_by_play_game_ids(db, seasons, season_type):
    """
    Returns a set of all game_ids that already exist in the play_by_play table for the given seasons and season_type.
    """
    delta_query = f'''
        SELECT DISTINCT "{Columns.GAME_ID}"
        FROM {Tables.PLAY_BY_PLAY}
        WHERE "{Columns.SEASON}" IN :seasons AND "{Columns.SEASON_TYPE}" = :season_type
    '''
    existing = db.read(delta_query, params={'seasons': tuple(seasons), 'season_type': season_type})
    if existing is not None and not existing.empty:
        return set(existing[Columns.GAME_ID].tolist())
    return set()



def main():
    parser = argparse.ArgumentParser(description='Pull NBA team game logs for given seasons and season type.')
    season_arg(parser)
    season_type_arg(parser)
    game_id_arg(parser)
    delta_arg(parser)
    args = parser.parse_args()

    # Use credentials from database/creds.py
    db = PostgresClient(
        dbname=creds.dbname,
        user=creds.user,
        password=creds.password,
        host=creds.host,
        port=creds.port
    )

    # Argument validation: must provide only one mode
    has_game_id = args.game_id is not None
    has_season_and_type = args.season is not None and args.season_type is not None
    delta_run = getattr(args, 'delta', False) 

    if has_game_id and has_season_and_type:
        raise Exception("You must provide either --game_id or both --season and --season_type, but not both.")
    if not has_game_id and not has_season_and_type:
        raise Exception("You must provide either --game_id or both --season and --season_type.")


    if has_game_id:
        # Single game mode
        if delta_run:
            if play_by_play_exists(db, args.game_id):
                print(f"Play-by-play data already exists for game_id {args.game_id}. Skipping.")
                return
        df = fetch_play_by_play_by_game_id(args.game_id)
        db.write(df, Tables.PLAY_BY_PLAY)
        print(f"Wrote play-by-play for game_id {args.game_id} to table {Tables.PLAY_BY_PLAY}")
        return

    if has_season_and_type:
        seasons = [s.strip() for s in args.season.split(',') if s.strip()]
        # Multi-game mode: get all game_ids for these seasons and this season_type
        query = f'''
            SELECT DISTINCT "{Columns.GAME_ID}"
            FROM {Tables.TEAM_GAME_LOG}
            WHERE "{Columns.SEASON}" IN :seasons AND "{Columns.SEASON_TYPE}" = :season_type
        '''
        result = db.read(query, params={'seasons': tuple(seasons), 'season_type': args.season_type})
        if result is None or result.empty:
            print(f"No games found for seasons {seasons} and type {args.season_type}")
            return
        game_ids = result[Columns.GAME_ID].tolist()
        # If delta, filter out game_ids already in play_by_play for these seasons and season_type
        if delta_run:
            existing_game_ids = get_existing_play_by_play_game_ids(db, seasons, args.season_type)
            if existing_game_ids:
                game_ids = [gid for gid in game_ids if gid not in existing_game_ids]
                print(f"Delta mode: {len(existing_game_ids)} games already exist in play_by_play, {len(game_ids)} remaining to fetch.")
            else:
                print(f"Delta mode: No games found in play_by_play for these seasons and type.")
        dfs = []
        written_games = 0
        for i, gid in enumerate(game_ids, 1):
            try:
                df = fetch_play_by_play_by_game_id(gid)
                dfs.append(df)
            except Exception as e:
                print(f"Failed to fetch play-by-play for game_id {gid}: {e}")
            # Every 10 games, write to DB and clear dfs
            if i%10 == 0:
                try:
                    all_df = pd.concat(dfs)
                    db.write(all_df, Tables.PLAY_BY_PLAY)
                    written_games = i
                    print(f"Wrote play-by-play for {written_games} of {len(game_ids)} games to table {Tables.PLAY_BY_PLAY}")
                    dfs = []
                except Exception as e:
                    print(f"Failed to write play-by-play for {i} games: {' '.join(all_df[Columns.GAME_ID].unique())}")
                    print(f"Error: {e}")
                    # Clear dfs to avoid memory issues
                    dfs = []
        # Write any remaining DataFrames
        if dfs:
            all_df = pd.concat(dfs)
            db.write(all_df, Tables.PLAY_BY_PLAY)
            written_games += len(dfs)
            print(f"Wrote play-by-play for {written_games} of {len(game_ids)} games to table {Tables.PLAY_BY_PLAY}")
        if written_games == 0:
            print("No play-by-play data fetched.")
        return

if __name__ == "__main__":
    main()
