import argparse
import pandas as pd
from api.smart import smart
from database.db_client import PostgresClient
from database.db_constants import Tables, Columns
from database.creds import creds
from utils.arg_parser import season_arg, season_type_arg, game_id_arg, delta_arg
from utils.utils import add_season_and_type, add_id, fill_nulls,extract_season_from_game_id, extract_season_type_from_game_id

# --- Helper Functions ---
def convert_time_to_seconds(period, time_str):
    # Converts 'MM:SS' to seconds from game start for a given period
    minutes, seconds = map(int, time_str.split(':'))
    if period <= 4:
        return (period - 1) * 12 * 60 + (12 * 60 - (minutes * 60 + seconds))
    else:
        return 4 * 12 * 60 + (period - 5) * 5 * 60 + (5 * 60 - (minutes * 60 + seconds))

def get_period_time_bounds(period):
    # Returns (start_time, end_time) in tenths of a second for a given period
    if period <= 4:
        # Each regulation period is 12 minutes
        period_start = (period - 1) * 12 * 60
        period_end = period_start + 12 * 60
    else:
        # Overtime periods are 5 minutes
        period_start = 4 * 12 * 60 + (period - 5) * 5 * 60
        period_end = period_start + 5 * 60 
    return period_start * 10, period_end * 10  # Convert to tenths of seconds

def fetch_play_by_play(game_id, db=None):
    # Read play-by-play from the database, not the API
    if db is None:
        raise ValueError("Database client must be provided to fetch play-by-play from DB.")
    query = f'SELECT * FROM {Tables.PLAY_BY_PLAY} WHERE "{Columns.GAME_ID}" = :game_id'
    df = db.read(query, params={"game_id": game_id})
    if df is None or df.empty:
        raise Exception(f"No play-by-play data found in DB for game_id {game_id}")
    return fill_nulls(df)

def fetch_box_score(game_id, period):
    # Returns player box score for a specific period, using correct time bounds
    start_time, end_time = get_period_time_bounds(int(period))
    box = smart.box_score_traditional(
        game_id,
        start_period=int(period),
        end_period=int(period),
        start_range=start_time+5,
        end_range=end_time-5,
        range_type=2
    )
    if 'PlayerStats' in box:
        return fill_nulls(box['PlayerStats'])
    elif 'PlayerStats' in box.get('resultSets', {}):
        return fill_nulls(box['resultSets']['PlayerStats'])
    else:
        raise Exception('Box score missing PlayerStats')

def extract_subs(pbp):
    subs = pbp[pbp[Columns.EVENTMSGTYPE] == 8].copy()
    subs[Columns.PERIOD] = subs[Columns.PERIOD].astype(int)
    subs[Columns.SECONDS_FROM_START] = subs.apply(
        lambda row: convert_time_to_seconds(row[Columns.PERIOD], row[Columns.PCTIMESTRING]), axis=1)
    # Sort by SECONDS_FROM_START ascending, then EVENTNUM ascending
    subs = subs.sort_values([Columns.SECONDS_FROM_START, Columns.EVENTNUM], ascending=[True, True])
    return subs

def get_starters_for_period(subs, box, period):
    players_in_period = box[box['MIN'].notnull()][Columns.PLAYER_ID].unique()
    starters = []
    for pid in players_in_period:
        player_subs = subs[(subs[Columns.PERIOD] == period) & 
                           ((subs['PLAYER1_ID'] == pid) | (subs['PLAYER2_ID'] == pid))]
        if player_subs.empty:
            # No sub events: must be a starter
            starters.append(pid)
        else:
            first_event = player_subs.iloc[0]
            if first_event['PLAYER1_ID'] == pid:
                # First event is sub OUT: started the period
                starters.append(pid)
            # If first event is sub IN, not a starter
    return starters

def process_game(game_id, season, season_type, db):
    pbp = fetch_play_by_play(game_id, db=db)
    periods = sorted(pbp[Columns.PERIOD].unique())
    records = []
    for period in periods:
        # Filter pbp and subs for this period only
        pbp_period = pbp[pbp[Columns.PERIOD] == period]
        subs = extract_subs(pbp_period)
        box = fetch_box_score(game_id, period)
        starters = get_starters_for_period(subs, box, period)
        print(starters)
        if len(starters) != 10:
            print(f"Game {game_id} period {period}: found {len(starters)} starters, expected 10.")
            raise Exception(f"Game {game_id} period {period}: found {len(starters)} starters, expected 10.")
        for pid in starters:
            team_id = box[box[Columns.PLAYER_ID] == pid][Columns.TEAM_ID].iloc[0]
            records.append({
                Columns.GAME_ID: game_id,
                Columns.SEASON: season,
                Columns.SEASON_TYPE: season_type,
                Columns.PERIOD: period,
                Columns.PLAYER_ID: pid,
                Columns.TEAM_ID: team_id
            })
    df = pd.DataFrame(records)
    df = add_season_and_type(df, season, season_type)
    df = add_id(df, [Columns.GAME_ID, Columns.PERIOD, Columns.PLAYER_ID])
    return fill_nulls(df)

def main():
    parser = argparse.ArgumentParser(description='Determine players on court at start of each period for NBA games.')
    season_arg(parser)
    season_type_arg(parser)
    game_id_arg(parser)
    delta_arg(parser)
    args = parser.parse_args()

    db = PostgresClient(
        dbname=creds.dbname,
        user=creds.user,
        password=creds.password,
        host=creds.host,
        port=creds.port
    )

    if args.game_id:
        # Determine season and season_type from game_id if not provided
        season = args.season or extract_season_from_game_id(args.game_id)
        season_type = args.season_type or extract_season_type_from_game_id(args.game_id)
        df = process_game(args.game_id, season, season_type, db)
        db.write(df, Tables.PLAYERS_ON_COURT_AT_START_OF_PERIOD)
        print(f"Processed game {args.game_id}")
    else:
        seasons = [s.strip() for s in args.season.split(',') if s.strip()]
        for season in seasons:
            # Get all game_ids for this season/type
            q = f'SELECT DISTINCT "{Columns.GAME_ID}" FROM {Tables.TEAM_GAME_LOG} WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
            result = db.read(q, params={'season': season, 'stype': args.season_type})
            game_ids = result[Columns.GAME_ID].tolist()
            for gid in game_ids:
                try:
                    df = process_game(gid, season, args.season_type, db)
                    db.write(df, Tables.PLAYERS_ON_COURT_AT_START_OF_PERIOD)
                    print(f"Processed game {gid}")
                except Exception as e:
                    print(f"Failed for game {gid}: {e}")
    db.close()

if __name__ == '__main__':
    main()
