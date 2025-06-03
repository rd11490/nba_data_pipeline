import argparse
import pandas as pd
from api.smart import smart
from database.db_client import database_client
from database.db_constants import Tables, Columns
from utils.arg_parser import season_arg, season_type_arg, game_id_arg, delta_arg
from utils.utils import add_season_and_type, add_id, fill_nulls,extract_season_from_game_id, extract_season_type_from_game_id

"""
NOTE: This script turned out to be unnecessary. A rotations api exists that provides the players on court at the start of each period.
"""

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

def fetch_play_by_play(game_id):
    # Read play-by-play from the database, not the API
    query = f'SELECT * FROM {Tables.PLAY_BY_PLAY} WHERE "{Columns.GAME_ID}" = :game_id'
    df = database_client.read(query, params={"game_id": game_id})
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
    if not subs.empty:
        subs[Columns.SECONDS_FROM_START] = subs.apply(
            lambda row: convert_time_to_seconds(row[Columns.PERIOD], row[Columns.PCTIMESTRING]), axis=1)
        # Sort by SECONDS_FROM_START ascending, then EVENTNUM ascending
        subs = subs.sort_values([Columns.PERIOD, Columns.SECONDS_FROM_START, Columns.EVENTNUM], ascending=[True,True, True])
    else:
        # Ensure SECONDS_FROM_START column exists even if empty
        subs[Columns.SECONDS_FROM_START] = []
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

def get_starters_for_period_pbp(pbp, period):
    period_pbp = pbp[pbp[Columns.PERIOD] == period]
    subs = period_pbp[period_pbp[Columns.EVENTMSGTYPE] == 8]

    player_1 = period_pbp[[Columns.PLAYER1_ID, Columns.PLAYER1_TEAM_ID]].rename(
        columns={Columns.PLAYER1_ID: Columns.PLAYER_ID, Columns.PLAYER1_TEAM_ID: Columns.TEAM_ID}
    ).drop_duplicates()
    player_2 = period_pbp[[Columns.PLAYER2_ID, Columns.PLAYER2_TEAM_ID]].rename(
        columns={Columns.PLAYER2_ID: Columns.PLAYER_ID, Columns.PLAYER2_TEAM_ID: Columns.TEAM_ID}
    ).drop_duplicates()
    player_3 = period_pbp[[Columns.PLAYER3_ID, Columns.PLAYER3_TEAM_ID]].rename(
        columns={Columns.PLAYER3_ID: Columns.PLAYER_ID, Columns.PLAYER3_TEAM_ID: Columns.TEAM_ID}
    ).drop_duplicates()

    players = pd.concat([player_1, player_2, player_3], ignore_index=True)
    players = players[players[Columns.TEAM_ID] != 0]
    players = players.drop_duplicates()
    players_in_period = list(map(tuple, players[[Columns.PLAYER_ID, Columns.TEAM_ID]].values))

    starters = []
    for (pid, tid) in players_in_period:
        player_subs = subs[(subs[Columns.PERIOD] == period) & 
                           ((subs['PLAYER1_ID'] == pid) | (subs['PLAYER2_ID'] == pid))]
        if player_subs.empty:
            # No sub events: must be a starter
            starters.append((pid, tid))
        else:
            first_event = player_subs.iloc[0]
            if first_event['PLAYER1_ID'] == pid:
                # First event is sub OUT: started the period
                starters.append((pid, tid))
            # If first event is sub IN, not a starter
    return starters


def process_game(game_id, season, season_type):
    pbp = fetch_play_by_play(game_id)
    periods = sorted(pbp[Columns.PERIOD].unique())
    records = []
    for period in periods:
        # Filter pbp and subs for this period only
        pbp_period = pbp[pbp[Columns.PERIOD] == period].copy()
        subs = extract_subs(pbp_period)
        box = fetch_box_score(game_id, period)
        starters = get_starters_for_period(subs, box, period)
        if len(starters) == 10:
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
        else:
            print(f"Game {game_id} period {period}: found {len(starters)} starters, expected 10., Trying using PBP")
            starters = get_starters_for_period_pbp(pbp_period, period)
            if len(starters) == 10:
                for (pid, team_id) in starters:
                    records.append({
                        Columns.GAME_ID: game_id,
                        Columns.SEASON: season,
                        Columns.SEASON_TYPE: season_type,
                        Columns.PERIOD: period,
                        Columns.PLAYER_ID: pid,
                        Columns.TEAM_ID: team_id
                    })
            else:
                print(f"Game {game_id} period {period}: found {len(starters)} starters using PBP, expected 10. Skipping this period.")
                raise Exception(f"Game {game_id} period {period}: found {len(starters)} starters, expected 10. Skipping this period.")
        
    df = pd.DataFrame(records)

    return fill_nulls(df)

def filter_game_ids_delta(game_ids, season, season_type):
    """
    Given a list of game_ids, remove those already present in the output table for the given season and season_type.
    """
    q_delta = f'SELECT DISTINCT "{Columns.GAME_ID}" FROM {Tables.PLAYERS_ON_COURT_AT_START_OF_PERIOD} WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
    result_delta = database_client.read(q_delta, params={'season': season, 'stype': season_type})
    if result_delta is not None and not result_delta.empty:
        existing_game_ids = set(result_delta[Columns.GAME_ID].tolist())
        before_count = len(game_ids)
        filtered_game_ids = [gid for gid in game_ids if gid not in existing_game_ids]
        after_count = len(filtered_game_ids)
        print(f"Delta mode: {before_count - after_count} games already exist in {Tables.PLAYERS_ON_COURT_AT_START_OF_PERIOD}, {after_count} remaining to process for season {season}.")
        return filtered_game_ids
    else:
        print(f"Delta mode: No games found in {Tables.PLAYERS_ON_COURT_AT_START_OF_PERIOD} for season {season} and type {season_type}.")
        return game_ids

def get_game_ids(season, season_type):
    q = f'SELECT DISTINCT "{Columns.GAME_ID}" FROM {Tables.TEAM_GAME_LOG} WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
    result = database_client.read(q, params={'season': season, 'stype': season_type})
    game_ids = result[Columns.GAME_ID].tolist()
    return game_ids

def write_frames(dfs, games_to_process, i, season, season_type):
    all_df = pd.concat(dfs)
    all_df = add_season_and_type(all_df, season, season_type)
    all_df = add_id(all_df, [Columns.GAME_ID, Columns.PERIOD, Columns.PLAYER_ID])
    database_client.write(all_df, Tables.PLAYERS_ON_COURT_AT_START_OF_PERIOD)
    print(f"Wrote {i}/{games_to_process} games to {Tables.PLAYERS_ON_COURT_AT_START_OF_PERIOD}")

def main():
    parser = argparse.ArgumentParser(description='Determine players on court at start of each period for NBA games.')
    season_arg(parser)
    season_type_arg(parser)
    game_id_arg(parser)
    delta_arg(parser)
    args = parser.parse_args()

    # Enforce: only one of (game_id) or (season and season_type) can be provided
    has_game_id = args.game_id is not None
    has_season_and_type = args.season is not None and args.season_type is not None
    if has_game_id and has_season_and_type:
        raise Exception("You must provide either --game_id or both --season and --season_type, but not both.")
    if not has_game_id and not has_season_and_type:
        raise Exception("You must provide either --game_id or both --season and --season_type.")

    if args.game_id:
        # Determine season and season_type from game_id if not provided
        season_in = extract_season_from_game_id(args.game_id)
        season_type = extract_season_type_from_game_id(args.game_id)
        df = process_game(args.game_id, season_in, season_type)
        database_client.write(df, Tables.PLAYERS_ON_COURT_AT_START_OF_PERIOD)
        print(f"Processed game {args.game_id}")
    else:
        seasons = [s.strip() for s in args.season.split(',') if s.strip()]
        season_type = args.season_type
        for season in seasons:
            # Get all game_ids for this season/type
            game_ids = get_game_ids(season, season_type)

            # If delta, filter out game_ids already in players_on_court_at_start_of_period
            if getattr(args, 'delta', False):
                game_ids = filter_game_ids_delta(game_ids, season, season_type)

            dfs = []
            games_to_process = len(game_ids)
            for i, gid in enumerate(game_ids, 1):
                print(f"Processing game {gid}")

                try:
                    df = process_game(gid, season, args.season_type)
                    dfs.append(df)
                    print(f"Processed game {gid}")
                except Exception as e:
                    print(f"Failed for game {gid}: {e}")
                # Write to DB every 10 games
                if i%10 == 0:
                    
                    write_frames(dfs, games_to_process, i, season, season_type)
                    dfs = []
            # Write any remaining games
            if dfs:
                write_frames(dfs, games_to_process, i, season, season_type)
    database_client.close()

if __name__ == '__main__':
    main()
