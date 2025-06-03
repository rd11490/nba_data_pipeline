import argparse
import pandas as pd
import json
from api.smart import smart
from database.db_client import database_client
from database.db_constants import Tables, Columns
from utils.arg_parser import season_arg, season_type_arg, game_id_arg, delta_arg
from utils.utils import add_id, fill_nulls, convert_time_to_seconds, check_duplicate_ids
import concurrent.futures

def fetch_rotations(game_id):
    q = f'SELECT * FROM {Tables.ROTATIONS} WHERE "{Columns.GAME_ID}" = :game_id'
    df = database_client.read(q, params={'game_id': game_id})
    if df is None or df.empty:
        raise Exception(f"No rotations found for game_id {game_id}")
    df[Columns.STINTS] = df[Columns.STINTS].apply(json.loads)
    return df

def fetch_play_by_play(game_id):
    q = f'SELECT * FROM {Tables.PLAY_BY_PLAY} WHERE "{Columns.GAME_ID}" = :game_id'
    df = database_client.read(q, params={'game_id': game_id})
    if df is None or df.empty:
        raise Exception(f"No play_by_play found for game_id {game_id}")
    return df

def get_players_at_start_of_period(team_id, period, game_id):
    """
    Returns a sorted list of PLAYER_IDs for the given game_id, period, and team_id from the players_on_court_at_start_of_period table.
    If game_id is not provided, attempts to infer from team_rot if possible (for compatibility).
    """
    # If game_id is not provided, try to infer from team_rot
    q = f'SELECT "{Columns.PLAYER_ID}" FROM players_on_court_at_start_of_period WHERE "{Columns.GAME_ID}" = :game_id AND "{Columns.PERIOD}" = :period AND "{Columns.TEAM_ID}" = :team_id'
    df = database_client.read(q, params={'game_id': game_id, 'period': period, 'team_id': team_id})
    if df is None or df.empty:
        return None
    players = df[Columns.PLAYER_ID].tolist()
    players.sort()
    return players


def get_team_game_log(game_id):
    """
    Fetches the team_game_log DataFrame for a given game_id, returns DataFrame with TEAM_ID and MATCHUP columns.
    """
    q = f'SELECT "{Columns.TEAM_ID}", "MATCHUP" FROM {Tables.TEAM_GAME_LOG} WHERE "{Columns.GAME_ID}" = :game_id'
    df = database_client.read(q, params={'game_id': game_id})
    return df

def get_team_ids_from_pbp(pbp):
    jump = pbp[(pbp[Columns.EVENTMSGTYPE] == 10) & (pbp[Columns.EVENTMSGACTIONTYPE] == 0)]
    if not jump.empty:
        jump = jump.iloc[0]
        team1 = jump[Columns.PLAYER1_TEAM_ID]
        team2 = jump[Columns.PLAYER2_TEAM_ID]
        return team1, team2
    return None, None

def get_team_ids_from_game_log(game_id):
    df = get_team_game_log(game_id)
    if df is None or df.empty or len(df) != 2:
        raise Exception(f"Could not determine teams for game_id {game_id} from team_game_log")
    team1_row = df[df['MATCHUP'].str.contains('vs')]
    team2_row = df[df['MATCHUP'].str.contains('@')]
    if team1_row.empty or team2_row.empty:
        raise Exception(f"Could not parse MATCHUP for game_id {game_id}: {df['MATCHUP'].tolist()}")
    team1 = team1_row.iloc[0][Columns.TEAM_ID]
    team2 = team2_row.iloc[0][Columns.TEAM_ID]
    return team1, team2

def get_team_ids(game_id, pbp):
    """
    Returns (team1, team2) for the game. Tries jump ball first, falls back to team_game_log if needed.
    team1: home team (MATCHUP contains 'vs'), team2: away team (MATCHUP contains '@').
    """
    team1, team2 = get_team_ids_from_pbp(pbp)
    if team1 is None or team2 is None:
        print(f"Jump ball not found in play-by-play for game_id {game_id}, falling back to team_game_log")
        team1, team2 = get_team_ids_from_game_log(game_id)
    return team1, team2    

def get_initial_players(rot_df, team_id):
    team_rot = rot_df[rot_df[Columns.TEAM_ID] == team_id]
    starters = team_rot[team_rot[Columns.STINTS].apply(lambda stints: any(s['IN_TIME_REAL'] == 0 for s in stints))]
    players = starters[Columns.PLAYER_ID].tolist()
    players.sort()
    return players

def update_players_for_sub(team_players, row):
    out_id = getattr(row, Columns.PLAYER1_ID)
    in_id = getattr(row, Columns.PLAYER2_ID)

    if out_id in team_players:
        idx = team_players.index(out_id)
        team_players[idx] = in_id
    else:
        raise Exception(f"ROW: {getattr(row, Columns.EVENTNUM)} Player {out_id} not found in current team players: {team_players}")
    team_players.sort()
    return team_players

def update_players_for_stint_change(team_players, team_rot, seconds_from_start):
    # Remove players whose OUT_TIME_REAL == seconds_from_start*10, add those whose IN_TIME_REAL == seconds_from_start*10
    out_players = team_rot[team_rot[Columns.STINTS].apply(lambda stints: any(s['OUT_TIME_REAL'] == seconds_from_start*10 for s in stints))][Columns.PLAYER_ID].tolist()
    in_players = team_rot[team_rot[Columns.STINTS].apply(lambda stints: any(s['IN_TIME_REAL'] == seconds_from_start*10 for s in stints))][Columns.PLAYER_ID].tolist()
    for pid in out_players:
        if pid in team_players:
            team_players.remove(pid)
    for pid in in_players:
        if pid not in team_players:
            team_players.append(pid)
    team_players.sort()
    return team_players

def process_game(game_id):
    rot_df = fetch_rotations(game_id)
    pbp = fetch_play_by_play(game_id)
    pbp[Columns.SECONDS_FROM_START] = pbp.apply(lambda row: convert_time_to_seconds(row[Columns.PERIOD], row[Columns.PCTIMESTRING]), axis=1)
    # Sort by SECONDS_FROM_START asc, then EVENTNUM asc
    pbp = pbp.sort_values([Columns.PERIOD, Columns.SECONDS_FROM_START, Columns.EVENTNUM], ascending=[True, True, True]).reset_index(drop=True)
    team1, team2 = get_team_ids(game_id, pbp)
    team1_rot = rot_df[rot_df[Columns.TEAM_ID] == team1].copy()
    team2_rot = rot_df[rot_df[Columns.TEAM_ID] == team2].copy()
    team1_rot[Columns.STINTS] = team1_rot[Columns.STINTS].apply(lambda s: s if isinstance(s, list) else json.loads(s))
    team2_rot[Columns.STINTS] = team2_rot[Columns.STINTS].apply(lambda s: s if isinstance(s, list) else json.loads(s))
    team1_players = get_initial_players(team1_rot, team1)
    team2_players = get_initial_players(team2_rot, team2)
    pbp = pbp.reset_index(drop=True)
    team1_players_current = team1_players.copy()
    team2_players_current = team2_players.copy()
    # Always use 5 player columns for each team
    team1_cols = [f'{Columns.TEAM1_PLAYER}{i+1}' for i in range(5)]
    team2_cols = [f'{Columns.TEAM2_PLAYER}{i+1}' for i in range(5)]
    # Use dictionary of arrays for efficient column assignment
    player_cols = {col: [] for col in team1_cols + team2_cols}
    for row in pbp.itertuples(index=False):
        # Substitution
        if getattr(row, Columns.EVENTMSGTYPE) == 8:
            if getattr(row, Columns.PLAYER1_TEAM_ID) == team1:
                team1_players_current = update_players_for_sub(team1_players_current, row)
            elif getattr(row, Columns.PLAYER1_TEAM_ID) == team2:
                team2_players_current = update_players_for_sub(team2_players_current, row)
        # Stint change (EVENTMSGTYPE == 12)
        if getattr(row, Columns.EVENTMSGTYPE) == 12:
            seconds_from_start = getattr(row, Columns.SECONDS_FROM_START)
            team1_players_new = update_players_for_stint_change(team1_players_current, team1_rot, seconds_from_start)
            team2_players_new = update_players_for_stint_change(team2_players_current, team2_rot, seconds_from_start)
            if len(team1_players_new) != 5:
                team1_players_new = get_players_at_start_of_period(team_id=str(int(team1)), period=getattr(row, Columns.PERIOD), game_id=game_id)
            if len(team2_players_new) != 5:
                team2_players_new = get_players_at_start_of_period(team_id=str(int(team2)), period=getattr(row, Columns.PERIOD), game_id=game_id)
            
            team2_players_current = team2_players_new
            team1_players_current = team1_players_new

        # Append player columns for this row (always 5, pad with None if fewer)
        for i in range(5):
            player_cols[team1_cols[i]].append(team1_players_current[i])
            player_cols[team2_cols[i]].append(team2_players_current[i])
    # Assign all player columns at once
    for col, arr in player_cols.items():
        pbp[col] = arr
    pbp = fill_nulls(pbp)
    pbp = add_id(pbp, [Columns.GAME_ID, Columns.EVENTNUM])
    check_duplicate_ids(pbp, id_col=Columns.ID)
    print(f"Processed game {game_id}")
    return pbp

def filter_game_ids_delta(game_ids, season, season_type):
    q = f'SELECT DISTINCT "{Columns.GAME_ID}" FROM play_by_play_with_players WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
    result = database_client.read(q, params={'season': season, 'stype': season_type})
    if result is not None and not result.empty:
        existing_game_ids = set(result[Columns.GAME_ID].tolist())
        filtered_game_ids = [gid for gid in game_ids if gid not in existing_game_ids]
        print(f"Delta mode: {len(game_ids) - len(filtered_game_ids)} games already exist, {len(filtered_game_ids)} remaining to process for season {season}.")
        return filtered_game_ids
    else:
        return game_ids

def get_game_ids(season, season_type):
    q = f'SELECT DISTINCT "{Columns.GAME_ID}" FROM {Tables.TEAM_GAME_LOG} WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
    result = database_client.read(q, params={'season': season, 'stype': season_type})
    if result is None or result.empty:
        return []
    return result[Columns.GAME_ID].tolist()

def write_frames(dfs, db, games_to_process, i):
    all_df = pd.concat(dfs)
    db.write(all_df, 'play_by_play_with_players')
    print(f"Wrote {i}/{games_to_process} games to play_by_play_with_players")

def main():
    parser = argparse.ArgumentParser(description='Pull NBA play-by-play with player columns for given seasons and season type.')
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
        pbp = process_game(args.game_id)
        database_client.write(pbp, 'play_by_play_with_players')
        print(f"Processed game {args.game_id}")
    else:
        seasons = [s.strip() for s in args.season.split(',') if s.strip()]
        
        for season in seasons:
            game_ids = get_game_ids(season, args.season_type)
            if getattr(args, 'delta', False):
                game_ids = filter_game_ids_delta(game_ids, season, args.season_type)
            dfs = []
            games_to_process = len(game_ids)
            batch_size = 25
            def process_and_collect(gid):
                try:
                    pbp = process_game(gid)
                    return pbp
                except Exception as e:
                    print(f"Failed for game {gid}: {e}")
                    return None

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_gid = {executor.submit(process_and_collect, gid): gid for gid in game_ids}
                i = 0
                for future in concurrent.futures.as_completed(future_to_gid):
                    result = future.result()
                    i += 1
                    if result is not None:
                        dfs.append(result)
                    if i % batch_size == 0 and dfs:
                        write_frames(dfs, database_client, games_to_process, i)
                        dfs = []
                if dfs:
                    write_frames(dfs, database_client, games_to_process, i)
    database_client.close()

if __name__ == '__main__':
    main()
