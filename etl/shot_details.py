import argparse
import pandas as pd
import json
from api.smart import smart
from database.db_client import database_client
from database.db_constants import Tables, Columns
from utils.arg_parser import season_arg, season_type_arg, player_id_arg, delta_arg
from utils.utils import add_id, fill_nulls

def fetch_player_shot_chart(player_id, team_id, season, season_type):
    # Fetch shot chart data for a player/season/team
    df = smart.get_shot_chart_detail(player_id=player_id, team_id=team_id, season=season, season_type=season_type)
    if df is None or df.empty:
        return None
    df[Columns.PLAYER_ID] = player_id
    df[Columns.TEAM_ID] = team_id
    df[Columns.SEASON] = season
    df[Columns.SEASON_TYPE] = season_type
    # Add id column (PLAYER_ID, GAME_ID, GAME_EVENT_ID)
    if Columns.GAME_ID in df.columns and 'GAME_EVENT_ID' in df.columns:
        df = add_id(df, [Columns.PLAYER_ID, Columns.GAME_ID, 'GAME_EVENT_ID'])
    else:
        df = add_id(df, [Columns.PLAYER_ID, Columns.TEAM_ID, Columns.SEASON, Columns.SEASON_TYPE])
    df = fill_nulls(df)
    df = df.drop_duplicates()
    return df

def get_player_team_combos(season, season_type, player_id=None):
    # Query unique PLAYER_ID, TEAM_ID, SEASON, SEASON_TYPE from rotations table
    q = f'SELECT DISTINCT "{Columns.PLAYER_ID}", "{Columns.TEAM_ID}", "{Columns.SEASON}", "{Columns.SEASON_TYPE}" FROM {Tables.ROTATIONS} WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
    params = {'season': season, 'stype': season_type}
    if player_id:
        q += f' AND "{Columns.PLAYER_ID}" = :player_id'
        params['player_id'] = player_id
    result = database_client.read(q, params=params)
    if result is None or result.empty:
        return []
    return result[[Columns.PLAYER_ID, Columns.TEAM_ID, Columns.SEASON, Columns.SEASON_TYPE]].drop_duplicates().to_dict('records')

def filter_combos_delta(season, season_type, combos):
    # Remove combos already present in shot_details table
    if not combos:
        return []
    q = f'SELECT DISTINCT "{Columns.PLAYER_ID}", "{Columns.TEAM_ID}", "{Columns.SEASON}", "{Columns.SEASON_TYPE}" FROM shot_details WHERE "{Columns.SEASON}" = :season AND "{Columns.SEASON_TYPE}" = :stype'
    result = database_client.read(q, params={'season': season, 'stype': season_type})
    if result is None or result.empty:
        return combos
    existing = set(tuple(x) for x in result[[Columns.PLAYER_ID, Columns.TEAM_ID, Columns.SEASON, Columns.SEASON_TYPE]].values.tolist())
    filtered = [c for c in combos if (c[Columns.PLAYER_ID], c[Columns.TEAM_ID], c[Columns.SEASON], c[Columns.SEASON_TYPE]) not in existing]
    print(f"Delta mode: {len(combos) - len(filtered)} combos already exist in shot_details, {len(filtered)} remaining to process for season {season}.")
    return filtered

def write_frames(dfs, db, total, i):
    all_df = pd.concat(dfs)
    db.write(all_df, 'shot_details')
    print(f"Wrote {i}/{total} player-team combos to shot_details")

def main():
    parser = argparse.ArgumentParser(description='Pull NBA shot chart details for given players/seasons and season type.')
    season_arg(parser)
    season_type_arg(parser)
    player_id_arg(parser)
    delta_arg(parser)
    args = parser.parse_args()

    if not args.season or not args.season_type:
        raise Exception("You must provide both --season and --season_type.")

    seasons = [s.strip() for s in args.season.split(',') if s.strip()]
    for season in seasons:
        combos = get_player_team_combos(season, args.season_type, getattr(args, 'player_id', None))
        if getattr(args, 'delta', False):
            combos = filter_combos_delta(season, args.season_type, combos)
        dfs = []
        total = len(combos)
        for i, combo in enumerate(combos, 1):
            try:
                df = fetch_player_shot_chart(combo[Columns.PLAYER_ID], combo[Columns.TEAM_ID], combo[Columns.SEASON], combo[Columns.SEASON_TYPE])
                if df is not None and not df.empty:
                    dfs.append(df)
                print(f"Processed player {combo[Columns.PLAYER_ID]} team {combo[Columns.TEAM_ID]} season {combo[Columns.SEASON]}")
            except Exception as e:
                print(f"Failed for player {combo[Columns.PLAYER_ID]} team {combo[Columns.TEAM_ID]}: {e}")
            if i % 10 == 0 and dfs:
                write_frames(dfs, database_client, total, i)
                dfs = []
        if dfs:
            write_frames(dfs, database_client, total, i)
    database_client.close()

if __name__ == '__main__':
    main()
