import argparse
from api.smart import smart
from database.db_client import database_client
from utils.utils import add_id, add_season_and_type, fill_nulls
from utils.arg_parser import season_arg, season_type_arg

from database.db_constants import Tables, Columns

def parse_args():
    parser = argparse.ArgumentParser(description='Pull NBA team game logs for given seasons and season type.')
    season_arg(parser)
    season_type_arg(parser)
    return parser.parse_args()

def main():
    args = parse_args()
    seasons = [s.strip() for s in args.season.split(',') if s.strip()]
    season_type = args.season_type

    for season in seasons:
        print(f"Processing season {season} ({season_type})...")
        df = smart.get_teams_game_log(season_type=season_type, season=season)
        if df is None or df.empty:
            print(f"No data for {season} {season_type}")
            continue
        # Add season and season_type columns
        df = add_season_and_type(df, season, season_type)
        df = add_id(df, [Columns.GAME_ID, Columns.TEAM_ID])
        # Fill NaN/nulls
        df = fill_nulls(df)
        # Write to DB
        print(df)
        database_client.write(df, Tables.TEAM_GAME_LOG)
        print(f"Written {len(df)} rows for {season} {season_type}.")
    database_client.close()

if __name__ == '__main__':
    main()
