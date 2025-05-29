from api import smart, NBATeams
from database import creds
from database.db_init import create_database_if_not_exists
import pandas as pd


pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 500)

dfs = smart.get_shot_chart_detail(player_id='201143', team_id=NBATeams.BostonCeltics, season='2024-25', season_type='Regular Season')
print(dfs.head())