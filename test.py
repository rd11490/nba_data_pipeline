from api import smart, NBATeams
from database import creds
from database.db_init import create_database_if_not_exists
import pandas as pd


pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 500)

dfs = smart.game_rotation('0022400236')
away = dfs['AwayTeam']
home = dfs['HomeTeam']
df = pd.concat([away, home], ignore_index=True)
df.to_csv('game_rotation.csv', index=False)