from api import smart, NBATeams
from database import creds
from database.db_init import create_database_if_not_exists


create_database_if_not_exists('nba', creds.user, creds.password)