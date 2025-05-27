# Team Game Log ETL

Script: `etl/team_game_log.py`

Pulls NBA team game logs for one or more seasons and a season type, and writes them to the database.

## Arguments

| Argument         | Short | Required | Description                                 | Example Value         |
|------------------|-------|----------|---------------------------------------------|----------------------|
| --season         | -s    | Yes      | Comma-separated list of NBA seasons         | 2010-11,2024-25      |
| --season_type    | -st   | Yes      | NBA season type                            | Regular Season       |

## Example Usage

```sh
./.venv/bin/python -m etl.team_game_log --season 2010-11,2024-25 --season_type "Regular Season"
```

---

# Play By Play ETL

Script: `etl/play_by_play.py`

Pulls NBA play-by-play data for a single game or for all games in one or more seasons and a season type, and writes them to the database. Supports delta mode to only fetch missing games.

## Arguments

| Argument         | Short | Required | Description                                                      | Example Value         |
|------------------|-------|----------|------------------------------------------------------------------|----------------------|
| --season         | -s    | Yes*     | Comma-separated list of NBA seasons (required if no --game_id)   | 2010-11,2024-25      |
| --season_type    | -st   | Yes*     | NBA season type (required if no --game_id)                       | Regular Season       |
| --game_id        | -g    | Yes*     | NBA Game ID (required if no --season/--season_type)              | 0022400061           |
| --delta          | -d    | No       | Only fetch games not already in the DB (idempotent/incremental)  | (flag, no value)     |

*You must provide either --game_id or both --season and --season_type, but not both at the same time.

## Example Usage

Fetch all play-by-play for 2010-11 through 2024-25 regular seasons:

```sh
./.venv/bin/python -m etl.play_by_play --season 2010-11,2011-12,2012-13,2013-14,2014-15,2015-16,2016-17,2017-18,2018-19,2019-20,2020-21,2021-22,2022-23,2023-24,2024-25 --season_type "Regular Season"
```

Fetch play-by-play for a single game:

```sh
./.venv/bin/python -m etl.play_by_play --game_id 0022400061
```

Fetch only missing play-by-play for 2023-24 and 2024-25 regular seasons (delta mode):

```sh
./.venv/bin/python -m etl.play_by_play --season 2023-24,2024-25 --season_type "Regular Season" --delta
```


