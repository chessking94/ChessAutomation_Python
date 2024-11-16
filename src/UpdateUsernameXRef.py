# import argparse
import datetime as dt
import json
import logging
import os
import requests

import pandas as pd
import sqlalchemy as sa

# TODO: Convert messy variables for update query to dictionary and .get methods


def ChessComUserUpdate(username):
    conn_str = os.getenv('ConnectionStringOdbcRelease')
    connection_url = sa.engine.URL.create(
        drivername='mssql+pyodbc',
        query={"odbc_connect": conn_str}
    )
    engine = sa.create_engine(connection_url)
    conn = engine.connect().connection

    qry_text = "SELECT PlayerID, Username FROM ChessWarehouse.dbo.UsernameXRef WHERE Source = 'Chess.com' AND UserStatus NOT IN ('DNE')"
    if username:
        qry_text = qry_text + f" AND Username = '{username}'"
    logging.debug(qry_text)
    users = pd.read_sql(qry_text, engine).values.tolist()

    rec_ct = len(users)
    if rec_ct == 0:
        logging.warning('No users found!')
    else:
        csr = conn.cursor()
        ctr = 1
        for i in users:
            logging.debug(f'Chess.com {ctr}/{rec_ct}: {i[1]}')
            sql_cmd = ''
            url = f'https://api.chess.com/pub/player/{i[1]}'
            headers = eval(os.getenv('CDCUserAgent'))

            with requests.get(url, headers=headers) as resp:
                if resp.status_code != 200:
                    logging.warning(f'Unable to complete request to {url}! Request returned code {resp.status_code}')
                    if resp.status_code == 404:
                        sql_cmd = f"UPDATE ChessWarehouse.dbo.UsernameXRef SET UserStatus = 'DNE' WHERE PlayerID = {i[0]}"
                    else:
                        # could handle these differently, but should happen very rarely if ever
                        sql_cmd = f"UPDATE ChessWarehouse.dbo.UsernameXRef SET UserStatus = NULL WHERE PlayerID = {i[0]}"
                else:
                    # last active datetime and status
                    json_data = resp.content
                    json_loaded = json.loads(json_data)
                    last_online = json_loaded['last_online']
                    sql_date = str(dt.datetime.fromtimestamp(last_online))
                    if json_loaded['status'][0:6] == 'closed':
                        user_status = 'Closed'
                    else:
                        user_status = 'Open'

                    # ratings and game counts
                    url_ratings = f'https://api.chess.com/pub/player/{i[1]}/stats'
                    with requests.get(url_ratings, headers=headers) as resp2:
                        if resp2.status_code != 200:
                            logging.warning(f'Unable to complete request to {url_ratings}! Request returned code {resp2.status_code}')
                        else:
                            json_rating_data = resp2.content
                            json_rating_loaded = json.loads(json_rating_data)

                            bullet_rating = 'NULL'
                            blitz_rating = 'NULL'
                            rapid_rating = 'NULL'
                            daily_rating = 'NULL'
                            bullet_games = '0'
                            blitz_games = '0'
                            rapid_games = '0'
                            daily_games = '0'
                            sum_keys = ['win', 'loss', 'draw']
                            if 'chess_bullet' in json_rating_loaded:
                                bullet_rating = json_rating_loaded['chess_bullet']['last']['rating']
                                bullet_games = sum(v for k, v in json_rating_loaded['chess_bullet']['record'].items() if k in sum_keys)
                            if 'chess_blitz' in json_rating_loaded:
                                blitz_rating = json_rating_loaded['chess_blitz']['last']['rating']
                                blitz_games = sum(v for k, v in json_rating_loaded['chess_blitz']['record'].items() if k in sum_keys)
                            if 'chess_rapid' in json_rating_loaded:
                                rapid_rating = json_rating_loaded['chess_rapid']['last']['rating']
                                rapid_games = sum(v for k, v in json_rating_loaded['chess_rapid']['record'].items() if k in sum_keys)
                            if 'chess_daily' in json_rating_loaded:
                                daily_rating = json_rating_loaded['chess_daily']['last']['rating']
                                daily_games = sum(v for k, v in json_rating_loaded['chess_daily']['record'].items() if k in sum_keys)

                            # set SQL command
                            sql_cmd = f"UPDATE ChessWarehouse.dbo.UsernameXRef SET LastActiveOnline = '{sql_date}'"
                            sql_cmd = sql_cmd + f", UserStatus = '{user_status}'"
                            sql_cmd = sql_cmd + f', BulletRating = {bullet_rating}'
                            sql_cmd = sql_cmd + f', BlitzRating = {blitz_rating}'
                            sql_cmd = sql_cmd + f', RapidRating = {rapid_rating}'
                            sql_cmd = sql_cmd + f', DailyRating = {daily_rating}'
                            sql_cmd = sql_cmd + f', BulletGames = {bullet_games}'
                            sql_cmd = sql_cmd + f', BlitzGames = {blitz_games}'
                            sql_cmd = sql_cmd + f', RapidGames = {rapid_games}'
                            sql_cmd = sql_cmd + f', DailyGames = {daily_games}'
                            sql_cmd = sql_cmd + f' WHERE PlayerID = {i[0]}'

            if sql_cmd != '':
                logging.debug(sql_cmd)
                csr.execute(sql_cmd)
                conn.commit()

            ctr = ctr + 1

    conn.close()
    engine.dispose()


def LichessUserUpdate(username):
    conn_str = os.getenv('ConnectionStringOdbcRelease')
    connection_url = sa.engine.URL.create(
        drivername='mssql+pyodbc',
        query={"odbc_connect": conn_str}
    )
    engine = sa.create_engine(connection_url)
    conn = engine.connect().connection

    qry_text = "SELECT PlayerID, Username FROM ChessWarehouse.dbo.UsernameXRef WHERE Source = 'Lichess' AND UserStatus NOT IN ('DNE')"
    if username:
        qry_text = qry_text + f" AND Username = '{username}'"
    logging.debug(qry_text)
    users = pd.read_sql(qry_text, engine).values.tolist()

    rec_ct = len(users)
    if rec_ct == 0:
        logging.warning('No users found!')
    else:
        csr = conn.cursor()
        ctr = 1
        for i in users:
            logging.debug(f'Lichess {ctr}/{rec_ct}: {i[1]}')
            sql_cmd = ''
            url = f'https://lichess.org/api/user/{i[1]}'
            headers = {'Authorization': f'Bearer {os.getenv("LichessAPIToken")}'}

            with requests.get(url, headers=headers) as resp:
                if resp.status_code != 200:
                    logging.warning(f'Unable to complete request to {url}! Request returned code {resp.status_code}')
                    if resp.status_code == 404:
                        sql_cmd = f"UPDATE ChessWarehouse.dbo.UsernameXRef SET UserStatus = 'DNE' WHERE PlayerID = {i[0]}"
                    else:
                        # could handle these differently, but should happen very rarely if ever
                        sql_cmd = f"UPDATE ChessWarehouse.dbo.UsernameXRef SET UserStatus = NULL WHERE PlayerID = {i[0]}"
                else:
                    json_data = resp.content
                    json_loaded = json.loads(json_data)
                    if json_loaded.get('disabled'):
                        sql_cmd = f"UPDATE ChessWarehouse.dbo.UsernameXRef SET UserStatus = 'Closed' WHERE PlayerID = {i[0]}"
                    else:
                        # last active datetime
                        last_online = json_loaded.get('seenAt')//1000
                        sql_date = str(dt.datetime.fromtimestamp(last_online))

                        # ratings and game counts
                        bullet_rating = 'NULL'
                        blitz_rating = 'NULL'
                        rapid_rating = 'NULL'
                        daily_rating = 'NULL'
                        bullet_games = '0'
                        blitz_games = '0'
                        rapid_games = '0'
                        daily_games = '0'
                        if json_loaded['perfs'].get('bullet'):
                            bullet_rating = json_loaded['perfs']['bullet'].get('rating')
                            bullet_games = json_loaded['perfs']['bullet'].get('games')
                        if json_loaded['perfs'].get('blitz'):
                            blitz_rating = json_loaded['perfs']['blitz'].get('rating')
                            blitz_games = json_loaded['perfs']['blitz'].get('games')
                        if json_loaded['perfs'].get('rapid'):
                            rapid_rating = json_loaded['perfs']['rapid'].get('rating')
                            rapid_games = json_loaded['perfs']['rapid'].get('games')
                        if json_loaded['perfs'].get('correspondence'):
                            daily_rating = json_loaded['perfs']['correspondence'].get('rating')
                            daily_games = json_loaded['perfs']['correspondence'].get('games')

                        # set SQL command
                        sql_cmd = f"UPDATE ChessWarehouse.dbo.UsernameXRef SET LastActiveOnline = '{sql_date}'"
                        sql_cmd = sql_cmd + ", UserStatus = 'Open'"
                        sql_cmd = sql_cmd + f', BulletRating = {bullet_rating}'
                        sql_cmd = sql_cmd + f', BlitzRating = {blitz_rating}'
                        sql_cmd = sql_cmd + f', RapidRating = {rapid_rating}'
                        sql_cmd = sql_cmd + f', DailyRating = {daily_rating}'
                        sql_cmd = sql_cmd + f', BulletGames = {bullet_games}'
                        sql_cmd = sql_cmd + f', BlitzGames = {blitz_games}'
                        sql_cmd = sql_cmd + f', RapidGames = {rapid_games}'
                        sql_cmd = sql_cmd + f', DailyGames = {daily_games}'
                        sql_cmd = sql_cmd + f' WHERE PlayerID = {i[0]}'

            if sql_cmd != '':
                logging.debug(sql_cmd)
                csr.execute(sql_cmd)
                conn.commit()

            ctr = ctr + 1

    conn.close()
    engine.dispose()


def main():
    # vrs_num = '3.0'
    # parser = argparse.ArgumentParser(
    #     description='Chess.com and Lichess User Updater',
    #     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    #     usage=argparse.SUPPRESS
    # )
    # parser.add_argument(
    #     '-v', '--version',
    #     action='version',
    #     version='%(prog)s ' + vrs_num
    # )
    # parser.add_argument(
    #     '-u', '--username',
    #     help='Username'
    # )
    # parser.add_argument(
    #     '-s', '--site',
    #     default=None,
    #     nargs='?',
    #     choices=['Chess.com', 'Lichess'],
    #     help='Website of user'
    # )
    # args = parser.parse_args()
    # config = vars(args)

    # username = config['username']
    # site = config['site']

    username = None
    site = None

    if username and not site:
        logging.critical(f'Username {username} provided but no site provided!')
        raise SystemExit

    if site in ['Chess.com', None]:
        ChessComUserUpdate(username)
    if site in ['Lichess', None]:
        LichessUserUpdate(username)


if __name__ == '__main__':
    main()
