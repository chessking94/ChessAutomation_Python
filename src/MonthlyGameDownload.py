import datetime as dt
import fileinput
import logging
import os
import shutil
import subprocess

import chess
import chess.pgn
import pandas as pd
import requests
import sqlalchemy as sa

from Utilities_Python import notifications

from base import base


class MonthlyGameDownload(base):
    abbreviation = 'GAMES'

    def require_pgnextract(self) -> bool:
        return True

    def _go(self):
        if self.test_mode:
            logging.error('Test mode not supported for this process')
        else:
            self._archiveold()
            self._chesscomgames()
            self._lichessgames()
            self._processfiles()

    def _merge_files(self, file_path: str, output_filename: str):
        cmd_text = f'copy /B *.pgn {output_filename} >nul'
        result = subprocess.run(cmd_text, cwd=file_path, capture_output=True, text=True, shell=True)
        if result.returncode != 0:
            logging.critical(f'Error merging game files: {result.stderr}')
            raise SystemExit

    def _archiveold(self):
        output_path = self.config.get('downloadRoot')
        archive_path = os.path.join(output_path, 'archive')

        if not os.path.isdir(archive_path):
            os.makedirs(archive_path)

        file_list = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
        if len(file_list) > 0:
            for file in file_list:
                old_name = os.path.join(output_path, file)
                new_name = os.path.join(archive_path, file)
                shutil.move(old_name, new_name)

    def _chesscomgames(self):
        conn_str = self.config.get('connectionString')
        connection_url = sa.engine.URL.create(
            drivername='mssql+pyodbc',
            query={'odbc_connect': conn_str}
        )
        engine = sa.create_engine(connection_url)

        qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM ChessWarehouse.dbo.UsernameXRef "
        qry_text += "WHERE SelfFlag = 1 AND Source = 'Chess.com'"
        users = pd.read_sql(qry_text, engine).values.tolist()
        engine.dispose()

        today = dt.date.today()
        first = today.replace(day=1)
        lastMonth = first - dt.timedelta(days=1)
        yyyy = lastMonth.strftime('%Y')
        mm = lastMonth.strftime('%m')

        dload_root = self.config.get('downloadRoot')
        dload_path = os.path.join(dload_root, 'ChessCom')
        if not os.path.isdir(dload_path):
            os.mkdir(dload_path)  # root will already exist, that is checked earlier in process

        headers = eval(os.getenv('CDCUserAgent'))
        for i in users:
            url = f'https://api.chess.com/pub/player/{i[1]}/games/{yyyy}/{mm}/pgn'
            dload_name = f'{i[1]}_{yyyy}{mm}.pgn'
            dload_file = os.path.join(dload_path, dload_name)
            with requests.get(url, stream=True, headers=headers) as resp:
                if resp.status_code != 200:
                    logging.error(f'Unable to complete request to {url}! Request returned code {resp.status_code}')
                else:
                    with open(dload_file, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8196):
                            f.write(chunk)

        file_list = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
        if len(file_list) > 0:
            merge_name = 'merge.pgn'
            self._merge_files(dload_path, merge_name)

            clean_name = f'ChessCom_{yyyy}{mm}.pgn'
            cmd_list = ['pgn-extract', '-N', '-V', '-D', '--quiet', '--output', clean_name, merge_name]
            result = subprocess.run(cmd_list, cwd=dload_path, capture_output=True, text=True)
            if result.returncode != 0:
                logging.critical(f'Unable to pre-process Chess.com game file: {result.stderr}')
                raise SystemExit

            dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
            for filename in dir_files:
                if filename != clean_name:
                    fname_relpath = os.path.join(dload_path, filename)
                    os.remove(fname_relpath)

            old_loc = os.path.join(dload_path, clean_name)
            new_loc = os.path.join(dload_root, clean_name)
            os.rename(old_loc, new_loc)

    def _lichessgames(self):
        conn_str = self.config.get('connectionString')
        connection_url = sa.engine.URL.create(
            drivername='mssql+pyodbc',
            query={'odbc_connect': conn_str}
        )
        engine = sa.create_engine(connection_url)

        qry_text = "SELECT ISNULL(LastName, '') + ISNULL(FirstName, '') AS PlayerName, Username FROM ChessWarehouse.dbo.UsernameXRef "
        qry_text += "WHERE SelfFlag = 1 AND Source = 'Lichess'"
        users = pd.read_sql(qry_text, engine).values.tolist()
        engine.dispose()

        today = dt.date.today()
        first = today.replace(day=1)
        lastmonth = first - dt.timedelta(days=1)
        start_dte = dt.datetime(year=lastmonth.year, month=lastmonth.month, day=1, hour=0, minute=0, second=0)
        end_dte = dt.datetime(year=today.year, month=today.month, day=1, hour=0, minute=0, second=0)
        utc_start = str(int(start_dte.replace(tzinfo=dt.timezone.utc).timestamp())) + '000'
        utc_end = str(int(end_dte.replace(tzinfo=dt.timezone.utc).timestamp())) + '000'
        yyyy = lastmonth.strftime('%Y')
        mm = lastmonth.strftime('%m')

        dload_root = self.config.get('downloadRoot')
        dload_path = os.path.join(dload_root, 'Lichess')
        if not os.path.isdir(dload_path):
            os.mkdir(dload_path)  # root will already exist, that is checked earlier in process

        headers = {'Authorization': f'Bearer {os.getenv("LichessAPIToken")}'}
        for i in users:
            dload_url = f'https://lichess.org/api/games/user/{i[1]}?clocks=true&evals=true&since={utc_start}&until={utc_end}'
            dload_name = f'{i[1]}_{yyyy}{mm}.pgn'
            dload_file = os.path.join(dload_path, dload_name)
            with requests.get(dload_url, headers=headers, stream=True) as resp:
                if resp.status_code != 200:
                    logging.error(f'Unable to complete request to {dload_url}! Request returned code {resp.status_code}')
                else:
                    with open(dload_file, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8196):
                            f.write(chunk)

        file_list = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
        if len(file_list) > 0:
            merge_name = 'merge.pgn'
            self._merge_files(dload_path, merge_name)

            clean_name = f'Lichess_{yyyy}{mm}.pgn'
            cmd_list = ['pgn-extract', '-N', '-V', '-D', '--quiet', '--output', clean_name, merge_name]
            result = subprocess.run(cmd_list, cwd=dload_path, capture_output=True, text=True)
            if result.returncode != 0:
                logging.critical(f'Unable to pre-process Lichess game file: {result.stderr}')
                raise SystemExit

            dir_files = [f for f in os.listdir(dload_path) if os.path.isfile(os.path.join(dload_path, f))]
            for filename in dir_files:
                if filename != clean_name:
                    fname_relpath = os.path.join(dload_path, filename)
                    os.remove(fname_relpath)

            old_loc = os.path.join(dload_path, clean_name)
            new_loc = os.path.join(dload_root, clean_name)
            os.rename(old_loc, new_loc)

    def _processfiles(self):
        output_path = self.config.get('downloadRoot')

        today = dt.date.today()
        first = today.replace(day=1)
        lastmonth = first - dt.timedelta(days=1)
        yyyy = lastmonth.strftime('%Y')
        mm = lastmonth.strftime('%m')

        merge_name = f'PersonalOnline_All_{yyyy}{mm}_Merged.pgn'
        self._merge_files(output_path, merge_name)

        # delete original files
        dir_files = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
        for filename in dir_files:
            if filename != merge_name:
                fname_relpath = os.path.join(output_path, filename)
                os.remove(fname_relpath)

        # update correspondence game TimeControl tag; missing from Lichess games
        updated_tc_name = os.path.splitext(merge_name)[0] + '_TimeControlFixed' + os.path.splitext(merge_name)[1]
        ofile = os.path.join(output_path, merge_name)
        nfile = os.path.join(output_path, updated_tc_name)
        searchExp = '[TimeControl "-"]'
        replaceExp = '[TimeControl "1/86400"]'
        wfile = open(nfile, 'w')
        for line in fileinput.input(ofile):
            if searchExp in line:
                line = line.replace(searchExp, replaceExp)
            wfile.write(line)
        wfile.close()

        # sort game file
        pgn = open(os.path.join(output_path, updated_tc_name), mode='r', encoding='utf-8', errors='replace')

        idx = []
        game_date = []
        game_text = []
        gm_idx = 0
        gm_txt = chess.pgn.read_game(pgn)
        while gm_txt is not None:
            idx.append(gm_idx)
            game_date.append(gm_txt.headers['Date'])
            game_text.append(gm_txt)
            gm_txt = chess.pgn.read_game(pgn)
            gm_idx = gm_idx + 1

        sort_name = os.path.splitext(updated_tc_name)[0] + '_Sorted' + os.path.splitext(updated_tc_name)[1]
        sort_file = open(os.path.join(output_path, sort_name), 'w', encoding='utf-8')
        idx_sort = [x for _, x in sorted(zip(game_date, idx))]
        for i in idx_sort:
            sort_file.write(str(game_text[i]) + '\n\n')
        sort_file.close()
        pgn.close()

        # create White and Black files
        conn_str = self.config.get('connectionString')
        connection_url = sa.engine.URL.create(
            drivername='mssql+pyodbc',
            query={'odbc_connect': conn_str}
        )
        engine = sa.create_engine(connection_url)

        qry_text = "SELECT DISTINCT Username FROM ChessWarehouse.dbo.UsernameXRef WHERE SelfFlag = 1 AND UserStatus = 'Open'"
        df = pd.read_sql(qry_text, engine)
        users = df['Username'].tolist()
        engine.dispose()

        white_tag = 'WhiteTag.txt'
        white_tag_full = os.path.join(output_path, white_tag)
        with open(white_tag_full, 'w') as wt:
            for u in users:
                wt.write(f'White "{u}"')
                wt.write('\n')

        black_tag = 'BlackTag.txt'
        black_tag_full = os.path.join(output_path, black_tag)
        with open(black_tag_full, 'w') as bl:
            for u in users:
                bl.write(f'Black "{u}"')
                bl.write('\n')

        white_all = f'White_All_{yyyy}{mm}{os.path.splitext(sort_name)[1]}'
        black_all = f'Black_All_{yyyy}{mm}{os.path.splitext(sort_name)[1]}'

        cmd_list = ['pgn-extract', '--quiet', f'-t{white_tag}', '--output', white_all, sort_name]
        result = subprocess.run(cmd_list, cwd=output_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Failure extracting White games: {result.stderr}')
            raise SystemExit

        cmd_list = ['pgn-extract', '--quiet', f'-t{black_tag}', '--output', black_all, sort_name]
        result = subprocess.run(cmd_list, cwd=output_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Failure extracting Black games: {result.stderr}')
            raise SystemExit

        # create CC and Live color-specific files
        cc_tag = 'CCTag.txt'
        cc_tag_full = os.path.join(output_path, cc_tag)
        with open(cc_tag_full, 'w') as cc:
            cc.write('TimeControl >= "86400"')

        live_tag = 'LiveTag.txt'
        live_tag_full = os.path.join(output_path, live_tag)
        with open(live_tag_full, 'w') as lv:
            lv.write('TimeControl <= "86399"')

        white_cc = f'PersonalOnline_CC_White_{yyyy}{mm}{os.path.splitext(white_all)[1]}'
        black_cc = f'PersonalOnline_CC_Black_{yyyy}{mm}{os.path.splitext(black_all)[1]}'
        white_live = f'PersonalOnline_Live_White_{yyyy}{mm}{os.path.splitext(white_all)[1]}'
        black_live = f'PersonalOnline_Live_Black_{yyyy}{mm}{os.path.splitext(black_all)[1]}'

        cmd_list = ['pgn-extract', '--quiet', f'-t{cc_tag}', '--output', white_cc, white_all]
        result = subprocess.run(cmd_list, cwd=output_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Failure extracting White CC games: {result.stderr}')
            raise SystemExit

        cmd_list = ['pgn-extract', '--quiet', f'-t{cc_tag}', '--output', black_cc, black_all]
        result = subprocess.run(cmd_list, cwd=output_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Failure extracting Black CC games: {result.stderr}')
            raise SystemExit

        cmd_list = ['pgn-extract', '--quiet', f'-t{live_tag}', '--output', white_live, white_all]
        result = subprocess.run(cmd_list, cwd=output_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Failure extracting White Live games: {result.stderr}')
            raise SystemExit

        cmd_list = ['pgn-extract', '--quiet', f'-t{live_tag}', '--output', black_live, black_all]
        result = subprocess.run(cmd_list, cwd=output_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Failure extracting Black Live games: {result.stderr}')
            raise SystemExit

        # backtrack and create complete CC and Live files
        cc_all = f'PersonalOnline_CC_All_{yyyy}{mm}{os.path.splitext(sort_name)[1]}'
        cmd_list = ['pgn-extract', '--quiet', f'-t{cc_tag}', '--output', cc_all, sort_name]
        result = subprocess.run(cmd_list, cwd=output_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Failure extracting complete CC game file: {result.stderr}')
            raise SystemExit

        live_all = f'PersonalOnline_Live_All_{yyyy}{mm}{os.path.splitext(sort_name)[1]}'
        cmd_list = ['pgn-extract', '--quiet', f'-t{live_tag}', '--output', live_all, sort_name]
        result = subprocess.run(cmd_list, cwd=output_path, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Failure extracting complete Live game file: {result.stderr}')
            raise SystemExit

        # clean up
        os.remove(os.path.join(output_path, merge_name))
        os.remove(os.path.join(output_path, updated_tc_name))
        os.remove(os.path.join(output_path, white_all))
        os.remove(os.path.join(output_path, black_all))
        os.remove(white_tag_full)
        os.remove(black_tag_full)
        os.remove(cc_tag_full)
        os.remove(live_tag_full)

        old_name = os.path.join(output_path, sort_name)
        dte = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
        all_name = f'{yyyy}{mm}-All_PersonalOnline__{dte}{os.path.splitext(sort_name)[1]}'  # intentionally leaving the time control piece empty
        new_name = os.path.join(output_path, all_name)
        os.rename(old_name, new_name)

        # delete empty files, if no games are in a file
        dir_files = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
        for f in dir_files:
            if os.path.getsize(os.path.join(output_path, f)) == 0:
                os.remove(os.path.join(output_path, f))

        # queue complete monthly game file for analysis
        if os.path.exists(new_name):
            analysis_path = self.config.get('analysisDir')
            final_name = os.path.join(analysis_path, all_name)
            os.rename(new_name, final_name)

        # move all remaining files to a different directory (are for Chessbase)
        chessbase_dir = self.config.get('chessbaseDir')
        if os.path.isdir(chessbase_dir):
            dir_files = [f for f in os.listdir(output_path) if os.path.isfile(os.path.join(output_path, f))]
            for f in dir_files:
                # since chessbase_dir might be on an unavailable network drive, handle possible exceptions
                try:
                    shutil.move(os.path.join(output_path, f), os.path.join(chessbase_dir, f))
                except Exception as e:
                    notifications.SendTelegramMessage(f"MonthlyGameDownload: Unable to move file '{f}' to '{chessbase_dir}' ({str(e)})")
                    error_dir = os.path.join(output_path, 'Errors')
                    if not os.path.isdir(error_dir):
                        os.mkdir(error_dir)
                    shutil.move(os.path.join(output_path, f), os.path.join(output_path, 'Errors', f))

        # send completion notification
        tg_msg = f'MonthlyGameDownload for {yyyy}-{mm} has completed'
        notifications.SendTelegramMessage(tg_msg)
