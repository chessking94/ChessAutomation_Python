import datetime as dt
import logging
import os
import shutil
import zipfile

from bs4 import BeautifulSoup
import pyodbc
import requests
from Utilities_Python import notifications

CONFIG = {}

# TWIC blocks calls to the site without a header (returns a 406), use a generic User-Agent to get around it
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (Chrome/120.0.0.0 Safari/537.36)"
}


class TWICTableEntry:
    def __init__(self, issue_number: int, release_date: dt.datetime, pgn_link: str, game_count: int):
        self.issue_number = issue_number
        self.release_date = release_date
        self.pgn_link = pgn_link
        self.game_count = game_count


def build_twic_list() -> list:
    issue_idx, release_idx, pgn_idx, game_idx = 0, 0, 0, 0
    twic_list = []

    with requests.get('https://theweekinchess.com/twic', headers=HEADERS) as resp:
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', class_='results-table')
        for i, row in enumerate(table.find_all('tr')):
            if i == 0:
                continue  # skip the first row
            elif i == 1:
                cells = row.find_all(["td", "th"])
                for i, cell in enumerate(cells):
                    match cell.get_text(strip=True):
                        case 'TWIC':
                            issue_idx = i
                        case 'Date':
                            release_idx = i
                        case 'PGN':
                            pgn_idx = i
                        case 'Games':
                            game_idx = i
            else:
                issue_number, release_date, pgn_link, game_count = None, None, None, None
                cells = row.find_all(['td', 'th'])
                for i, cell in enumerate(cells):
                    if i == issue_idx:
                        issue_number = int(cell.get_text(strip=True))
                    elif i == release_idx:
                        release_date = dt.datetime.strptime(cell.get_text(strip=True), '%Y-%m-%d')
                    elif i == pgn_idx:
                        link = cell.find('a')
                        if link:
                            pgn_link = link.get('href')
                    elif i == game_idx:
                        game_count = int(cell.get_text(strip=True))

                if pgn_link is not None:
                    twic_list.append(TWICTableEntry(issue_number, release_date, pgn_link, game_count))

    return twic_list


def build_processed_set(conn: pyodbc.Connection) -> set:
    sql_cmd = 'SELECT IssueNumber FROM ChessWarehouse.dbo.TWICLog'
    with conn.cursor() as csr:
        csr.execute(sql_cmd)
        issues = [row[0] for row in csr.fetchall()]

    return set(issues)


def download_twic_issue(url: str) -> str | None:
    with requests.get(url, headers=HEADERS, stream=True) as resp:
        if resp.status_code != 200:
            logging.error(f'Unable to complete download to {url}! Request returned code {resp.status_code}')
            return None
        else:
            dload_path = CONFIG.get('downloadDir')
            file_name = url.split('/')[-1]
            with open(os.path.join(dload_path, file_name), 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive chunks
                        f.write(chunk)

            return os.path.join(dload_path, file_name)


def process_issue(conn: pyodbc.Connection, filename: str, issue: TWICTableEntry):
    # unzip file
    extract_dir = os.path.splitext(filename)[0]
    with zipfile.ZipFile(filename, 'r') as z:
        z.extractall(extract_dir)

    # pre-process the PGN file to my liking
    zip_contents = [f for f in os.listdir(extract_dir) if os.path.isfile(os.path.join(extract_dir, f))]
    for file in zip_contents:
        clean_name = f'{os.path.splitext(file)[0]}_clean.pgn'
        cmd_text = f'pgn-extract -N -V -D --quiet --fixresulttags --output {clean_name} {file} >nul'
        if os.getcwd != extract_dir:
            os.chdir(extract_dir)
        os.system('cmd /C ' + cmd_text)

    # move PGN to directory for Workflow to pick up for the PGN formatting step
    merge_name = f'{os.path.splitext(os.path.basename(filename))[0].replace('g', '')}.pgn'
    cmd_text = f'copy /B *_clean.pgn {merge_name}'
    if os.getcwd != extract_dir:
        os.chdir(extract_dir)
    os.system('cmd /C ' + cmd_text)
    os.rename(merge_name, os.path.join(CONFIG.get('outputDir'), os.path.basename(merge_name)))

    # post-processing cleanup
    os.chdir(os.path.dirname(filename))  # necessary to delete extract_dir
    os.remove(filename)
    shutil.rmtree(extract_dir)

    sql_cmd = 'INSERT INTO ChessWarehouse.dbo.TWICLog (IssueNumber, ReleaseDate, DownloadFile, GameCount) '
    sql_cmd += 'VALUES (?, ?, ?, ?)'
    params = [issue.issue_number, issue.release_date, os.path.basename(filename), issue.game_count]
    with conn.cursor() as csr:
        csr.execute(sql_cmd, params)
        csr.commit()


def main(config: dict):
    global CONFIG
    if isinstance(config, dict):
        CONFIG = config

    twic_list = build_twic_list()

    conn_str = os.getenv('ConnectionStringOdbcRelease')
    with pyodbc.connect(conn_str) as conn:
        processed_set = build_processed_set(conn)
        pending_download = [i for i in twic_list if i.issue_number not in processed_set]  # leave only issues not already processed
        pending_download = sorted(pending_download, key=lambda i: i.issue_number)
        ct = 0
        for issue in pending_download:
            assert isinstance(issue, TWICTableEntry)
            filename = download_twic_issue(issue.pgn_link)

            if filename is not None:
                ct += 1

                # copy zip file to backup directory
                shutil.copy2(filename, os.path.join(CONFIG.get('backupDir'), os.path.basename(filename)))

                # process issue
                process_issue(conn, filename, issue)

        if ct > 0:
            notifications.SendTelegramMessage(f'A total of {ct} TWIC file(s) have been successfully downloaded')
