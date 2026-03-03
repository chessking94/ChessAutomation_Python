import argparse
import logging
import os
from pathlib import Path
import socket
import subprocess

from Utilities_Python import misc

import DownloadTWIC
import MonthlyGameDownload
import UpdateUsernameXRef

PROCESS_CHOICES = [
    'GAMES',
    'TWIC',
    'USERS'
]

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def check_for_pgnextract():
    result = subprocess.run('pgn-extract -h', shell=True, capture_output=True)
    if 'is not recognized' in str(result.stderr):
        raise RuntimeError(f'pgn-extract not found on {socket.gethostname()}')


def main():
    vrs_num = '1.0'
    parser = argparse.ArgumentParser(
        description='Chess Automation',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        usage=argparse.SUPPRESS
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s ' + vrs_num
    )
    parser.add_argument(
        '-p', '--process',
        choices=PROCESS_CHOICES,
        help='Process name'
    )
    args = parser.parse_args()
    config = vars(args)

    process_name = config['process']

    match process_name:
        case None:
            script_name = 'ChessAutomationPython'  # need to set something
        case 'GAMES':
            script_name = 'MonthlyGameDownload'
        case 'TWIC':
            script_name = 'DownloadTWIC'
        case 'USERS':
            script_name = 'UpdateUsernameXRef'

    _ = misc.initiate_logging(script_name, CONFIG_FILE)  # intentionally putting this later than usual to use a different script_name

    match process_name:
        case None:
            logging.error('No process parameter passed to ChessAutomation_Python')
        case 'GAMES':
            check_for_pgnextract()
            config = misc.get_config(process_name, CONFIG_FILE)
            MonthlyGameDownload.main(config)
        case 'TWIC':
            check_for_pgnextract()
            config = misc.get_config(process_name, CONFIG_FILE)
            DownloadTWIC.main(config)
        case 'USERS':
            UpdateUsernameXRef.main()


if __name__ == '__main__':
    main()
