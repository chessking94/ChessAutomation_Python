import argparse
import logging

from Utilities_Python import misc

from config import CONFIG_FILE
import MonthlyGameDownload
import UpdateUsernameXRef

PROCESS_CHOICES = [
    'GAMES',
    'USERS'
]


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
        case 'USERS':
            script_name = 'UpdateUsernameXRef'

    _ = misc.initiate_logging(script_name, CONFIG_FILE)  # intentionally putting this later than usual to use a different script_name

    match process_name:
        case None:
            logging.error('No process parameter passed to ChessAutomation_Python')
        case 'GAMES':
            MonthlyGameDownload.main()
        case 'USERS':
            UpdateUsernameXRef.main()


if __name__ == '__main__':
    main()
