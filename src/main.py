import argparse
from pathlib import Path

from automation import misc

from config import CONFIG_FILE
import MonthlyGameDownload
import UpdateUsernameXRef


def main():
    script_name = Path(__file__).stem
    _ = misc.initiate_logging(script_name, CONFIG_FILE)

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
        default=None,
        help='Process name'
    )
    args = parser.parse_args()
    config = vars(args)

    process_name = config['process'].upper()

    match process_name:
        case 'GAMES':
            MonthlyGameDownload.main()
        case 'USERS':
            UpdateUsernameXRef.main()


if __name__ == '__main__':
    main()
