import argparse
import importlib
import inspect
import logging
import os
from pathlib import Path

from Utilities_Python import misc

from base import base

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def main():
    modules = {}

    import_dir = os.path.dirname(os.path.abspath(__file__))
    for filename in os.listdir(import_dir):
        if filename.endswith('.py') and filename not in ('main.py', 'base.py'):
            module_name = filename[:-3]
            module = importlib.import_module(module_name)

            # only accept processes which inherit from the base class
            for _, cls in inspect.getmembers(module, inspect.isclass):
                if issubclass(cls, base) and cls is not base:
                    modules[cls.abbreviation] = cls()
                    break

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
        nargs='?',
        help='Process name'
    )

    # dynamic arguments could be anything, they will be handled by the subprocess
    parser.add_argument(
        '--dynamic1',
        default=None,
        nargs='?',
        help='Dynamic argument 1'
    )
    args = parser.parse_args()
    config_args = vars(args)

    dynamic_args = {
        'dynamic1': config_args['dynamic1']
    }

    process_name = str(config_args['process']).upper()

    script_name = 'ChessAutomationPython'  # default log file name

    process_object = modules.get(process_name, None)
    if process_object is not None:
        assert isinstance(process_object, base)
        script_name = process_object.__class__.__name__

    _ = misc.initiate_logging(script_name, CONFIG_FILE)  # intentionally putting this later than usual to use a different script_name

    if process_object is None:
        err_msg = f"Invalid process parameter '{process_name}' passed to ChessAutomation_Python (choose from "
        err_msg += ', '.join(modules.keys()) + ')'
        logging.critical(err_msg)
    else:
        shared_config = misc.get_config('environment', CONFIG_FILE)
        process_object.update_config(shared_config)
        process_object.update_config(dynamic_args)

        if process_object.require_pgnextract():
            process_object.check_for_pgnextract()

        test_mode = False
        process_config = misc.get_config(process_name, CONFIG_FILE)
        process_object.main(process_config, test_mode)


if __name__ == '__main__':
    main()
