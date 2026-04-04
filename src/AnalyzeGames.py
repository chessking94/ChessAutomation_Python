import logging
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess

from base import base


class AnalyzeGames(base):
    abbreviation = 'ANALYZE'

    def _go(self):
        analysis_dir = self.config.get('dynamic1')
        if not os.path.exists(analysis_dir):
            logging.critical(f"Analysis directory '{analysis_dir}' does not exist")
            raise SystemExit

        python_env = self.config.get('analysisEnv')
        if not os.path.exists(python_env):
            logging.critical(f"Python environment '{python_env}' does not exist on {socket.gethostname()}")
            raise SystemExit

        analysis_program = self.config.get('analysisProgram')
        if not os.path.exists(analysis_program):
            logging.critical(f"Analysis program '{analysis_program}' does not exist on {socket.gethostname()}")
            raise SystemExit

        pending_files = [f for f in Path(analysis_dir).iterdir() if f.is_file() and f.suffix == '.pgn']

        # exclude files that have already started being analyzed, denoted by a ".game" file in the same directory
        inprogress_stems = {f.stem for f in Path(analysis_dir).iterdir() if f.suffix == '.game'}
        pending_files = [f for f in pending_files if f.stem not in inprogress_stems]

        if self.test_mode:
            for f in pending_files:
                gm = self._AnalysisFile(f)
                msg = f"Pending file: '{os.path.basename(f)}'."
                err = gm.is_valid()
                if err is None:
                    msg += ' Valid: True'
                else:
                    msg += f' Invalid: {err}'
                logging.info(msg)
        else:
            for fn in pending_files:
                file_path = os.path.dirname(fn)
                af = self._AnalysisFile(fn)
                err = af.is_valid()
                if err is not None:
                    error_path = os.path.join(file_path, 'invalid')
                    if not os.path.exists(error_path):
                        os.mkdir(error_path)

                    new_name = os.path.join(error_path, os.path.basename(fn))
                    os.rename(fn, new_name)
                    logging.error(f"Unable to parse chess analysis filename '{os.path.basename(fn)}': {err}")
                else:
                    inprogress_stems = {f.stem for f in Path(analysis_dir).iterdir() if f.suffix == '.game'}
                    if Path(af.filename).stem not in inprogress_stems:
                        af.process_file(python_env, analysis_program)

    class _AnalysisFile:
        def __init__(self, filename: str):
            self.filename = filename
            self.source, self.time_control = self._parse_filename()

        def _parse_filename(self) -> tuple[str, str]:
            # expectation is the filename is of the form "FreeFormText_Source_TimeControlDetail_yyyyMMdd_hhmmss.pgn"
            pattern = r'^.+_(?P<source>[^_]+)_(?P<time_control>[^_]+)?_(?P<date>\d{8})_(?P<time>\d{6})\.pgn$'

            match = re.match(pattern, os.path.basename(self.filename))
            if not match:
                return (None, None)

            return (match.group('source'), match.group('time_control'))

        def is_valid(self) -> str | None:
            # TODO: implement proper validation of these someday

            rtn = None
            if self.source is None:
                reason = 'Invalid source'
                if rtn is None:
                    rtn = reason
                else:
                    rtn += f'|{reason}'

            # if self.time_control is None:
            #     reason = 'Invalid time control'
            #     if rtn is None:
            #         rtn = reason
            #     else:
            #         rtn += f'|{reason}'

            return rtn

        def process_file(self, analysis_env: str, analysis_program: str):
            err = self.is_valid()
            if err is not None:
                # this should never be hit, but is a safeguard just in case
                logging.error(f"Unable to process file '{self.filename}': {err}")
            else:
                clean_env = os.environ.copy()

                # reset these env vars just in case
                clean_env.pop('VIRTUAL_ENV', None)
                clean_env.pop('PYTHONPATH', None)

                cmd_list = [
                    analysis_env,
                    analysis_program,
                    '--pgn', self.filename,
                    '--source', self.source
                ]
                if self.time_control is not None:
                    cmd_list.extend(['--time', self.time_control])

                result = subprocess.run(cmd_list, env=clean_env, capture_output=True, text=True)
                if result.returncode != 0:
                    logging.critical(f'Error analyzing file: {result.stderr}')
                    raise SystemExit

                # move file to an archive directory
                if os.path.exists(self.filename):
                    archive_dir = os.path.join(os.path.dirname(self.filename), 'Archive')
                    if not os.path.exists(archive_dir):
                        os.mkdir(archive_dir)

                    shutil.move(self.filename, os.path.join(archive_dir))
