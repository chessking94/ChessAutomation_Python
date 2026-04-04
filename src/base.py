from abc import ABC, abstractmethod
import logging
import socket
import subprocess


class base(ABC):
    """ Define an abbreviation for CLI access """
    abbreviation: str = None

    def __init__(self):
        # enforce the equivalent of a MustOverride property
        if 'abbreviation' not in self.__class__.__dict__:
            raise NotImplementedError(f"{self.__class__.__name__} must define an 'abbreviation' attribute")

        self.config = {}
        self.test_mode = False

    def update_config(self, config: dict):
        if isinstance(config, dict):
            self.config.update(config)

    def main(self, config: dict | None, test_mode: bool = False):
        """ Entry point for the module """
        self.update_config(config)

        self.test_mode = test_mode
        self._go()

    @abstractmethod
    def _go(self):
        """ Logic for child process """
        pass

    def require_pgnextract(self) -> bool:  # define a method of the same name in the subclass to override this
        """ Indicates if a process requires command line tool PGN-Extract """
        return False

    def check_for_pgnextract(self):
        """ Check if PGN-Extract is on PATH """
        result = subprocess.run('pgn-extract -h', shell=True, capture_output=True)
        if 'is not recognized' in str(result.stderr):
            err_msg = f'pgn-extract not found on {socket.gethostname()}'
            logging.critical(err_msg)
            raise SystemExit
