import logging
import json
import os
from pathlib import Path
import re

import pika

from base import base


class QueueAnalysisFiles(base):
    abbreviation = 'QUEUEANALYSIS'

    def _go(self):
        analysis_dir = self.config.get('analysisDir')
        pending_files = [f for f in Path(analysis_dir).iterdir() if f.is_file() and f.suffix == '.pgn']

        if self.test_mode:
            for f in pending_files:
                gm = self._AnalysisFile(f)
                logging.info(f"Pending file: '{os.path.basename(f)}'. Valid: {gm.is_valid()}")
        else:
            rabbitmq_host = self.config.get('rabbitmqHost')
            rabbitmq_user = self.config.get('rabbitmqUser')
            rabbitmq_pass = self.config.get('rabbitmqPass')
            rabbitmq_route = self.config.get('rabbitmqRoute')
            connection = None
            channel = None

            try:
                credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
                connection_params = pika.ConnectionParameters(
                    host=rabbitmq_host,
                    credentials=credentials,
                    heartbeat=0  # disable connection timeout, otherwise it dies after 60 seconds by default
                )

                connection = pika.BlockingConnection(connection_params)
                channel = connection.channel()

                channel.queue_declare(queue=rabbitmq_route, durable=True)

                try:
                    for fn in pending_files:
                        file_path = os.path.dirname(fn)
                        af = self._AnalysisFile(fn)
                        if not af.is_valid():
                            error_path = os.path.join(file_path, 'invalid')
                            if not os.path.exists(error_path):
                                os.mkdir(error_path)

                            new_name = os.path.join(error_path, os.path.basename(fn))
                            os.rename(fn, new_name)
                            logging.error(f'Unable to parse and queue chess analysis file: {os.path.basename(fn)}')
                        else:
                            queue_path = os.path.join(file_path, 'queued')
                            if not os.path.exists(queue_path):
                                os.mkdir(queue_path)

                            new_name = os.path.join(queue_path, af.basename)
                            os.rename(fn, new_name)

                            payload = {
                                'filename': os.path.join(queue_path, af.basename),
                                'source': af.source,
                                'timeControlDetail': af.time_control
                            }
                            message = json.dumps(payload)
                            channel.basic_publish(
                                exchange='',
                                routing_key=rabbitmq_route,
                                body=message,
                                properties=pika.BasicProperties(
                                    delivery_mode=2  # persist message
                                )
                            )
                except Exception as e:
                    logging.critical(f'Unexpection exception: {e}')
                finally:
                    channel.close()
                    connection.close()

            except Exception as e:
                logging.critical(f'Error setting up RabbitMQ: {e}')

                if channel is not None:
                    channel.close()
                if connection is not None:
                    connection.close()

    class _AnalysisFile:
        def __init__(self, filename: str):
            self.filename = filename
            self.basename, self.source, self.time_control = self._parse_filename()

        def _parse_filename(self) -> tuple[str, str]:
            # expectation is the filename is of the form "FreeFormText_Source_TimeControlDetail_yyyyMMdd_hhmmss.pgn"
            pattern = r'^.+_(?P<source>[^_]+)_(?P<time_control>[^_]+)?_(?P<date>\d{8})_(?P<time>\d{6})\.pgn$'

            basename = os.path.basename(self.filename)
            match = re.match(pattern, basename)
            if not match:
                return (None, None, None)

            return (basename, match.group('source'), match.group('time_control'))

        def is_valid(self) -> bool:
            if self.basename is None:
                return False

            return True
