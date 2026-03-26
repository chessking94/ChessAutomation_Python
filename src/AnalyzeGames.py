import json
import logging
import os
import socket
import subprocess

import pika

from base import base


class AnalyzeGames(base):
    abbreviation = 'ANALYZE'

    def _go(self):
        if self.test_mode:
            filename = ''
            source = ''
            timeControlDetail = None
            af = self._AnalysisFile(filename, source, timeControlDetail)
            self._process_file(af)
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

                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(
                    queue=rabbitmq_route,
                    on_message_callback=lambda ch, method, properties, body: self._callback(
                        ch, method, properties, body
                    )
                )

                logging.info('Waiting for messages. To exit press CTRL+C')
                try:
                    channel.start_consuming()
                except KeyboardInterrupt:
                    logging.info('Process stopped by Ctrl+C')
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
        def __init__(self, filename: str | None, source: str | None, timeControlDetail: str | None):
            self.filename = filename
            self.source = source
            self.timeControlDetail = timeControlDetail

        def is_valid(self) -> str | None:
            rtn = None
            if self.filename is None:
                rtn = 'Invalid filename'

            if self.source is None:
                reason = 'Invalid source'
                if rtn is None:
                    rtn = reason
                else:
                    rtn += f'|{reason}'

            # # TODO: implement proper handling of this someday
            # if self.timeControlDetail is None:
            #     reason = 'Invalid time control'
            #     if rtn is None:
            #         rtn = reason
            #     else:
            #         rtn += f'|{reason}'

            return rtn

    def _callback(self, ch, method, properties, body):
        message = body.decode()
        logging.info('Message received')

        try:
            message_body = json.loads(message)
            assert isinstance(message_body, dict)
            filename = message_body.get('filename', None)
            source = message_body.get('source', None)
            timeControlDetail = message_body.get('timeControlDetail', None)
            af = self._AnalysisFile(filename, source, timeControlDetail)

            err = af.is_valid()
            if err is None:
                logging.info(f'Filename = {os.path.basename(filename)}')
                self._process_file(af)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                logging.error(err)
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        except Exception as e:
            logging.error(f'Error processing message: {e}')
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _process_file(self, af: _AnalysisFile):
        clean_env = os.environ.copy()
        clean_env.pop('VIRTUAL_ENV', None)
        clean_env.pop('PYTHONPATH', None)

        python_env = self.config.get('analysisEnv')
        if not os.path.exists(python_env):
            logging.critical(f"Python environment '{python_env}' does not exist on {socket.gethostname()}")
            raise SystemExit

        analysis_program = self.config.get('analysisProgram')
        if not os.path.exists(analysis_program):
            logging.critical(f"Analysis program '{analysis_program}' does not exist on {socket.gethostname()}")
            raise SystemExit

        cmd_list = [python_env, analysis_program, '--pgn', af.filename, '--source', af.source]
        if af.timeControlDetail is not None:
            cmd_list.extend(['--time', af.timeControlDetail])
        result = subprocess.run(cmd_list, env=clean_env, capture_output=True, text=True)
        if result.returncode != 0:
            logging.critical(f'Error analyzing file: {result.stderr}')
            raise SystemExit
