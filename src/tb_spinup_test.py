from os import environ
import logging
import datetime
from pathlib import Path

environ['logs_dir'] = str(Path(str(Path(__file__).parents[1]), 'logs'))

# Set the logging config
logging.basicConfig(filename=environ['logs_dir'] + f'spinuptest_{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                    filemode='w',
                    format=' %(levelname)s - %(asctime)s - %(message)s',
                    level=logging.DEBUG)

for vari in environ:
    if vari == 'SPINUP':
        logging.info(f'{vari}, {environ[vari]}')

import t_spinup

for vari in environ:
    if vari == 'spinup':
        logging.info(f'{vari}, {environ[vari]}')

import c_laads