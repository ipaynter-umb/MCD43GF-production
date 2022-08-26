import logging
import datetime
from os import environ
import dotenv

def load_env_vars():

    dotenv.load_dotenv()

def main():

    load_env_vars()

    # Specify a logging level to record (CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET)
    # Selecting a "lower" level will record that level, and all levels above
    # For example "WARNING" will mean that "ERROR" and "CRITICAL" logs are recorded
    logging_level = logging.WARNING

    #

    # Set the logging config
    logging.basicConfig(filename=environ['log_files_path'] + f'example_{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                        filemode='w',
                        format=' %(levelname)s - %(asctime)s - %(message)s',
                        level=logging_level)

    logging.critical(f'Create a new critical log.')
    logging.error(f'Create a new error log.')
    logging.warning(f'Create a new warning log.')
    logging.info(f'Create a new info log.')
    logging.debug(f'Create a new debug log.')

if __name__ == '__main__':

    main()

