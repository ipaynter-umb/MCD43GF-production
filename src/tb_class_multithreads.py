from concurrent.futures import ThreadPoolExecutor, as_completed
from random import random
from time import sleep
from numpy import arange
import logging
import datetime
from os import environ
import dotenv
from pathlib import Path

dotenv.load_dotenv()

#BASE_PATH = Path(environ['log_files_path'])

# Set the logging config
logging.basicConfig(filename=environ['log_files_path'] + f'{datetime.datetime.now():%Y%m%d%H%M%S}.log',
                    filemode='w',
                    format=' %(levelname)s - %(asctime)s - %(message)s',
                    level=logging.NOTSET)

class SimpleClass:

    def __init__(self, number):

        sleep(random() * 3)
        self.number = number
        logging.info(f'Instantiate {type(self)} object with number {self.number}.')

numbers = list(arange(0, 10, 1))

with ThreadPoolExecutor(max_workers=3) as executor:
    future_events = {executor.submit(SimpleClass, number): number for number in numbers}

    for completed_event in as_completed(future_events):
        simple_class_obj = completed_event.result()
        print(type(simple_class_obj), simple_class_obj.number)
