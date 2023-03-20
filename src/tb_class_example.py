import t_requests
import tb_laads_tools
from time import time
from numpy import round
from dotenv import load_dotenv
from os import environ

load_dotenv('.env.daacs')

print(environ['test'])

exit()

s = tb_laads_tools.get_laads_session()

url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/2020/040/VNP46A2.A2020040.h00v06.001.2021053191602.h5"
#url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A2/2020/040.json"

ref_md5 = "7b5fb11681c5c6a6ac0f6bc5ab62f959"

stime = time()

t_requests.ask_nicely(s,
                      url,
                      hash_func=t_requests.validate_request_md5,
                      validation_func=t_requests.validate_request_hdf5,
                      hash_to_check=ref_md5)

print(f'{round(time() - stime, decimals=2)} seconds')

exit()


class Car:

    def __init__(self, user_provided_color, doors, brand):

        self.color = user_provided_color
        self.doors = doors
        self.brand = brand

    def change_color(self, new_color):

        self.color = new_color

car_one = Car('red', 4, 'jaguar')
print(car_one)

car_two = Car('blue', 2, 'ford')
print(car_two)

list_of_cars = [car_one, car_two]

for car in list_of_cars:

    car.change_color('green')

print(car_one)
print(car_two)
