from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import random
import t_misc


# A function wait 0-2 seconds then multiply an input by 2 and return it
def times_two(number):
    # Pick a random number of seconds to wait
    random_wait = random.randint(0, 2)
    # Print information
    print(f'Multiplying {number} by 2, waiting {random_wait} seconds.')
    # Wait
    sleep(random_wait)
    # Return the result
    return number * 2


def write_number(number):
    with open(f'F:/UMB/MCD43GF/{number}', mode='w') as f:
        f.write(str(number))


# A list of work for multithread workers to perform
list_of_work = [0, 1, 2, 3]

t_misc.multithread(times_two, list_of_work, as_completed_func=None)

exit()

with ThreadPoolExecutor(max_workers=2) as executor:

    futures = [executor.submit(times_two, work) for work in list_of_work]

    for future in as_completed(futures):
        write_number(future.result())


# A list of work for multithread workers to perform
list_of_work = [0, 1, 2, 3]


print('Running ThreadPoolExecutor using map, no output collected.')
with ThreadPoolExecutor(max_workers=2) as executor:

    executor.map(times_two, list_of_work)

print("\n=====================\n")

print('Running ThreadPoolExecutor using submit, collecting the output in a list.')
with ThreadPoolExecutor(max_workers=2) as executor:

    futures = [executor.submit(times_two, work) for work in list_of_work]

print("\n")

print("The futures list variable looks like this:")
print(futures)

print("\n")

print("And each Future object consists of:")
for future in futures:
    print(future.__dict__)

print("\n=====================\n")

print('Running ThreadPoolExecutor using submit, collecting the output in a dictionary.')
with ThreadPoolExecutor(max_workers=2) as executor:

    futures = {executor.submit(times_two, work): work for work in list_of_work}

print("\n")

print("The futures dictionary variable looks like this:")
print(futures)

print("\n")

print("Where the keys are Future objects, and the values are the results:")
for future in futures.keys():
    print(future, futures[future])

