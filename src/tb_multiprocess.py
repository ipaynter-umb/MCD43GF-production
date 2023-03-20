from concurrent.futures import ProcessPoolExecutor
from t_misc import listify

# The rules for ProcessPoolExecutor functions:
# The ProcessPoolExecutor can only be used (call map method etc.) if inside a __name__ == '__main__': block
# OR if inside a function that is only called from within a __name__ == '__main__': block


# A function to multiply an input by 2
def times_two(number):

    print(f'Multiplying {number} by 2.')

    return number * 2


# We can instantiate the ProcessPoolExecutor object outside a function  (no idea why you would want to)
executor = ProcessPoolExecutor(max_workers=3)


# A function to instantiate a ProcessPoolExecutor and run some work
def mp(mp_func, work):

    with ProcessPoolExecutor(max_workers=3) as func_exec:

        func_exec.map(mp_func, work)


if __name__ == "__main__":
    # Make a list of work for the ProcessPoolExecutor to do.
    list_of_work = [3, 5, 6, 7]
    # Map the work to the ProcessPoolExecutor
    executor.map(times_two, list_of_work)
    # Call the function to do the same work
    mp(times_two, list_of_work)
