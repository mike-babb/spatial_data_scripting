# mike babb
# sicss - 2018: seattle
# June 22, 2018
# demonstrate working with geopandas and multiprocessing: computing squares

####
# PART 2: BACKGROUND ON MULTIPROCESSING
####

# external libraries
from multiprocessing import Pool

# The general concept to multiprocessing (within python) is to pass a list of
# objects to a function which then sends each object and the NAME of a function
# to a processor on the computer. The processor executes the function on the
# passed object and returns the result. The results from all of the processors
# are then returned in a list to the main processor.

# Multiprocessing isn't too difficult, but we can't just sprinkle it in a script
# and have it run. It requires some additional work on our part.
# Several points:
# * It's important to think about what you're sending to each core and what's
# being returned. There is communication overhead with passing objects
# between processors.
# * Accordingly, it's very easy to do too much on each core and return too much
# and exhaust system resources.
# * Multiprocessing (within python) is not ALWAYS faster than serialization.
# Going from one core to three cores does mean a three-fold decrease in
# execution time. This is because there is communication overhead between the
# main core and the sub cores.
# * It's important to think of your workflow in terms of user-defined functions
# rather than one long script.
# * While there can be huge gains from parallelization, it's important to
# consider the trade offs between optimizing the code to run in parallel
# and simply running the code.

####
# THE MULTIPROCESSING MODULE
####

# https://docs.python.org/3/library/multiprocessing.html
# If you take a look at the above page, you'll see the following line:
# if __name__ == '__main__':
# __main__ is an internal function that is called every time a python function
# is run. When executing a *.py file that features multiprocessing, we need to
# explicitly protect the __main__ function from sub threads. Otherwise each
# sub process spawned would call additional sub-processes over and over again.

####
# A SIMPLE EXAMPLE - COMPUTE SQUARES
####

def square_integer(x):
    """
    Simple function to compute the square of an integer.
    :param x: integer.
    :return: integer. The square of x.
    """
    return x**2


if __name__ == '__main__':

    ####
    # DEMONSTRATE MULTIPROCESSING BY COMPUTING SQUARES
    ####

    integer_list = list(range(1, 11))
    print(integer_list)

    # here's how we would compute this using list comprehension
    square_list = [x**2 for x in integer_list]
    print(square_list)

    # And here's the parallelized version:
    # Start the multiprocessor. Using 'with' as part of a control flow controls
    # the initialization and shutdown of the individual cores.
    with Pool(processes=8) as p:
        square_list = p.map(square_integer, integer_list)

    print(square_list)