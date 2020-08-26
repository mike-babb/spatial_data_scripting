# mike babb
# sicss - 2018: seattle
# June 22, 2018
# demonstrate working with geopandas and multiprocessing: parallel loading

####
# PART 3: LOADING SHAPEFILES IN PARALLEL
####

# standard libraries
from multiprocessing import Pool
import os
import time

# external libraries
import geopandas as gpd
import pandas as pd

####
# DEFINE FUNCTIONS
####

def get_sfile_list(sfile_path):
    """
    Generate a list of fully qualified shapefile paths and names.
    :param sfile_path: string. Path to a directory which shapefiles.
    :return: list. Strings that represent the
    """

    sfile_list = os.listdir(sfile_path)
    sfile_list = [os.path.join(sfile_path, x) for x in sfile_list if x[-4:] == '.shp']

    return sfile_list


def gather_sfiles(sfile_list):
    """
    Use multi-processing to read in all shapefiles in a directory
    :param sfile_list:
    :return:geopandas geodataframe
    """
    # send to our multiprocessor
    time_start = time.time()

    # processes refers to how many cores to use. My computer has 8 cores, but
    # I've found that 6 is the optimal cores to use. Usually.
    with Pool(processes=8) as p:
        gdf_list = p.map(gpd.read_file, sfile_list)

    print('...concatenating...')
    gdf = pd.concat(gdf_list)
    print('...', len(gdf), 'rows imported.')

    # Delete the list to free up memory.
    # Not absolutely necessary, but good to do when working in a parallel
    # environment
    del gdf_list
    time_end = time.time()
    time_proc = time_end - time_start
    print('Parallel data import:', round(time_proc), 'seconds.')

    return gdf


####
# IDENTIFICATION OF THE MAIN THREAD
####

if __name__ == '__main__':

    ####
    # LOAD MANY SHAPEFILES
    ####

    # shapefile path
    sfile_path = 'H:/data/census_geography/tracts/tracts2017/shapefiles'

    # list of shapefiels
    sfile_list = get_sfile_list(sfile_path=sfile_path)
    print(sfile_list)
    print(len(sfile_list))

    gdf = gather_sfiles(sfile_list)

    print(type(gdf))
    print(gdf.head())


