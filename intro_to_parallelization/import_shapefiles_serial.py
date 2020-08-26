# mike babb
# sicss - 2018: seattle
# June 22, 2018
# demonstrate working with geopandas: serial loading shapefiles

####
# PART 1: LOAD DATA USING GEOPANDAS IN SERIAL
####

# standard libraries
import os
import time

# external libraries
import numpy as np
import geopandas as gpd
import pandas as pd

####
# BACKGROUND AND GOALS
####

# Loading one shapefile is very easy, especially with geopandas.
# But let's say we want to import a directory of shapefiles into geopandas and
# then concatenate them. Also very easy with python and (geo)pandas!

####
# LOCATION OF DATA AND DATA FORMAT
####

# download at least ten, or all of the compressed shapefiles located here:
# ftp://ftp2.census.gov/geo/tiger/TIGER2017/TRACT/
# once the files have finished downloading, uncompress them to the same
# directory.

# a shapefile is a type of vector data storage format and consists of at
# least three files with the same base name # and upwards of 10
# https://en.wikipedia.org/wiki/Shapefile

# we're going to work with the 2017 vintage of the 2010 tract files.
# a quick note on the system scheme of these files.
# tl_2017_NN_tracts.*
# tl: tiger line. This refers to a data product from the US Census Bureau's
# geography division:
# T.I.G.E.R. Topologically Integrated Geographic Encoding and Referencing
# It's not relevant to this tutorial, but this digital data format was
# a hallmark in the development of spatial data.
# (To be clear, the shapefiles come from the TIGER files.)
# https://www.census.gov/newsroom/blogs/director/2014/11/happy-25th-anniversary-tiger.html
# http://census.maps.arcgis.com/apps/MapJournal/index.html?appid=2b9a7b6923a940db84172d6de138eb7e
# 2017: vintage of the data
# NN: state FIPS code: Federal Information Processing Standard.
# The entire code list can be found here:
# https://www.census.gov/geo/reference/ansi_statetables.html
# tracts: the geographic (sub)unit. Census tracts, in this case.

####
# LOADING A SHAPEFILE
####

# shapefile path
sfile_path = 'H:/data/census_geography/tracts/tracts2017/shapefiles'

# let's pick the file for washington: FIPS code 53
sfile_name = 'tl_2017_53_tract.shp'
# let's use a python function to build the fully qualified path to our shapefiles.
sfile_pn = os.path.join(sfile_path, sfile_name)
print(sfile_pn)

# while python can work with a file path with a mixture of forward and
# backward slashes spaces, it's a little confusing for us to look at.
# we can fix that with:
print(os.path.normpath(sfile_pn))

# not necessary, but helpful, if we're printing out the names of files we're
# working with.

# let's read in the file using geopandas
gdf = gpd.read_file(filename=sfile_pn)

# what type of object did we create?
print(type(gdf))
# we have a geopandas geodataframe. An extension of the pandas dataframe

# let's look at the first few rows.
print(gdf.head())
# Looks like a dataframe! With an additional 'geometry' field!

# because geopandas extends pandas, we have access to all of the pandas'
# techniques.
# How many tracts are in Washington?
n_tracts = len(gdf)
print('There are', n_tracts, 'tracts in Washington.')

# How many counties are in Washington? We can answer this question because
# geographies are stored hierarchically:
# Nation --> States --> Counties --> Tracts --> Blockgroups --> Blocks

n_counties = len(gdf['COUNTYFP'].unique().tolist())
print('There are', n_counties, 'counties in Washington.')

# How many tracts per county are there?
select_columns = ['COUNTYFP', 'GEOID']
tracts_per_county = gdf[select_columns].groupby(['COUNTYFP']).agg(np.size)
# let's look at the distribution of the counts of tracts per county
print(tracts_per_county.describe())

####
# LOADING MULTIPLE SHAPEFILES
####

# what if we want to load all of the shapefiles we downloaded into python?
# We could type out 51 different names and do that. But that's error prone.
# Let's use another function to help with this.

# get a list of files using the os.listdir() function
sfile_list = os.listdir(sfile_path)

# we generated a list of files in the our working directory.
# how many?
print(len(sfile_list))

# that's a lot of files!

# let's look at the first few items in our list:
print(sfile_list[:10])

# It looks like our list contains all files relevant to the shapefile. We just
# need to identify the *.shp portion of the shapefile. Most parts of the
# shapefile are loaded when working with geopandas, but we refer to the
# *.shp portion.

# We'll use another python technique to prune the list of file names:
# List comprehension.
# # https://docs.python.org/3/tutorial/datastructures.html

sfile_list = [x for x in sfile_list if x[-4:] == '.shp']

# this is equivalent to the following:
output_list = []
for x in sfile_list:
    if x[-4:] == '.shp':
        output_list.append(x)

# reassign the original list
sfile_list = output_list[:]

# there we go...
print(len(sfile_list))

# 50 states + DC...
# so, let's read them all in using a loop
# append the geodataframe to a list
# concatenate the geodataframes together
# let's time this.

# our list to hold the shapefiles we load
gdf_list = []

# start the timer
time_start = time.time()
for isfn, sfile_name in enumerate(sfile_list):
    # programmatically build the fully qualified path to each shapefile
    sfile_pn = os.path.join(sfile_path, sfile_name)

    # inform us what's going on
    print('...reading:', sfile_name, isfn, 'of', len(sfile_list))

    # load the file
    gdf = gpd.read_file(filename=sfile_pn)

    # add it to the list of geodataframes
    gdf_list.append(gdf)

print('...concatenating...')
gdf = pd.concat(gdf_list)
print('...', len(gdf), 'rows imported.')
print(type(gdf))
print(gdf.head())

# Delete the list to free up memory.
# Not absolutely necessary, but good to do when working with lots of data.
del gdf_list
# stop the timer
time_end = time.time()
time_proc = time_end - time_start
print('Serialized data import:', round(time_proc, 2), 'seconds.')

# build the county identifier: state FIPS code + county FIPS code
# compute some stats
gdf['COUNTYFIPS'] = gdf['STATEFP'] + gdf['COUNTYFP']
n_counties = len(gdf['COUNTYFIPS'].unique().tolist())
print('There are', n_counties, 'counties in the US.')

# How many tracts per county are there?
select_columns = ['COUNTYFIPS', 'GEOID']
tracts_per_county = gdf[select_columns].groupby(['COUNTYFIPS']).agg(np.size)
# let's look at the distribution of the counts of tracts per county
print(tracts_per_county.describe())

####
# WRAP UP
####
# 15 seconds is fine. And in truth we'd probably accept that.
# But for the purposes of this tutortial, let's parallelize this!
