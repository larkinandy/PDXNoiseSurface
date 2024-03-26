# genNearTableParallel.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: calculate distance to road segments within a set distance 
#          from each grid point.  Used for deriving buffer variable 
#          estimates.  

# import libraries
from multiprocessing import Pool
import os
import time
import random

# define global constants
INPUT_FOLDER = "H:/Noise/implementation/screenedFishnet/"
NEAR_FOLDER = "F:/Noise/near/"
ROADS = "H:/Noise/implementation/PDX10m.shp"
N_CPUS = 12

sucessfulImport = False
# needed when using the ArcGIS license for a large # of parallel threads
while(sucessfulImport == False):
    try:
        #print("try again")
        import arcpy
        from arcpy.ia import * 
        from arcpy import env
        from arcpy.sa import *
        arcpy.env.overwriteOutput = True
        arcpy.CheckOutExtension("Spatial")
        sucessfulImport = True
    except Exception as e:
        print("couldn't import arcgis: %s" %(str(e)))
        time.sleep(1)

# my current version of Arcpy doesn't correctly import unless it preceeds the pandas import
import pandas as ps


########## HELPER FUNCTIONS #############


# calculate distance between grid points and road segments 
# INPUTS:
#    pointsShapefile (str) - relative filepath to shapefile containing grid points
def generateNearTableSingle(pointsShapefile):

    # look to see if the shapefile has already been processed.  
    # If so, return early to avoid redundant processing
    outputFile = NEAR_FOLDER + pointsShapefile[:-4] + ".csv"
    if(os.path.exists(outputFile)):
        nearData = ps.read_csv(outputFile)
        if(999 in list(set(nearData['IN_FID'])) or pointsShapefile == 'b0.shp'):
            print("%s has already been processed" %(pointsShapefile))
            return

    # calculate distance between road network and grid points 
    nearFeatures = "in_memory/inMemoryFeatureClass"
    arcpy.CopyFeatures_management(ROADS, nearFeatures)
    a = arcpy.analysis.GenerateNearTable(
        in_features=INPUT_FOLDER + pointsShapefile,
        near_features=nearFeatures,
        out_table= NEAR_FOLDER + pointsShapefile[:-4] + ".csv",
        search_radius='2000 Meters',
        location="NO_LOCATION",
        angle="NO_ANGLE",
        closest="ALL",
        closest_count='#',
        method="GEODESIC",
        distance_unit="Meters"
    )

    # clean up.  Arcpy doesn't always remove temporary variables after completing the function call
    arcpy.management.Delete(nearFeatures)

# given an array of filenames, find only files with a 'shp' extension
# INPUTS:
#    filesToProcess (str array) - list of files to filter
# OUTPUTS:
#    list of files with an 'shp' extension
def processFiles(filesToProcess):
    screenedFiles = []
    for file in filesToProcess:
        if file[-3:] == 'shp':
            screenedFiles.append(file)
    return(screenedFiles)

####################### MAIN FUNCTION ##################
if __name__ == '__main__':
    
    # get list of point subset shapefiles 
    filesToProcess = processFiles(os.listdir(INPUT_FOLDER))

    # randomly shuffle.  If errors are uncountered and pools terminate early, this helps spread the workload 
    # uniformly across cpus when the error is corrected and the script is restarted
    random.shuffle(filesToProcess)
    
    # create a pool of workers, one worker for each free CPU.  I wouldn't recommend going above the CPU
    # count via hyperthreading, arcpy performance doesn't seem to work well when worker count goes above
    # physical core count
    pool = Pool(processes=N_CPUS)
    res = pool.map_async(generateNearTableSingle,filesToProcess)
    res.get()
