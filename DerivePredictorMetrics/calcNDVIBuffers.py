# calcNDVIBuffers.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: Extract values from NDVI rasters to a set of partitioned shapefiles

# import libraries
from multiprocessing import Pool
import os
import random
import time

# define global constants
INPUT_FOLDER = "folder path where files are stored"
GDB = 'absolute filepath to geo database where NDVI rasters are stored'
NDVI_RASTERS = GDB + "NDVI450Raster nd450m;" + GDB + "NDVI10mRaster nd10m"

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

########## HELPER FUNCTIONS #############

# screen a list of files to find those with an "shp" extension
# INPUTS:
#    filesToProcess (str array) - list of candidate files
# OUTPUTS:
#    screenedFiles (str array) - candidate files with an 'shp' extension
def processFiles(filesToProcess):
    screenedFiles = []
    for file in filesToProcess:
        if file[-3:] == 'shp':
            screenedFiles.append(file)
    return(screenedFiles)

# exact NDVI values to points in a shapefile
# INPUTS:
#    pointsFile (str) - absolute filepath to shapefile
def extractNDVI(pointsFile):

    arcpy.sa.ExtractMultiValuesToPoints(
        in_point_features=INPUT_FOLDER + pointsFile,
        in_rasters=NDVI_RASTERS,
        bilinear_interpolate_values="NONE"
    )

####################### MAIN FUNCTION ##################
    
if __name__ == '__main__':  
    filesToProcess = processFiles(os.listdir(INPUT_FOLDER))
    random.shuffle(filesToProcess)
    pool = Pool(processes=3)
    res = pool.map_async(extractNDVI,filesToProcess)
    res.get()