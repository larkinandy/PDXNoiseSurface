# genNearTableParallelMisc.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: calculate distance to all polyline and point predictor variables
#          within a set distance from each grid point.  Used for deriving 
#          buffer variable estimates

# import libraries
from multiprocessing import Pool
import os
import time
import random
import string

# define global constants
INPUT_FOLDER = "H:/Noise/implementation/screenedFishnet/"

# create an array of global constants for the predictor datasets
INPUT_PREDICTORS = []
for fileName in ['TriMet_Routes/tm_routes.shp',
                 'Portland_Emergency_Transportation_Routes/Portland_Emergency_Transportation_Routes.shp',
                 'Street_Lights/Street_Lights.shp',
                 'Recommended_Bicycle_Routes/Recommended_Bicycle_Routes.shp'
                 ]:
    absFilepath = "H:/Noise/LUR/PredictorData/" + fileName
    if(not(os.path.exists(outputFiabsFilepathlepath))):
        print("filepath for %s does not exist" %(fileName))
    INPUT_PREDICTORS.append(absFilepath)

BUFFER_SIZES = [10,20,20,20]
N_CPUS = 12

# distance to each predictor variable is stored in a seperate folder
OUTPUT_FOLDERS = []
for name in ['sl','er','bi','tm']:
    OUTPUT_FOLDERS.append("F:/Noise/nearMisc/" + name + "/")

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

# thanks to user 'sth' on Stack Overflow
# https://stackoverflow.com/questions/2030053/how-to-generate-random-strings-in-python
# create a random sequence of characters.  This is used to ensure in memory datasets for each thread has a unique name
# INPUTS:
#    length (int) - number of characters in the randomly generated sequence
# OUTPUTS:
#    randomly generated string of length (length)
def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

# calculate distance from points to a single predictor variable
# INPUTS:
#    pointsShapefile (str) - relative name of shapefile located in the INPUT_FOLDER
#    nearShapefile (str) - absolute filepath to predictor variable shapefile
#    outputFolder (str) - absolute folderpath where results will be stored in .csv format
#    bufferSize (int) - maximum allowable distance between points and predictor variables
def generateNearTableSingle(pointsShapefile,nearShapefile,outputFolder,outputFile,bufferSize):

    # check if the shapefile has already been completely processed (each shapefile contains 1000 points)
    # if so, return to prevent redundant processing
    if(os.path.exists(outputFile)):
        nearData = ps.read_csv(outputFile)
        if(999 in list(set(nearData['IN_FID']))):
            print("%s has already been processed" %(pointsShapefile))
            return

    # calculate distance from points to predictor variable
    nearFeatures = "in_memory/fc" + randomword(10)
    arcpy.CopyFeatures_management(nearShapefile, nearFeatures)
    a = arcpy.analysis.GenerateNearTable(
        in_features=INPUT_FOLDER + pointsShapefile,
        near_features=nearFeatures,
        out_table=outputFolder + outputFile,
        search_radius=str(bufferSize) + ' Meters',
        location="NO_LOCATION",
        angle="NO_ANGLE",
        closest="ALL",
        closest_count='#',
        method="GEODESIC",
        distance_unit="Meters"
    )

    # clean up to prevent memory leaks
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

# calcualte distance form points to all predictor variables
# INPUTS:
#    pointsFile (str) - relative name of shapefile located in the INPUT_FOLDER
def genAllNearTables(pointsFile):

    # for each predictor variable, calculate distance between points and the predictor variable
    for index in range(len(BUFFER_SIZES)):
        predictor = INPUT_PREDICTORS[index]
        bufferSize = BUFFER_SIZES[index]
        outputFolder = OUTPUT_FOLDERS[index]
        outputFile = pointsFile[:-3] + "csv"
        if not(os.path.exists(outputFolder + outputFile)):
            generateNearTableSingle(pointsFile,predictor,outputFolder,outputFile,bufferSize)


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
    res = pool.map_async(genAllNearTables,filesToProcess)
    res.get()