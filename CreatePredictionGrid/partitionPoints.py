# partitionPoints.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: Partition a grid of points into 1000 point subsets.
#          Purpose is to facilitate downstream data parallelism

# import libraries
import arcpy
import os
arcpy.env.overwriteOutput=True

# define global constants
PARENT_FOLDER = "H:/Noise/implementation/"
GDB = PARENT_FOLDER + "fishnet.gdb/"
OUTPUT_FOLDER = PARENT_FOLDER + "screenedFishnet/"
nearFolder = PARENT_FOLDER + "near/"
N_POINTS = 6428930 # number of points in grid
BATCH_SIZE = 1000 # max number of points for each subset
########## HELPER FUNCTIONS #############

# copy 1000 sequential points from the fishnet and save as a new shapefile
# INPUTS:
#    FID_Min (int) - lower bound (inclusive) of the point id range to include in the subset
#    FID_Max (int) - upper bound (exclusive) of the point id range to include in the subset
def partitionFishnet(FID_Min,FID_Max):
    whereClause = "OBJECTID >= " + str(FID_Min) + " AND OBJECTID < " + str(FID_Max)
    a = arcpy.conversion.FeatureClassToFeatureClass(
        GDB + "fishnetWithoutRiverBuildingRoadCity", 
        OUTPUT_FOLDER,
        whereClause
    )

########## MAIN FUNCTION #############
if __name__ == '__main__':
    minVal = 0 # index starts at 0
    while minVal < N_POINTS:
        # arcpy doesn't like files that start with a number.  b is short for 'batch'
        outputFile = 'b' + str(minVal) + '.shp' 
        if not(os.path.exists(OUTPUT_FOLDER + outputFile)):
            partitionFishnet(minVal,minVal + BATCH_SIZE,outputFile)
        minVal +=BATCH_SIZE