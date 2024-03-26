# calcRdAngleParallel.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: For each point in a grid, calculate the angular relationship between
#          the point and all 10m road segments within 2km

# import libraries
from multiprocessing import Pool
import os
import time
import math 
import numpy as np
import random
import pandas as ps
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# define global constants
ANGLE_PARENT_FOLDER = "E:/Noise/wind/"
OUTPUT_PARENT_FOLDER = "E:/Noise/rdAngle/"
OUTPUT_BACKUP_FOLDER = "Y:/Noise/rdAngle/" # needed because files were too large to store on a single drive
GRID_FOLDER = "D:/Noise/screenedFishnet/"
ROAD_NETWORK = "D:/Noise/Roads/PDX10m.shp"

# needed when using the ArcGIS license for a large # of parallel threads
sucessfulImport = False
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

# calculate angles between a single monitor and all road segments within 2km
# INPUTS:
#    monitorNum (int) - monitor index within the shapefile (e.g. 1,2,...999)
#    monitorShp (str) - absolute filepath to shapefile containing grid points
#    roadFile (str) - absolute filepath to shapefile containing road network
#    angleFile (str) - absolute filepath to shapefile containing angular degrees
#                      radiating outward from the selected grid point
#    outputFile (str) - absolute filepath where results should be stored in .csv format
#    fileSig (str) - unique indicator for the shapefile 
#                    (e.g. b1000 for shapefile containing gird points 1000-1999)
def calcRoadAngle(pointNum,monitorShp,roadFile,angleFile,outputFile,fileSig):

    # unique identifier in attribute tables for the selected point
    pointIndicator = 'FID_w' + str(pointNum)

    # select the point of interest from the grid shapefile
    selectedPoint = arcpy.management.SelectLayerByAttribute(
        in_layer_or_view=monitorShp,
        selection_type="NEW_SELECTION",
        where_clause="FID = " + str(pointNum),
        invert_where_clause=None
    )

    # restrict roads to within 2km of the selected point
    roadSubset = arcpy.management.SelectLayerByLocation(
        in_layer=roadFile,
        overlap_type="WITHIN_A_DISTANCE",
        select_features=selectedPoint,
        search_distance="2000 Meters",
        selection_type="NEW_SELECTION",
        invert_spatial_relationship="NOT_INVERT"
    )

    # overlap road segments with angles radiating outward from grid point.
    # this gets the angle of each road segment relative to the grid point
    intersects = "in_memory/RdJoin".format(fileSig)
    arcpy.analysis.Intersect([roadSubset,angleFile], intersects, "", "", "INPUT")
    
    # convert the columns of interest to a pandas dataframe 
    cols = ['osm_id','FID_PDX10m',pointIndicator]
    rdAngles = ps.DataFrame(arcpy.da.FeatureClassToNumPyArray(in_table=intersects, field_names=cols, skip_nulls=False, null_value=-99999))
    rdAngles.sort_values(by=[pointIndicator,'FID_PDX10m'],inplace=True)
    rdAngles.rename(columns={pointIndicator:'angle'},inplace=True)
    rdAngles.to_csv(outputFile,index=False)

    # clean up. Can't count on arcpy to release intermediates after exiting the function
    arcpy.management.Delete(roadSubset)
    arcpy.management.Delete(selectedPoint)
    arcpy.management.Delete(intersects)

# claculate angles between road segments and all grid points in a shapefile
def processSingleSig(fileSig):
    print("processing fileSig %s" %(fileSig))
    angleFolder = ANGLE_PARENT_FOLDER+ fileSig + "/"
    outputFolder = OUTPUT_PARENT_FOLDER + fileSig + "/"
    pointsShp = GRID_FOLDER + fileSig + ".shp"
    if not os.path.exists(outputFolder):
        os.mkdir(outputFolder)

    # each shapefile contains up to 1000 grid points.
    # for each point, calculate the angles between nearby roads and the point
    for monitorNum in range(1000):
        windFile = angleFolder + "w" + str(monitorNum) + ".shp"
        outputFile = outputFolder + "a" + str(monitorNum) + ".csv" 
        fileBackup = OUTPUT_BACKUP_FOLDER + fileSig + "/" + str(monitorNum) + ".csv"

        # if the monitor has already between processed, continue to the next monitor to avoid
        # redundant processing
        if os.path.exists(windFile) and not os.path.exists(outputFile):
            if not(os.path.exists(fileBackup)):
                calcRoadAngle(monitorNum,pointsShp,ROAD_NETWORK,windFile,outputFile,fileSig)
    print("completed processing filesig %s" %(fileSig))



####################### MAIN FUNCTION ##################
if __name__ == '__main__':

    # get list of point subset shapefiles
    fileSigs = os.listdir(ANGLE_PARENT_FOLDER)

    # randomly shuffle.  If errors are uncountered and pools terminate early, this helps spread the workload 
    # uniformly across cpus when the error is corrected and the script is restarted
    random.shuffle(fileSigs)

    # create a pool of workers, one worker for each free CPU.  I wouldn't recommend going above the CPU
    # count via hyperthreading, arcpy performance doesn't seem to work well when worker count goes above
    # physical core count
    pool = Pool(processes=50)
    res = pool.map_async(processSingleSig,fileSigs)
    res.get()