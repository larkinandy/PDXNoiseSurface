# calcBldgDistance.py
# Author: Andrew Larkin
# Date Created: March 22, 2024
# Summary: Find the nearest building at each angular degreee for a large set of grid points n=6.5 million).
# To speed up computation, the grid is partitioned into subset shapefiles with 1000 points in each shapefile.  
# Subsets can then be independently processed (data parallelism)

# import libraries
from multiprocessing import Pool
import os
import time
import random
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# needed when using the ArcGIS license for a large # of parallel threads
sucessfulImport = False
while(sucessfulImport == False):
    try:
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


# define global constants
BUILDINGS = "H:/Noise/building/buildingMergedDissolve2/buildingMergedDissolve2.shp"
POINT_ANGLE_FOLDER = "J:/w2"
POINT_PARTITION_FOLDER = "J:/screenedFishnet/"
OUTPUT_FOLDER = "F:/Noise/bldgDist/"
N_CPUS = 12

########## HELPER FUNCTIONS #############

# given a single point within a grid and a single angle, determine closest building that intersects the angle
# INPUTS:
#    angleFile (str) - absolute filepath of shapefile containing polygons that represent
#                      radial angles of a point
#    monitorFile (str) - absolute filepath of shapefile containing points of interest
#    buildings (str) - absolute filepath of shapefile containing building footprints
#    outputFile (str) - absolute filepath where results should be stored in .csv format
#    fileSig (str) - nomenclature indicating which batch of points is currently being analyzed (e.g. b1000 for points 1000-1999)
def calcDistToNearestBldg(angleFile,monitorFile,monitorNum,buildings,outputFile,fileSig):
    
    monitorIndicator = 'FID_w' + str(monitorNum) # attribute field value extracted from the angleFile attribute table

    # select the current grid point from the shapefile containing 1000 grid points
    selectedGridPoint = arcpy.management.SelectLayerByAttribute(
        in_layer_or_view=monitorFile,
        selection_type="NEW_SELECTION",
        where_clause="FID = " + str(monitorNum),
        invert_where_clause=None
    )

    # select only buildings within 2000m of the selected grid point
    buildingSubset = arcpy.management.SelectLayerByLocation(
        in_layer=buildings,
        overlap_type="WITHIN_A_DISTANCE",
        select_features=selectedGridPoint,
        search_distance="2000 Meters",
        selection_type="NEW_SELECTION",
        invert_spatial_relationship="NOT_INVERT"
    )

    #intersect the selected buildings and radial angles - this assigns one or more radial angles to each building
    intersects = "in_memory/testJoin".format(fileSig)
    arcpy.analysis.Intersect([buildingSubset,angleFile], intersects, "", "", "INPUT")

    # convert the intersect analysis from a feature class to a pandas dataframe
    cols = ['FID','FID_buildingMergedDissolve2',monitorIndicator]
    intersectData = ps.DataFrame(arcpy.da.FeatureClassToNumPyArray(in_table=intersects, field_names=cols, skip_nulls=False, null_value=-99999))

    # calculate distance between selected buildings and the selected grid point
    TempTab = 'in_memory\\{}_Table'.format(fileSig)
    arcpy.analysis.GenerateNearTable(
        in_features=selectedGridPoint,
        near_features=intersects,
        out_table=TempTab,
        search_radius='#',
        location="NO_LOCATION",
        angle="NO_ANGLE",
        closest="ALL",
        closest_count='#',
        method="GEODESIC",
        distance_unit="Meters"
        )
    
    # conver the distance calculation to a pandas dataframe
    cols = ['NEAR_FID', 'NEAR_DIST']
    dists = ps.DataFrame(arcpy.da.FeatureClassToNumPyArray(in_table=TempTab, field_names=cols, skip_nulls=False, null_value=-99999))

    # join the distance and identified angle dataframes
    merged = intersectData.merge(dists,how='right',left_on='FID',right_on='NEAR_FID')
    merged.sort_values(by=['NEAR_DIST',monitorIndicator],ascending=True,inplace=True)
    merged.drop_duplicates(subset=[monitorIndicator],inplace=True)
    merged.sort_values(by=[monitorIndicator],ascending=True,inplace=True)
    merged['angle'] = merged[monitorIndicator]
    merged['dist'] = merged['NEAR_DIST']
    merged = merged[['angle','dist']]

    # for angles without any buildings within 2000m, assign a default value of 2000
    filler = ps.DataFrame({
        'angle': [x for x in range(360)],
        'dist':[2000 for x in range(360)]
    })
    merged = ps.concat([merged,filler])
    merged.sort_values(by=['dist','angle'],ascending=True,inplace=True)
    merged.drop_duplicates(subset=['angle'],inplace=True)
    merged.sort_values(by=['angle'],ascending=True,inplace=True)
    merged['monitor'] = [monitorNum for x in range(360)]

    # arcpy for some reason can hang onto intermediate files even after the function instance is closed. 
    # prevent memory leaks by explicitly deleting intermediate files
    arcpy.management.Delete(TempTab)
    arcpy.management.Delete(intersects)
    arcpy.management.Delete(selectedAirMonitor)
    arcpy.management.Delete(buildingSubset)

    # save distance and angle information to csv
    merged.to_csv(outputFile,index=False)

# given an absolute folder filepath, find files within the folder
# with a 'shp' extension, and return the absolute filepaths to 'shp' files
# INPUTS:
#    folder (str) - absolute filepath to the folder containing the shp files
# OUTPUTS:
#    an array of absolute filepaths to 'shp' files within the folder
def findShapefiles(folder):
    candidateFiles = os.listdir(folder)
    screenedFiles = []
    for file in candidateFiles:
        if file[-3:] == 'shp':
            screenedFiles.append(folder + "/" + file)
    return(screenedFiles)

# given a shapefile with 1000 points, calculate distance to nearest building for each radial angle,
# for each point in the shapefile
# INPUTS:
#    fileSig (str) - unique identifier for each shapefile in a set of shapefiles, all contained within 
#                    the same folder (e.g. b1000 for points 1000-1999)
def calcDistToNearestBldgSig(fileSig):

    # each point in the shapefile has a second shapefile containing triangle polygons, one polygon for
    # each radial angle
    shpFiles = findShapefiles(POINT_ANGLE_FOLDER + "/" + fileSig)

    # shapefile containing the 1000 grid points
    monitorFile =  POINT_PARTITION_FOLDER + fileSig + ".shp"
    outputFolder = OUTPUT_FOLDER + fileSig
    if not(os.path.exists(outputFolder)):
        os.mkdir(outputFolder)

    # for each of the 1000 grid points, identify the buildings for each radial angle, and calculate
    # distance to nearest building within each angle
    for index in range(1000):

        # print progress indicator to screen for every 100 points (10%) that are processed
        if(index%100==0):
            print("processing monitor %i for file Sig %s" %(index,fileSig))

        angleFile = POINT_PARTITION_FOLDER + "/" + fileSig + "/w" + str(index) + ".shp"
        if(angleFile in shpFiles):
            outputFile = outputFolder + "/bldg" + str(index) + ".csv"
            if not(os.path.exists(outputFile)):
                calcDistToNearestBldg(
                    angleFile,
                    monitorFile,
                    index,
                    BUILDINGS,
                    outputFile,
                    fileSig
                )
        
####################### MAIN FUNCTION ##################

if __name__ == '__main__':

    # get list of point subset shapefiles
    fileSigs = os.listdir(POINT_PARTITION_FOLDER)

    # randomly shuffle.  If errors are uncountered and pools terminate early, this helps spread the workload 
    # uniformly across cpus when the error is corrected and the script is restarted
    random.shuffle(fileSigs)

    # create a pool of workers, one worker for each free CPU.  I wouldn't recommend going above the CPU
    # count via hyperthreading, arcpy performance doesn't seem to work well when worker count goes above
    # physical core count
    pool = Pool(processes=N_CPUS)
    res = pool.map_async(calcDistToNearestBldgSig,fileSigs)
    res.get()