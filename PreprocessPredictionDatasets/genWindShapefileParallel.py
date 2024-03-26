# genWindShapefileParallel.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: For each point in a grid, generate 360 polygons representing the area
#          covered by 1 radial degree/polygon (n=360 polygons/point)

# import libraries
from multiprocessing import Pool
import os
import time
import math 
import random

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
        sr = arcpy.SpatialReference()   # WGS84 spatial reference
        sr.factoryCode = 4326
        sr.create()
        sucessfulImport = True
    except Exception as e:
        print("couldn't import arcgis: %s" %(str(e)))
        time.sleep(1)

# define global constants
PARENT_OUTPUT = "E:/Noise/wind/"
INPUT_FOLDER = "D:/Noise/screenedFishnet/"
N_CPUS = 32 # number of parallel processes
EARTH_RADIUS = 6378140.0                # used to calculate angular direction
DISTANCE = 55000.0/EARTH_RADIUS         # used to calculate bearing and angular direction

########## HELPER FUNCTIONS #############

# given an origin piont and heading, calcualte the end point for a line segment that represnts one side of a radial triangle
# INPUTS:
#    origLat (float) - origin latitude (grid point)
#    origLong (float) - origin longitude (grid point)
#    bearing (float) - direction or vector of line segment
#    distance (float) - length of line segment
# OUTPUTS:
#    newLatit (float) - latitude coordinate for the end point of the line segment
#    newLongit (float) - longitude coordinate for the end point of the line segment
def calcCoords(origLat,origLong,bearing,distance):    
    latRadians = origLat*0.0174533          # convert latitude from degrees to radians
    longRadians = origLong*0.0174533        # convert longitude from degrees to radians
    
    # calculate the new latitude coordinate for a point that is 'distance' units away using the haversine formula (see weblink above)
    newLatit = math.asin(math.sin(latRadians)*math.cos(distance) + math.cos(latRadians)*math.sin(distance)*math.cos(bearing))/0.0174533
    
    # calculate the new longitude coordinate for a point that is 'distance' units away using the haversine formula
    longitP1 = math.sin(bearing)*math.sin(distance)*math.cos(latRadians)
    longitP2 = math.cos(distance) - math.sin(latRadians)*math.sin(newLatit*0.0174533)    
    newLongit = longRadians + math.atan2(longitP1,longitP2)
    newLongit = newLongit/0.0174533
    return([newLatit,newLongit])

# create a shapefile with 360 polygons, one for each angular degree radiating outward from a grid point
# INPUTS:
#   dataTuple[0] (str) - unique identifier for each set of grid points (e.g. 'b1000' for points 1000-1999)
#   dataTuple[1] (int) - unique identifier for a single grid point within the grid point set (e.g. 1,2,3 ... 999)
#   dataTuple[2] (float) - latitude of the selected grid point
#   dataTuple[3] (float) - longitude of the selected grid point
def createRadialShapefile(dataTuple):
    outputFolder = PARENT_OUTPUT + dataTuple[0] + "/" 
    monitorIndex = dataTuple[1]
    lat = dataTuple[3]
    lon = dataTuple[2]

    # absolute filepath to where polygons will be stored
    shapefileName = outputFolder + "w" + str(monitorIndex) + ".shp"
    if os.path.exists(shapefileName):
        return
    a = arcpy.CreateFeatureclass_management(outputFolder,"w" + str(monitorIndex) + ".shp","POLYGON",'#','#','#',sr)

    # for each angular degree, create a polygon radiating outward from the grid point and store in the shapefile
    cursor = arcpy.da.InsertCursor(shapefileName, ['SHAPE@'])
    for angle in range(360):
        bearing1 = ((angle + 0.5)%360.0)*0.0174533       # center of triangle + 0.5 degrees
        bearing2 = ((angle - 0.5)%360.0)*0.0174533       # center of triangle - 0.5 degrees
        gridPointLoc = arcpy.Point(lon,lat)       # grid point, origin for the polygon
        coordP1 = calcCoords(lat,lon,bearing1,DISTANCE)  # 2nd corner of the polygon
        upPoint1 = arcpy.Point(coordP1[1],coordP1[0])
        coordP2 = calcCoords(lat,lon,bearing2,DISTANCE)  # 3rd corner of the polygon
        upPoint2 = arcpy.Point(coordP2[1],coordP2[0])

        # create a polygon from the corner points and add to the shapefile
        array = arcpy.Array([gridPointLoc,upPoint1,upPoint2])
        polygon = arcpy.Polygon(array,sr)
        rowVals = tuple([polygon])
        a = cursor.insertRow(rowVals)

    # cleanup
    del cursor

# get the coordinates for all points in a shapefile
# INPUTS:
#    inputShapefile (str) - absolute filepath to a shapefile
# OUTPUTS:
#    an array of tuples.  Each tuple contains:
#        tuples[0] (str) - unique identifier corresponding to shapefile name
#                          (e.g. b1000 for shapefile containing grid points 1000-1999)
#        tuples[1] (int) - unique identifier for grid point within batch (e.g. 1,2,...999)
#        tuples[2] (float) - grid point latitude
#        tuples[3] (float) - gird point longitude
def getCoords(inputShapefile):
    fields = ['FID', 'SHAPE@XY']
    tuples = []
    batchSig = inputShapefile[inputShapefile.rfind('/')+1:-4]
    if not os.path.exists(PARENT_OUTPUT + batchSig):
        os.mkdir(PARENT_OUTPUT + batchSig)
    with arcpy.da.SearchCursor(inputShapefile, fields) as cursor:
        for row in cursor:
            tuples.append((batchSig,row[0],row[1][0],row[1][1]))
    return(tuples)

# create shapefiles capturing radial angles for all grid points in a points shapefile
# INPUTS:
#    shapefile (str) - relative filepath to shapefile containing grid points
def createAnglesForSingleShp(shapefile):

    # for each grid point in the shapefile, extract details needed to create radial angles
    dataTuples = getCoords(INPUT_FOLDER + shapefile)

    # create radial angles for each grid point (one tuple for each grid point)
    for tupleVal in dataTuples:
        createRadialShapefile(tupleVal)

# find files with 'shp' extension in a folder
# INPUTS:
#    folder (str) - absoluste filepath to folder that contains shapefiles
# OUTPUTS:
#    list of filenames for shapefiles in the folder
def findShapefiles(folder):
    candidateFiles = os.listdir(folder)
    screenedFiles = []
    for file in candidateFiles:
        if file[-3:] == 'shp':
            screenedFiles.append(file)
    return(screenedFiles)

####################### MAIN FUNCTION ##################
if __name__ == '__main__':

    # get list of point subset shapefiles
    screenedShapefiles = findShapefiles(INPUT_FOLDER)
    print("number of shapefiles found: %i" %(len(screenedShapefiles)))
    
    # randomly shuffle.  If errors are uncountered and pools terminate early, this helps spread the workload 
    # uniformly across cpus when the error is corrected and the script is restarted
    random.shuffle(screenedShapefiles)
  
    # create a pool of workers, one worker for each free CPU.  I wouldn't recommend going above the CPU
    # count via hyperthreading, arcpy performance doesn't seem to work well when worker count goes above
    # physical core count
    pool = Pool(processes=N_CPUS)
    res = pool.map_async(createAnglesForSingleShp,screenedShapefiles)
    res.get()
