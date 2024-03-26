# calcIsShielding.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: determine whether every grid piont is shielded from each
#          road segment within 2000m

# import libraries
from multiprocessing import Pool
import os
import time
import random
import pandas as ps
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# define global constants
NEAR_ROADS_FOLDER =  "Y:/noise/near/"
NEAR_BLDGS_FOLDER = "E:/Noise/bldgDist/"
RD_ANGLE_FOLDER = "E:/Noise/rdAngle/"
OUTPUT_FOLDER =  "G:/Noise/shieldingBinary/"
N_CPUS = 32

########## HELPER FUNCTIONS #############

# identify which roads a single grid point is shielded from by buildings
# INPUTS:
#    roadDists (pandas ataframe) - distance from road segments to grid point
#    pointNum (int) - unique identifier for grid point within shapefile (e.g. 1,2,...999)
#    bldgFile (str) - absolute filepath to file containing data for each road segment 
#                     (e.g. speed, pavement type)
#    rdAngleFile (str) - absolute filepath to file describing the angle of each road
#                        segment relative to the the grid point indicted by pointNum
#    outputFile (str) - aboluste filepath where output should be written in csv format
def createShieldingOnePoint(roadDists,pointNum,bldgFile,rdAngleFile,outputFile):

    # subset dataset to just roads within 2000m of the grid point
    roadSubset = roadDists[roadDists['IN_FID']==pointNum]

    # load datasets containing road and building angles
    bldgShielding = ps.read_csv(bldgFile)
    rdAngle = ps.read_csv(rdAngleFile)

    # join datasets and determine if buildings within the same angle shield the grid point from 
    # roads in the same angle
    joined = roadSubset.merge(rdAngle,how='inner',left_on='NEAR_FID',right_on='FID_PDX10m')
    joined = joined.merge(bldgShielding,how='inner',on='angle')
    joined['isShielded'] = (joined['NEAR_DIST'] >= joined['dist'])*1

    # subset dataaset to just the newly derived variables and save to csv
    joined = joined[['isShielded','monitor','FID_PDX10m']]
    joined.to_csv(outputFile,index=False)

# for all grid points in a shapefile, determine which roads each grid point is shielded from
# INPUTS:
#    fileSig (str) - unique identifier corresponding to the shapefile name and grid coverage
#                    (e.g. b1000 corresponds to grid points 1000-1999)
def processSingleFileSig(fileSig):

    # distances to roads within 2000m of these specific grid points
    roadFile = NEAR_ROADS_FOLDER + fileSig + ".csv"
    outputFolder =  OUTPUT_FOLDER + fileSig + "/" 

    if not os.path.exists(outputFolder):
        os.mkdir(outputFolder)

    # do not process is distance to roads has not yet been calculated 
    if not(os.path.exists(roadFile)):
        print("can't calculate binary shielding for fileSig %s: dist to road not available" %(fileSig))
        return

    roadDists = ps.read_csv(roadFile)

    # for each grid point in the shapefile (n=1000), determine which roads the grid point is shielded from
    for pointNum in range(1000):
        outputFile = outputFolder + "sh" + str(pointNum) + ".csv"

        # if grid point has already been processed, skip to the next grid point
        if not os.path.exists(outputFile):
            buildingShiledingFile = NEAR_BLDGS_FOLDER + str(fileSig) + "/bldg" + str(pointNum) + ".csv"

            # do not process is distance to buildings has not yet been calcualted
            if not(os.path.exists(buildingShiledingFile)):
                print("can't process point %i for fileSig %s: building shielding is not available" %(pointNum,fileSig))
            else:
                rdAngleFile = RD_ANGLE_FOLDER + fileSig + "/a" + str(pointNum) + ".csv"
                
                # do not process if the radial angle of each road segment relative to grid point
                # has not yet been calcualted
                if not(os.path.exists(rdAngleFile)):
                    print("can't process point %i for fileSig %s: road angle is not available" %(pointNum,fileSig))
                else:
                    createShieldingOnePoint(roadDists,pointNum,buildingShiledingFile,rdAngleFile,outputFile)
            

####################### MAIN FUNCTION ##################

if __name__ == '__main__':

    # get list of point subset shapefiles
    fileSigs = os.listdir(NEAR_BLDGS_FOLDER)

    # randomly shuffle.  If errors are uncountered and pools terminate early, this helps spread the workload 
    # uniformly across cpus when the error is corrected and the script is restarted
    random.shuffle(fileSigs)

    # create a pool of workers and distribute shapefiles and instructions to each worker 
    pool = Pool(processes=N_CPUS)
    res = pool.map_async(processSingleFileSig,fileSigs)
    res.get()
