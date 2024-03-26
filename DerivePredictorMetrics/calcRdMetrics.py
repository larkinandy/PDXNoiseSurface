# calcRdMetrics.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: given preprocessed variable datasets, calculate buffer variables 
#          used as inputs in the land use regression model


# import libraries 
import pandas as ps
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

OUTPUT_FOLDER = "H:/Noise/implementation/pcca700mdp/"
BUFFER_DISTANCES = [700] # only the 700 meter buffer is used in this script.  More buffer distances 
                         # are required for the script that calculates shield-modified road metrics
ROADS = "H:/Noise/LUR3/PDX10m.csv" # road network partitioned into 10m segments
NEAR_FOLDER = "F:/Noise/near/" # contains pre-calculated values of distance from grid points to road segments

########## HELPER FUNCTIONS #############

# normalize input data by distance to monitor 
# INPUTS:
#    roadMetrics (pandas dataframe) - contains road metrics to normalize, as well as distnace to monitor
# OUTPUTS:
#     input data, with selected variables updated with normalization
def normalizeByDistance(roadMetrics):
    roadMetrics['PctCars'] = roadMetrics['PctCars']/roadMetrics['NEAR_DIST']
    return(roadMetrics)

# rename road variables to conform to designated nomenclature scheme.  New variable names contain
# information about the input variable, buffer distance, distance-weighting, and statistic (e.g. mean, sum)
# INPUTS:
#   data (pandas dataframe) - dataset containing road metrics 
#   buffer dist (int) - buffer distance used to create metrics in the dataset
#   metricChar (char) - single character indicating whether metrics are mean, sum, or quantile
#   weightChar (char) - single character indicating whether metrics are inverse distance weighted
#   roadType (char) - single character indicating what road network subset the metrics were calculated
#                     for (e.g. primary roads, residential roads, etc.)
# OUTPUTS:
#   input dataset with column names reformated to conform to designated nomenclature scheme
def standardizeVarNomenclature(data,bufferDist,metricChar,weightChar,roadType):
    dataSubset = data[['IN_FID','PctCars']]

    # unique identifier for each point
    varNames = ['monitor_id']

    # only one road metric is used in the land use regression model.  However, many 'shield' modified road 
    # networks are used and processed using the same nomenclature function in a seperate script.
    for prefix in ['pcca']:
        varNames.append(prefix + str(bufferDist) + metricChar + weightChar + roadType)
    
    # rename columns to conform to designated nomenclature 
    dataSubset.columns = varNames
    return(dataSubset)

# calculate road metrics for a single buffer distance 
# INPUTS:
#    bufferDist (int) - maximum allowable distance between grid point and road segements to
#                       include in metric
#    roadData (pandas dataframe) - preprocessed road data used to calculate road metrics
#    roadType (char) - single character indicator what road network subset the metrics will be calcualted
#                      for (e.g. primary roads, residential roads, etc.)
# OUTPUTS:
#    calculated road metrics in pandas dataframe format
def extractSingleBufferEstimate(bufferDist,roadData,roadType):
    
    # restrict road data to road segments within the buffer radius
    filteredData = roadData[roadData['NEAR_DIST']<=bufferDist]

    # calculate mean metrics from the road data and normalize by distance to grid points
    meanNorm = normalizeByDistance(filteredData).groupby('IN_FID').mean()
    meanNorm = meanNorm.reset_index()

    # rename columns to conform to designated nomenclature
    combData = standardizeVarNomenclature(meanNorm,bufferDist,'m','d',roadType)
    return(combData)

# calculate road metrics for all buffer distances of interest
# INPUTS:
#    bufferDistances (array int) - contains a list of all buffer distances of interest
#    nearDist (pandas dataframe) - distance from grid points to road segments
#    roadMeasures (pandas dataframe) - variables for each road segment (e.g. speed, pavement type)
#    roadType (char) - single character indicator what road network subset the metrics will be calcualted
#                      for (e.g. primary roads, residential roads, etc.)
# OUTPUTS:
#    bufferEst (pandas dataframe) - calculated road metrics for all buffer distances
def extractBufferEstimatesForRoads(bufferDistances,nearDist,roadMeasures,roadType):
    stationMeasures = nearDist.merge(roadMeasures,how='inner',left_on='NEAR_FID',right_on='OID_')
    bufferEst = extractSingleBufferEstimate(bufferDistances[0],stationMeasures,roadType)
    for buff in bufferDistances[1:]:
        bufferEst = bufferEst.merge(extractSingleBufferEstimate(buff,stationMeasures,roadType),how='outer',on='monitor_id')
    bufferEst = bufferEst.fillna(0)
    return(bufferEst)

# process dataset containing distances from grid points to road segments
# needed to avoid issues when distance weighting by values less than 1
# INPUTS:
#    nearFile (pandas dataframe) - distance from grid points to road segments
# OUTPUTS:
#    nearFile with all distances less than 1 rounded up to 1 meter
def processNearData(nearFile):
    nearData = ps.read_csv(nearFile)
    nearData1 = nearData[nearData['NEAR_DIST']>=1]
    nearData2 = nearData[nearData['NEAR_DIST']<1]
    nearData2['NEAR_DIST'] = 1
    return(ps.concat([nearData1,nearData2]))

# load road network into memory and create road classification subsets
# OUTPUTS:
#    primaryRds (pandas dataframe) - subset of primary/secondary roads
#    tertRds (pandas dataframe) - subset of tertiary/unclassified roads
#    resRds (pandas dataframe) - subset of residential roads
#    allRds (pandas dataframe) - the entire road network
def preprocessRoadData():
    roadData = ps.read_csv(ROADS)
    primaryRds = roadData[roadData['roadType']==0]
    tertRds = roadData[roadData['roadType']==1]
    resRds = roadData[roadData['roadType']==2]
    allRds = roadData[roadData['roadType']<4]
    return([primaryRds,tertRds,resRds,allRds])

# derive road metrics for a single grid point shapefile, containing 1000 grid points
# save results to csv file
# INPUTS:
#    sig (str) - unique identifier indicating which shapefile to process
#    primaryRoads (pandas dataframe) - dataset containing information about primary/secondary roads
#                                      (e.g. speed, pavement type)
def processSingleSig(sig,primaryRoads):
    roadDistFile = NEAR_FOLDER + str(sig) + ".csv"

    # verify road distances have alraedy been preprocessed in a previous script before continuing
    if not(os.path.exists(roadDistFile)):
        print("cannot create shielding buffers for sig %s: road distances not available" %(sig))
        return
    outputFile = OUTPUT_FOLDER + str(sig) + ".csv"
    
    # if the shapefile has already been processed, return early to avoid redundant processing
    if os.path.exists(outputFile):
        return

    # prepare dataset containing distance from grid points to road segments
    nearData = processNearData(roadDistFile)

    # derive road metrics for primary/secondary roads
    primaryRds = extractBufferEstimatesForRoads(BUFFER_DISTANCES,nearData,primaryRoads,'p')

    # select only the variables used in the land use regression model and save to .csv
    primaryRds = primaryRds[["pcca700mdp",'monitor_id']]
    primaryRds.to_csv(outputFile,index=False)

# generate a list of unique indicators for each grid shapefile that needs to be processed
# OUTPUTS:
#    list of unique indicators
def generateFileSigs():
    fileSigs = []
    curNum = 0
    while (curNum < 6428001):
        fileSigs.append('b' + str(curNum))
        curNum+=1000
    return(fileSigs)


####################### MAIN FUNCTION ##################
if __name__ == '__main__':
    primaryRoads,tertiaryRoads,resRoads,allRoads = preprocessRoadData()
    fileSigs = generateFileSigs()

    # for each shapefile containing grid points, calculate the road buffer metrics
    for sig in fileSigs:
        processSingleSig(sig,primaryRoads)
        
