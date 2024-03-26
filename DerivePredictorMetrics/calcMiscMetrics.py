# calcMiscMetrics.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: given preprocessed variable datasets, calculate buffer variables 
#          used as inputs in the land use regression model


# import libraries 
from multiprocessing import Pool
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as ps

# define global constants
ABBREV = ['er','bi','tm','sl']
BUFFER_DISTANCES = [20,20,10,20] # each variable only has one buffer size in the LUR model (1:1 match)
MULTIPLIER = [10,10,10,1] # polyline values should be multiplied by 10 (to count as 10m of segment)
NEAR_FOLDER = "F:/Noise/nearMisc/" # contains pre-calculated values of distance from grid points to road segments
OUTPUT_FOLDER = "F:/Noise/miscMetrics/"
########## HELPER FUNCTIONS #############

# rename misc variables to conform to designated nomenclature scheme.  New variable names contain
# information about the input variable, buffer distance, distance-weighting, and statistic (e.g. mean, sum)
# INPUTS:
#   data (pandas dataframe) - dataset containing road metrics 
#   bufferDist dist (int) - buffer distance used to create metrics in the dataset
#   metricChar (char) - single character indicating whether metrics are mean, sum, or quantile
#   weightChar (char) - single character indicating whether metrics are inverse distance weighted
#   variable (char) - single character indicating what variable metrics were calcualted for (e.g. bus routes)
# OUTPUTS:
#   input dataset with column names reformated to conform to designated nomenclature scheme
def standardizeVarNomenclaturePolyline(data,bufferDist,metricChar,weightChar,variable):
    # reduce dataset to columns with grid point id and summary statistics incorrectly named as 'NEAR_FID'
    dataSubset = data[['IN_FID','NEAR_FID']]
    # each misc variable only has 2 columns: grid point id and a single metric
    varNames = ['monitor_id', variable + str(bufferDist) + metricChar + weightChar + 'o']
    dataSubset.columns = varNames
    return(dataSubset)

# calculate metrics for a single misc variable.  Each misc variable only has one metric and 
# buffer distance, greatly simplifying calculations
# INPUTS:
#    bufferDist (int) - maximum allowable threshold between grid point and variable segment
#    inData (pandas dataframe) - contains distance relationships between grid points and variables
#    variable (str) - abbreviation used to designate varible in nomenclature
#    multiplier (int) - for polyline variables, multiply by the length of the segment (10 meters)
# OUTPUTS:
#    renamedData (pandas dataframe) - derived metric for the input variable with proper nomenclature
def extractSingleBufferEstimate(bufferDist,inData,variable,multiplier):
    filteredData = inData[inData['NEAR_DIST']<=bufferDist] # filter only for roads within the treshold
    density = filteredData.groupby('IN_FID').count()*multiplier # multiply by length of polyline segment
    density = density.reset_index()
    renamedData = standardizeVarNomenclaturePolyline(density,bufferDist,'c','u',variable)
    return(renamedData)

# load data and calculate metric for a single misc feature.  
# INPUTS:
#    fileSig (str) - unique indentifier for each batch of grid points 
#    (e.g. b1000 for gird points 1000-1999)
#    featureAbbrev (str) - abbreiation used to dsignate variable in nomenclature
#    multiplier (int) - for polyline variables, multiply by the length of the segment (10m)
#    buffer (int) - maximum allowable threshold between grid point and variable segment
# OUTPUTS:
#    bufferEst (pandas dataframe) - derived metric for the input variable
def processSingleFeature(fileSig,featureAbbrev,multiplier,buffer):

    # check if near distances have been calcualted for this batch.  
    # skip if preprocessing is not yet complete
    nearTable = NEAR_FOLDER + featureAbbrev +"/" + fileSig + ".csv"
    if not(os.path.exists(nearTable)):
        print("couldn't process polyline feature %s for fileSig %s" %(featureAbbrev,fileSig))
        return
    nearData = ps.read_csv(nearTable)
    bufferEst = extractSingleBufferEstimate(buffer,nearData,featureAbbrev,multiplier)
    bufferEst = bufferEst.fillna(0)
    return(bufferEst)

# calculate misc metrics for a single batch of grid points
# INPUTS:
#    fileSig (str) - unique identifier for each batch of grid points
#    (e.g. b1000 for grid points 1000-1999)
def processSingleSig(fileSig):
    outputFile = OUTPUT_FOLDER + fileSig + ".csv"

    # check if metrics have already been calculated for this batch.
    # skip if already processed to reduce redundancy
    if(os.path.exists(outputFile)):
        return
    
    # calculate metrics for one varible to seed the dataset
    miscBuffers = processSingleFeature(fileSig,ABBREV[0],MULTIPLIER[0],BUFFER_DISTANCES[0])

    # calculate metrics for the remaining variables
    for index in range(1,len(BUFFER_DISTANCES)):
        miscBuffers = miscBuffers.merge(
            processSingleFeature(fileSig,ABBREV[index],MULTIPLIER[index],BUFFER_DISTANCES[index]),
            how='outer',on='monitor_id'
        )
    
    # grid points without varibles within the treshold have na values (e.g. no stret lights)
    # replace NAs with 0s
    miscBuffers = miscBuffers.fillna(0)
    miscBuffers.to_csv(outputFile,index=False)

####################### MAIN FUNCTION ##################
if __name__ == '__main__':

    curNum = 0
    while curNum < 6429000:
        processSingleSig('b' + str(curNum))
        print(curNum)
        curNum +=1000
    
    