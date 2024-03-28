# predictGridPointsLEQ.py
# Author: Andrew Larkin
# Date Created: March 25th, 2024
# Summary: predict LEQ values and contributions from each predictor variable
#          using a previously developed linear regression model

# import libraries
import os
import arcpy
import pandas as ps # in my version of arcpy, pandas needs to be imported after arcpy

# define global constants
MISC_BUFFER_FOLDER = "F:/Noise/miscMetrics/"
ROAD_BUFFER_FOLDER = "H:/Noise/implementation/pcca700mdp/"
SHIELDING_BUFFER_FOLDER = "Z:/Noise/shieldingBuffers/"
NDVI_SHAPEFILE_FOLDER = "H:/Noise/implementation/screenedFishnet/"
OUTPUT_FOLDER = "H:/Noise/implementation/predictions/"
JOIN_FOLDER_QD = "Z:/Noise/joinTableQuickDraw/"
JOIN_FOLDER_BE = "Z:/Noise/joinTableBeasty/"

# linear regression model varaibles and coefficients
VARIABLES = ['shpche800mup','ushpcca50mup','ushemme1200mup','ushpche1200sup',
             'ushsped250qur','ushpcca10mut','ushsped450sut','ushpcca2000mdt',
             'ushvefr20mua','pcca700mdp','sl20cuo','er20cuo',
             'bi20cuo','tm10cuo','nd10m','nd450m']
COEF = [3.911e-01,8.237e-02,9.087e-09,6.740e-04,
        1.119e-01,3.570e-02,7.669e-04,1.160e+01,
        2.703e-04,8.448e+00,-1.102e+00,3.107e-02,
        1.608e-02,1.271e-02,-1.140e+01,-1.382e+01]
INTERCEPT = 5.487e+01 # linear regression model intercept

########## HELPER FUNCTIONS #############

# identify which grid points have all predictor variables derived 
# OUTPUTS:
#    array of filenames for grid points that are ready for LEQ prediction
def getFinishedSigs():
    finishedSigs = []
    shieldedCSVS = os.listdir(SHIELDING_BUFFER_FOLDER)
    for csv in shieldedCSVS:
        if(os.path.exists(MISC_BUFFER_FOLDER + csv) and os.path.exists(ROAD_BUFFER_FOLDER + csv)):
            finishedSigs.append(csv[:-4])
    return(finishedSigs)

# load data into memory for a batch of grid points
# INPUTS:
#    fileSig (str) - unique identifier for the batch of grid points
#    (e.g. b1000 for grid points 1000-1999)
# OUTPUTS:
#    sigData (pandas dataframe) - variables and metadata needed to predict and geoference LEQ
def loadData(fileSig):
    sigData = ps.read_csv(MISC_BUFFER_FOLDER + fileSig + ".csv")
    sigData = sigData.merge(ps.read_csv(ROAD_BUFFER_FOLDER + fileSig + ".csv"),
                            how = 'outer', on='monitor_id')
    
    sigData = sigData.merge(getNDVIShapefileVals(fileSig),
                            how='outer',on='monitor_id')

    sigData = sigData.merge(processShieldData(SHIELDING_BUFFER_FOLDER + fileSig + ".csv",fileSig),
                            how='outer',on='FID_1')
    sigData = sigData.fillna(0)
    return(sigData)

# unpack nested arrays in a tuple
# INPUTS:
#    inTuple (tuple) - contains 4 values, the 2nd value needs to be unpacked
# OUTPUTS:
#    unpacked tuple
def unpackTuple(inTuple):
     return((inTuple[0],inTuple[1],inTuple[2][0],inTuple[2][1],inTuple[3],inTuple[4]))

# exact NDVI metrics and lat/long coordinates from a batch of grid points shapefile
# INPUTS:
#    fileSig (str) - unique identifier for the batch of grid points
#    (e.g. b1000 for grid points 1000-1999)
# OUTPUTS:
#    df (pandas dataframe) - contains NDVI metrics, unique identifier (FID), 
#    and lat/long coords for all grid points in the shapefile
def getNDVIShapefileVals(fileSig):
        inFilepath = NDVI_SHAPEFILE_FOLDER + fileSig + ".shp"
    
        tempData  = arcpy.da.FeatureClassToNumPyArray(in_table=inFilepath, field_names=['FID','FID_1','Shape','nd450m','nd10m'], skip_nulls=False, null_value=-99999)        
        tempData = list(map(unpackTuple,tempData))
        df = ps.DataFrame(tempData,columns=['monitor_id', 'FID_1','longitude','latitude', 'nd450m','nd10m'])
        return(df)

# predict LEQ and contributions of each predictor variable using the linear regression model 
# INPUTS:
#    predictorData (pandas dataframe) - contains predictor variables and metadata for each grid point
# OUTPUTS:
#    predictorData dataframe, appended with predicted LEQ and contributions of each predictor
#    variable
def genPredictions(predictorData):
    index = 0
    nPoints = predictorData.count()[0]
    predictorData['LEQ'] = [INTERCEPT for x in range(nPoints)]
    for index in range(len(VARIABLES)):
        predName = 'x' + str(index) 
        predictorData[predName] = predictorData[VARIABLES[index]]*COEF[index]
        if(VARIABLES[index] in (['nd10m','nd450m'])):
           predictorData[predName] = predictorData[predName].clip(upper=0)
        predictorData['LEQ'] += predictorData[predName]
    predictorData['LEQ'] = predictorData['LEQ'].round(0).astype(int)
    return(predictorData)
    
# load data and predict LEQ for a batch of grid points
# INPUTS:
#    fileSig (str) - unique identifier for the batch of grid points
#    (e.g. b1000 for grid points 1000-1999)
# OUTPUTS:
#    modelPredictions (pandas dataframe) - predicted LEQ and data needed to geofrernce predictions
def processFileSig(fileSig):
    predictorData = loadData(fileSig)
    modelPredictions = genPredictions(predictorData)
    modelPredictions['batch'] = [fileSig for x in range(modelPredictions.count()[0])]
    return(modelPredictions)

# a temporary fix to correct differences between shapefiles on 2 different worksattions.  Not 
# required for future studies
# map grid point ids on workstation 1 to grid ids on workstation2
def joinStations(fileSig):
    qd = ps.read_csv(JOIN_FOLDER_QD + fileSig + ".csv")
    qd = qd[['OID_','FID_1']]
    be = ps.read_csv(JOIN_FOLDER_BE + fileSig + ".csv")
    be['FID_be'] = [x for x in range(be.count()[0])]
    joined = qd.merge(be,how='inner',on='FID_1')
    return(joined)

# load shield metrics into memory.  A temporary fix to correct differences in shapefiles
# future studies can read the data directly into memory
def processShieldData(shieldFile,sig):
    shieldData = ps.read_csv(shieldFile)
    joinMatch = joinStations(sig) # a temporary fix, not required in future studies
    shieldData = shieldData.merge(joinMatch,how='inner',left_on='monitor_id',right_on='FID_be')
    shieldData.drop(columns=['monitor_id'],inplace=True)
    shieldData.to_csv("C:/users/larki/Desktop/temp.csv",index=False)
    return(shieldData)

####################### MAIN FUNCTION ##################
if __name__ == '__main__':

    # get list of grid point batches that are ready to be analyzed
    sigsToProcess = getFinishedSigs()
    print("found %i grid point batches to process" %(len(sigsToProcess)))

    # for each grid point batch, predict LEQ.
    dfArr = []
    for fileSig in sigsToProcess:
        dfArr.append(processFileSig(fileSig))

    # combine batches and save to disk
    df = ps.concat(dfArr)
    df.to_csv(OUTPUT_FOLDER + "LEQ.csv",index=False)
    