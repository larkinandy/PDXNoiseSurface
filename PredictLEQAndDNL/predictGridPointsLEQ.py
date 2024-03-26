import os
import arcpy
import pandas as ps

MISC_BUFFER_FOLDER = "F:/Noise/miscMetrics/"
ROAD_BUFFER_FOLDER = "H:/Noise/implementation/pcca700mdp/"
SHIELDING_BUFFER_FOLDER = "Z:/Noise/shieldingBuffers/"
NDVI_SHAPEFILE_FOLDER = "H:/Noise/implementation/screenedFishnet/"
OUTPUT_FOLDER = "H:/Noise/implementation/predictions/"

VARIABLES = ['shpche800mup','ushpcca50mup','ushemme1200mup','ushpche1200sup',
             'ushsped250qur','ushpcca10mut','ushsped450sut','ushpcca2000mdt',
             'ushvefr20mua','pcca700mdp','sl20cuo','er20cuo',
             'bi20cuo','tm10cuo','nd10m','nd450m']
COEF = [3.911e-01,8.237e-02,9.087e-09,6.740e-04,
        1.119e-01,3.570e-02,7.669e-04,1.160e+01,
        2.703e-04,8.448e+00,-1.102e+00,3.107e-02,
        1.608e-02,1.271e-02,-1.140e+01,-1.382e+01]
INTERCEPT = 5.487e+01

def getFinishedSigs():
    finishedSigs = []
    shieldedCSVS = os.listdir(SHIELDING_BUFFER_FOLDER)
    for csv in shieldedCSVS:
        finishedSigs.append(csv[:-4])
    return(finishedSigs)

def loadData(fileSig):
    sigData = ps.read_csv(MISC_BUFFER_FOLDER + fileSig + ".csv")
    sigData = sigData.merge(ps.read_csv(ROAD_BUFFER_FOLDER + fileSig + ".csv"),
                            how = 'outer', on='monitor_id')
    sigData = sigData.merge(ps.read_csv(SHIELDING_BUFFER_FOLDER + fileSig + ".csv"),
                            how='outer',on='monitor_id')
    sigData = sigData.fillna(0)
    sigData = sigData.merge(getNDVIShapefileVals(fileSig),
                            how='inner',on='monitor_id')
    return(sigData)

def unpackTuple(inTuple):
     return((inTuple[0],inTuple[1][0],inTuple[1][1],inTuple[2],inTuple[3]))

def getNDVIShapefileVals(fileSig):
        inFilepath = NDVI_SHAPEFILE_FOLDER + fileSig + ".shp"
        cols = field_names = [f.name for f in arcpy.ListFields(inFilepath)]
        tempData  = arcpy.da.FeatureClassToNumPyArray(in_table=inFilepath, field_names=['FID','Shape','nd450m','nd10m'], skip_nulls=False, null_value=-99999)        
        tempData = list(map(unpackTuple,tempData))
        df = ps.DataFrame(tempData,columns=['monitor_id', 'longitude','latitude', 'nd450m','nd10m'])
        return(df)

def genPredictions(predictorData):
    index = 0
    print(len(VARIABLES))
    print(len(COEF))
    nPoints = predictorData.count()[0]
    predictorData['LEQ'] = [INTERCEPT for x in range(nPoints)]
    for index in range(len(VARIABLES)):
        predName = 'x' + str(index) 
        print(predictorData[VARIABLES[index]]+1)
        predictorData[predName] = predictorData[VARIABLES[index]]*[COEF[index] for x in range(nPoints)]
        predictorData['LEQ'] += predictorData[predName]
    return(predictorData)
    

def processFileSig(fileSig):
    predictorData = loadData(fileSig)
    modelPredictions = genPredictions(predictorData)
    return(modelPredictions)


####################### MAIN FUNCTION ##################
if __name__ == '__main__':

    sigsToProcess = getFinishedSigs()
    print("found %i grid point batches to process" %(len(sigsToProcess)))
    dfArr = []
    for fileSig in sigsToProcess:
        dfArr.append(processFileSig(fileSig))
    df = ps.concat(dfArr)
    df.to_csv(OUTPUT_FOLDER + "LEQ.csv",index=False)
    