import pandas as ps
import os
from multiprocessing import Pool

BUFFER_DISTS = [10,20,50,250,450,1200,1400,2000]
METRICS_TO_KEEP = ['shpche800mup','ushpcca50mup','ushpche1200sup','ushpcme1400qdp',
                   'ushemme1200mup','ushsped250qur','ushpcca10mut','ushsped450sut',
                   'ushpcca2000mdt','ushvefr20mua']
ROADS = "D:/Noise/Roads/PDX10m.csv"
NEAR_FOLDER = "F:/Noise/near/"
SHIELDING_FOLDER = "G:/Noise/isShielded/"
OUTPUT_FOLDER = "G:/Noise/shielding/buffers/"


def normalizeByDistance(roadMetrics):
    dist = roadMetrics['NEAR_DIST']
    roadMetrics['speed'] = roadMetrics['speed']/dist
    roadMetrics['ADTVolume'] = roadMetrics['ADTVolume']/dist
    roadMetrics['PctCars'] = roadMetrics['PctCars']/dist
    roadMetrics['PctHeavyTr'] = roadMetrics['PctHeavyTr']/dist
    roadMetrics['PctMedTruc'] = roadMetrics['PctMedTruc']/dist
    roadMetrics['emissionHT'] = roadMetrics['emissionHT']/dist
    return(roadMetrics)


def standardizeVarNomenclature(data,bufferDist,metricChar,weightChar,roadType,isShielded):
    dataSubset = data[['IN_FID','speed','ADTVolume','PctCars','PctHeavyTr',
                       'PctMedTruc','emissionHT']]
    
    varNames = ['monitor_id']
    preprefix = 'sh'
    if(isShielded==False):
        preprefix = 'ush'
    for prefix in [
        'sped','vefr','pcca','pche','pcme','emhe']:
        varNames.append(preprefix + prefix + str(bufferDist) + metricChar + weightChar + roadType)
    dataSubset.columns = varNames
    return(dataSubset)



def extractSingleBufferEstimate(bufferDist,roadMetrics,roadType,shielding,isShielded):
    filteredData = roadMetrics[roadMetrics['NEAR_DIST']<=bufferDist]
    if(isShielded):
        filteredData = filteredData.merge(shielding,how='inner',left_on=['IN_FID','NEAR_FID'],right_on=['monitor','FID_PDX10m'])
    else:
        filteredData = filteredData.merge(shielding,how='left',left_on=['IN_FID','NEAR_FID'],right_on=['monitor','FID_PDX10m'])
        filteredData = filteredData[filteredData['isShielded'].isnull()]
    meanData = filteredData.groupby('IN_FID').mean()
    sumData = filteredData.groupby('IN_FID').sum()
    qData = filteredData.groupby('IN_FID').quantile([0.90])
    meanNorm = normalizeByDistance(filteredData).groupby('IN_FID').mean()
    #sumNorm = normalizeByDistance(filteredData).groupby('IN_FID').sum()
    qNorm = normalizeByDistance(filteredData).groupby('IN_FID').quantile([0.90])
    meanData = meanData.reset_index()
    sumData = sumData.reset_index()
    qData = qData.reset_index()
    meanNorm = meanNorm.reset_index()
    #sumNorm = sumNorm.reset_index()
    qNorm = qNorm.reset_index()
    combData = standardizeVarNomenclature(meanData,bufferDist,'m','u',roadType,isShielded)
    combData = combData.merge(standardizeVarNomenclature(meanNorm,bufferDist,'m','d',roadType,isShielded), how = 'inner',on='monitor_id')
    combData = combData.merge(standardizeVarNomenclature(sumData,bufferDist,'s','u',roadType,isShielded), how = 'inner',on='monitor_id')
    #combData = combData.merge(standardizeVarNomenclature(sumNorm,bufferDist,'s','d',roadType,isShielded), how = 'inner',on='monitor_id')
    combData = combData.merge(standardizeVarNomenclature(qData,bufferDist,'q','u',roadType,isShielded), how = 'inner',on='monitor_id')
    combData = combData.merge(standardizeVarNomenclature(qNorm,bufferDist,'q','d',roadType,isShielded), how = 'inner',on='monitor_id')
    return(combData)


def extractBufferEstimatesForRoads(bufferDistances,nearDist,roadMeasures,roadType,shielded,isShielded):
    stationMeasures = nearDist.merge(roadMeasures,how='inner',left_on='NEAR_FID',right_on='OID_')
    bufferEst = extractSingleBufferEstimate(bufferDistances[0],stationMeasures,roadType,shielded,isShielded)
    for buff in bufferDistances[1:]:
        bufferEst = bufferEst.merge(extractSingleBufferEstimate(buff,stationMeasures,roadType,shielded,isShielded),how='outer',on='monitor_id')
    bufferEst = bufferEst.fillna(0)
    return(bufferEst)


def loadIsShieldedForSig(sig):
    dfArr = []
    folderPath = SHIELDING_FOLDER + sig + "/"
    filesToMerge = os.listdir(folderPath)
    for file in filesToMerge:
        dfArr.append(ps.read_csv(folderPath + file))
    combDF = ps.concat(dfArr)
    shieldedData = combDF[combDF['isShielded']==1]
    return(shieldedData)

def checkIsShieldingComplete(sig):
    folderPath = SHIELDING_FOLDER + sig + "/"
    filesToMerge = os.listdir(folderPath)
    if(len(filesToMerge)==1000):
        return(True)
    return(False)

def processNearData(nearFile):
    nearData = ps.read_csv(nearFile)
    nearData1 = nearData[nearData['NEAR_DIST']>=1]
    nearData2 = nearData[nearData['NEAR_DIST']<1]
    nearData2['NEAR_DIST'] = 1
    return(ps.concat([nearData1,nearData2]))

def preprocessRoadData():
    roadData = ps.read_csv(ROADS)
    primaryRds = roadData[roadData['roadType']==0]
    tertRds = roadData[roadData['roadType']==1]
    resRds = roadData[roadData['roadType']==2]
    allRds = roadData[roadData['roadType']<4]
    return([primaryRds,tertRds,resRds,allRds])


def checkForFiles(sig):

    # check if distances to road have been calculated yet.  Skip this batch of grid points if they haven't
    roadDistFile = NEAR_FOLDER + str(sig) + ".csv"
    if not(os.path.exists(roadDistFile)):
        print("cannot create shielding buffers for sig %s: road distances not available" %(sig))
        return False
    
    # check if shielding metrics are complete for this set of grid points.  Skip this batch if they haven't
    shieldingReady = checkIsShieldingComplete(sig)
    if(shieldingReady == False):
        print("cannot create shielding buffers for sig %s: shielding filters are not available" %(sig))
        return False
    return True


def processSingleSig(sig):
    
    # preliminary check if scripts preprocessing data are finished for this batch of grid points
    # skip this batch is preprocessing is not yet complete
    preprocessingComplete = checkForFiles(sig)
    if(preprocessingComplete==False):
        return
    
    # load distance from gird points to nearby roads
    roadDistFile = NEAR_FOLDER + str(sig) + ".csv"
    nearData = processNearData(roadDistFile)

    # load shielding filters
    shieldedData = loadIsShieldedForSig(sig)

    # load road network. Look into if this data could be trimmed before hand in future updates
    primaryRoads,tertiaryRoads,resRoads,allRoads = preprocessRoadData()

    # extract buffer etimates for unshielded residential roads
    resRdsUnshielded = extractBufferEstimatesForRoads(BUFFER_DISTS,nearData,resRoads,'r',shieldedData,False)

    # extract buffer estimates for unshielded primary roads
    primaryUnshielded = extractBufferEstimatesForRoads(BUFFER_DISTS,nearData,primaryRoads,'p',shieldedData,False)

    # extract buffer estimates for shielded primary roads
    primaryShielded = extractBufferEstimatesForRoads([800],nearData,primaryRoads,'p',shieldedData,True)

    # extract buffer estimates for unshielded tertiary roads 
    tertUnshielded = extractBufferEstimatesForRoads(BUFFER_DISTS,nearData,tertiaryRoads,'t',shieldedData,False)

    # extract buffer estimates for unshielded all roads
    allUnshielded = extractBufferEstimatesForRoads(BUFFER_DISTS,nearData,allRoads,'a',shieldedData,False)

    # merge buffer estimates into single dataframe and save to csv
    mergedBuffers = resRdsUnshielded.merge(primaryUnshielded,how='outer',on='monitor_id')
    mergedBuffers = mergedBuffers.merge(primaryShielded,how='outer',on='monitor_id')
    mergedBuffers = mergedBuffers.merge(resRdsUnshielded,how='outer',on='monitor_id')
    mergedBuffers = mergedBuffers.merge(tertUnshielded,how='outer',on='monitor_id')
    mergedBuffers = mergedBuffers.merge(allUnshielded,how='outer',on='monitor_id')
    mergedBuffers = mergedBuffers[METRICS_TO_KEEP]
    mergedBuffers.to_csv(OUTPUT_FOLDER + str(sig) + ".csv",index=False)

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


####################### MAIN FUNCTION ##################
if __name__ == '__main__':
    
    fileSigs = processFiles(os.listdir())
    pool = Pool(processes=2)
    res = pool.map_async(processSingleSig,fileSigs)
    res.get()
