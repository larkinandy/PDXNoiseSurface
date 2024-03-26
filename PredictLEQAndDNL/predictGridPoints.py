import os
import arcpy
import pandas as ps

MISC_BUFFER_FOLDER = "F:/Noise/miscMetrics/"
ROAD_BUFFER_FOLDER = "H:/Noise/implementation/pcca700mdp/"
SHIELDING_BUFFER_FOLDER = "Z:/Noise/shieldingBuffers/"
NDVI_SHAPEFILE_FOLDER = "H:/Noise/implementation/screenedFishnet/"

def getFinishedSigs():
    finishedSigs = []
    shieldedCSVS = os.listdir(SHIELDING_BUFFER_FOLDER)
    for csv in shieldedCSVS:
        finishedSigs.append(csv[:-4])
    return(finishedSigs)

def combineCSVs(fileSig):
    sigData = ps.read_csv(MISC_BUFFER_FOLDER + fileSig + ".csv")
    sigData = sigData.merge(ps.read_csv(ROAD_BUFFER_FOLDER + fileSig + ".csv"),
                            how = 'inner', on='monitor_id')
    sigData = sigData.merge(ps.read_csv(SHIELDING_BUFFER_FOLDER + fileSig + ".csv"),
                            how='inner',on='monitor_id')
    sigData = sigData.fillna(0)
    return(sigData)

def processFileSig(fileSig):
    csvData = combineCSVs(sigsToProcess[0])
    print(csvData.head())

####################### MAIN FUNCTION ##################
if __name__ == '__main__':

    sigsToProcess = getFinishedSigs()
    print("found %i grid point batches to process" %(len(sigsToProcess)))

    processFileSig(sigsToProcess[0])
    