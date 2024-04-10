# createRasters.py
# Author: Andrew Larkin
# Date Created: March 27th, 2024
# Summary: given a grid of DNL and LEQ predictions, filter records 
#          and create a final raster interpolated for 

# import libraries
import arcpy
arcpy.env.overwriteOutput=True

# define global constants
HOME_FOLDER = "H:/Noise/"
PREDICTION_FOLDER = HOME_FOLDER + "implementation/predictions/"
TEMP_FOLDER = PREDICTION_FOLDER + "tempFiles.gdb/"
LEQ_POINT_FILE = PREDICTION_FOLDER + "LEQ.csv"
LEQ_SHAPEFILE = TEMP_FOLDER + "LEQ"
DNL_POINT_FILE = PREDICTION_FOLDER + "DNL.csv"
DNL_SHAPEFILE = TEMP_FOLDER + "DNL"
LEQ_RASTER_FILE = PREDICTION_FOLDER + "LEQ.tif"
DNL_RASTER_FILE = PREDICTION_FOLDER + "DNL.tif"
BUILDINGS_SHAPEFILE = HOME_FOLDER + "building/buildingMergedDissolve2/buildingMergedDissolve2.shp"
WATER_SHAPEFILE = HOME_FOLDER + "roads/oregon-latest-free.shp/gis_osm_water_a_free_1.shp"
CITY_BOUNDARY = HOME_FOLDER + "CityBoundary/Portland_City_Boundary/Portland_City_Boundary.shp"

# CRS that lat/long coords are stored in
CRS_SHAPEFILE = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]];-400 -400 1000000000;-100000 10000;-100000 10000;8.98315284119521E-09;0.001;0.001;IsHighPrecision'
# CRS with units in meters
CRS_RASTER = 'PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0]]'


####################### HELPER FUNCTIONS ##################

# created a shapefile from csv and remove points contained
# within building footprints and water bodies
# INPUTS:
#    inCSV (str) - absoluste filepath to grid point csv
#    outFilepath (str) - absolute filepath to where the shapefile should be written
def createShapefile(inCSV,outFilepath):
    tempShp = 'in_memory/tempShp' # hold the shapefile in memory until its fully screened

    # create shapefile from csv.  Coords are in WGS84
    arcpy.management.XYTableToPoint(
        in_table=inCSV,
        out_feature_class=tempShp,
        x_field="longitude",
        y_field="latitude",
        z_field=None,
        coordinate_system=CRS_SHAPEFILE
    )

    # remove points that are within water bodies
    selectedFeatures = arcpy.management.SelectLayerByLocation(
        in_layer=tempShp,
        overlap_type="INTERSECT",
        select_features=WATER_SHAPEFILE,
        search_distance="#",
        selection_type="NEW_SELECTION",
        invert_spatial_relationship="INVERT"
    )

    # remove points that are within building footprints
    selectedFeatures2 = arcpy.management.SelectLayerByLocation(
        in_layer=selectedFeatures,
        overlap_type="INTERSECT",
        select_features=BUILDINGS_SHAPEFILE,
        search_distance="#",
        selection_type="REMOVE_FROM_SELECTION",
        invert_spatial_relationship="NOT_INVERT"
    )

    # save filtered shapefile
    arcpy.CopyFeatures_management(selectedFeatures2, outFilepath)

    # cleanup
    arcpy.management.Delete(tempShp)
    arcpy.management.Delete(selectedFeatures)
    arcpy.management.Delete(selectedFeatures2)


# convert grid points to 10m raster, and screen raster
# INPUTS:
#    inShapefile (str) - absolute filepath to shapefile containing grid points
#    valueField (str) - name of the attribute field that contains raster values
#    outputRaster (str) - aboluste filepath to where the created raster should be stored
def makeRaster(inShapefile,valueField,outputRaster):

    # hold the raster in memory until it's fully cleaned
    rasterFilepath = 'in_memory/raster'

    # create a raster from a points shapefile. Change the CRS so units of cell size are in meters
    with arcpy.EnvManager(outputCoordinateSystem=CRS_RASTER):
        arcpy.conversion.PointToRaster(
            in_features=inShapefile,
            value_field=valueField,
            out_rasterdataset=rasterFilepath,
            cell_assignment="MOST_FREQUENT",
            priority_field="NONE",
            cellsize=10,
            build_rat="BUILD"
        )

    # interpolate to fill in building footprints
    output = arcpy.sa.Con(arcpy.sa.IsNull(rasterFilepath), 
                 arcpy.sa.FocalStatistics(rasterFilepath,arcpy.sa.NbrRectangle(2,2, "CELL"),"MEAN"),
                 rasterFilepath)
    
    # clamp maximum values t
    output = arcpy.sa.Con(output > 86, 86,output)
    output = arcpy.sa.Con(output < 44, 44,output)
    for x in range(3):
        output = arcpy.sa.Con(arcpy.sa.IsNull(output), 
                 arcpy.sa.FocalStatistics(output,arcpy.sa.NbrRectangle(2,2, "CELL"),"MEAN"),
                 output)
        
    
        
    # remove pixels that are in water bodies
    output = arcpy.sa.ExtractByMask(
        in_raster=output,
        in_mask_data=WATER_SHAPEFILE,
        extraction_area="OUTSIDE",
        analysis_extent='#'
    )

    # remove pixels that are outside city boundaries
    output = arcpy.sa.ExtractByMask(
        in_raster=output,
        in_mask_data=CITY_BOUNDARY,
        extraction_area="INSIDE",
        analysis_extent='#'
    )

    # convert raster from float to int to reduce file size
    output = arcpy.sa.Int(arcpy.sa.SetNull(output,output,"VALUE<0"))

    # save raster
    output.save(outputRaster)



####################### MAIN FUNCTION ##################
if __name__ == '__main__':
    #createShapefile(LEQ_POINT_FILE,LEQ_SHAPEFILE)
    makeRaster(LEQ_SHAPEFILE,'LEQ',LEQ_RASTER_FILE)
    #createShapefile(DNL_POINT_FILE,DNL_SHAPEFILE)
    #makeRaster(DNL_SHAPEFILE,'DNL',DNL_RASTER_FILE)
