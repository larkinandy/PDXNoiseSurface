# createGrid.py
# Author: Andrew Larkin
# Date Created: March 22, 2024
# Summary: Create a 10m grid of points covering Portland, OR city boundaries in shapefile format
# Thanks to https://spatial-dev.guru/2022/05/22/create-fishnet-grid-using-geopandas-and-shapely/
# for demonstrating how to create a fishnet in geopandas

# import libraries
import geopandas as gpd
from shapely import geometry

# define global constants
PDX_BOUNDARY_PATH = 'Absolute filepath to PDX city boundary shapefile'
OUTPUT_PATH = 'Absolute filepath to output fishnet shapefile'
CRS = 'EPSG:3857'
RESOLUTION = 10

########## HELPER FUNCTIONS #############

# create a grid of points within a defined boundary
# INPUTS:
#    boundary (shapefile) - limits of grid extent
#    resolution (int) - resolution of grid points, in meters
#    crs (string) - coordinate reference system for the grid points
# OUTPUTS:
#    an array of the grid points, stored in geometry format
def createPointGrid(boundary,resolution,crs):
    # get cityBoundaries from input shapefile
    cityBoundaries = gpd.read_file(boundary)
    
    # Reproject to projected coordinate system
    cityBoundaries = cityBoundaries.to_crs(crs)
    
    # Get the extent of the shapefile
    total_bounds = cityBoundaries.total_bounds

    # Get minX, minY, maxX, maxY
    minX, minY, maxX, maxY = total_bounds

    # initialize array and first geom point
    x, y = (minX, minY)
    geom_array = []

    # iterate through all points in the grid, first by longitude and then latitude 
    while y <= maxY:
        while x <= maxX:
            geom =geometry.Point([x,y])
            geom_array.append(geom)
            x += resolution
        x = minX
        y += resolution
    return(geom_array)

# create a point grid in shapefile format
def createFishnet(boundary,resolution,crs,outputFile):
    pointGrid = createPointGrid(boundary,resolution,crs)
    fishnet = gpd.GeoDataFrame(pointGrid, columns=['geometry']).set_crs(crs)
    fishnet.to_file(OUTPUT_PATH)


########## MAIN FUNCTION #############
    
if __name__ == '__main__':
    createFishnet(PDX_BOUNDARY_PATH,RESOLUTION,CRS,OUTPUT_PATH)