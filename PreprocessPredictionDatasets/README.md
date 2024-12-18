![GitHub Logo](../images/1x/Stage2.png)

# Preprocess Prediction Datasets
 Stage 2 of the pipeline.  Transforrm datasets to formats better suited for calculating predictor variables.  Example preprocessing steps include claculating distance to nearest building and roads, and identifying the heading (angle) between grid points and nearby features

### Files ###
**[calcBldgDistanceParallel.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/calcBldgDistanceParallel.py)** - calculate distance from grid points to buildings.  Also identify angular relationship between buildings and grid points <br>
**[calcRdAngleParallel.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/calcRdAngleParallel.py)** - Identify angular relationship between each grid point and road segments within 2000m. <br>
**[genAngleShapefileParallel.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/genAngleShapefileParallel.py)** - create shapefiles to capture the radial angle between grids and surrounding land use features <br>
**[genNearTableParallel.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/genNearTableParallel.py)** - calculate distance from road segments to grid points <br>
**[genNearTableParallelMisc.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/genNearTableParallelMisc.py)** - calculate distance from grid points to misc features such as street lights and trimet routes <br>
