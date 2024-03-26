# Preprocess Prediction Datasets
 Stage 2 of the pipeline.  Transforrm datasets to formats better suited for calculating predictor variables.  Example preprocessing steps include claculating distance to nearest building and roads, and identifying the heading (angle) between grid points and nearby features

#### TODO: add image summarizing this stage of the pipeline

### Files ###
**[calcBldgDistance.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/calcBldgDistance.py)** - calculate distance from grid points to buildings.  Also identify angular relationship between buildings and grid points <br>
**[calcRdAngleParallel.py]()** - Identify angular relationship between each grid point and road segments within 2000m <br.
**[genNearTableParallel.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/genNearTableParallel.py)** - calculate distance from road segments to grid points <br>
**[genNearTableParallelMisc.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/genNearTableParallelMisc.py)** - calculate distance from grid points to misc features such as street lights and trimet routes <br>
**[genWindShapefileParallel.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/PreprocessPredictionDatasets/genWindShapefileParallel.py)** - create shapefiles to capture the radial angle between grids and surrounding land use features <br>

TODO: upload calcRdAngleParallel.py
TO"DO: add hyperlink to calcRdAngleParallel.py

