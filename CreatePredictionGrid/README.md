# CreatePredictionGrid
First stage in the noise surface pipeline.  Create a grid of points across Portland city boundaries at 10m resolution, and partition the grid into subsets for data parallelism in stages 2 and 3

### Files ###
**[createGrid.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/CreatePredictionGrid/createGrid.py)** - create a grid of points across Portland at 10m resolution <br>
**[partitionPoints.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/CreatePredictionGrid/partitionPoints.py)** - partition the grid into subsets (n=1000 points/subset) for data parallelism

#### TODO: upload partitionPoints.py and add hyperlink
#### TODO: add image at top summarizing what the purpose of this stage is
