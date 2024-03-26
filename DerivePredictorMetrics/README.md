# Derive Predictor Metrics
Stage 3 of the pipeline. Using preprocessed datasets, calculate variable metrics used in the land use regression model (e.g. average speed of vehicles driving on primary roads within 20m)

### Grid Point Summary Statistics ###
| Variable  | Buffer (m) | Statistic | IDW | Road Type  | Shield Adjustment  | mean  | IQR | 
| ------------- | ------------- | ------------- | ------------- | ------------- | ------------- | ------------- |------------- |
| percent cars  | 10  | mean  | False  | tertiary/unclassified  | unshielded roads only  | TBD  | TBD | 
| bus routes  | 10  | mean  | NA  | NA  | NA  | TBD  | TBD |
| NDVI  | 10  | mean  | NA  | NA  | NA  | TBD  | TBD |
| AADT  | 20  | mean  | False  | all road types  | unshielded roads only  | TBD  | TBD |
| emergency vehicle routes  | 20  | mean  | False  | NA  | NA  | TBD  | TBD |
| street lights  | 20  | mean  | False  | NA  | NA  | TBD  | TBD |
| bicycle routes | 20  | mean  | False  | NA  | NA  | TBD  | TBD |
| percent cars | 50  | mean  | False  | primary/secondary  | unshielded roads only  | TBD  | TBD |
| vehicle speed | 250  | quantile  | False  | residential  | unshielded roads only  | TBD  | TBD |
| vehicle speed  | 450  | sum  | False  | tertiary/unclassified | unshielded roads only  | TBD  | TBD |
| NDVI  | 450  | sum  | NA  | NA | NA  | TBD  | TBD |
| percent cars | 700  | mean  | True  | primary/secondary  | shielded and unshielded roads  | TBD  | TBD |
| percent heavy trucks | 800  | mean  | False  | primary/secondary  | unshielded roads only  | TBD  | TBD |
| percent heavy trucks | 1200  | sum  | False  | primary/secondary  | unshielded roads only  | TBD  | TBD |
| heavy truck emissions | 1200  | mean  | False  | primary/secondary  | unshielded roads only  | TBD  | TBD |
| percent cars | 2000  | mean  | True  | tertiary/unclassified  | unshielded roads only  | TBD  | TBD |

abbreviations: **IQR** - Interquartile range, **IDW** - Inverse Distance Weighted, **NDVI** - normalized difference vegetation index, **AADT** - Annual Average Daily Traffic 


### Files ###
**[calcIsShielding.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/DerivePredictorMetrics/calcIsShielding.py)** - for each grid point, determine which roads are shielded by buildings <br>
**[calcMiscMetrics.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/DerivePredictorMetrics/calcMiscMetrics.py)** - calculate metrics for each variable not directly attached to the road network polyline file or NDVI (e.g. number of street lights, bus routes, bicycle routes)  <br>
**[calcNDVIBuffers.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/DerivePredictorMetrics/calcNDVIBuffers.py)** - calculate NDVI metrics.  NDVI is the only variable in raster format <br>
**[calcRdMetrics.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/DerivePredictorMetrics/calcRdMetrics.py)** - calculate road metrics for those that do not involve a shield modifier <br>
**[calcShieldingMetrics.py](https://github.com/larkinandy/PDXNoiseSurface/blob/main/DerivePredictorMetrics/calcShieldingMetrics.py)** - calculate road metrics for those that do leverage a shield modifier <br>
