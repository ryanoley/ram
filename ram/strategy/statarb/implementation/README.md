## Model Selection via ComboSearch

The `ram` platform is designed to serve user-defined trading logics, read `Strategies`, with training and testing data, and provide an organizational framework to standardize analysis and increase productivity. As such one of the key outputs is the simulated, out-of-sample daily returns for a Strategy, and oftentimes many versions of that logic with different hyperparameters. This structure is organized as a matrix of returns with dates in the rows, and versions of the logic in the columns.

After this matrix has been generated, `ComboSearch` is used to select the hyperparameters that maximize a performance metric, typically the Sharpe ratio. It is worth noting that `ComboSearch` cares little about the actual hyperparameters (or even Strategy for that matter), but searches over the columns for an optimal time-series. As the name suggests, the algorithm combines multiple time-series columns together to aid generalization and diversify idiosyncratic model-risk. By generating random combinations of `n` time series, it iteratively searches to improve the performance metric. Though it is a brute force mechanism and global optimality is out of reach (for example, combinations of five columns from a set of 250 has approximately 7.8 billion different combinations), the algorithm produces results that improve through time the longer it searches.

As `ComboSearch` grinds away, it will checkpoint the best current results into a file called `current_top_params.json`, which holds the parameters that should be used in production.


## Find Top Params

Collect the top runs from `current_top_params.json` and update the `implementation_top_models` in `statarb_configs.py`. NOTE: Be sure these configs are on the cloud platform (also committed) before re-training models.


## (Re-) Training models

1. On local client, update the prepped data versions for the aforementioned runs. This can be done by simply running the version through the main command. The `d_update` with a given version will drop the final period's data, and recreate with most up-to-date data:

```
python ram/strategy/statarb/main.py -dv          	  # List all version for strategy
python ram/strategy/statarb/main.py -d_update 10      # Update version
```

2. Upload new files to GCP Storage:

```
python ram/data/data_gcp_manager.py -ls                 # List all strategies
python ram/data/data_gcp_manager.py -s 4 -ld            # List data versions for strategy
python ram/data/data_gcp_manager.py -s 4 -d 17 --upload
```

3. Check that `statarb_configs.py` is updated

4. Train models by simply invoking:

```
python ram/strategy/statarb/main.py -i
```

5. Download cached models to local file system

```
python ram/data/data_gcp_manager.py -ls                 # List all strategies
python ram/data/data_gcp_manager.py -s 4 -lc            # List data versions for strategy
python ram/data/data_gcp_manager.py -s 4 -c 17 --download
```






## Updating models

1. Pull new data for all sectors, all cycles

```
python get_data.py --training_pull
```

The model selection process needs the most up-to-date data to re-select models.
However, only one cycle gets to update its SecCodes. The last daily date
should be the final trading day of the previous month.

2. Upload data and param files to GCP

All `sector_id_data`, `raw_sector_x` and `column_params_sector_x` files
should be uploaded to:

```
ram_data/implementation/LongPeadStrategy
```

3. Train production models

Make sure GCP instance repo is up to date, the run

```
python training.py --all_params
```

4. Run model selection in cloud

```
python training.py --model_selection
```

5. Re-run training and cache models

```
python training.py --cache_models
```

6. Download trained models
