## Model Selection via ComboSearch

The `ram` platform is designed to serve user-defined trading logics, read `Strategies`, with training and testing data, and provide an organizational framework to standardize analysis and increase productivity. As such one of the key outputs is the simulated, out-of-sample daily returns for a Strategy, and oftentimes many versions of that logic with different hyperparameters. This structure is organized as a matrix of returns with dates in the rows, and versions of the logic in the columns.

After this matrix has been generated, `ComboSearch` is used to select the hyperparameters that maximize a performance metric, typically the Sharpe ratio. It is worth noting that `ComboSearch` cares little about the actual hyperparameters (or even Strategy for that matter), but searches over the columns for an optimal time-series. As the name suggests, the algorithm combines multiple time-series columns together to aid generalization and diversify idiosyncratic model-risk. By generating random combinations of `n` time series, it iteratively searches to improve the performance metric. Though it is a brute force mechanism and global optimality is out of reach (for example, combinations of five columns from a set of 250 has approximately 7.8 billion different combinations), the algorithm produces results that improve through time the longer it searches.

As `ComboSearch` grinds away, it will checkpoint the best current results into a file called `current_top_params.json`, which holds the parameters that should be used in production.


## Model Selection output

1. From the Cloud run, download the `current_top_params.json` file to `statarb/implementation` folder

2. On the local client, run `statarb/implementation/preprocess_new_models.py` to create blueprints for the daily run.

3. Update preprocess_version in `statarb_config`


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
python ram/strategy/statarb/main.py -i -w
```

5. Download cached models to `implementation/StatArbStrategy/trained_models`

```
python ram/data/data_gcp_manager.py -ls                 # List all strategies
python ram/data/data_gcp_manager.py -s 4 -lc            # List data versions for strategy
python ram/data/data_gcp_manager.py -s 4 -c 17 --download
```


## Morning pre-processing

1. Run


