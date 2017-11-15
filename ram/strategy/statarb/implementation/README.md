## Model Selection via ComboSearch

The `ram` platform is designed to serve user-defined trading logics, read `Strategies`, with training and testing data, and provide an organizational framework to standardize analysis and increase productivity. As such one of the key outputs is the simulated, out-of-sample daily returns for a Strategy, and oftentimes many versions of that logic with different hyperparameters. This structure is organized as a matrix of returns with dates in the rows, and versions of the logic in the columns.

After this matrix has been generated, `ComboSearch` is used to select the hyperparameters that maximize a performance metric, typically the Sharpe ratio. It is worth noting that `ComboSearch` cares little about the actual hyperparameters (or even Strategy for that matter), but searches over the columns for an optimal time-series. As the name suggests, the algorithm combines multiple time-series columns together to aid generalization and diversify idiosyncratic model-risk. By generating random combinations of `n` time series, it iteratively searches to improve the performance metric. Though it is a brute force mechanism and global optimality is out of reach (for example, combinations of five columns from a set of 250 has approximately 7.8 billion different combinations), the algorithm produces results that improve through time the longer it searches.

As `ComboSearch` grinds away, it will checkpoint the best current results into a file called `current_top_params.json`, which holds the parameters that should be used in production.


## (Re-) Training models

1. Update the prepped data versions for the aforementioned runs. This can be done by simply running the version through the main command. The `dp` with a given version will drop the final period's data, and recreate with most up-to-date data.

```
python ram/strategy/long_pead/main.py -lv          # List all version for strategy
python ram/strategy/long_pead/main.py -dp 10       # Update version
```

2. Upload new files to GCP Storage

```
python ram/data/data_gcp_manager.py -ls              # List all strategies
python ram/data/data_gcp_manager.py -s 4 -lv         # List all version for strategy
python ram/data/data_gcp_manager.py -s 4 -v 17 -up   # Upload
```

3. Restart run, which will delete the final file, re-stack data, re-fit model and report most up-to-date results. Because the stacking and training can take some time for each model, one can spin up multiple instances. Currently, I think one can get away with running 16 cores, but be sure to use the `highmem` version of this image.

```
d=ram/strategy/long_pead/implementation/training/

bash $d/01_update_runs.sh list      # List all runs
bash $d/01_update_runs.sh run 10    # Rerun index 10
```

4. Re-run ComboSearch to from Python Notebook:

```
ram/strategy/long_pead/implementation/Current StatArb Implementation.ipynb
```

5. If satisfied with the most recent ComboSearch, point the following script at the name of the directory where the data was stored.

```
d=ram/strategy/long_pead/implementation/training/

bash $d/02_train_implementation_models.sh combo_9898
```

6. Download cached models to local file system






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
