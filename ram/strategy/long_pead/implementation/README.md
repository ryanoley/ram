## Model Selection via ComboSearch

When architecting the trade, one must decide which runs will go into the final model. Once an architecture has been settled on (i.e. the simulated results from ComboSearch are acceptable), the runs (and other assumptions like ComboSearch params and drop_params functionality) must be noted.

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
bash ram/strategy/long_pead/implementation/rerun.sh list      # List all runs
bash ram/strategy/long_pead/implementation/rerun.sh run 10    # List all runs
```


4. Re-run ComboSearch to get top_params

5. Run xxx.py to train models

6. Download cached models to local file system





## Finding params to train

1. Update all three cycle versions per sector. Data should be up to final day of month

2. Full simulation to decide

3. Run script to get combos, and put into implementation config


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
