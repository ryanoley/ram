
## Updating models

1. Pull new data for all sectors, all cycles

```
python get_data.py --training_pull
```

The model selection process needs the most up-to-date data to re-select models.
However, only one cycle gets to update its SecCodes. The last daily date
should be the final trading day of the previous month.

2. Upload data and param files to GCP

All `raw_training_data_sector_x` and `column_params_sector_x` files
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
