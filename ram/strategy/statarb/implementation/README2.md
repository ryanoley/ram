# Implementation Training

## 1. Move Model Selection Top Parameters File

The Model Selection parameter file should be in:
```
GITHUB/ram/ram/strategy/statarb/implementation/params/
```

#### Download config file from Google Cloud

```
gsutil cp gs://ram_data/model_selection/{model_selection_dir}/current_params_{model_selection_run}.json .
```

TODO: Create `data_gcp_manager.py` routine for this


## 2. Set Parameter File Name in StatArb Config

The config file is at: `ram/strategy/statarb/statarb_config.py`


## 3. Make sure prepped_data directories are up to date:

```python
python ram/strategy/statarb/main.py -dv
```

The Max Data Date column should be the final date of the training period.

#### To update versions of prepped data

```python
python ram/strategy/statarb/main.py -d_update {version/index number}
```

#### To move files to GCP cloud instance from local clients

```python
python ram/data/data_gcp_manager.py -ls                 # List all strategies
python ram/data/data_gcp_manager.py -s 4 -ld            # List data versions for strategy
python ram/data/data_gcp_manager.py -s 4 -d 17 --upload
```


## 4. Train models

This step requires the correct config to be set from step 2.

```python
python ram/strategies/statarb/main.py -i -w
```


## 5. Download Trained Model Directory

```
python ram/data/data_gcp_manager.py -ls                 # List all strategies
python ram/data/data_gcp_manager.py -s 4 -lm            # List all trained model dirs
python ram/data/data_gcp_manager.py -s 4 -m 17 --download
```


## 6. Set Trained Models Name in StatArb Config

The config file is at: `ram/strategy/statarb/statarb_config.py`


# Daily Training


## 1. Get Raw Data (10 am)

`get_daily_raw_data.py`
