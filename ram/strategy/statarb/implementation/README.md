# Monthly Retraining

All implementations are built off of a simulation (`run_xxxx`). This simulation relies on specific `prepped_data` directories. Also, the implementation relies upon a Model Selection routine to select the best hyperparameters from the simulation. The relevant `run_xxxx` exists in the `ram/strategy/statarb/implementation/ImplementationModelSelection.ipynb`.

Assuming the `run_xxxx` is still the simulation used in production, these are the major steps (more details later) for updating:

1. Update and Upload Data to GCP
2. Using new data on GCP, restart the `run_xxxx` to get the most up-to-date simulated results
3. Re-run the ModelSelection on this updated `run_xxxx`
4. Re-train and download the new SKLearn models and other data structures for local implementation.

---
### 1. Update and Upload Data

1. Update the appropriate `prepped_data` directories for the given models:
```
python ram/strategy/statarb/main.py -dl
python ram/strategy/statarb/main.py -du {3}
```

2. Upload the new data directories to GCP:
```
python ram/data/data_gcp_manager.py -sl
python ram/data/data_gcp_manager.py -s {3} -dl
python ram/data/data_gcp_manager.py -s {3} -d {7} --upload
```

---
### 2. Get Up-To-Date Simulation Results

1. Using GCP Compute Engine, create a new instance. Use these parameters:
  * Zone: US-East1-b
  * Machine Type: 16 vCPUs with 60 GB memory
  * Boot Disk (click Change, navigate to Custom Images tab): base-python27-image
  * Identity and API Access (under Service Account): ram-default-service-account

2. SSH into newly created instance, navigate to `ram` directory, and update.
```
cd ~/projects/ram
git pull
sudo make install
```

*Note: Prompt will ask if running on GCP. Type `y` and re-run sudo make install*

3. Restart `run_xxxx`:
```
python ram/strategy/statarb/main.py -rl
python ram/strategy/statarb/main.py -r {6}
```

4. Stop instance. Do not delete.

---
### 3. Model Selection

1. Start and SSH into the GCP instance *notebook1*

2. Update the instance `ram` repo, then start the Jupyter Notebook Server from the command line
```
cd ~/projects/ram
git pull
sudo make install

cd ~/
start_jupyter_notebook.sh
```

You can minimize the Browser (do not close as it will stop the notebook server).

3. On the GCP Compute Engine page, click the External IP link. Adjust address to `http` from `https`, and navigate to port 5000:
```
http://35.229.125.75:5000
```

4. Open `projects/ram/ram/strategy/statarb/implementation/ImplementationModelSelection.ipynb`

5. Select from Dropdown Kernel, "Restart and Run All"

6. Write down (or copy) the model selection timestamp. This should be located about cell 8 and looks like this:

```
Reading and aggregating runs...
Finished aggregating runs...

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Writing run as: CombinationSearch_20180523173207
Max date: 2018-05-07
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
```

The relevant piece is: **CombinationSearch_20180523173207**





#### 4. Implementation Training

## 1. Move Model Selection Top Parameters File

The Model Selection parameter file should be in:
```
GITHUB/ram/ram/strategy/statarb/implementation/params/
```

#### Download config file from Google Cloud

```
python data_gcp_manager.py -msl
python data_gcp_manager.py -ms `x` --download
```

NOTE: Downloads directly to GITHUB directory for StatArb


## 2. Set Parameter File Name in StatArb Config

The config file is at: `ram/strategy/statarb/statarb_config.py`


## 3. Make sure prepped_data directories are up to date:

```python
python ram/strategy/statarb/main.py -dv
```

The Max Data Date column should be beyond the final date of the training period.

#### To update versions of prepped data

```python
python ram/strategy/statarb/main.py -d_update {version/index number}
```

#### To move files to GCP cloud instance from local clients

```python
python ram/data/data_gcp_manager.py -sl                 # List all strategies
python ram/data/data_gcp_manager.py -s 4 -ld            # List data versions for strategy
python ram/data/data_gcp_manager.py -s 4 -d 17 --upload
```

TODO: Re-design command line arguments.


## 4. Train models

Training can happen locally or on GCP.

**This step requires the correct config to be set from step 2.**

```python
python ram/strategies/statarb/main.py -i -w
```


## 5. Download Trained Model Directory

```
python ram/data/data_gcp_manager.py -sl                 # List all strategies
python ram/data/data_gcp_manager.py -s 4 -ml            # List all trained model dirs
python ram/data/data_gcp_manager.py -s 4 -m 17 --download
```


## 6. Set Trained Models Name in StatArb Config

The config file is at: `ram/strategy/statarb/statarb_config.py`


## 7. Re-Run daily workflow

* `get_version_data.py`
* `prep_data.py`



# Daily Execution

## Morning pre-processing




## 1. Get Raw Data (10 am)

```
python ram/strategy/statarb/implementation/get_daily_raw_data.py
```




# Other Details


Model Selection implements some logic for selecting hyper-parameters out-of-sample. The ModelSelection base class handles all the organizational components, from reading data and creating train/test indexes, to handling the output of equity curves and selected hyperparameters.

Model selection requires access to the output return files from the simulation, therefore, will happen where those files exist. The StatArb Notebook `ImplementationModelSelection.ipynb` can be used to generate the optimized parameters in `RAM_DATA/model_selection/`.




## Odd Ticker Hash

`DATA/ram/implementation/StatArbStrategy/live_pricing/odd_ticker_hash.json`

Used to map `{ QADirectTicker: EzeTicker , ...}`

As far as I can tell, the "odd" tickers should be identified upon importing the current tickers, and checking them in the `live_prices/LIVE_PRICING.xls`

There should be some process for verifying that Bloomberg Tickers/CUSIPs are properly merging with QADirect IDs as well.


