## DataHandler SQL Interface

`get_filtered_univ_data` will return data between two dates, for IDs that meet some criteria on a third date.
`get_id_data` will return data between two dates for specific IDs that are provided



### Features

Features must adhere to a strict format to be returned correctly. Best to use examples to see formatting:


##### Example 1 (VARIABLES): 10-day moving average for Close prices and Open prices

`PRMA10_Close`
`PRMA10_Open`


##### Example 2 (VARIABLE MANIPULATION): Lagged, led, and ranked 10 day moving averages

`LAG1_PRMA10_Close`
`LEAD1_PRMA10_Close`
`LAG1_RANK_PRMA10_Close`

### Data columns available

* ROpen, RHigh, RLow, RClose, RVWAP, RVolume, RCashDividend (Raw values)
* Open, High, Low, Close, VWAP, Volume (Split and Div Adjusted - DEFAULT for technical variable calculations)
* AvgDolVol, MarketCap, SplitFactor
* GSECTOR, GGROUP (Gics Sector and Group data)
* SI
* Ticker

### Technical variables (20-period example construction provided)

* MA20 : Moving Average
* PRMA20 : Price Relative to Moving Average
* VOL20 : Volatility
* BOLL20 : Bollinger Band
* DISCOUNT20 : Discount from High
* RSI20 : Relative Strength Index
* MFI20 : Money Flow Index

### Shifting values

* LEADx : Return value from x rows forward
* LAGx : Return value from x rows backward

### Additional

* RANK : Rank 0-n all, 1 representing lowest value (DOES THIS MAKE SENSE?)

### Filter Args

These are used to filter the universe on the filter date before there is a selection of IDs by some other criteria.
```python
filter_args = {'filter': 'AvgDolVol',
               'where': 'MarketCap >= 200 and GSECTOR not in (50, 55)',
               'univ_size': 1500}
```

##### Note:

* All characters must be capitalized


