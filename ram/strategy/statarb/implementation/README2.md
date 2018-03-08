
# Implementation Training

## Workflow through files and functions

Documents files and functions that are used, and the dependencies.

The entire process is kicked off through:

```python
python ram/strategies/statarb/main.py -s 1 -i
```



### List of Files/Directories that must be present

*


### Functions

1. `import_current_top_params`

Gets the parameters for the models that have been selected. The file is one big dictionary, with the following form:

```python
{'StatArbStrategy_run_0002_454': {''}}

```

