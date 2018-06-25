# RAM

Master Platform

### Make Environment

```
conda create -n ram python=2.7
source activate ram
pip install -r requirements.txt
make install
```

### Cluster Configuration:

In order to use aws cluster controller, you must have two environment variables set on your local machine:

* AWS_SECRET_ACCESS_KEY
* AWS_USER_ID
