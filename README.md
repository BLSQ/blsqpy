[![Build Status](https://travis-ci.org/BLSQ/blsqpy.svg?branch=master)](https://travis-ci.org/BLSQ/blsqpy)

# blsq-py 

python script to help with data science

# Usage

## Credentials externalisation

by default will look for files in

`~/.credentials/...`


this code will look for '/home/xxxxxx/.credentials/pointenv'
and return it's content as a dict
```
from blsqpy.dot import Dot
config = Dot.load_env("pointenv")
```

This is used in hooks (PostgresHook and S3ExportHook)
to look up db connections or s3 connection settings

#### S3ExportsHook

```python
from blsqpy.s3export_hooks import S3ExportsHook
s3 = S3ExportsHook("s3_readonly")
# ==> will look for /home/xxxxxx/.credentials/s3_readonly
```

and s3_readonly will look like this

```
ACCESS_KEY=AKIA...
SECRET_KEY=.........
BUCKET_NAME=bucket-name
```

#### PostgresHook


```python
from blsqpy.postgres_hook import PostgresHook
PostgresHook("dhis2_db_connection")
# ==> will look for /home/xxxxxx/.credentials/dhis2_db_connection
```

dhis2_db_connection will look like
```
url=postgres://user:password@server.com:5432/dbname
```

or

```
user=user
password=password
host=server.com
port=5432
dbname=dbname
```

# Development

```
python3 setup.py develop --user
```

This one assume you have blsq created the local config needed

```
python3 test.py --user
```


## Testing

### Additional dependencies

Run

```
./bin/setup
```
or

```
pip3 install mamba --user
pip3 install twisted --user
pip3 install expects --user
```

### Run the specs

run ./bin/test

```
mamba --format=documentation --enable-coverage
coverage report ./blsqpy/*.py
```

if you want to consult html reports

```
coverage html ./blsqpy/*.py
google-chrome ./htmlcov/index.html
```

