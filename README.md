[![Build Status](https://travis-ci.org/BLSQ/blsqpy.svg?branch=master)](https://travis-ci.org/BLSQ/blsqpy) [![Maintainability](https://api.codeclimate.com/v1/badges/782c911a01ede5eacbbd/maintainability)](https://codeclimate.com/github/BLSQ/blsqpy/maintainability) [![Test Coverage](https://api.codeclimate.com/v1/badges/782c911a01ede5eacbbd/test_coverage)](https://codeclimate.com/github/BLSQ/blsqpy/test_coverage)

# blsq-py

python script to help with data science work related to DHIS2.

# Usage

## Installation

easiest for the moment... use virtualenv and leave on the edge

```
pip install --upgrade git+https://github.com/blsq/blsqpy.git
```

## Credentials externalisation

To access data the package relies on the creation of hooks 
(PostgresHook and S3ExportHook) 

To look up db connections or s3 connection 
settings for these hook we use credentials files that need to be stored locally.

This files follow the structure

```
user=xxxx
password=xxxx
port=5432
dbname=xxxxx
```
And by default will look for files in

`~/.credentials/...`

Folder and files need to be created previous to working with the package.
Credential files, are for the moment exchanged personally between team members.

This code will look for '/home/xxxxxx/.credentials/pointenv'
and return it's content as a dict

```
from blsqpy.dot import Dot
config = Dot.load_env("pointenv")
```
 

# Package Structure

Package is structured with the idea of encapsulate each step of the data analysis
within a module, with addtional support ones.


##Connections

Access to data can happen through database(DB),AWS S3 or DHIS2 API

So the first part related to connectors is delegated to *PostgresHook* and *S3ExportsHook*
both object use the credential files stored locally to generate future connectors
for the SQL/S3 queries.

It's important to notice that data acquisiton through DHIS2 API is integrated 
within its general module for data information


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
##DHIS2 Metada Information

Modules *dhis2* and *dhis2_client* are the ones used for obtaining metada from our 
system. First one is built upon the hooks we have created previously, while in
the second case, it connects directly to DHIS2 API.

Once we have created our hook object we pass it to the dhis object as its 
only parameter.

dhis object will inmediately on its creation generate a series of internal
tables (by SQL queries) as attributes that overview separately most of the 
sections of the DHIS2 maintenance area.

```
dataelement: DataFrame
    Names,UIDs and IDs of data elements in the DHIS
categoryoptioncombo: DataFrame
    Names,UIDs and IDs of category option combos in the DHIS
dataset: DataFrame
    Names,UIDs and IDs of datasets in the DHIS
dataelementgroup: DataFrame
    Names,UIDs and IDs of dataelement groups in the DHIS        
orgunitgroup: DataFrame
    Names,UIDs and IDs of organisation groups in the DHIS   
orgunitstructure: DataFrame
```

Additionally it contains a series of functions to obtain more detailed
information on user indicated datasets, or data element/orgunit structures.

It also contains the functions to extract geographical information (GEOJSON) 
from the system ( it uses functiosn from the module *geometry*)

(Note: for dhis_client is a part still on progress as long as its use has been
limited so far)

##DHIS2 Data Extraction

Note:So far we only developed this for DB connections.

The module reponsilbe is *coverage*. To generate it's object we feed it the 
previously generated dhis2.

Each coverage object is thought as a framework of conditions to extraction:
As most times it happens that all extractions hace the same limitations geographically
or in time, its easier to declare them once as general attributes for all the extraction
processes.

All extraction processes use previously created SQL queries templates . These 
templates are saved in the "queries" subfolder and with "jinja2" package
what the scripts do is feed the empty spaces with the details of each call.
Jinja syntaxis allows not only filling details but also conditional declaration 
of parts of the query.

All the functions used to transform the parameters givne in this module to SQL
language are stored in in the *query* module, is most cases as static methods.

##DHIS2 Data Processing

Functiosn for further working on the results are stored in the *data_process* 
module. However, a coming soon restructuration will review this part of the
package too.


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
