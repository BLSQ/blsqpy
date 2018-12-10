# blsq-py

python script to help with data science

# Development

```
python3 setup.py develop --user
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

