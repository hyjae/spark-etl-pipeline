# Instruction
### Installing Pipenv

Need to install Python3.x beforehand

```bash
pip3 install pipenv
```

### Installing this Projects' Dependencies

Make sure that you're in the project's root directory (the same one in which the `Pipfile` resides), and then run,

```bash
pipenv install --dev
```

### Run a Program

stand-alone
```bash
$SPARK_HOME/bin/spark-submit \
--master spark://192.168.210.147:7077 \
--py-files packages.zip \
--packages mysql:mysql-connector-java:8.0.15 \
--files configs/etl_config.json \
--executor-memory 25g \
jobs/etl_job.py
```

yarn
`https://spark.apache.org/docs/latest/running-on-yarn.html`
```bash
$SPARK_HOME/bin/spark-submit \
--master yarn \
--driver-memory 4g \
--executor-memory 20g \ 
--py-files packages.zip \
--packages mysql:mysql-connector-java:8.0.15 \
--files configs/etl_config.json \
jobs/etl_job.py
```