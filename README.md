# Simple Map Reduce (smr)

smr lets you easily build map-reduce jobs with simple python code.
It does not use hadoop, or any other map-reduce framework in any way.

## Installation
`pip install git+git://github.com/idyedov/smr.git`
or just
```python setup.py install```

### Dependencies
  boto is required for communication with AWS services like S3
  paramiko is required for smr-ec2 to communicate with EC2 instances through SSH

## Usage
  ```smr config.py```
  ```smr-ec2 config.py```

### config.py
config.py has all the information about the job you want to run, including
the code for map and reduce functions. Please look ad smr/default_config.py for
explanation and usage of all the config params.

## smr scripts

### smr-map
 * takes config location as the first argument
 * reads file names to process from STDIN, one per line
 * passes each file name to MAP_FUNC that's defined in config
 * outputs processed files to STDERR, one per line
   - prepends "+" if it was successfull in processing that file
   - prepends "!" if it couldn't process the file
 *  should output results to be passed to reducer to STDOUT

### smr-reduce
 * should take STDOUT from smr-map as STDIN
 * will run OUTPUT_RESULTS_FUNC that's defined in config when finished

### smr
 * runs NUM_WORKERS smr-map workers where NUM_WORKERS is specified in config
 * runs a single smr-reduce process
 * divides up files to process amongst smr-map workers
 * puts the output of STDOUT of smr-map workers into STDIN of smr-reduce

### smr-ec2
 * same functionality as smr, but boot up AWS_EC2_WORKERS EC2 instances and run smr-map on them

## TODO
 * add option to output results to s3
 * add option to process local data instead of s3
 * better documentation
 * add simple examples & data sets
 * add ability for multi-level reducers
