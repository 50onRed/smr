# Simple Map Reduce (smr)

smr lets you easily build map-reduce jobs with simple python code.
It does not use hadoop, or any other map-reduce framework in any way.

[![wercker status](https://app.wercker.com/status/f23eabc2fec80a2ccd09ee7126c9b140/m "wercker status")](https://app.wercker.com/project/bykey/f23eabc2fec80a2ccd09ee7126c9b140)

## Installation
`pip install git+git://github.com/idyedov/smr.git`
or just
```python setup.py install```

### Dependencies
 * boto is required for communication with AWS services like S3
 * paramiko is required for smr-ec2 to communicate with EC2 instances through SSH

## Usage

### CLI tools
```smr config.py``` or ```smr-ec2 config.py```

### integrate into your code
```python
from smr import run, run_ec2, get_default_config
config = get_default_config()
config.config = "config.py"
run(config) # or run_ec2(config)
```

jobs directory has a sample job that uses common crawl public dataset on S3.

### config.py
config.py has all the information about the job you want to run, including
the code for map and reduce functions. Please look ad smr/default_config.py for
explanation and usage of all the config params.

The most important parameters that you should implement in config are:
 * MAP_FUNC: function that will take a single argument of local filename to be processed for your smr job.
     Each line that it prints to STDOUT will be sent to REDUCE_FUNC as an argument
 * REDUCE_FUNC: function that takes a single string argument of a map function output
 * INPUT_DATA: list of URIs to process in the format of s3://bucket_name/path or file://absolute/path
     * you can use {year} or {year:04d} macros in INPUT_DATA if you specify start_date
     * you can use {month} or {month:02d} macros in INPUT_DATA if you specify start_date
     * you can use {day} or {day:02d} macros in INPUT_DATA if you specify start_date
 * OUTPUT_RESULTS_FUNC: function that's called when the job is finished, takes no arguments

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
