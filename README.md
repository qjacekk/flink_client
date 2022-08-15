# flink_client
Flink REST API CLI and/or **interactive** client

## Why
- Flink GUI is nice, Flink CLI is not too bad, but I need more,
- I prefer CLI over webUI for important ops (e.g restarting a job with a snapshot),
- my Flink clusters are running on docker and I don't want to use 'docker exec ...' every time or install Flink client on each machine,
- I need to log my Flink ops and keep a history of jobs that have been started (including the parameters I pass to my Flink jobs),
- I need a way to add new commands and script some more complex ops,

## What
This is a simple Flink REST API client that can be used for daily ops on a Flink cluster.

It works in two modes:
- like a regular CLI client e.g.:
> $ flink.py --help
```commandline
Connecting to url: http://a.b.c.d:xxxx
usage: flink.py [-h] cmd ...

optional arguments:
  -h, --help      show this help message and exit

commands:
  cmd             one of:
    get           Execute REST API Get operation
    jobs          List running jobs
    delete_jar    Delete jar
    upload_jar    Upload jar to Flink
    run_with_jarid
    run           Run a job implemented in class_name using default jarid.
    run_with_jar  Upload a jar file and run a job implemented in class_name
    stop_job      Stop a job gracefully with a savepoint.
    savepoint     Triggers a savepoint, and optionally cancels the job afterwards.
    kill          Kill job. WARNING: This command requires user's confirmation.
    kill_all      Kill all running jobs. WARNING: This command requires user's confirmation.
    job_config    Get job configuration
    job_exc       Get job exceptions
    job_res       Get job result
    job_chk       Get job checkpoints
    info          Get cluster info
    job           Get job info

```

> $ flink.py jobs
```commandline
Connecting to url: http://a.b.c.d:
c55be98e927a37adec7bff6fa767cbb7 RUNNING      my_flink_job_1
2b4d49ce92a22e21a60b72c3683ed8b9 RUNNING      my_flink_job_2
```

> $ flink.py run com.example.MyClass --my_param 123 --my_ohter_param 456
```commandline
Connecting to url: http://a.b.c.d:xxxx
jarid=e90d5d65-a13a-4df9-a4c6-791807e47b0e_my_flink_jar-1.0.jar
Job started with jobid=cd3d996432f6acd21248e10b566ff9bf
```
 
- as an **interactive** client (execute without parameters):
> flink.py
```commandline
Connecting to url: http://a.b.c.d:xxxx
> help
Commands:
  help/h/?         Commands help
  exit/quit/q      Exit
  log              enable logging to console
  jarid            Print default jarid

  get              Execute REST API Get operation
  jobs             List running jobs
  delete_jar       Delete jar
  upload_jar       Upload jar to Flink
  run_with_jarid
  run              Run a job implemented in class_name using default jarid.
  run_with_jar     Upload a jar file and run a job implemented in class_name
  stop_job         Stop a job gracefully with a savepoint.
  savepoint        Triggers a savepoint, and optionally cancels the job afterwards.
  kill             Kill job. WARNING: This command requires user's confirmation.
  kill_all         Kill all running jobs. WARNING: This command requires user's confirmation.
  job_config       Get job configuration
  job_exc          Get job exceptions
  job_res          Get job result
  job_chk          Get job checkpoints
  info             Get cluster info
  job              Get job info

Type <command> -h for details on command's parameters.

> info
Info:
Url: http://a.b.c.d:xxxx
  Task Managers           : 1
  Slots                   : 8
  Slots available         : 5
  Jobs:
    Running   : 3
    Cancelled : 1
    Failed    : 0

> jobs
c55be98e927a37adec7bff6fa767cbb7 RUNNING      my_job_1
cd3d996432f6acd21248e10b566ff9bf RUNNING      my_job_2
2b4d49ce92a22e21a60b72c3683ed8b9 RUNNING      my_job_3
```

## Setup
- install python 3.x
- install requirements from requirements.txt
- edit the following params in flink.py:
  - URL=<flink_host>:<port>
  - DEFAULT_LOG_FILE_PREFIX = "<path_to_log_dir>/flink_client_"
  - DEFAULT_CFG_FILE_PREFIX = "<path_to_configs>/flink_client_cfg_"
- optionally change the following as well:
  - DEFAULT_LOG_LEVEL = logging.INFO  # set to logging.DEBUG for debugging (e.g. see REST req/resp)
  - DEFAULT_SAVEPOINTS_DIR = "/opt/flink/state/savepoints"  # Flink cluster path!
- make it executable (chmod a+x flink.py) and optionally modify the _shebang_ (#!/usr/bin/python) at the beginning of the script to point at your python interpreter,
- add the dir containing flink.py to $PATH
- execute: flink.py

## Requirements
- python 3.x
- requests
- pyyaml

Runs on Linux and Windows.

## Other notes
### Logs
Both log and config files are saved with a suffix that's unique for the URL so if you have more than one instance you can make a copy of the script, change URL and you'll have two separate config&log files.

### default_jarid
Flink requires a jar file containing user's jobs to be uploaded to the cluster. This can be done in two ways:
  - upload a jar with 'upload_jar' command and then execute a job with 'run' or
  - use 'run_with_jar' command, providing jar_path as the first argument

In both cases the tool will set _default_jarid_ which is a reference to the recently uploaded jar file.
This _default_jarid_ will be used when you run a job with 'run' command next time, so you don't have to upload the jar file every time.

