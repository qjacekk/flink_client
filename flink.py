#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Jacek Karas
import yaml
import requests
import time
import json
import io
import os
import re
import sys
from inspect import signature, getdoc, Parameter
import logging
import logging.handlers
import argparse
from functools import partial
from datetime import datetime, timedelta
import hashlib
__version__ = 0.1

#USER CONFIG: modify this section for this particular flink instance --->
URL = "http://192.168.1.46:8082"
DEFAULT_LOG_FILE_PREFIX = "./flink_client_"
DEFAULT_CFG_FILE_PREFIX = "./flink_client_cfg_"
DEFAULT_LOG_LEVEL = logging.INFO  # set to logging.DEBUG for debugging (e.g. see REST req/resp)
DEFAULT_SAVEPOINTS_DIR = "/opt/flink/state/savepoints"  # Flink cluster path!
# <--- end of USER CONFIG


FLINK_ID = hashlib.sha256(URL.encode()).hexdigest()[-8:]
JOBID_PATT = re.compile(r"[0-9a-fA-F]{32}")
# ==============================================#
# =====      interactive C L I              ====#
# ==============================================#
tool_name = os.path.basename(__file__)
LOG_FORMATTER = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
#log.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=log.INFO, stream=sys.stdout)
PARPAT = re.compile(":param (\S+):(.+)")

# patching argparse for interactive cli
class CmdError(Exception):
    pass

class ErrorCatchingArgumentParser(argparse.ArgumentParser):
    def exit(self, status=0, message=None):
        raise CmdError(message)

    def error(self, message):
        args = {'prog': self.prog, 'message': message}
        self.exit(2, ('%(prog)s: ERROR: %(message)s\n') % args)


class CLI:
    """
    CLI handling class
    Supports both single command mode (run and exit) and interactive mode.
    """
    def __init__(self):
        self.cmds = {}  # dict cmd_name -> cmd_function
        # setup logging
        self.log = logging.getLogger(tool_name)
        self.log.setLevel(DEFAULT_LOG_LEVEL)
        log_file_name = DEFAULT_LOG_FILE_PREFIX + FLINK_ID + '.log'
        h = logging.handlers.RotatingFileHandler(log_file_name, 'a', 10 * 1024 * 1024, 3)
        h.setFormatter(LOG_FORMATTER)
        self.log.addHandler(h)
        self.ch = None
        if len(sys.argv) > 1:  # command mode
            self.parser = argparse.ArgumentParser(prog=tool_name)
            self.subparsers = self.parser.add_subparsers(title="commands", help="one of:", metavar='cmd')
        else:  # interactive mode
            self.parser = ErrorCatchingArgumentParser(prog='')
            self.subparsers = self.parser.add_subparsers(title="commands", help="one of:", metavar='cmd', parser_class=ErrorCatchingArgumentParser)
        self.internal_cmds = {'help/h/?': "Commands help", "exit/quit/q": "Exit",
                         "log": "enable logging to console", "jarid": "Print default jarid"}
        self.cmd_max_len = max([len(c) for c in self.internal_cmds])

    def log_to_console(self, enable=True):
        if enable:
            if len(self.log.handlers) > 1:
                return
            else:
                self.ch = logging.StreamHandler()
                self.ch.setFormatter(LOG_FORMATTER)
                self.log.addHandler(self.ch)
        else:
            if len(self.log.handlers) <= 1:
                self.log.removeHandler(self.ch)

    def print(self, *args, **kwargs):
        with io.StringIO() as out:
            print(*args, file=out, **kwargs)
            self.log.info(out.getvalue())
            print(out.getvalue(), end='')

    def command(self, func=None, **kwargs):
        """ Decorator """
        if func is None:
            return partial(self.command, **kwargs)
        cmd_name = func.__name__
        self.cmds[cmd_name] = func
        doc = getdoc(func)
        if doc:
            doc = doc.split("\n")
        cmd_help = doc[0] if doc and not doc[0].startswith(":param") else ""
        params_help = dict([m.groups() for m in map(PARPAT.match, doc) if m]) if doc else {}
        sp = self.subparsers.add_parser(cmd_name, help=cmd_help, description=cmd_help, add_help=False)
        sp.add_argument('-h', action='help', help='show this help message')
        sp.set_defaults(__cmd_name__=cmd_name)
        sig = signature(func)
        params = sig.parameters
        # add args subparser
        for p in params:
            par_args = {}
            p_name = p  # positional name for argparse
            phelp = params_help.get(p, '')
            if params[p].annotation != Parameter.empty:
                if params[p].annotation == list:  # this is a workaround
                    par_args['nargs'] = argparse.REMAINDER
                elif params[p].annotation == bool:
                    par_args['action'] = 'store_true'
                else:
                    par_args['type'] = params[p].annotation
                    phelp += f" (type: {params[p].annotation.__name__})"
            if params[p].default != Parameter.empty:
                phelp += f" (default: {params[p].default})"
                p_name = f'-{p_name}'  # optional
                par_args['default'] = params[p].default
            par_args['help'] = phelp
            if p in kwargs and isinstance(kwargs[p], list):
                par_args['choices'] = kwargs[p]
            sp.add_argument(p_name, **par_args)
        self.cmd_max_len = max(self.cmd_max_len, len(cmd_name))
        return func

    def run(self):
        if len(sys.argv) > 1:
            self.handleArgs()
        else:
            done = False
            while not done:
                comm = input('> ')
                done = self.handleCmd(comm.strip().split())

    def handleArgs(self, params=None):
        args = self.parser.parse_args(params)
        args = args.__dict__
        cmd_name = args.pop('__cmd_name__')
        self.log.info(f"EXEC: {cmd_name} {args}")
        f = self.cmds[cmd_name]
        f(**args)

    def handleCmd(self, params):
        if len(params) == 0:
            return
        self.log.info(f"CMD: {' '.join(params)}")
        # handle internal CLI commands
        if len(params) == 1:
            cmd = params[0]
            if cmd in ['help', '?', 'h']:
                print("Commands:")
                for c,h in self.internal_cmds.items():
                    print(f"  {{:<{self.cmd_max_len}}}   {{}}".format(c, h))
                print()
                for sa in self.subparsers._get_subactions():
                    print(f"  {{:<{self.cmd_max_len}}}   {{}}".format(sa.dest, sa.help))
                print()
                print("Type <command> -h for details on command's parameters.")
                return
            elif cmd in ['exit', 'quit', 'q']:
                return True
            elif cmd == 'log':  # for debugging only
                if params:
                    if params[0] == 'on':
                        self.log_to_console(True)
                    elif params[0] == 'off':
                        self.log_to_console(False)
                return
            elif cmd == 'jarid':
                self.print(cfg.default_jarid if cfg.default_jarid is not None else "Default jarid is not set.")
                return
        try:
            self.handleArgs(params)
        except CmdError as ce:
            if ce.args[0]:
                self.print(ce)
        #except Exception as e:
        #    self.print(f"Error when running command: {params}", e)


c = CLI()

#
#
# Config Handler
#
#

CFG_UPDATE_TS = 'cfg_update_utc'  # str utc isoformat
CFG_FLINK_URL = 'flink_url'
CFG_DEFAULT_JARID = 'default_jarid'  # str
CFG_JOBS = 'jobs'  # dict(jobid-> {startts, jarid, jobclass, params}
EMPTY_CFG = {
    CFG_FLINK_URL: URL,
    CFG_JOBS: {},
    CFG_UPDATE_TS: None,
    CFG_DEFAULT_JARID: None
}
class ConfigHandler:
    def __init__(self, cfg_file=None, sync=True, logger=None):
        if cfg_file is None:
            cfg_file = DEFAULT_CFG_FILE_PREFIX + FLINK_ID + '.json'
        self.cfg_file = cfg_file
        self.log = logger
        self.sync = sync
        self.cfg = None
        try:
            with open(cfg_file) as cf:
                self.cfg = json.load(cf)
        except Exception as e:
            self.error(f"Error when loading config file {cfg_file}", e)
        if not self.cfg:
            self.cfg = EMPTY_CFG
        self.default_jarid = self.cfg[CFG_DEFAULT_JARID]
    def error(self, *args):
        if self.log:
            self.log.error(args)
        print('ERROR', *args)
    def info(self, *args):
        if self.log:
            self.log.info(args)
        print(*args)
    def flush(self):
        self.cfg[CFG_UPDATE_TS] = datetime.utcnow().isoformat()
        with open(self.cfg_file, 'w') as cf:
            json.dump(self.cfg, cf, indent=4)
    def set_def_jar(self, jarid):
        self.cfg[CFG_DEFAULT_JARID] = jarid
        self.default_jarid = jarid
        self.info(f"Set default jarid: {jarid}")
        self.flush()
    def add_job(self, jobid, jarid, jobclass, params):
        self.cfg[CFG_JOBS][jobid] = {'start_utc': datetime.utcnow().isoformat(),
                                     'jarid': jarid,
                                     'class': jobclass,
                                     'params': params}
        self.flush()
    def show(self):
        print(json.dumps(self.cfg, indent=4))


cfg = ConfigHandler(logger=c.log)

#
###########################################
#               FLINK CLIENT              #
###########################################
#

VERBOSE = False
API_VERSION = "v1"
OPS = {
    "config": "config",
    "jars": "jars",
    "jobs": "jobs",
    "overview": "overview",
    "taskmanagers": "taskmanagers",
    "jobs_metrics": "jobs/metrics",
    "jobs_overview": "jobs/overview",
    "jm_config": "jobmanager/config",
    "jm_logs": "jobmanager/logs",
    "jm_metrics": "jobmanager/metrics",
    "tm_metrics": "taskmanagers/metrics"
}

def _get_url(operation):
    return f"{URL}/{API_VERSION}/{operation}"

def _pprint(d : dict):
    c.print(yaml.dump(d, default_flow_style=False))

def _print_failure_cause(fc):
    if isinstance(fc, dict):
        if 'class' in fc:
            c.print(f"Class: {fc['class']}")
        if 'stack-trace' in fc:
            c.print("Stack trace:")
            c.print(fc['stack-trace'])

def _rest(op, method='GET', files=None, json=None, ok_code=200, timeout=None):
    url = _get_url(op)
    c.log.debug(f"REQ: method={method}, url={url}, json={json}")
    res = requests.request(method=method, url=url, files=files, json=json, timeout=timeout)
    c.log.debug(f"RESP: status_code={res.status_code}")
    c.log.debug(f"    : result={res.json()}")
    if res.status_code == ok_code:
        return res.json()
    else:
        c.log.error(f"Got status code {res.status_code}")
        rj = res.json()
        if 'errors' in rj:
            errors = rj['errors']
            for e in errors:
                c.print(e)
    return None

def _validate_jobid(job):
    """
    Get valid job id
    :param job: jobid or job name
    :return: valid jobid that's present on the cluster
    """
    jobs = get_jobs()
    if JOBID_PATT.fullmatch(job):
        if job not in get_jobs():
            c.print(f"No job with jobid:{job} found on the cluster.")
            job = None
    else:
        jobids = [jid for jid,name in jobs.items() if name == job]
        job = None
        if len(jobids) == 1:
            job = jobids[0]
        elif len(jobids) > 1:
            c.print(f'More than one job {job} found on the cluster, use jobid instead of job name.')
        else:
            c.print(f"Job: {job} not found on the cluster.")
    return job

def _check_free_slots():
    r = _rest('overview')
    if r:
        if r['slots-available'] > 0:
            return True
        else:
            ans = input("No free slots available. Continue? (Y/n): ")
            if ans == 'Y':
                return True
    else:
        c.print("Unable to verify free slots on the cluster.")
    return False

def get_jobs(running=False):
    """
    Returns dict jobid->name
    :param running:
    :return:
    """
    r = _rest(OPS['jobs_overview'])
    if r and r['jobs']:
        if running:
            return dict([(job['jid'], job['name']) for job in r['jobs'] if job['state'] == 'RUNNING'])
        else:
            return dict([(job['jid'], job['name']) for job in r['jobs']])
    return None


def get_loaded_jars(timeout=None):
    r = _rest('jars', timeout=timeout)
    if r:
        jars = {}
        for f in r['files']:
            jars[f['id']] = {'name': f['name'], 'uploaded': f['uploaded']}
        return jars
    return None

def _init_config():
    jar_id = cfg.default_jarid
    print(f"Connecting to url: {URL}")
    jars = get_loaded_jars()
    if jar_id and jar_id in jars:
        return jar_id
    if jar_id:
        c.print(f"Default jar {jar_id} is no longer available on the cluster.")
    else:
        if len(jars) == 1:
            jar_id = list(jars.keys())[0]
            cfg.set_def_jar(jar_id)
            return jar_id

#
# COMMANDS
#
@c.command(op=list(OPS.keys()))
def get(op: str):
    """
    Execute REST API Get operation
    :param op: name of REST GET operation
    """
    c.print(op)
    c.print("="*len(op))
    r = _rest(OPS[op])
    _pprint(r)

@c.command
def jobs(all:bool = False):
    """
    List running jobs
    :param all: show all jobs
    """
    r = _rest(OPS['jobs_overview'])
    if r and r['jobs']:
        for job in r['jobs']:
            if all or job['state'] == 'RUNNING':
                c.print(f"{job['jid']} {job['state']:<12} {job['name']}")
    else:
        c.print("No jobs are running on", URL)

@c.command
def delete_jar(jarid:str):
    """
    Delete jar
    :param jarid: hex id of the jar
    :return:
    """
    r = _rest(f'jars/{jarid}', method='DELETE')
    if r is not None and cfg.default_jarid == jarid:
        cfg.set_def_jar(None)

@c.command
def upload_jar(jar_file_path:str):
    """
    Upload jar to Flink
    :param jar_file_path: path of the jar file to upload
    """
    # step 1 - upload jar with /jars/upload
    if not os.path.exists(jar_file_path):
        c.print(f"JAR file {jar_file_path} not found!")
        return
    filename = os.path.basename(jar_file_path)
    files = {
        "file": (filename, (open(jar_file_path, "rb")), "application/x-java-archive")
    }
    c.print(f"Uploading {filename} to {URL}")
    r = _rest('jars/upload', method='POST', files=files)

    if r:
        filename = r['filename']
        #c.print("filename:", filename)
        status = r['status']
        if status == 'success':
            jarid = filename.split('/')[-1]
            c.print("jarid:", jarid)
            cfg.set_def_jar(jarid)
            return jarid
        else:
            c.print(f"Got status {status} when uploading jar.")
            return None

@c.command
def run_with_jarid(jarid:str, class_name:str, job_args:list, allow_non_restored_state=False, savepoint_path=None):
    if not _check_free_slots():
        return
    # step 2: run
    req_data = {'entryClass': class_name, 'allowNonRestoredState': allow_non_restored_state}
    if job_args:
        req_data['programArgsList'] = job_args
    if savepoint_path:
        req_data['savepointPath'] = savepoint_path
    r = _rest(f'jars/{jarid}/run', method='POST', json=req_data)
    if r:
        jobid = r['jobid']
        c.print(f"Job started with jobid={jobid}")
        cfg.add_job(jobid, jarid, class_name, job_args)
        return True
    else:
        return False

@c.command
def run(class_name:str, job_args:list, allow_non_restored_state:bool=False, savepoint_path:str=None):
    """
    Run a job implemented in class_name using default jarid.
    :param class_name:
    :param class_name: name of the class to execute
    :param job_args: a list of parameters to pass to the class
    :param allow_non_restored_state: Boolean value that specifies whether the job submission should be rejected if the savepoint contains state that cannot be mapped back to the job
    :param savepoint_path: the path of the savepoint to restore the job from
    """
    #print(f"cls: {class_name}, all_n_r:{allow_non_restored_state}, spoint:{savepoint_path}, jobargs:{job_args}")
    if cfg.default_jarid:
        c.print(f"jarid={cfg.default_jarid}")
        run_with_jarid(cfg.default_jarid, class_name, job_args, allow_non_restored_state, savepoint_path)
    else:
        c.print("Default jar is not set. Use upload_jar command to upload a jar or run_job command to run with jarid.")

@c.command
def run_with_jar(jar_path:str, class_name:str, job_args:list, allow_non_restored_state:bool=False, savepoint_path:str=None):
    """
    Upload a jar file and run a job implemented in class_name
    :param jar_path: path to the jar file
    :param class_name: name of the class to execute
    :param job_args: a list of parameters to pass to the class
    :param allow_non_restored_state: Boolean value that specifies whether the job submission should be rejected if the savepoint contains state that cannot be mapped back to the job
    :param savepoint_path: the path of the savepoint to restore the job from
    """
    if not _check_free_slots():
        return
    jarid = upload_jar(jar_path)
    if jarid:
        # step 2: run
        run_with_jarid(jarid, class_name, job_args, allow_non_restored_state, savepoint_path)

@c.command
def stop_job(job: str, drain: bool = True, savepoint_dir: str = DEFAULT_SAVEPOINTS_DIR):
    """
    Stop a job gracefully with a savepoint.
    :param job: jobid or job name
    :param drain: emit a MAX_WATERMARK before taking the savepoint to flush out any state waiting for timers to fire
    :param savepoint_dir: a path to directory to store the savepoint
    """
    jobid = _validate_jobid(job)
    if not jobid:
        return
    req_data = {'drain': drain, 'targetDirectory':savepoint_dir}
    r = _rest(f'jobs/{jobid}/stop', method='POST', ok_code=202, json=req_data)
    if r:
        trigger_id = r['request-id']
        c.print('Trigger id:', trigger_id, 'waiting for the operation to complete', end='')
        # wait for the trigger to complete
        while True:
            r = _rest(f'jobs/{jobid}/savepoints/{trigger_id}')
            if r['status']['id'] == 'COMPLETED':
                print()
                op = r['operation']
                if 'failure-cause' in op:
                    print('Operation failed with cause:')
                    _print_failure_cause(op['failure-cause'])
                    break
                elif 'location' in op:
                    loc = op['location']
                    print('Job stopped with savepoint', loc)
                    return loc
            elif r['status']['id'] == 'IN_PROGRESS':
                print('.', end='', flush=True)
            else:
                c.log.warning(f"Unexpected response: {r}")
            time.sleep(1)

@c.command
def savepoint(job:str, cancel:bool=False, savepoint_dir:str=DEFAULT_SAVEPOINTS_DIR):
    """
    Triggers a savepoint, and optionally cancels the job afterwards.
    :param job: jobid or job name
    :param cancel:
    :param savepoint_dir: a path to directory to store the savepoint
    """
    jobid = _validate_jobid(job)
    if not jobid:
        return
    req_data = {'cancel-job': cancel, 'target-directory':savepoint_dir}
    r = _rest(f'jobs/{jobid}/savepoints', method='POST', ok_code=202, json=req_data)
    if r:
        trigger_id = r['request-id']
        c.print('Trigger id:', trigger_id, 'waiting for the operation to complete', end='')
        # wait for the trigger to complete
        while True:
            r = _rest(f'jobs/{jobid}/savepoints/{trigger_id}')
            if r['status']['id'] == 'COMPLETED':
                print()
                op = r['operation']
                if 'failure-cause' in op:
                    c.print('Operation failed with cause:')
                    _print_failure_cause(op['failure-cause'])
                    break
                elif 'location' in op:
                    loc = op['location']
                    c.print('Created savepoint:', loc)
                    return loc
            elif r['status']['id'] == 'IN_PROGRESS':
                print('.', end='', flush=True)
            else:
                c.log.warning(f"Unexpected response: {r}")
            time.sleep(1)


@c.command
def kill(job):
    """
    Kill job. WARNING: This command requires user's confirmation.
    :param job: jobid or job name
    """
    jobid = _validate_jobid(job)
    if not jobid:
        return
    rjobs = get_jobs(running=True)
    if jobid in rjobs:
        c.print(f"Job {rjobs[jobid]} with jobid={jobid} will be terminated.")
        ans = input("ARE YOU SURE? (Y/n): ")
        if ans == 'Y':
            r = _rest(f'jobs/{jobid}', method='PATCH', ok_code=202)
            if r is not None:
                c.print(f"Terminated job {jobid}")
        else:
            c.print("Kill command aborted.")
    else:
        c.print(f"No RUNNING job with jobid: {jobid}")
@c.command
def kill_all():
    """
    Kill all running jobs. WARNING: This command requires user's confirmation.
    """
    rjobs = get_jobs(running=True)
    if rjobs:
        c.print("The following jobs will be killed::")
        for jobid in rjobs:
            c.print(jobid, rjobs[jobid])
        ans = input("ARE YOU SURE? (Y/n): ")
        if ans == 'Y':
            for jobid in rjobs:
                r = _rest(f'jobs/{jobid}', method='PATCH', ok_code=202)
                if r is not None:
                    c.print(f"Terminated job {jobid}")
        else:
            c.print("Kill command aborted.")


@c.command
def job_config(job:str):
    """
    Get job configuration
    :param job: jobid or job name
    """
    jobid = _validate_jobid(job)
    if jobid:
        r = _rest(f'jobs/{jobid}/config')
        _pprint(r)

@c.command
def job_exc(job):
    """
    Get job exceptions
    :param job: jobid or job name
    """
    jobid = _validate_jobid(job)
    if jobid:
        r = _rest(f'jobs/{jobid}/exceptions')
        _pprint(r)

@c.command
def job_res(job):
    """
    Get job result
    :param job: jobid or job name
    """
    jobid = _validate_jobid(job)
    if jobid:
        r = _rest(f'jobs/{jobid}/execution-result')
        _pprint(r)

@c.command
def job_chk(job):
    """
    Get job checkpoints
    :param job: jobid or job name
    """
    jobid = _validate_jobid(job)
    if jobid:
        r = _rest(f'jobs/{jobid}/checkpoints')
        _pprint(r)


INFO_OVW_COMMON = {'Task Managers': 'taskmanagers', 'Slots': 'slots-total', 'Slots available': 'slots-available'}
INFO_OVW_JOBS = {'Running': 'jobs-running', 'Cancelled': 'jobs-cancelled', 'Failed': 'jobs-failed'}
@c.command
def info():
    """
    Get cluster info
    :return:
    """
    # get overview
    msg = f"Info:\nUrl: {URL}\n"
    r = _rest('overview')
    if r:
        for m, f in INFO_OVW_COMMON.items():
            msg += f"  {m:<24}: {r[f]}\n"
        msg += "  Jobs:\n"
        for m, f in INFO_OVW_JOBS.items():
            msg += f"    {m:<10}: {r[f]}\n"
        c.print(msg)

INFO_JOBID = {'Name': 'name', 'State': 'state', 'Started': 'start-time',
              'Duration': 'duration', 'Ended': 'end-time'}
INFO_JOB_CHK_CNT = {'Completed': 'completed', 'Failed': 'failed', 'In Progress': 'in_progress',
                    'Restored': 'restored', 'Total': 'total'}

INFO_JOB_CHK_DETAILS = {'Status': 'status', 'Type': 'checkpoint_type', 'Started': 'trigger_timestamp',
                        'Duration': 'end_to_end_duration', 'Size': 'state_size', 'Savepoint': 'is_savepoint'}

def _format_fields(dd: dict, fd: dict, indent=''):
    msg = ''
    for m, f in fd.items():
        if f in dd and dd[f] not in [None, 0, -1]:
            msg += f"{indent}{m:<10}: {dd[f]}"
            if 'time' in f:
                try:
                    hv = datetime.fromtimestamp(int(dd[f] / 1000))
                    msg += f' ({hv.isoformat()})'
                except:
                    pass
            elif 'duration' in f:
                try:
                    hv = timedelta(seconds=dd[f])
                    msg += f' ({hv})'
                except:
                    pass
            msg += '\n'
    return msg

@c.command
def job(job: str):
    """
    Get job info
    :param job: jobid or job name
    """
    jobid = _validate_jobid(job)
    msg = f'Job id: {jobid}\n'
    if jobid:
        # jobs/jobid
        job_details = _rest(f'jobs/{jobid}')
        if job_details:
            msg += _format_fields(job_details, INFO_JOBID, indent='  ')
        # jobs/jobid/checkpoints
        chkpts = _rest(f'jobs/{jobid}/checkpoints')
        if chkpts:
            msg += "Checkpoints:\n"
            ccnt = chkpts['counts']
            msg += _format_fields(ccnt, INFO_JOB_CHK_CNT, indent='  ')

            if chkpts['latest']:
                msg += "Latest:\n"
                for ct in ['completed', 'failed', 'restored']:
                    chkp = chkpts['latest'].get(ct, None)
                    if chkp:
                        msg += _format_fields(chkp, INFO_JOB_CHK_DETAILS, indent='    ')
        # get log location from:
        # jobs/jobid/vertices/vertex/taskmanagers
        if 'vertices' in job_details:
            msg += "Task Manager:\n"
            unique_tms = set() # set of task_manager_ids
            tasks = {}  # dict vertex_name -> [task_manager_id]
            tmhost = {} # task_manager_id -> task_manager_host  # TODO: check if necessary
            for vertex in job_details['vertices']:
                v_id = vertex.get('id')
                v_name = vertex.get('name')
                if v_id:
                    tasks[v_name] = []
                    vtms = _rest(f'jobs/{jobid}/vertices/{v_id}/taskmanagers')
                    for tm in vtms['taskmanagers']:
                        tid = tm['taskmanager-id']
                        unique_tms.add(tid)
                        tasks[v_name].append(tid)
                        tmhost[tid] = tm['host']
            for tm in unique_tms:
                tasks = [t for t in tasks if tm in tasks[t]]
                msg += f"  id: {tm}\n    host : {tmhost[tm]}\n    tasks: {tasks}"
    c.print(msg)


_init_config()
try:
    c.run()
finally:
    cfg.flush()
