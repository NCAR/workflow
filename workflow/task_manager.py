#! /usr/bin/env python
from __future__ import print_function

import os
import stat
import sys
import socket
import time
import re
import tempfile
from subprocess import Popen,PIPE,call
from datetime import datetime
from glob import glob

#-- total runtime metrics
PROGRAM_START = datetime.now()  # init timer
QUEUE_MAX_HOURS = 20.           # trigger "stop" after QUEUE_MAX_HOURS

#-- job lists
JID = []           # the list of active job IDs
MAXJOBS = 400      # max number of jobs to keep in the queue

#--  account
ACCOUNT = 'NCGD0011'

#-- default conda environment for analyses
CONDA_ENV = 'py3_geyser'
CONDA_PATH = 'PATH=/glade/work/mclong/miniconda3/bin'

USER_MAIL = os.environ['USER']+'@ucar.edu'

#-- machine dependencies
hostname = socket.gethostname()
if any(s in hostname for s in ['cheyenne','geyser','caldera','pronghorn']):
    Q_SYSTEM = 'SLURM'
    SCRATCH = os.path.join('/glade/scratch',os.environ['USER'])
else:
    #TODO: implement Q_SYSTEM = None
    raise ValueError('hostname not found')


#-- where to place log and run file output output
JOB_FILE_PREFIX = 'task_manager.calc'
TMPDIR = os.path.join(SCRATCH,'tmp')
if not os.path.exists(TMPDIR):
    stat = call(['mkdir','-p',TMPDIR])
    if stat != 0: raise

JOB_LOG_DIR = os.path.join(SCRATCH,'task-manager')
if not os.path.exists(JOB_LOG_DIR):
    stat = call(['mkdir','-p',JOB_LOG_DIR])
    if stat != 0: raise

#-- job status codes
_job_stat_run = 'RUN'
_job_stat_done = 'DONE'
_job_stat_fail = 'FAILED'
_job_stat_pend = 'PENDING'
_job_stat_recheck = 'RECHECK'

#------------------------------------------------------------------------
#--- FUNCTION
#------------------------------------------------------------------------

def report_status(msg):
    '''
    print a status message with timestamp
    '''
    date = datetime.now()
    print(date.strftime("%Y %m %d %H:%M:%S")+': ' + msg)

#------------------------------------------------------------------------
#--- FUNCTION
#------------------------------------------------------------------------

def total_elapsed_time():
    '''
    compute total time elapsed since initialization
    return time in hours
    '''
    return (datetime.now() - PROGRAM_START).total_seconds()/3600.

#------------------------------------------------------------------------
#--- FUNCTION
#------------------------------------------------------------------------

def _wait_on_jobs(job_wait_list=[],njob_target=0):
    '''
    wait on a list of job IDs
    return when the number of running jobs has reached njob_target
    '''

    ok = True

    if not job_wait_list:
        job_wait_list = JID

    #-- check total time
    stop_now = False
    if total_elapsed_time() > QUEUE_MAX_HOURS:
        report_status('total elapsed time: %.4f h'%total_elapsed_time())
        stop_now = True

    #-- check number of running jobs
    njob_running = len(job_wait_list)
    if njob_running <= njob_target:
        return ok,stop_now

    report_status('waiting on %d jobs'%njob_running)
    if njob_target == 0:
        print('-'*50)
        for jid in job_wait_list:
            print(jid,end=' ')
        print()
        print('-'*50)
        print()

    #-- wait on jobs
    job_status = {}
    first_run = True
    fail_list = []
    while (njob_running > njob_target):

        #-- loop over active jobs
        active_jobs = []
        for jid in job_wait_list:

            #-- check status and report on first pass or if changed
            job_status_jid = status(jid)

            if njob_target == 0:
                if not first_run:
                    if not job_status[jid] == job_status_jid:
                        report_status(jid+' status: '+job_status_jid)
                else:
                    report_status(jid+' status: '+str(job_status_jid))
            job_status[jid] = job_status_jid

            #-- status dependent actions
            if job_status_jid in [_job_stat_pend,_job_stat_run]:
                active_jobs.append(jid)

                #-- test if job had dependencies
                dependencies_list = job_dependencies(jid)
                if dependencies_list:
                    #-- kill if any these in fail_list
                    if any(j in fail_list for j in dependencies_list):
                        kill(jid)
                        report_status(jid+' killed due to failed dependencies')

            elif job_status_jid == _job_stat_done:
                pass

            elif job_status_jid is None: #-- assume the job has completed successfully (the queueing system forgets)
                pass

            elif job_status_jid == _job_stat_fail:
                fail_list.append(jid)
                ok = False
            else:
                ok = False
                report_status(jid+' unknown message: '+job_status_jid)

        #-- update list of active jobs to those still active
        job_wait_list[:] = active_jobs
        njob_running = len(active_jobs)
        del active_jobs[:]

        #-- finish loop
        first_run = False
        time.sleep(1)

    #-- update module variable
    JID[:] = job_wait_list

    #-- exit with messages
    if total_elapsed_time() > QUEUE_MAX_HOURS:
        stop_now = True

    if not ok:
        print()
        print('-'*50)
        report_status('Failed jobs:')
        for jid in fail_list:
            print(jid,end=' ')
        print()
        print('-'*50)
        print()
    else:
        report_status('Done waiting.')

    if njob_running != 0:
        report_status('%d active jobs remain.'%njob_running)

    return ok,stop_now

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def _os_status(jid):
    return _job_stat_done

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def _os_call(command,**kwargs):
    ok = True
    stop = False
    cmd_line = []
    if isinstance(command[0],list):
        cmd_line = 'set -e ; '+'; '.join([' '.join(cmd) for cmd in command])
    else:
        cmd_line = ' '.join(command)

    env = os.environ.copy()

    p = Popen(cmd_line,
          stdin=None,
          stdout=PIPE,
          stderr=PIPE,
          env=env,
          shell=True)
    stdout, stderr = p.communicate()

    stdout = stdout.decode('UTF-8')
    stderr = stderr.decode('UTF-8')

    jid = p.pid
    ok = p.returncode == 0

    if not ok:
        print('os submit failed!')
        print('Command:')
        print(cmd_line)
        print('\nstdout:')
        print(stdout)
        print('\nstderr:')
        print(stderr)
        raise

    if total_elapsed_time() > QUEUE_MAX_HOURS:
        stop = True

    return jid,ok,stop

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def _slurm_batch_submit(command,
                        constraint=None,
                        partition='dav',
                        account='',
                        conda_env='',
                        modules = [],
                        module_purge = False,
                        time_limit = '24:00:00',
                        memory = '100GB',
                        email = False,
                        depjob = None,
                        job_name=''):

    #-- init return args
    ok = True
    stop = False

    if not conda_env and CONDA_ENV:
        conda_env = CONDA_ENV

    if not account:
        account = ACCOUNT

    #-- determine the job name if not provided
    cmd_line = []
    if isinstance(command[0],list):
        cmd_line = [' '.join(cmd) for cmd in command]
    else:
        cmd_line = [' '.join(command)]

    if not job_name:
        job_name = cmd_line[0].split(' ')[0]

    #-- set up batch script
    job_datetime = datetime.now().strftime('%Y%m%d-%H%M%S')

    #-- get environment...include in batch?
    env = os.environ.copy()

    fid,batch_script_file = tempfile.mkstemp(dir=JOB_LOG_DIR,
                                             prefix=JOB_FILE_PREFIX+'.'+job_datetime+'.',
                                             suffix='.run')
    stdoe = batch_script_file.replace('.run','.%J.out')

    #-- construct batch file
    #---- slurm directives
    batch_script_pre = ['#!/bin/bash',
                        '#SBATCH -J '+job_name.split(' ')[0],
                        '#SBATCH -n 1',
                        '#SBATCH --ntasks-per-node=1',
                        '#SBATCH -p '+partition,
                        '#SBATCH -A '+account,
                        '#SBATCH -t '+time_limit,
                        '#SBATCH --mem='+memory,
                        '#SBATCH -e '+stdoe,
                        '#SBATCH -o '+stdoe]
    if constraint is not None:
        batch_script_pre.append('#SBATCH -C '+constraint)
        
    if email:
        batch_script_pre.append('#SBATCH --mail-type=ALL')
        batch_script_pre.append('#SBATCH --mail-user='+USER_MAIL)

    depjobstr = ''
    if isinstance(depjob,list):
        #-- cull list if status is None
        depjob_culled = [jid for jid in depjob
                         if _slurm_job_status(jid) is not None]
        if len(depjob) != len(depjob_culled):
            print('Some job dependencies not found:')
            print(depjob)
            print(depjob_culled)
            depjob = depjob_culled

        depjobstr = ':'.join(depjob)

    elif isinstance(depjob,str):
        if _slurm_job_status(depjob) is not None:
            depjobstr = depjob

    if depjobstr:
        batch_script_pre.append('#SBATCH -d afterok:'+depjobstr)
    #---- end slurm directives

    batch_script_pre.extend([
        'if [ -z $MODULEPATH_ROOT ]; then',
        '  unset MODULEPATH_ROOT',
        'else',
        '  echo "NO MODULEPATH_ROOT TO RESET"',
        'fi',
        'if [ -z $MODULEPATH ]; then',
        '  unset MODULEPATH',
        'else',
        '  echo "NO MODULEPATH TO RESET"',
        'fi',
        'if [ -z $LMOD_SYSTEM_DEFAULT_MODULES ]; then',
        '  unset LMOD_SYSTEM_DEFAULT_MODULES',
        'else',
        '  echo "NO LMOD_SYSTEM_DEFAULT_MODULES TO RESET"',
        'fi',
        'source /etc/profile',
        'export TERM='+env['TERM'],
        'export HOME='+env['HOME']])

    #-- I get the following error in Python reading netcdf4 files:
    # ImportError: /lib64/libk5crypto.so.3: symbol krb5int_buf_len, version
    #   krb5support_0_MIT not defined in file libkrb5support.so.0 with link time
    #   reference
    # this fixes it:
    batch_script_pre.append('unset LD_LIBRARY_PATH')

    batch_script_pre.append('export {CONDA_PATH}:$PATH'.format(CONDA_PATH=CONDA_PATH))
    batch_script_pre.append('export PYTHONUNBUFFERED=False')
    batch_script_pre.append('export TMPDIR='+TMPDIR)

    if module_purge:
        batch_script_pre.append('module purge')

    if modules:
        for module in modules:
            batch_script_pre.append('module load '+module)
        batch_script_pre.append('module list')

    if conda_env:
        batch_script_pre.append('source activate %s'%conda_env)

    batch_script_post = ['exit ${?}']

    #-- write run script
    with open(batch_script_file,'w') as fid:
        for line in batch_script_pre+cmd_line+batch_script_post:
            fid.write('%s\n'%line)

    #-- make run script executable
    st = os.stat(batch_script_file)
    os.chmod(batch_script_file, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH )

    #-- submit the job
    p = Popen(['sbatch',batch_script_file],
              stdin=None,
              stdout=PIPE,
              stderr=PIPE,
              env=env)
    stdout, stderr = p.communicate()

    stdout = stdout.decode('UTF-8')
    stderr = stderr.decode('UTF-8')

    #-- parse return string to get job ID
    try:
        jid = stdout.splitlines()[-1].split(' ')[-1].strip()
        JID.append(jid)
    except:
        print('SLURM sbatch failed!')
        print('Command:')
        print(command)
        print('\nstdout:')
        print(stdout)
        print('\nstderr:')
        print(stderr)
        raise

    #-- print job id and job submission string
    scmd = '; '.join(cmd_line)
    print('-'*50)
    print('%s (%s): %s'%(jid,os.path.basename(batch_script_file),scmd))
    print('-'*50)
    print()

    if total_elapsed_time() > QUEUE_MAX_HOURS:
        stop = True

    #-- return job id
    return jid,ok,stop

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def _slurm_scontrol_show_job(jid):
    '''
    return job status parsing scontrol command
    '''
    err = False

    p = Popen(['scontrol','show','job',jid],
              stdin=None, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()

    stdout = stdout.decode('UTF-8')
    stderr = stderr.decode('UTF-8')

    #-- jobs disappear, so if the job status cannot be found, return None
    if stderr.strip() == 'slurm_load_jobs error: Invalid job id specified':
        return None

    elif stderr.strip() == 'slurm_load_jobs error: Socket timed out on send/recv operation':
        status_dict = _slurm_scontrol_show_job(jid)

    else:
        try:
            status_dict = {}
            for item in filter(None,re.split(' |\n',stdout)):
                key,val = item.split('=',1)
                status_dict[key] = val
        except:
            print('SLURM scontrol failed:')
            print(stdout)
            print(stderr)
            print(status_dict)
            raise

    return status_dict

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def _slurm_job_dependencies(jid):
    status_dict = _slurm_scontrol_show_job(jid)

    if status_dict is None:
        return []
    elif status_dict['Dependency'] == '(null)':
        return []
    else:
        return [s.split(':')[1] for s in status_dict['Dependency'].split(',')]


#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def _slurm_job_status(jid):
    '''
    return job status parsing scontrol command
    '''

    stat_codes = {'RUNNING': _job_stat_run,
                  'COMPLETED': _job_stat_done,
                  'COMPLETING': _job_stat_recheck,
                  'FAILED': _job_stat_fail,
                  'TIMEOUT': _job_stat_fail,
                  'CANCELLED': _job_stat_fail,
                  'OUT_OF_MEMORY':_job_stat_fail,
                  'PENDING': _job_stat_pend}

    status_dict = _slurm_scontrol_show_job(jid)

    if status_dict is None:
        return None
    elif status_dict['JobState'] in stat_codes:
        return stat_codes[status_dict['JobState']]
    else:
        raise ValueError('Unknown job status message: %s'%status_dict['JobState'])


#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def kill(jid):
    if Q_SYSTEM is None:
        call(['kill',jid])
    elif Q_SYSTEM == 'LSF':
        call(['bkill',jid])
    elif Q_SYSTEM == 'SLURM':
        call(['scancel',jid])

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def job_dependencies(jid):
    if Q_SYSTEM is None:
        return []
    elif Q_SYSTEM == 'LSF':
        return []
    elif Q_SYSTEM == 'SLURM':
        return _slurm_job_dependencies(jid)

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def submit(cmdi,**kwargs):

    #-- if number of jobs is at max, wait
    if len(JID) >= MAXJOBS:
        print('Job count at threshold.')
        ok = wait(JID,njob_target=0)
        stop_program(ok)

    if Q_SYSTEM is None:
        jid,ok,stop = _os_call(cmdi,**kwargs)
    elif Q_SYSTEM == 'LSF':
        jid,ok,stop = _bsub(cmdi,**kwargs)
    elif Q_SYSTEM == 'SLURM':
        jid,ok,stop = _slurm_batch_submit(cmdi,**kwargs)

    stop_program(ok,stop)
    return jid

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def wait(job_wait_list=[],njob_target=0,closeout=False):
    ok,stop = _wait_on_jobs(job_wait_list,njob_target)
    if not closeout:
        stop_program(ok,stop)
    return ok

#----------------------------------------------------------------
#---- function
#----------------------------------------------------------------

def status(jid):

    stat_out = None
    if Q_SYSTEM is None:
        stat_out = _os_status(jid)
    elif Q_SYSTEM == 'LSF':
        stat_out = _bstat(jid)
    elif Q_SYSTEM == 'SLURM':
        stat_out = _slurm_job_status(jid)
        i = 0
        while stat_out == _job_stat_recheck and i < 100:
            stat_out = _slurm_job_status(jid)
            i += 1
            time.sleep(0.5)

        if stat_out == _job_stat_recheck:
            stat_out = _job_stat_fail

    return stat_out

#----------------------------------------------------------------
#---- FUNCTION
#----------------------------------------------------------------

def stop_program(ok=False,stop=False):
    if not ok:
        if JID:
            print('waiting on remaining jobs')
            ok = wait(closeout=True)
        print('EXIT ERROR')
        sys.exit(1)
    elif stop:
        print('QUEUE TIMER EXPIRED')
        ok = wait(closeout=True)
        stop_program(ok)
        sys.exit(43)

#------------------------------------------------------------------------
#--- main
#------------------------------------------------------------------------

if __name__ == "__main__":
    '''
    call this script with task and arguments to use functions at command line
    tasks: submit, status, wait, peek
    example:
      wait on job(s)
        ./lsf_tools.py wait job_wait_list
    '''

    task = sys.argv[1]
    args = sys.argv[2:]

    if task == "submit":
        jid = submit(args,email=True)

    elif task == "status":
        print(status(args[0]))

    elif task == "wait":
        wait(args)

    elif task == "peek":

        job_datetime='????????-??????'
        random_str = '*'
        stdoe = os.path.join(JOB_LOG_DIR,'.'.join([JOB_FILE_PREFIX,job_datetime,random_str,args[0],'out']))
        print(stdoe)
        jout = glob(stdoe)
        jout.sort()
        if jout:
            try:
                call(['less',jout[-1]])
            except:
                call(['reset'])
                call(['clear'])
                exit()
        else:
            print(args[0]+' not found.')

    else:
        print(task+' not found.')
