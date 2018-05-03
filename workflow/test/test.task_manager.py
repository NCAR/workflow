#! /usr/bin/env python
from subprocess import call
from workflow import task_manager as tm
import time


# stop script (graceful exit) and wait on jobs after:
tm.QUEUE_MAX_HOURS = 1000./3600.

# max number of jobs in queue
tm.MAXJOBS = 15

# number of jobs to submit and wait on
N = 10

# loop and submit
for i in range(0,N+1):
    time.sleep(0)
    jid = tm.submit([['touch','test.file.%03d'%i],['sleep','2'],['echo','done']])

print('waiting')
time.sleep(0)

# submit dependent job
jid = tm.submit(['ls','-1'],depjob=tm.JID)

# wait on all jobs
ok = tm.wait()

for i in range(0,N+1):
    call(['rm','-f','test.file.%03d'%i])
