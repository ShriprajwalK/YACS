import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import json
import datetime
import matplotlib.dates as mdates

def convert_to_datetime(datetime_string):
    k = datetime.datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S,%f')
    return k


policies = ['LL', 'RR', 'RD']

avg_times = {}
median_times = {}

for policy in policies:
    with open(f'log_master_{policy}.log') as f:
        logs = f.readlines()

    job_received_log = []
    job_completed_log = []
    for log in logs:
        log = log.split('%')
        log[0] = log[0].strip()
        if log[1] == 'MASTER RECEIVED JOB':
            job_received_log.append(log)
        elif log[1] == 'JOB FINISHED WITH ID:':
            job_completed_log.append(log)
    for i in job_received_log:
        i[2] = json.loads(i[2])['job_id']

    job_times = {}
    for i in job_received_log:
        for j in job_completed_log:
            if i[2] == j[2]:
                job_times[i[2]] = (convert_to_datetime(j[0]) - convert_to_datetime(i[0])).total_seconds()

    avg_times[policy] = sum(list(job_times.values()))/len(job_times)
    sorted_times = sorted(list(job_times.values()))
    mid = len(job_times) // 2
    median_times[policy] = (sorted_times[mid] + sorted_times[~mid])/2


print('avg_times_job', avg_times)
print('median_times_job', median_times)


X = np.arange(len(avg_times))
ax = plt.subplot(111)
ax.bar(X, avg_times.values(), width=0.2, color='b', align='center')
ax.bar(X-0.2, median_times.values(), width=0.2, color='g', align='center')
ax.legend(('Mean time','Median time'))
plt.xticks(X, avg_times.keys())
plt.title("Scheduler stats", fontsize=17)
plt.savefig('Mean_median_scheduling_algos')
plt.show()


avg_times = {}
median_times = {}


for policy in policies:
    logs1 = []
    logs2 = []
    logs3 = []
    with open(f'YACS/worker/log_worker_1_{policy}.log') as f1, open(f'YACS/worker/log_worker_2_{policy}.log') as f2, open(f'YACS/worker/log_worker_3_{policy}.log') as f3:
        logs1 = f1.readlines()
        logs2 = f2.readlines()
        logs3 = f3.readlines()

    job_received_log = []
    job_completed_log = []
    for logs in [logs1, logs2, logs3]:
        for log in logs:
            log = log.split('%')
            log[0] = log[0].strip()
            if log[1] == 'TASK RECEIVED':
                job_received_log.append(log)
            elif log[1] == 'TASK COMPLETED':
                job_completed_log.append(log)

    for i in job_received_log:
        i[2] = json.loads(i[2])['task_id']
    for i in job_completed_log:
        #i[2] = json.loads(i[2])['task_id']
        i[2] = i[2].split(',')[0].split(':')[1].strip()

    job_times = {}
    for i in job_received_log:
        for j in job_completed_log:
            if i[2] == j[2][1:-1]:
                job_times[i[2]] = (convert_to_datetime(j[0]) - convert_to_datetime(i[0])).total_seconds()
    sorted_times = sorted(list(job_times.values()))
    avg_times[policy] = sum(list(job_times.values()))/len(sorted_times)
    mid = len(sorted_times) // 2
    median_times[policy] = (sorted_times[mid] + sorted_times[~mid])/2

    ts_tasks_1 = {}
    ts_tasks_2 = {}
    ts_tasks_3 = {}
    ct = 0
    for log in logs1:
        log = log.split('%')
        log[0] = log[0].strip()
        if log[1] == 'TASK RECEIVED':
            ct += 1
        elif log[1] == 'TASK COMPLETED':
            ct -= 1
            ts_tasks_1[convert_to_datetime(log[0])] = ct

    for log in logs2:
        log = log.split('%')
        log[0] = log[0].strip()
        if log[1] == 'TASK RECEIVED':
            ct += 1
        elif log[1] == 'TASK COMPLETED':
            ct -= 1
            ts_tasks_2[convert_to_datetime(log[0])] = ct

    for log in logs3:
        log = log.split('%')
        log[0] = log[0].strip()
        if log[1] == 'TASK RECEIVED':
            ct += 1
        elif log[1] == 'TASK COMPLETED':
            ct -= 1
        ts_tasks_3[convert_to_datetime(log[0])] = ct



print('avg_times_job', avg_times)
print('median_times_job', median_times)

X = np.arange(len(avg_times))
ax = plt.subplot(111)
ax.bar(X, avg_times.values(), width=0.2, color='b', align='center')
ax.bar(X-0.2, median_times.values(), width=0.2, color='g', align='center')
ax.legend(('Mean time','Median time'))
plt.xticks(X, avg_times.keys())
plt.title("Scheduler stats", fontsize=17)
plt.show()
