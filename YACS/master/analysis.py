import matplotlib as plt
import numpy
import json
import datetime

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


print('avg_times', avg_times)
print('median_times', median_times)
