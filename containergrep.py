#!/usr/bin/env python

import requests
import json
import sys
import threading
import copy
import os
import getopt

container = 'f'
max_items = 1
concurrency = 1
run = 1
max_retries = 5
debug = 0

bytecount = {}
group_bytes = {}

def get_url_and_token():
    user = os.getenv('ST_USER', None)
    auth = os.getenv('ST_AUTH', None)
    key = os.getenv('ST_KEY', None)

    if not (user and auth and key):
        print 'must supply ST_USER, ST_AUTH and ST_KEY env vars'
        sys.exit(1)

    res = requests.get(auth, headers={'x-storage-user': user,
                                      'x-storage-pass': key})

    return(res.headers.get('x-storage-url', None),
           res.headers.get('x-auth-token', None))


def get_container_list(endpoint, container, authtoken):
    last_item = None
    done = False

    url = '%s/%s' % (endpoint, container)

    result = []

    while not done:
        params = {'format': 'json'}
        if last_item is not None:
            params['marker'] = last_item

        print 'fetching container listing'
        res = requests.get('%s/%s' % (endpoint, container),
                           params=params,
                           headers={'X-Auth-Token': authtoken})

        try:
            intermediate_result = json.loads(res.text)
        except Exception as e:
            print res.text
            sys.exit(1)

        for item in intermediate_result:
            bytecount[item['name']] = item['bytes']

        if(len(intermediate_result) == 10000):
            last_item = intermediate_result[-1]['name']
        else:
            done = True

        result += [x['name'] for x in intermediate_result]

    return result

def zebra_execute(json, endpoint, token):
    return requests.post(
        endpoint,
        headers={
            "X-Auth-Token": token,
            "Content-Type": "application/json",
            "X-ZeroVM-Execute": "1.0"},
        data=json)

def gen_manifests(max_mappers, container, file_list):
    offset = 0
    manifests = []
    group = 0

    while(offset < len(file_list)):
        nodes = []
        bytes = 0

        list_subset = file_list[offset:offset+max_mappers]

        idx = 0;
        # while(idx < len(list_subset)):
        #     file_type = 'plain'
        #     if(list_subset[idx].endswith('.gz')):
        #         file_type = 'gzip'
        #     elif (list_subset[idx].endswith('.bz2')):
        #         file_type = 'bz2'

        #     bytes += bytecount[list_subset[idx]]

        #     if file_type != 'plain':
        #         c = {'name': 'catter-%d-%d' % (group, idx),
        #              'exec': {'path': 'swift://./nexe/busybox.nexe',
        #                       'args': '%s /dev/stdin' % 'zcat' if file_type == 'gzip' else 'bzcat'},
        #              'file_list': [
        #                  {'device': 'stdin',
        #                   'path': 'swift://./%s/%s' % (container, list_subset[idx])},
        #                  {'device': 'stdout',
        #                   'path': 'zvm://mapper-%d-%d:input' % (group, idx)}]}
        #         nodes.append(c)

        #     m = {'name': 'mapper-%d-%d' % (group, idx),
        #          'exec': {'path': 'swift://./nexe/busybox.nexe',
        #                   'args': 'egrep \"terms\" /dev/input'},
        #          'file_list': [{'device': 'stdout', 'path': 'zvm://reducer:mapper-%d-%d' % (group, idx)}]}

        #     if file_type == 'plain':
        #         m['file_list'].append({'device': 'input', 'path': 'swift://./%s/%s' % (container, list_subset[idx])})

        #     nodes.append(m)
        #     idx += 1

        # r = {'name': 'reducer',
        #      'exec': {'path': 'file://python:python' },
        #      'file_list': [
        #          {'device': 'stdin',
        #           'path': 'swift://./py/reducer3.py'},
        #          {'device': 'stdout'},
        #          {'device': 'python'}]}
        # nodes.append(r)

        while(idx < len(list_subset)):
            file_type = 'plain'
            if(list_subset[idx].endswith('.gz')):
                file_type = 'gzip'
            elif (list_subset[idx].endswith('.bz2')):
                file_type = 'bz2'

            bytes += bytecount[list_subset[idx]]

            m = {'name': 'mapper-%d-%d' % (group, idx),
                 'exec': {'path': 'file://python:python',
                          'args': '/dev/stdin /dev/input /dev/out/reducer %s "%s:%s"' % (file_type, container, list_subset[idx])},
                 'file_list': [
                     {'device': 'stdin',
                      'path': 'swift://./py/pygrep.py'},
                     {'device': 'input',
                      'path': 'swift://./%s/%s' % (container, list_subset[idx])},
                     {'device': 'python'}],
                 'connect': ['reducer']}
            nodes.append(m)
            idx += 1

        r = {'name': 'reducer',
             'exec': {'path': 'file://python:python' },
             'file_list': [
                 {'device': 'stdin',
                  'path': 'swift://./py/reducer.py'},
                 {'device': 'stdout'},
                 {'device': 'python'}]}
        nodes.append(r)

        debugnodes = copy.deepcopy(nodes)
        map(lambda x: x['file_list'].append({'device': 'stderr', 'content_type': 'text/plain', 'path': 'swift://./py/%s.txt' % x['name']}), debugnodes)

        manifest = json.dumps(nodes)
        if debug:
            manifest = json.dumps(debugnodes)

        manifests.append(manifest)

        offset += len(list_subset)
        sys.stdout.write('.')

        group_bytes[group] = bytes

        with open('filelist-%d.txt' % group, 'w') as f:
            for file in list_subset:
                f.write('%s: %d\n' % (file, bytecount[file]))
            f.write('\nTotal: %d\n' % bytes)

        with open('manifest-%d.json' % group, 'w') as f:
            f.write(json.dumps(debugnodes, indent=4))

        group += 1


    print
    return manifests

run_lock = threading.Lock()
run_cond = threading.Condition(run_lock)

def single_thread_runner(job, url, token):
    job['state'] = 'running'
    response = zebra_execute(job['manifest'], url, token)

    job['status_code'] = response.status_code
    job['result'] = response.content

    run_cond.acquire()

    job['state'] = 'terminated'

    if job['status_code'] != 200 or job['result'].startswith('Timeout'):
        job['state'] = 'failed'
        job['retries'] += 1
        if job['retries'] <= max_retries:
            job['state'] = 'pending'

    run_cond.notify()
    run_cond.release()

def job_runner(manifests, concurrency, url, token):
    done = False
    current_concurrency = 4
    job_data = {}
    for idx, manifest in enumerate(manifests):
        job_data[idx] = {'manifest': manifest,
                         'state': 'pending',
                         'result': '',
                         'retries': 0,
                         'tid': None,
                         'idx': idx}

    run_cond.acquire()

    while not done:
        terminated = [ x for x in job_data if job_data[x]['state'] == 'terminated' ]

        for term in terminated:
            job_data[term]['tid'].join()
            job_data[term]['state'] = 'complete'

        available = [ x for x in job_data if job_data[x]['state'] == 'pending' ]
        running = [ x for x in job_data if job_data[x]['state'] in 'running' ]

        complete_count = len(job_data) - len(available)

        if (complete_count > current_concurrency) and (concurrency > current_concurrency):
            current_concurrency = min(concurrency, current_concurrency + 2)

        if (len(available) + len(running)) == 0:
            done = True

        jobs_to_spin = current_concurrency - len(running)
        if(jobs_to_spin > 0):
            jobs = available[0:jobs_to_spin]
            for job in jobs:
                # print 'starting job %d' % job
                job_data[job]['status'] = 'running'
                job_data[job]['tid'] = threading.Thread(target=single_thread_runner, args=[job_data[job], url, token])
                job_data[job]['tid'].start()

        print ''.join([{'f': 'x', 'r': str(job_data[x]['retries']), 'p': '.', 'c': ' '}.get(job_data[x]['state'][0], ' ') for x in job_data])

        if not done:
            run_cond.wait()

    print 'all jobs done'

    failed_jobs = []

    result = ''
    for idx in range(0, len(manifests)):
        if job_data[idx]['status_code'] == 200 and not job_data[idx]['result'].startswith('Timeout'):
            result += job_data[idx]['result']
        else:
            failed_jobs.append('%d (%d)' % (idx, job_data[idx]['status_code']))

    return (failed_jobs, result)

try:
    opts, args = getopt.getopt(sys.argv[1:], 't:m:c:d')
except getopt.GetoptError as e:
    print str(e)
    print "%s [-t <threads>] [-m max_items] [-c container] [-d]" % sys.argv[0]
    sys.exit(1)

for o, a in opts:
    if o == '-t':
        concurrency = int(a)
    elif o == '-m':
        max_items = int(a)
    elif o == '-c':
        container = a
    elif o == '-d':
        run = 0
    else:
        print 'bad argument'
        sys.exit(1)

url, token = get_url_and_token()

if not url and token:
    print 'cannot get url and auth token'
    sys.exit(1)

files = get_container_list(url, container, token)

def is_valid(filename):
    proscribed_types = ['.tar.gz', '.rpm', '.jar', '.deb', '.egg']
    for ending in proscribed_types:
        if filename.endswith(ending):
            return False

    return True

whitelist = [x for x in files if is_valid(x)]

print 'Generating manifests for %d files' % len(whitelist)

manifests = gen_manifests(max_items, container, whitelist)

if run:
    (fails, result) = job_runner(manifests, concurrency, url, token)

    print result

    print '----------------------------------------------'
    print 'Failed jobs: %s' % fails
    print '----------------------------------------------'
