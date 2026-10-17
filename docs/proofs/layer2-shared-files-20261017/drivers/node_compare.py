#!/usr/bin/env python3
"""Before/after timings through real LastDB requests on fresh generated data."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import signal
import socket
import statistics
import subprocess
import threading
import time

PARSER = argparse.ArgumentParser(description=__doc__)
PARSER.add_argument('--before', required=True, type=Path)
PARSER.add_argument('--after', required=True, type=Path)
PARSER.add_argument('--output', required=True, type=Path)
PARSER.add_argument('--read-rounds', type=int, default=21)
PARSER.add_argument('--write-rounds', type=int, default=5)
PARSER.add_argument('--concurrent-rounds', type=int, default=5)
PARSER.add_argument('--phase',choices=['all','mixed'],default='all')
PARSER.add_argument('--after-only',action='store_true')
PARSER.add_argument('--trace-validity',action='store_true')
ARGS = PARSER.parse_args()
ROOT = ARGS.output.resolve()
ROOT.mkdir(parents=True, mode=0o700, exist_ok=False)
SCHEMA = 'Layer2QueueBenchmark'
RECORDS = 320
RECEIPTS = []
RECEIPT_LOCK = threading.Lock()


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n')


def remember(value):
    with RECEIPT_LOCK:
        RECEIPTS.append(value)
        with (ROOT / 'receipts.jsonl').open('a') as output:
            output.write(json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n')


def body(number, revision):
    return ''.join(hashlib.sha256(f'Layer2Queue:{number}:{revision}:{chunk}'.encode()).hexdigest()
                   for chunk in range(16))


def identifier(number):
    return f'record-{number:06d}'


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=180)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


class Node:
    def __init__(self, variant, binary, letter):
        self.variant, self.binary = variant, binary.resolve(strict=True)
        self.directory = ROOT / letter
        self.directory.mkdir(mode=0o700)
        self.home = self.directory / 'h'
        self.home.mkdir(mode=0o700)
        self.socket, self.full_socket = self.directory / 'q.sock', self.directory / 'f.sock'
        if len(str(self.full_socket).encode()) + 8 >= 103:
            raise RuntimeError('Use a shorter output path for the owned sockets')
        self.expected = {}
        self.process = None
        self.log = None

    def call(self, path, value=None, with_cost=False):
        result, elapsed, cost, _ = self.call_timed(path,value,with_cost)
        return result, elapsed, cost

    def call_timed(self, path, value=None, with_cost=False):
        headers = {'Content-Type':'application/json', 'X-LastDB-Client':'layer2-queue-benchmark'}
        if with_cost:
            headers['x-lastdb-read-receipt'] = 'true'
        connection = UnixConnection(self.socket)
        raw = None if value is None else value if isinstance(value,str) else json.dumps(value, separators=(',', ':'))
        started = time.perf_counter_ns()
        try:
            connection.request('GET' if value is None else 'POST', path, raw, headers)
            response = connection.getresponse()
            encoded = response.read()
            result = json.loads(encoded)
            completed = time.perf_counter_ns()
            elapsed = completed - started
            if response.status != 200:
                remember({'variant':self.variant,'operation':'refusal','path':path,
                          'http_status':response.status,'result':result})
                raise RuntimeError(f'{self.variant} {path}: {response.status} {str(result)[:500]}')
            cost = response.getheader('x-lastdb-read-receipt')
            return result, elapsed, None if cost is None else json.loads(cost), completed
        finally:
            connection.close()

    def start(self):
        if self.process is not None:
            raise RuntimeError('Owned node is already active')
        environment = dict(os.environ, LASTDB_ISOLATED_COPY='1', LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
                           LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
                           LASTDB_RESIDENT_MODE='write', LASTDB_DISK_PACK='1', RUST_MIN_STACK='16777216')
        for name in ['LASTDB_DISK_CAP_BYTES','SENTRY_DSN','FOLD_SENTRY_DSN','RUST_LOG']:
            environment.pop(name, None)
        if ARGS.trace_validity:
            environment['RUST_LOG'] = 'warn,lastdb::native_warm_validity=debug'
        self.log = (self.directory / 'node.log').open('a')
        self.process = subprocess.Popen([str(self.binary),'--data-dir',str(self.home),
                                         '--socket-path',str(self.socket),
                                         '--full-socket-path',str(self.full_socket)],
                                        env=environment,stdin=subprocess.DEVNULL,stdout=self.log,stderr=self.log)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f'{self.variant} exited before health; see {self.directory / "node.log"}')
            if self.socket.exists():
                try:
                    result, _, _ = self.call('/health')
                    if result.get('status') == 'ok' or result.get('ok') is True:
                        return
                except (OSError, http.client.HTTPException):
                    pass
            time.sleep(.05)
        raise RuntimeError('Owned node health deadline')

    def stop(self):
        if self.process is None:
            return
        process = self.process
        forced = False
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                code = process.wait(timeout=120)
            except subprocess.TimeoutExpired:
                process.kill()
                code = process.wait(timeout=15)
                forced = True
        else:
            code = process.returncode
        remember({'variant':self.variant,'operation':'owned-node-stop','pid':process.pid,
                  'exit_code':code,'forced_kill':forced})
        self.process = None
        self.log.close()
        self.log = None
        if forced or code not in [0, -signal.SIGTERM]:
            raise RuntimeError(f'{self.variant} normal stop returned {code}')

    def mutation_payload(self, numbers, revision, operation, durability):
        mutations = []
        for number in numbers:
            fields = {'title':f'title-{number}-revision-{revision}', 'body':body(number,revision)}
            if operation == 'create':
                fields.update(bucket='generated', id=identifier(number))
            if operation == 'delete':
                fields = {}
            mutations.append({'type':'mutation','schema':SCHEMA,'mutation_type':operation,
                              'key_value':{'hash':'generated','range':identifier(number)},
                              'fields_and_values':fields,'durability':durability})
        return {'mutations':mutations}

    def mutate(self, numbers, revision, operation='update', durability='durable', payload=None):
        if payload is None:
            payload = self.mutation_payload(numbers, revision, operation, durability)
        result, elapsed, _, completed = self.call_timed('/api/mutations/batch', payload)
        if result.get('count') != len(numbers) or result.get('ok') is False:
            raise RuntimeError('Complete request count differs')
        if result.get('durability') not in ['queued','durable']:
            raise RuntimeError('Response lacks a known durability guarantee')
        if durability == 'durable' and result['durability'] != 'durable':
            raise RuntimeError('Durable request returned a weaker guarantee')
        for number in numbers:
            if operation == 'delete':
                self.expected.pop(number, None)
            else:
                self.expected[number] = {'title':f'title-{number}-revision-{revision}','body':body(number,revision)}
        return result, elapsed, completed

    def status(self):
        result, _, _ = self.call('/api/db/native-status')
        return result.get('data', result)['native_disk']

    def flush(self):
        result, elapsed, _, completed = self.call_timed('/api/db/flush', {})
        self.last_flush_response_end = completed
        if result.get('data', result).get('durable') is not True:
            raise RuntimeError('Explicit flush lacks a durable receipt')
        receipt = result.get('data', result).get('native_flush',{})
        if 'target_sequence' in receipt and receipt['completed_sequence'] < receipt['target_sequence']:
            raise RuntimeError('Explicit flush does not cover its captured accepted prefix')
        if self.status()['writer_failed']:
            raise RuntimeError('Explicit flush reports an unhealthy writer')
        return result, elapsed

    def seed(self):
        self.start()
        schema = {'isolated_copy':True,'schema':{
            'source':'system_seed','name':SCHEMA,'descriptive_name':'Layer 2 Queue Benchmark',
            'schema_type':'HashRange','key':{'hash_field':'bucket','range_field':'id'},
            'fields':['bucket','id','title','body'],
            'field_descriptions':{'bucket':'generated record hash','id':'generated record range',
                                  'title':'generated title','body':'generated body'}}}
        result, elapsed, _ = self.call('/api/schemas/seed-system', schema)
        remember({'variant':self.variant,'operation':'schema-setup','elapsed_nanos':elapsed,'result':result})
        for first in range(0, RECORDS, 64):
            result, elapsed, _ = self.mutate(list(range(first,first+64)), 0, 'create')
            remember({'variant':self.variant,'operation':'seed','batch':64,'first':first,
                      'elapsed_nanos':elapsed,'result':result})
        self.flush()
        self.stop()

    def query(self, numbers, shape='exact', phase='read', repeat=0):
        if shape == 'exact':
            selector = {'HashRangeKeys':[['generated',identifier(number)] for number in numbers]}
        elif shape == 'range':
            selector = {'HashRangeRange':{'hash':'generated','start':identifier(numbers[0]),
                                         'end':identifier(numbers[-1]+1)}}
        else:
            raise RuntimeError('Unknown query shape')
        result, elapsed, cost = self.call('/api/query', {
            'schema_name':SCHEMA,'fields':['title','body'],'filter':selector,'limit':len(numbers)}, True)
        records = result.get('results', [])
        wanted = {identifier(number):self.expected[number] for number in numbers if number in self.expected}
        actual = {record['key']['range']:{field:record['fields'].get(field) for field in ['title','body']}
                  for record in records}
        if actual != wanted or len(records) != len(wanted):
            save(ROOT/f'query-mismatch-{self.variant}-{phase}-{repeat}.json',
                 {'wanted':wanted,'actual_response':result,'shape':shape,'numbers':numbers})
            raise RuntimeError(f'{self.variant} query differs from the generated input')
        if any(record['fields'].get('bucket') != 'generated' for record in records):
            raise RuntimeError('Required hash field differs')
        if any(record['key'].get('hash') != 'generated' for record in records):
            raise RuntimeError('Returned key hash differs')
        if any('id' in record['fields'] and record['fields']['id'] != record['key']['range'] for record in records):
            raise RuntimeError('Returned range field differs from its key')
        flags = {'returned_count':len(records),'has_more':False,'next_cursor':None,
                 'unresolved_rows':0,'offset':0}
        if any(result.get(name) != wanted for name,wanted in flags.items()):
            raise RuntimeError(f'{self.variant} pagination or completeness metadata differs')
        absent = len(numbers)-len(wanted)
        if not isinstance(result.get('tombstoned_rows'),int) or not 0 <= result['tombstoned_rows'] <= absent:
            raise RuntimeError('Tombstone count exceeds the known absent keys')
        if shape == 'range' and [record['key']['range'] for record in records] != sorted(wanted):
            raise RuntimeError('Ordered range differs')
        if cost is None:
            raise RuntimeError('Requested read receipt is absent')
        digest = hashlib.sha256(json.dumps(result,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        remember({'variant':self.variant,'operation':'query','phase':phase,'shape':shape,
                  'batch':len(numbers),'repeat':repeat,'elapsed_nanos':elapsed,'cost':cost,
                  'response_sha256':digest,'expected_values_match':True,'returned_records':len(records)})
        return digest


NODES = {}
if not ARGS.after_only:
    NODES['before'] = Node('before',ARGS.before,'b')
NODES['after'] = Node('after',ARGS.after,'a')


def ordered(repeat):
    return list(NODES.values()) if repeat % 2 == 0 else list(reversed(list(NODES.values())))


def read_matrix():
    for shape in ['exact','range']:
        for count in [1,16,64,256]:
            numbers = list(range(32,32+count))
            primes = {node.variant:node.query(numbers,shape,'prime') for node in NODES.values()}
            for repeat in range(ARGS.read_rounds):
                for node in ordered(repeat):
                    if node.query(numbers,shape,'steady',repeat) != primes[node.variant]:
                        raise RuntimeError('Repeated full response differs from its prime')
            print(f'node reads {shape} {count} complete', flush=True)


def write_matrix():
    for durability in ['durable','queued']:
        for count in [1,16,64,256]:
            for repeat in range(ARGS.write_rounds):
                for node in ordered(repeat):
                    node.flush()
                    before = node.status()
                    result, elapsed, ack_end = node.mutate(list(range(32,32+count)),
                                                  (1 if durability == 'durable' else 2)*100000+count*100+repeat,
                                                  durability=durability)
                    accepted = node.status()
                    flush, flush_elapsed = node.flush()
                    after = node.status()
                    if after['frontier'] <= before['frontier']:
                        raise RuntimeError('Non-idempotent write does not advance the durable frontier')
                    remember({'variant':node.variant,'operation':'mutation','durability':durability,
                              'actual_durability':result['durability'],'response_end_nanos':ack_end,
                              'batch':count,'repeat':repeat,'elapsed_nanos':elapsed,'result':result,
                              'flush_elapsed_nanos':flush_elapsed,'flush':flush,
                              'request_start_to_durable_nanos':node.last_flush_response_end-(ack_end-elapsed),
                              'start':before,'after_ack':accepted,'end':after})
                    node.query(list(range(32,32+count)),phase='after-write',repeat=repeat)
            print(f'node writes {durability} {count} complete', flush=True)


def mixed_matrix():
    stable = list(range(32,96))
    for repeat in range(ARGS.write_rounds):
        for node in ordered(repeat):
            prime = node.query(stable,phase='mixed-prime',repeat=repeat)
            _, elapsed, _ = node.mutate(list(range(304,320)),300000+repeat)
            node.flush()
            digest = node.query(stable,phase='after-unrelated-write',repeat=repeat)
            remember({'variant':node.variant,'operation':'unrelated-mutation','batch':16,'repeat':repeat,
                      'elapsed_nanos':elapsed,'stable_values_match':True,
                      'response_metadata_changed':prime != digest})
    print('mixed read/write calls complete', flush=True)


def concurrent_matrix():
    request_count, batch = 4, 16
    for repeat in range(ARGS.concurrent_rounds):
        for node in ordered(repeat):
            node.flush()
            before = node.status()
            barrier = threading.Barrier(request_count+1)
            plans = []
            for slot in range(request_count):
                numbers = list(range(32+slot*batch,32+(slot+1)*batch))
                revision = 400000+repeat*request_count+slot
                payload = json.dumps(node.mutation_payload(numbers,revision,'update','queued'),separators=(',',':'))
                plans.append((numbers,revision,payload))
            def submit(slot):
                barrier.wait(timeout=10)
                numbers, revision, payload = plans[slot]
                result, elapsed, completed = node.mutate(numbers,revision,durability='queued',payload=payload)
                return {'slot':slot,'elapsed_nanos':elapsed,'result':result,
                        'actual_durability':result['durability'],'response_end_nanos':completed,
                        'end_after_start_nanos':completed-started}
            with ThreadPoolExecutor(max_workers=request_count) as pool:
                futures = [pool.submit(submit,slot) for slot in range(request_count)]
                started = time.perf_counter_ns()
                barrier.wait(timeout=10)
                results = [future.result() for future in futures]
            acks_elapsed = max(result['response_end_nanos'] for result in results)-started
            client_collection_elapsed = time.perf_counter_ns()-started
            accepted = node.status()
            flush, flush_elapsed = node.flush()
            after = node.status()
            remember({'variant':node.variant,'operation':'concurrent-queued','requests':request_count,
                      'records_per_request':batch,'repeat':repeat,'requests_results':results,
                      'all_acks_elapsed_nanos':acks_elapsed,'flush_elapsed_nanos':flush_elapsed,
                      'client_collection_elapsed_nanos':client_collection_elapsed,
                      'all_durable_elapsed_nanos':node.last_flush_response_end-started,'flush':flush,
                      'start':before,'accepted_after_all_acks':accepted,'end':after})
            node.query(list(range(32,96)),phase='after-concurrent-flush',repeat=repeat)
        print(f'concurrent queued round {repeat} complete', flush=True)


def delete_restore_calls():
    for node in NODES.values():
        numbers = list(range(32,48))
        result, elapsed, _ = node.mutate(numbers,500000,operation='delete',durability='durable')
        remember({'variant':node.variant,'operation':'delete-request','batch':16,
                  'elapsed_nanos':elapsed,'result':result})
        node.query(list(range(32,96)),phase='after-durable-delete-ack')
        node.flush()
        node.query(list(range(32,96)),shape='range',phase='after-durable-delete')
        result, elapsed, _ = node.mutate(numbers,500001,operation='create',durability='queued')
        remember({'variant':node.variant,'operation':'recreate-request','batch':16,
                  'elapsed_nanos':elapsed,'result':result})
        node.query(list(range(32,96)),phase='after-accepted-recreate')
        node.flush()
        node.query(list(range(RECORDS)),phase='after-durable-recreate')
    for node in NODES.values():
        numbers = list(range(32,48))
        result, elapsed, _ = node.mutate(numbers,500002,operation='delete',durability='queued')
        remember({'variant':node.variant,'operation':'queued-delete-boundary','batch':16,
                  'elapsed_nanos':elapsed,'result':result,'requested_durability':'queued'})
        if node.variant == 'after':
            if result['durability'] != 'durable':
                raise RuntimeError('Native delete must report its stronger disk guarantee')
            node.query(list(range(32,96)),phase='after-native-queued-delete-ack')
        node.flush()
        node.query(list(range(32,96)),phase='after-queued-delete-flush')
        node.mutate(numbers,500003,operation='create',durability='queued')
        node.query(list(range(32,96)),phase='after-native-delete-recreate')
        node.flush()
    print('delete and recreate requests complete', flush=True)


def normal_flush_calls():
    for repeat in range(3):
        for node in ordered(repeat):
            node.flush()
            before = node.status()
            result, ack, ack_end = node.mutate(list(range(32,48)),600000+repeat,durability='queued')
            polls = []
            deadline = time.monotonic()+90
            while True:
                status = node.status()
                observed = time.perf_counter_ns()
                polls.append({'after_ack_nanos':observed-ack_end,'status':status})
                if status['writer_failed']:
                    raise RuntimeError('Automatic flush reports a failed writer')
                if not status['accepted_batch'] and status['frontier'] > before['frontier']:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError('Normal flush deadline')
                time.sleep(.02)
            remember({'variant':node.variant,'operation':'normal-flush','batch':16,'repeat':repeat,
                      'ack_elapsed_nanos':ack,'disk_completion_after_ack_upper_nanos':observed-ack_end,
                      'actual_durability':result['durability'],
                      'polls':polls,'forced_flush_during_sample':False})
            node.query(list(range(32,48)),phase='after-normal-flush',repeat=repeat)
    print('normal flush observations complete', flush=True)


def summarize():
    groups = {}
    for row in RECEIPTS:
        operation = row['operation']
        if operation == 'query':
            key = (operation,row['variant'],row['phase'],row['shape'],row['batch'])
            nanos = row['elapsed_nanos']
        elif operation == 'mutation':
            key = (operation,row['variant'],row['durability']+'/'+row['actual_durability'],None,row['batch'])
            nanos = row['elapsed_nanos']
        elif operation == 'concurrent-queued':
            key = (operation,row['variant'],None,None,row['requests']*row['records_per_request'])
            nanos = row['all_durable_elapsed_nanos']
        else:
            continue
        groups.setdefault(key,[]).append((nanos,row))
    summaries = []
    for key, group in groups.items():
        times = sorted(item[0]/1e6 for item in group)
        result = dict(zip(['operation','variant','phase','shape','batch'],key))
        result.update(samples=len(times),median_ms=statistics.median(times),
                      p95_ms=times[math.ceil(.95*len(times))-1] if len(times)>=5 else None)
        if key[0] == 'query':
            for field in ['file_opens','file_open_attempts','read_operations','metadata_operations',
                          'directory_reads','directory_entries','payload_bytes']:
                result[field] = statistics.median(row['cost'][field] for _,row in group)
        if key[0] == 'concurrent-queued':
            result.update(all_acks_median_ms=statistics.median(row['all_acks_elapsed_nanos']/1e6 for _,row in group),
                          frontier_delta_median=statistics.median(row['end']['frontier']-row['start']['frontier']
                                                                 for _,row in group))
        summaries.append(result)
    save(ROOT/'summary.json',summaries)


save(ROOT/'inputs.json',{'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'records':RECORDS,'body_bytes':1024,'body_recipe':'SHA256 hex chunks: Layer2Queue:number:revision:chunk',
    'schema':SCHEMA,'seed_batch':64,'resident_mode':'write','disk_cap_bytes':None,'native':True,
    'read_rounds':ARGS.read_rounds,'write_rounds':ARGS.write_rounds,'concurrent_rounds':ARGS.concurrent_rounds,
    'phase':ARGS.phase,'after_only':ARGS.after_only,'trace_validity':ARGS.trace_validity,
    'platform':platform.platform(),'load_start':os.getloadavg(),'filesystem_cache':'unknown',
    'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'binaries':{node.variant:{'path':str(node.binary),'sha256':hashlib.sha256(node.binary.read_bytes()).hexdigest()}
                for node in NODES.values()}})
try:
    for node in NODES.values():
        node.seed()
    for node in NODES.values():
        node.start()
    read_matrix()
    if ARGS.phase == 'all':
        write_matrix()
    mixed_matrix()
    if ARGS.phase == 'all':
        concurrent_matrix()
        delete_restore_calls()
        normal_flush_calls()
    for node in NODES.values():
        node.query(list(range(RECORDS)),phase='final-all-known-keys')
        node.flush()
        node.stop()
        node.start()
        node.query(list(range(RECORDS)),phase='normal-reopen-generated-keys')
        storage, _, _ = node.call('/api/storage/app')
        remember({'variant':node.variant,'operation':'final-storage-report','result':storage})
finally:
    errors = []
    for node in NODES.values():
        try:
            node.stop()
        except Exception as error:
            errors.append(str(error))
    summarize()
    save(ROOT/'expected-values.json',{node.variant:{identifier(number):value for number,value in node.expected.items()}
                                      for node in NODES.values()})
    if errors:
        save(ROOT/'stop-errors.json',errors)
        raise RuntimeError('; '.join(errors))
save(ROOT/'completed.json',{'finished_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                            'load_end':os.getloadavg()})
print(str(ROOT/'summary.json'),flush=True)
