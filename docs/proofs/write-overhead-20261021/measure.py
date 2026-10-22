#!/usr/bin/env python3
"""Record product write and maintenance costs on one new, owned Mini home."""
import argparse
import concurrent.futures
import hashlib
import fcntl
import http.client
import json
import math
import os
from pathlib import Path
import signal
import socket
import statistics
import subprocess
import time

SCHEMA = 'DiskGrowthMeasure'

class UnixHTTP(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=240)
        self.path = path
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)

def request(sock, path, body=None):
    encoded = None if body is None else json.dumps(body, separators=(',', ':')).encode()
    conn = UnixHTTP(str(sock))
    start = time.perf_counter()
    try:
        conn.request('GET' if body is None else 'POST', path, encoded,
                     {'Content-Type':'application/json', 'X-LastDB-Client':'owned-write-overhead-measure'})
        response = conn.getresponse()
        raw = response.read()
        elapsed = time.perf_counter() - start
        result = json.loads(raw)
        if response.status >= 400:
            raise RuntimeError(f'{path}: HTTP {response.status}: {str(result)[:600]}')
        return result, elapsed
    finally:
        conn.close()

def unwrap(value):
    return value.get('data', value)

def body_for(n, revision):
    return ''.join(hashlib.sha256(f'owned-disk:{n}:{revision}:{c}'.encode()).hexdigest()
                   for c in range(16))

def mutations(numbers, revision, mode='durable', create=False):
    result = []
    for n in numbers:
        fields = {'title':f'title-{n}-revision-{revision}', 'body':body_for(n, revision)}
        if create:
            fields.update(bucket='measure', id=f'key-{n:06d}')
        result.append({'type':'mutation','schema':SCHEMA,
            'mutation_type':'create' if create else 'update', 'durability':mode,
            'key_value':{'hash':'measure','range':f'key-{n:06d}'},'fields_and_values':fields})
    return {'mutations':result}

def inventory(home):
    root = home / 'data' / 'disk-pack'
    files = {}
    totals = {}
    deadline = time.monotonic() + 15
    pending = [(root, 0)]
    while pending:
        parent, depth = pending.pop()
        if depth > 8 or time.monotonic() > deadline:
            raise RuntimeError('Owned native inventory exceeded depth/time bound')
        with os.scandir(parent) as entries:
            for entry in entries:
                if entry.is_symlink():
                    raise RuntimeError('Unexpected symlink in owned native home')
                if entry.is_dir(follow_symlinks=False):
                    pending.append((Path(entry.path), depth + 1))
                elif entry.is_file(follow_symlinks=False):
                    st = entry.stat(follow_symlinks=False)
                    name = str(Path(entry.path).relative_to(root))
                    family = name.split('/')[0] if '/' in name else 'root-markers'
                    files[name] = [st.st_size, st.st_blocks * 512]
                    total = totals.setdefault(family, {'files':0,'bytes':0,'allocated_bytes':0})
                    total['files'] += 1
                    total['bytes'] += st.st_size
                    total['allocated_bytes'] += st.st_blocks * 512
                    if len(files) > 262144:
                        raise RuntimeError('Owned native inventory exceeded file bound')
    return {'files':len(files), 'bytes':sum(x[0] for x in files.values()),
            'allocated_bytes':sum(x[1] for x in files.values()), 'families':totals, '_files':files}

def growth(before, after):
    result = {'files_net':after['files']-before['files'], 'bytes_net':after['bytes']-before['bytes'],
              'allocated_bytes_net':after['allocated_bytes']-before['allocated_bytes'], 'new_or_grown':{}}
    for name, values in after['_files'].items():
        previous = before['_files'].get(name, [0,0])
        added = max(0, values[0]-previous[0])
        if added:
            family = name.split('/')[0] if '/' in name else 'root-markers'
            entry = result['new_or_grown'].setdefault(family, {'bytes':0,'files':0})
            entry['bytes'] += added
            entry['files'] += int(name not in before['_files'])
    return result

def public_inventory(value):
    return {k:v for k,v in value.items() if k != '_files'}

def page_occupancy(home, listing):
    counts={}
    root=home/'data'/'disk-pack'
    for name,(size,_) in listing['_files'].items():
        family=name.split('/')[0]
        if family not in ('directory','locator') or size%16384:
            continue
        with (root/name).open('rb') as stream:
            for offset in range(0,size,16384):
                stream.seek(offset)
                header=stream.read(128)
                if header[:8]!=b'LDPAGE1\0':
                    continue
                used=128+int.from_bytes(header[48:52],'little')
                entry=counts.setdefault(family,{'pages':0,'page_bytes':0,'used_prefix_bytes':0,
                                              'pages_used_under_4KiB':0,'pages_used_under_8KiB':0})
                entry['pages']+=1
                entry['page_bytes']+=16384
                entry['used_prefix_bytes']+=used
                entry['pages_used_under_4KiB']+=int(used<=4096)
                entry['pages_used_under_8KiB']+=int(used<=8192)
    return counts

def stats(values):
    ordered = sorted(values)
    return {'samples':len(values),'median_ms':statistics.median(values)*1000,
            'p95_ms':ordered[math.ceil(.95*len(ordered))-1]*1000 if len(values)>=5 else None,
            'max_ms':max(values)*1000}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--binary',required=True)
    parser.add_argument('--scratch',required=True)
    parser.add_argument('--output',required=True)
    parser.add_argument('--automatic',action='store_true')
    args=parser.parse_args()
    scratch=Path(args.scratch).resolve()
    output=Path(args.output).resolve()
    output.mkdir(parents=True,exist_ok=True)
    home=scratch/'h'
    if home.exists():
        raise RuntimeError('Product measurement requires a fresh home')
    home.mkdir()
    sock=scratch/'q'
    full=scratch/'f'
    env=os.environ.copy()
    for key in list(env):
        if key.startswith(('LASTDB_', 'FOLDDB_', 'FOLD_', 'SENTRY_')):
            env.pop(key)
    env.update(LASTDB_ISOLATED_COPY='1',LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
               LASTDB_HOME=str(home),FOLDDB_HOME=str(home),RUST_MIN_STACK='16777216',
               RUST_LOG='warn',LASTDB_AUTO_MAINTENANCE='1' if args.automatic else '0')
    rows=[]
    measurement_lock=None
    log=(scratch/'node.log').open('wb')
    process=subprocess.Popen([args.binary,'--data-dir',str(home),'--socket-path',str(sock),
                              '--full-socket-path',str(full)],env=env,stdout=log,stderr=log,
                             stdin=subprocess.DEVNULL)
    ownership={'pid':process.pid,'uid':os.getuid(),'scratch':str(scratch),'home':str(home),
               'binary':str(Path(args.binary).resolve()),'automatic_maintenance':args.automatic}
    (output/'ownership.json').write_text(json.dumps(ownership,indent=2))
    def save():
        (output/'measurements.json').write_text(json.dumps(rows,indent=2))
    def status():
        return unwrap(request(sock,'/api/db/native-status')[0])['native_disk']
    def flush():
        return request(sock,'/api/db/flush',{})
    def snapshot(label):
        from io_probe import sample_process
        deadline=time.monotonic()+60
        stable=0
        previous=None
        while stable<5:
            native=status()
            marker=(native['generation'],native['frontier'],native['accepted_sequence'],native['completed_sequence'])
            drained=(not native['accepted_batches'] and native['accepted_sequence']==native['completed_sequence']
                     and not native['core_warm_dirty'])
            stable=stable+1 if drained and marker==previous else 0
            if not drained:
                flush()
            previous=marker
            if time.monotonic()>deadline:
                raise RuntimeError('Owned product queue did not reach a stable durable boundary')
            time.sleep(.1)
        value=inventory(home)
        rows.append({'kind':'checkpoint','label':label,'native':status(),
                     'inventory':public_inventory(value),'process_io':sample_process(process.pid),
                     'page_occupancy':page_occupancy(home,value)})
        save()
        return value
    def write_phase(label, count, width, offset, mode='durable'):
        before=snapshot(label+'-before')
        times=[]
        complete=[]
        payload=0
        for r in range(count):
            nums=[(offset+r*width+i)%512 for i in range(width)]
            req=mutations(nums,1000+len(rows)*10+r,mode)
            payload+=sum(len(json.dumps(m['fields_and_values'],separators=(',',':')).encode())
                         for m in req['mutations'])
            result, elapsed=request(sock,'/api/mutations/batch',req)
            flushed, ft=flush()
            times.append(elapsed)
            complete.append(elapsed+ft)
            rows.append({'kind':'write','phase':label,'round':r,'records':width,
                         'ack_seconds':elapsed,'start_to_explicit_flush_seconds':elapsed+ft,
                         'response':result,'flush_response':flushed})
            save()
        after=snapshot(label+'-after')
        summary={'kind':'phase','label':label,'records':count*width,'request_count':count,
                 'submitted_fields_json_bytes':payload,'ack':stats(times),'through_flush':stats(complete),
                 'growth':growth(before,after)}
        rows.append(summary)
        save()
        print(json.dumps(summary),flush=True)
    try:
        deadline=time.monotonic()+120
        while True:
            if process.poll() is not None:
                raise RuntimeError('Owned node exited during startup; inspect retained private log')
            try:
                request(sock,'/health')
                break
            except (OSError,http.client.HTTPException):
                if time.monotonic()>deadline:
                    raise RuntimeError('Owned node health deadline')
                time.sleep(.2)
        schema={'isolated_copy':True,'schema':{'source':'system_seed','name':SCHEMA,
                'descriptive_name':'Generated Disk Growth Measure','schema_type':'HashRange',
                'key':{'hash_field':'bucket','range_field':'id'},'fields':['bucket','id','title','body'],
                'field_descriptions':{'bucket':'generated hash','id':'generated range',
                                      'title':'generated title','body':'generated body'}}}
        rows.append({'kind':'setup','schema_reply':request(sock,'/api/schemas/seed-system',schema)[0]})
        flush()
        snapshot('empty-schema')
        seed_start=time.perf_counter()
        for first in range(0,512,32):
            reply,elapsed=request(sock,'/api/mutations/batch',mutations(range(first,first+32),0,create=True))
            if reply.get('count')!=32 or not reply.get('ok'):
                raise RuntimeError('Product seed reply lacks complete accepted count')
            rows.append({'kind':'seed','records':32,'elapsed_seconds':elapsed,'response':reply})
            save()
        flush()
        rows.append({'kind':'seed-total','records':512,'elapsed_seconds':time.perf_counter()-seed_start})
        lock_path=Path.home()/'.cache/fold-db-perf-guard/host-cargo.lock'
        measurement_lock=lock_path.open('a+')
        print('Seed complete; acquire the host build lock before timed phases',flush=True)
        fcntl.flock(measurement_lock.fileno(),fcntl.LOCK_EX)
        rows.append({'kind':'measurement-lock','path':str(lock_path),'acquired_unix_ns':time.time_ns()})
        snapshot('seeded-512')
        snapshot('idle-before')
        time.sleep(3)
        snapshot('idle-after')
        write_phase('single-early',24,1,0)
        write_phase('batch-32',8,32,32)
        write_phase('single-late',24,1,300)
        write_phase('queued-32',8,32,128,'queued')
        flush()
        snapshot('pre-compaction')
        query={'schema_name':SCHEMA,'fields':['title','body'],
               'filter':{'HashRangeKeys':[['measure','key-000001']]},'limit':1}
        baseline=[request(sock,'/api/query',query)[1] for _ in range(10)]
        rows.append({'kind':'query-baseline','timing':stats(baseline)})
        for r in range(2):
            before=snapshot(f'compact-{r}-before')
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                comp=pool.submit(request,sock,'/api/db/compact',{'dry_run':False,'grace_ms':0})
                time.sleep(.2)
                read=pool.submit(request,sock,'/api/query',query)
                write=pool.submit(request,sock,'/api/mutations/batch',mutations([400+r],9000+r))
                cr,ct=comp.result()
                rr,rt=read.result()
                wr,wt=write.result()
            flush()
            after=snapshot(f'compact-{r}-after')
            row={'kind':'compaction','round':r,'wall_seconds':ct,'read_probe_seconds':rt,
                 'write_probe_seconds':wt,'probe_delay_seconds':.2,'report':cr,
                 'read_reply':rr,'write_reply':wr,'growth':growth(before,after)}
            rows.append(row)
            save()
            print(json.dumps({k:v for k,v in row.items() if k not in ('read_reply','write_reply')}),flush=True)
        write_phase('single-after-compaction',24,1,450)
        snapshot('final')
        rows.append({'kind':'complete','success':True})
        save()
    except BaseException as error:
        rows.append({'kind':'failure','error':f'{type(error).__name__}: {error}'})
        save()
        raise
    finally:
        if measurement_lock is not None:
            measurement_lock.close()
        process.send_signal(signal.SIGTERM) if process.poll() is None else None
        try:
            process.wait(timeout=240)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        log.close()
        (output/'shutdown.json').write_text(json.dumps({'pid':process.pid,'exit_code':process.returncode,
                                                       'stopped':process.poll() is not None},indent=2))

if __name__=='__main__':
    main()
