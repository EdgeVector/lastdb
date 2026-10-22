#!/usr/bin/env python3
"""Measure byte-identical sparse metadata on the stopped generated product home."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

from measure import inventory, public_inventory, request, mutations, body_for, SCHEMA

def digest(path):
    sha=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):
            sha.update(block)
    return sha.hexdigest()

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--proof',required=True)
    p.add_argument('--profile',default='controlled')
    p.add_argument('--verify-only',action='store_true')
    args=p.parse_args()
    proof=Path(args.proof).resolve()
    owner=json.loads((proof/args.profile/'ownership.json').read_text())
    shutdown=json.loads((proof/args.profile/'shutdown.json').read_text())
    if not shutdown['stopped'] or shutdown['exit_code']!=0:
        raise RuntimeError('Owned source lacks clean shutdown')
    try:
        os.kill(owner['pid'],0)
    except ProcessLookupError:
        pass
    else:
        raise RuntimeError('Source PID still exists; refuse file replacement')
    scratch=Path(owner['scratch']).resolve()
    home=Path(owner['home']).resolve()
    if home!=scratch/'h' or scratch.stat().st_uid!=os.getuid():
        raise RuntimeError('Ownership or exact path mismatch')
    root=home/'data/disk-pack'
    before=inventory(home)
    root_before=digest(root/'CURRENT')
    results=[]
    for name,(size,_) in before['_files'].items():
        if args.verify_only:
            break
        if name.split('/')[0] not in ('directory','locator'):
            continue
        if size%16384:
            raise RuntimeError('Unexpected metadata part length')
        source=root/name
        old_hash=digest(source)
        old_alloc=source.stat().st_blocks*512
        dest=source.with_name(source.name+'.owned-sparse-'+uuid.uuid4().hex)
        with source.open('rb') as src,dest.open('xb') as dst:
            for offset in range(0,size,16384):
                page=src.read(16384)
                if page[:8]!=b'LDPAGE1\0':
                    raise RuntimeError('Unexpected metadata page')
                used=128+int.from_bytes(page[48:52],'little')
                if used>16384 or any(page[used:]):
                    raise RuntimeError('Metadata tail is not zero')
                dst.seek(offset)
                dst.write(page[:used])
            dst.truncate(size)
            dst.flush()
            os.fsync(dst.fileno())
        new_hash=digest(dest)
        if old_hash!=new_hash:
            raise RuntimeError('Sparse replacement changed bytes')
        os.replace(dest,source)
        directory=os.open(source.parent,os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        results.append({'path':name,'bytes':size,'sha256':old_hash,
                        'allocated_before':old_alloc,'allocated_after':source.stat().st_blocks*512})
    after=inventory(home)
    outcome={'method':'Stopped owned generated home; same bytes, hashes and lengths; only zero tails become holes',
             'before':public_inventory(before),'after':public_inventory(after),
             'CURRENT_sha256_before':root_before,'CURRENT_sha256_after':digest(root/'CURRENT'),
             'files':results,'all_file_hashes_equal':True}
    if not args.verify_only:
        (proof/'sparse-allocation.json').write_text(json.dumps(outcome,indent=2))
    rows=json.loads((proof/args.profile/'measurements.json').read_text())
    expected={n:0 for n in range(512)}
    offsets={'single-early':0,'batch-32':32,'single-late':300,'queued-32':128,
             'single-after-compaction':450}
    for index,row in enumerate(rows):
        if row['kind']=='write':
            rev=1000+index*10+row['round']
            for i in range(row['records']):
                n=(offsets[row['phase']]+row['round']*row['records']+i)%512
                expected[n]=rev
        elif row['kind']=='compaction':
            expected[400+row['round']]=9000+row['round']
    env=os.environ.copy()
    for key in list(env):
        if key.startswith(('LASTDB_','FOLDDB_','FOLD_','SENTRY_')):
            env.pop(key)
    env.update(LASTDB_ISOLATED_COPY='1',LASTDB_SUPPRESS_RECOVERY_PHRASE='1',
               LASTDB_HOME=str(home),FOLDDB_HOME=str(home),RUST_MIN_STACK='16777216',RUST_LOG='warn')
    log=(scratch/'reopen.log').open('wb')
    process=subprocess.Popen([owner['binary'],'--data-dir',str(home),'--socket-path',str(scratch/'q'),
                              '--full-socket-path',str(scratch/'f')],env=env,stdout=log,stderr=log,
                             stdin=subprocess.DEVNULL)
    receipt={'pid':process.pid,'automatic_maintenance':'default enabled'}
    try:
        deadline=time.monotonic()+120
        while True:
            try:
                request(scratch/'q','/health')
                break
            except OSError:
                if process.poll() is not None or time.monotonic()>deadline:
                    raise RuntimeError('Reopen failed; private log retained')
                time.sleep(.2)
        calls=[]
        for first in range(0,512,64):
            calls.append((first,{'schema_name':SCHEMA,'fields':['title','body'],
                'filter':{'HashRangeKeys':[['measure',f'key-{n:06d}'] for n in range(first,first+64)]},
                'limit':64}))
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures=[pool.submit(request,scratch/'q','/api/query',req) for _,req in calls]
            replies=[future.result() for future in futures]
        checked=0
        for reply,elapsed in replies:
            for row in reply['results']:
                n=int(row['key']['range'].split('-')[1])
                rev=expected[n]
                fields=row['fields']
                if fields['title']!=f'title-{n}-revision-{rev}' or fields['body']!=body_for(n,rev):
                    raise RuntimeError(f'Product value differs for generated key {n}')
                checked+=1
        if checked!=512:
            raise RuntimeError(f'Product returned {checked} of 512 records')
        receipt.update(records_checked=checked,all_current_values_match=True,
                       query_seconds=[elapsed for _,elapsed in replies],
                       native_status=request(scratch/'q','/api/db/native-status')[0])
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
        process.wait(timeout=240)
        log.close()
        receipt.update(exit_code=process.returncode,stopped=process.poll() is not None)
        name=args.profile+'-product-reopen.json' if args.verify_only else 'sparse-product-reopen.json'
        (proof/name).write_text(json.dumps(receipt,indent=2))
    print(json.dumps({'allocated_before':before['allocated_bytes'],'allocated_after':after['allocated_bytes'],
                      'same_logical_bytes':before['bytes']==after['bytes'],
                      'root_unchanged':outcome['CURRENT_sha256_before']==outcome['CURRENT_sha256_after'],
                      'reopen':{k:v for k,v in receipt.items() if k!='native_status'}}),flush=True)

if __name__=='__main__':
    main()
