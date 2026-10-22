#!/usr/bin/env python3
"""Summarize saved product measurements without additional node operations."""
import json
from pathlib import Path

root=Path(__file__).resolve().parent
summaries={}
for profile in ('controlled','default'):
    source=root/profile/'measurements.json'
    if not source.exists():
        continue
    rows=json.loads(source.read_text())
    checkpoints={row['label']:row for row in rows if row['kind']=='checkpoint'}
    phases=[]
    for row in rows:
        if row['kind']!='phase':
            continue
        before=checkpoints[row['label']+'-before']
        after=checkpoints[row['label']+'-after']
        a,b=before['process_io'],after['process_io']
        if (a['pid'],a['ri_proc_start_abstime'])!=(b['pid'],b['ri_proc_start_abstime']):
            raise RuntimeError('Process identity changed across phase')
        writes=[r for r in rows if r['kind']=='write' and r['phase']==row['label']]
        n=row['records']
        io=b['ri_diskio_byteswritten']-a['ri_diskio_byteswritten']
        delta=row['growth']['bytes_net']
        metadata=sum(row['growth']['new_or_grown'].get(kind,{}).get('bytes',0)
                     for kind in ('directory','locator','generations'))
        phases.append({'phase':row['label'],'records':n,'requests':row['request_count'],
                       'ack_median_ms':row['ack']['median_ms'],'ack_p95_ms':row['ack']['p95_ms'],
                       'through_flush_median_ms':row['through_flush']['median_ms'],
                       'records_per_second_measured_call_time':n/sum(r['start_to_explicit_flush_seconds'] for r in writes),
                       'retained_growth_bytes':delta,'retained_growth_per_record':delta/n,
                       'new_files_per_record':row['growth']['files_net']/n,
                       'process_disk_io_bytes':io,'process_disk_io_per_record':io/n,
                       'input_fields_json_bytes':row['submitted_fields_json_bytes'],
                       'metadata_growth_bytes':metadata,'metadata_fraction':metadata/delta,
                       'receipts_growth_bytes':row['growth']['new_or_grown'].get('generations',{}).get('bytes',0)})
    compactions=[]
    for row in rows:
        if row['kind']=='compaction':
            report=row['report'].get('data',row['report'])['compact']['native_disk']
            compactions.append({'round':row['round'],'wall_seconds':row['wall_seconds'],
                                'write_probe_seconds':row['write_probe_seconds'],
                                'warm_read_probe_seconds':row['read_probe_seconds'],**report})
    summaries[profile]={'completed':any(r['kind']=='complete' for r in rows),
                        'automatic_maintenance':json.loads((root/profile/'ownership.json').read_text())['automatic_maintenance'],
                        'all_checkpoints_drained':all(not c['native']['accepted_batches'] and
                          c['native']['accepted_sequence']==c['native']['completed_sequence'] and
                          not c['native']['core_warm_dirty'] for c in checkpoints.values()),
                        'phases':phases,'compactions':compactions,
                        'checkpoints':[{k:v for k,v in {'label':r['label'],'inventory':r['inventory'],
                           'parts':r['native']['parts'],'occupancy':r['page_occupancy']}.items()} for r in checkpoints.values()]}
    totals={key:sum(phase[key] for phase in phases) for key in (
        'records','requests','retained_growth_bytes','process_disk_io_bytes',
        'input_fields_json_bytes','metadata_growth_bytes','receipts_growth_bytes')}
    totals['metadata_fraction']=totals['metadata_growth_bytes']/totals['retained_growth_bytes']
    totals['estimated_growth_fraction_one_fewer_receipt']=totals['receipts_growth_bytes']/2/totals['retained_growth_bytes']
    summaries[profile]['update_phase_totals']=totals
    reopen=root/('default-product-reopen.json' if profile=='default' else 'sparse-product-reopen.json')
    if reopen.exists():
        receipt=json.loads(reopen.read_text())
        summaries[profile]['product_reopen']={key:receipt[key] for key in (
            'pid','records_checked','all_current_values_match','exit_code','stopped')}
allocation=root/'sparse-allocation.json'
if allocation.exists():
    row=json.loads(allocation.read_text())
    before=row['before']['allocated_bytes']
    after=row['after']['allocated_bytes']
    summaries['sparse_experiment']={'allocated_before':before,'allocated_after':after,
       'saved_bytes':before-after,'saved_percent':100*(before-after)/before,
       'logical_bytes_unchanged':row['before']['bytes']==row['after']['bytes'],
       'CURRENT_unchanged':row['CURRENT_sha256_before']==row['CURRENT_sha256_after'],
       'all_file_hashes_equal':row['all_file_hashes_equal'],
       'product_reopen':json.loads((root/'sparse-product-reopen.json').read_text())}
(root/'summary.json').write_text(json.dumps(summaries,indent=2))
print(json.dumps({profile:{k:v for k,v in data.items() if k!='checkpoints'}
                  for profile,data in summaries.items()},indent=2))
