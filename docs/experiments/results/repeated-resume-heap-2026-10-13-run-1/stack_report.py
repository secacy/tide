"""Postprocess pprof raw exports without resampling, normalization or cumulative double counting."""
from pathlib import Path
from collections import defaultdict
import json,re,sys,csv

INDEX=('alloc_objects','alloc_space','inuse_objects','inuse_space')
SAMPLE=re.compile(r'^\s+(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+(-?\d+):\s+([\d ]+)$')
LOCATION=re.compile(r'^\s+(\d+):\s+(0x[\da-f]+)\s+M=\d+(?:\s+(.*))?$')
FRAME=re.compile(r'^\s*(.+?)\s+(/\S+):(\d+):(\d+)\s+s=(\d+)$')

def parse(path):
 samples=[];locations={};current=None;section=None
 text=Path(path).read_text()
 if 'Period: 1\n' not in text or 'alloc_objects/count alloc_space/bytes inuse_objects/count inuse_space/bytes' not in text:raise ValueError('raw profile rate or counter order differs')
 for line in text.splitlines():
  if line=='Samples:':section='samples';continue
  if line=='Locations':section='locations';continue
  if line=='Mappings':section='mappings';continue
  if section=='samples':
   m=SAMPLE.match(line)
   if m:samples.append(([int(m[i]) for i in range(1,5)],[int(x) for x in m[5].split()]))
   elif re.match(r'^\s*-?\d',line):raise ValueError('unparsed numeric sample')
  if section=='locations':
   m=LOCATION.match(line)
   if m:
    current=int(m[1]);locations[current]=[]
    if m[3]:
     f=FRAME.match(m[3])
     locations[current].append(f'{f[1]} {f[2]}:{f[3]}' if f else m[3])
   else:
    f=FRAME.match(line)
    if f and current is not None:locations[current].append(f'{f[1]} {f[2]}:{f[3]}')
 if not samples or not locations:raise ValueError('raw samples or symbols missing')
 grouped=defaultdict(lambda:[0]*4)
 for values,ids in samples:
  if any(loc not in locations for loc in ids):raise ValueError('sample references missing location')
  stack=tuple(frame for loc in ids for frame in (locations[loc] or [f'UNKNOWN_LOCATION_{loc}']))
  for i,v in enumerate(values):grouped[stack][i]+=v
 totals=[sum(v[i] for v in grouped.values()) for i in range(4)]
 rows=[dict(stack=list(stack),values=dict(zip(INDEX,values))) for stack,values in grouped.items() if any(values)]
 rows.sort(key=lambda r:(-abs(r['values']['inuse_space']),r['stack']))
 return dict(totals=dict(zip(INDEX,totals)),samples=len(samples),distinct_stacks=len(rows),unknown_symbol_samples=sum(any(not locations[loc] for loc in ids) for _,ids in samples),stacks=rows)

if __name__=='__main__':
 root=Path(sys.argv[1]);out=Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=False)
 result={}
 for directory in sorted((root/'analysis').glob('[0-9][0-9]_*')):
  result[directory.name]={p.stem.removesuffix('.raw'):parse(p) for p in sorted(directory.glob('*.raw.txt'))}
 for reports in result.values():
  for late,early in [('late_active','early_active'),('session_idle','late_active'),('after_fixture','before_fixture')]:
   a,b,d=reports[late]['totals'],reports[early]['totals'],reports[late+'-minus-'+early]['totals']
   if d!={key:a[key]-b[key] for key in INDEX}:raise ValueError('raw delta and absolute sample sums differ')
 (out/'stack-summary.json').write_text(json.dumps(result,indent=2)+'\n')
 with (out/'profile-totals.csv').open('w',newline='') as f:
  w=csv.writer(f,lineterminator="\n");w.writerow(['slot','profile',*INDEX])
  for slot,reports in result.items():
   for name,report in reports.items():w.writerow([slot,name,*[report['totals'][k] for k in INDEX]])

 with (out/'selected-paths.csv').open('w',newline='') as f:
  w=csv.writer(f,lineterminator="\n");w.writerow(['slot','profile','allocation_path',*INDEX])
  tags=['newAudioInputBuffer','newResultBuffer','newAudioReplayBuffer','newResumableSession','(*audioInputBuffer).offer','(*audioReplayBuffer).append','(*resultBuffer).append','encoding/json.cachedTypeFields','hpack.buildRootHuffmanNode','runtime.malg','runtime.allocm','(*heapCaseObserver).capture']
  for slot,reports in result.items():
   for name,report in reports.items():
    for tag in tags:
     values=[sum(row['values'][key] for row in report['stacks'] if any(tag in frame for frame in row['stack'])) for key in INDEX]
     w.writerow([slot,name,tag,*values])
 summary=json.loads((root/'analysis/summary.json').read_text())
 with (out/'checkpoints.csv').open('w',newline='') as f:
  w=csv.writer(f,lineterminator="\n");w.writerow(['slot','point','start_ns','end_ns','late_ns','heap_alloc_after_gc','heap_objects_after_gc','buck_hash_sys_after_gc','profile_inuse_space','profile_inuse_objects'])
  for row in summary['cases']:
   slot=f'{row["index"]:02d}_{row["case"]}'
   for c in row['heap']['checkpoints']:
    totals=result[slot][c['name']]['totals']
    w.writerow([slot,c['name'],c['start_ns'],c['end_ns'],None if c['planned_ns'] is None else c['start_ns']-c['planned_ns'],c['after_gc']['heap_alloc'],c['after_gc']['heap_objects'],c['after_gc']['buck_hash_sys'],totals['inuse_space'],totals['inuse_objects']])
