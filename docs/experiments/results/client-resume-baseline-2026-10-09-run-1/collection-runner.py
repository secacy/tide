from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import json,os,subprocess,time
root=Path('docs/experiments/results/client-resume-baseline-2026-10-09-run-1')
p=root/'manifest.json'; m=json.loads(p.read_text())
assert m['status']=='prepared'
m['hardware']={'model':'Mac14,10','cpu':'Apple M2 Pro','memory_bytes':34359738368,'logical_cpus':12}
m['os']={'name':'macOS','version':'26.6.2','build':'25G83','arch':'arm64'}
env=os.environ.copy();env.update(m['go_environment'])
started=datetime.now(ZoneInfo('Asia/Taipei'));begin=time.monotonic()
m['started_at']=started.isoformat();m['status']='running'
m['wake_command']=['caffeinate','-i','-w',str(os.getpid())]
with (root/'wake.log').open('x') as wake_log:
 awake=subprocess.Popen(m['wake_command'],stdout=wake_log,stderr=subprocess.STDOUT)
 try:
  time.sleep(0.05)
  if awake.poll() is not None:raise RuntimeError('wake assertion did not remain active')
  p.write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n')
  with (root/'go-events.jsonl').open('x') as stdout,(root/'go-stderr.log').open('x') as stderr:
   completed=subprocess.run(m['command'],env=env,stdout=stdout,stderr=stderr)
  m['go_exit_code']=completed.returncode;m['wake_active_until_go_return']=awake.poll() is None
  m['status']='collected' if completed.returncode==0 else 'collected_with_failure'
 finally:
  if awake.poll() is None:awake.terminate();awake.wait(timeout=5)
  m['finished_at']=datetime.now(ZoneInfo('Asia/Taipei')).isoformat()
  m['elapsed_monotonic_s']=time.monotonic()-begin
  m['elapsed_wall_s']=(datetime.now(ZoneInfo('Asia/Taipei'))-started).total_seconds()
  p.write_text(json.dumps(m,ensure_ascii=False,indent=2)+'\n')
print(json.dumps({k:m.get(k) for k in ['status','go_exit_code','elapsed_monotonic_s','wake_active_until_go_return']}))
