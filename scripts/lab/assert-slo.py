#!/usr/bin/env python3
"""Fail closed SLO evaluator for JSONL observations; emits result.json."""
import argparse, json, math, pathlib, sys
p=argparse.ArgumentParser(); p.add_argument('--spec',required=True); p.add_argument('--observations',required=True); p.add_argument('--output',required=True); a=p.parse_args()
def fail(msg): print('FAIL: '+msg, file=sys.stderr); sys.exit(1)
try: spec=json.load(open(a.spec)) if a.spec.endswith('.json') else __import__('yaml').safe_load(open(a.spec))
except ModuleNotFoundError: fail('PyYAML is required for YAML SLO spec (install it; do not bypass evaluation)')
except Exception as e: fail('cannot read SLO spec: '+str(e))
try: rows=[json.loads(x) for x in open(a.observations) if x.strip()]
except Exception as e: fail('cannot read observations: '+str(e))
if not rows: fail('no observations')
results=[]
for objective in spec.get('objectives',[]):
 name=objective.get('name'); metric=objective.get('metric'); limit=objective.get('max_seconds'); percentile=objective.get('percentile',95); minimum=objective.get('minimum_samples',1)
 if not all([name,metric]) or not isinstance(limit,(int,float)) or not isinstance(percentile,(int,float)): fail('invalid objective schema')
 values=[]
 for index,row in enumerate(rows, start=1):
  timestamp=row.get('timestamp')
  if not isinstance(timestamp,str) or not timestamp.strip(): fail(f'observation {index} missing required timestamp')
  if metric not in row: fail(f'observation {index} missing required metric {metric}')
  try: values.append(float(row[metric]))
  except (ValueError,TypeError): fail(f'observation {index} has non-numeric {metric}')
 if len(values)<minimum: fail(f'{name}: {len(values)} samples, requires {minimum}')
 values.sort(); rank=max(0,math.ceil(percentile/100*len(values))-1); actual=values[rank]
 results.append({'name':name,'metric':metric,'samples':len(values),'percentile':percentile,'actual_seconds':actual,'max_seconds':limit,'pass':actual<=limit})
out={'schema_version':'1.0','result':'PASS' if all(x['pass'] for x in results) else 'FAIL','objectives':results}
pathlib.Path(a.output).write_text(json.dumps(out,indent=2)+'\n')
if out['result']!='PASS': fail('one or more SLO objectives failed')
print('PASS: SLO objectives satisfied')
