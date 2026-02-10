#!/usr/bin/env python3
import json,sys

if len(sys.argv)!=3:
    print('usage: compare.py baseline.json new.json')
    sys.exit(1)

base=json.load(open(sys.argv[1]))['results']
new=json.load(open(sys.argv[2]))['results']
print('benchmark,metric,baseline,new,delta_pct')
for b,n in zip(base,new):
    bench=b.get('bench','unknown')
    for k,v in b.items():
        if isinstance(v,(int,float)) and k in n and isinstance(n[k],(int,float)) and v!=0:
            d=(n[k]-v)/v*100.0
            print(f'{bench},{k},{v},{n[k]},{d:.2f}')
