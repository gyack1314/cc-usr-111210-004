"""显示清单身份和原始来源概况，或按清单采集提交边界只读副本。"""
import argparse,json,sys
from pathlib import Path
from .io import load
from .domain import summarize
from .collect import collect

def main():
    p=argparse.ArgumentParser();p.add_argument('command',choices=['inspect','collect']);p.add_argument('file');p.add_argument('outdir',nargs='?');a=p.parse_args()
    v,digest=load(a.file)
    if a.command=='inspect':
        print(json.dumps({'source_sha256':digest,'summary':summarize(v,Path(a.file).parent)},ensure_ascii=False,indent=2,allow_nan=False))
        return
    if not a.outdir:p.error('collect需要输出目录')
    try:report=collect(v,Path(a.file).parent,Path(a.outdir))
    except (ValueError,KeyError,TypeError) as e:
        print(json.dumps({'error':str(e)},ensure_ascii=False),file=sys.stderr);raise SystemExit(1)
    print(json.dumps({'manifest_sha256':digest,'report':report},ensure_ascii=False,indent=2,allow_nan=False))
if __name__=='__main__':main()
