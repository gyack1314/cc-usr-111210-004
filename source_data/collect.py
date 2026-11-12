"""按冻结清单还原指定提交边界的只读副本，逐页记录来源。"""
import hashlib,json,os,sqlite3
from pathlib import Path
from urllib.parse import quote
from .io import asset,integer
from . import wal

SCOPE_NOTICE='本副本只反映截至所选提交边界的已提交事务；边界之后的完整未提交frame与不完整尾部均未应用。采集不修复数据库，"最后已提交"不代表所有营业操作均已落盘。'
OUTPUT_NAME='snapshot.db'
REPORT_NAME='snapshot.report.json'

def _table_name(v):
    if not isinstance(v,str) or not v.isidentifier():raise ValueError('核对表名须为普通标识符')
    return v

def _jsonable(rows):
    return [[c.hex() if isinstance(c,bytes) else c for c in r] for r in rows]

def _fsync_dir(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)

def _verify_copy(path,table):
    """以只读immutable方式核对新副本，固定查询不执行任何源提供的SQL。"""
    uri='file:'+quote(str(path.resolve()))+'?mode=ro&immutable=1'
    con=sqlite3.connect(uri,uri=True)
    try:
        bad=con.execute('PRAGMA integrity_check').fetchall()
        if bad!=[('ok',)]:raise ValueError('副本integrity_check未通过')
        rows=con.execute(f'SELECT id,service,amount FROM "{table}" ORDER BY id').fetchall()
    finally:con.close()
    return rows

def collect(manifest,root,outdir):
    """还原提交边界并原子发布副本，返回采集报告。源db、wal、shm均不被修改。"""
    db,db_digest=asset(root,manifest['database'])
    wal_bytes,wal_digest=asset(root,manifest['wal'])
    prefix=integer(manifest['prefix_bytes'])
    if prefix>len(wal_bytes):raise ValueError('冻结前缀越过文件尾')
    if prefix<wal.HEADER_BYTES:raise ValueError('冻结前缀不足WAL头')
    max_pages=integer(manifest['max_pages'],minimum=1)
    table=_table_name(manifest['table'])
    choice=manifest['commit']
    if choice!='last':choice=integer(choice,minimum=1)
    dbh=wal.database_header(db)
    wh=wal.wal_header(wal_bytes)
    if wh['page_size']!=dbh['page_size']:raise ValueError('WAL页大小与主库不符')
    frame_list,tail=wal.frames(wal_bytes,prefix,wh,max_pages)
    commits=[f for f in frame_list if f['commit_pages']]
    if not commits:raise ValueError('冻结前缀内没有任何已提交事务')
    if choice=='last':boundary=commits[-1];sequence=len(commits)
    else:
        if choice>len(commits):raise ValueError('所选提交序号越出前缀内提交数')
        boundary=commits[choice-1];sequence=choice
    size=dbh['page_size'];total=boundary['commit_pages']
    source={}
    for f in frame_list:
        if f['index']>boundary['index']:break
        source[f['page']]=f
    images=[];pages=[]
    for p in range(1,total+1):
        f=source.get(p)
        if f is not None:
            data=wal_bytes[f['data_offset']:f['data_offset']+size]
            pages.append({'page':p,'origin':'wal-frame','frame':f['index'],'offset':f['offset'],'sha256':hashlib.sha256(data).hexdigest()})
        elif p<=dbh['pages']:
            off=(p-1)*size;data=db[off:off+size]
            pages.append({'page':p,'origin':'main-db','offset':off,'sha256':hashlib.sha256(data).hexdigest()})
        else:raise ValueError(f'第{p}页在主库与边界前frame中均无来源，拒绝补零伪造')
        images.append(data)
    asset(root,manifest['database']);asset(root,manifest['wal'])
    out=Path(outdir);out.mkdir(parents=True,exist_ok=True)
    final=out/OUTPUT_NAME
    if final.exists():raise ValueError('输出副本已存在，拒绝覆盖')
    tmp=out/f'.snapshot.tmp-{os.getpid()}'
    try:
        with open(tmp,'wb') as h:
            for data in images:h.write(data)
            h.flush();os.fsync(h.fileno())
        back=tmp.read_bytes()
        if len(back)!=total*size:raise ValueError('临时副本长度不符')
        for i,page in enumerate(pages):
            if hashlib.sha256(back[i*size:(i+1)*size]).hexdigest()!=page['sha256']:raise ValueError('临时副本逐页核对失败')
        os.chmod(tmp,0o444)
        rows=_verify_copy(tmp,table)
        try:
            os.link(tmp,final)
            _fsync_dir(out)
        except BaseException:
            final.unlink(missing_ok=True)
            raise
    finally:
        tmp.unlink(missing_ok=True)
    applied={f['index'] for f in source.values()}
    dispositions=[]
    for f in frame_list:
        if f['index'] in applied:state='applied'
        elif f['index']<=boundary['index']:state='superseded'
        elif f['commit_pages']:state='commit-not-selected'
        else:state='uncommitted'
        dispositions.append({'index':f['index'],'offset':f['offset'],'page':f['page'],'commit_pages':f['commit_pages'],'sha256':f['sha256'],'state':state})
    output_bytes=final.read_bytes()
    report={
        'status':'collected',
        'identity':{'database_sha256':db_digest,'wal_sha256':wal_digest,'prefix_bytes':prefix,'source_generation':manifest.get('source_generation')},
        'wal':{'format_version':wal.FORMAT_VERSION,'page_size':wh['page_size'],'checksum_byte_order':wh['byte_order'],'checkpoint_sequence':wh['checkpoint'],'salt1':wh['salt1'],'salt2':wh['salt2']},
        'frames':dispositions,
        'truncated_tail':tail,
        'commits':[{'sequence':i+1,'frame':f['index'],'database_pages':f['commit_pages']} for i,f in enumerate(commits)],
        'boundary':{'requested':manifest['commit'],'commit_sequence':sequence,'frame':boundary['index'],'database_pages':total},
        'pages':pages,
        'output':{'path':str(final),'bytes':len(output_bytes),'sha256':hashlib.sha256(output_bytes).hexdigest()},
        'verification':{'integrity_check':'ok','table':table,'row_count':len(rows),'rows':_jsonable(rows)},
        'scope_notice':SCOPE_NOTICE,
    }
    raw=json.dumps(report,ensure_ascii=False,indent=2,allow_nan=False).encode('utf8')
    rtmp=out/f'.report.tmp-{os.getpid()}'
    try:
        rtmp.write_bytes(raw)
        os.replace(rtmp,out/REPORT_NAME)
    finally:
        rtmp.unlink(missing_ok=True)
    return report
