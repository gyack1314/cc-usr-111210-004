"""读取冻结文件头与长度，不应用任何WAL页。"""
from .io import asset,integer

def summarize(v,root):
    db,d=asset(root,v['database']);wal,w=asset(root,v['wal'])
    if db[:16]!=b'SQLite format 3\x00':raise ValueError('主库签名不符')
    if integer(v['prefix_bytes'])>len(wal):raise ValueError('冻结前缀越过文件尾')
    if len(wal)<32:raise ValueError('WAL头不完整')
    return {'database_bytes':len(db),'wal_bytes':len(wal),'database_sha256':d,'wal_sha256':w,'requested_commit':v['commit'],'prefix_bytes':v['prefix_bytes']}
