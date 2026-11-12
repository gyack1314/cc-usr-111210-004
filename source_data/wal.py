"""WAL字节格式解析与校验，只读内存字节，不打开源文件。"""
import hashlib

MAGIC_LITTLE=0x377F0682
MAGIC_BIG=0x377F0683
FORMAT_VERSION=3007000
HEADER_BYTES=32
FRAME_HEADER_BYTES=24
MAX_PAGE_NUMBER=1073741823
SIGNATURE=b'SQLite format 3\x00'

def _u32(b):return int.from_bytes(b,'big')

def regular_page_size(n):return 512<=n<=65536 and n&(n-1)==0

def checksum(data,s0,s1,byte_order):
    """成对32位字滚动累加：s0=(s0+x0+s1) mod 2^32，s1=(s1+x1+s0) mod 2^32。"""
    if len(data)%8:raise ValueError('校验数据长度须为8的倍数')
    for i in range(0,len(data),8):
        x0=int.from_bytes(data[i:i+4],byte_order);x1=int.from_bytes(data[i+4:i+8],byte_order)
        s0=(s0+x0+s1)&0xFFFFFFFF;s1=(s1+x1+s0)&0xFFFFFFFF
    return s0,s1

def database_header(db):
    if len(db)<100:raise ValueError('主库头不完整')
    if db[:16]!=SIGNATURE:raise ValueError('主库签名不符，仅支持常规页大小的未加密SQLite文件')
    raw=_u32(db[16:18]);size=65536 if raw==1 else raw
    if not regular_page_size(size):raise ValueError('主库页大小非常规')
    if db[18]!=2 or db[19]!=2:raise ValueError('主库不在WAL模式，与同代WAL配对不符')
    if len(db)%size:raise ValueError('主库长度不是整页')
    return {'page_size':size,'pages':len(db)//size}

def wal_header(wal):
    if len(wal)<HEADER_BYTES:raise ValueError('WAL头不完整')
    magic=_u32(wal[0:4])
    if magic==MAGIC_LITTLE:order='little'
    elif magic==MAGIC_BIG:order='big'
    else:raise ValueError('WAL magic不符')
    if _u32(wal[4:8])!=FORMAT_VERSION:raise ValueError('仅支持WAL格式版本3007000')
    size=_u32(wal[8:12])
    if not regular_page_size(size):raise ValueError('WAL页大小非常规')
    s0,s1=checksum(wal[:24],0,0,order)
    if (s0,s1)!=(_u32(wal[24:28]),_u32(wal[28:32])):raise ValueError('WAL头校验和不符')
    return {'byte_order':order,'page_size':size,'checkpoint':_u32(wal[12:16]),'salt1':_u32(wal[16:20]),'salt2':_u32(wal[20:24]),'checksum':(s0,s1)}

def frames(wal,prefix,header,max_pages):
    """校验前缀内每个完整frame的salt、页号与滚动校验和，返回(frame列表, 截断尾部)。

    前缀末尾的不完整frame只记为截断；任何完整frame校验失败都抛错，
    不跳过损坏frame继续寻找后面的提交。frame头的checksum字段不参与自身校验。
    """
    size=header['page_size'];step=FRAME_HEADER_BYTES+size
    s0,s1=header['checksum'];out=[];off=HEADER_BYTES
    while off+step<=prefix:
        fh=wal[off:off+FRAME_HEADER_BYTES]
        pgno,dbsz=_u32(fh[0:4]),_u32(fh[4:8]);index=len(out)+1
        if (_u32(fh[8:12]),_u32(fh[12:16]))!=(header['salt1'],header['salt2']):raise ValueError(f'第{index}个frame的salt与WAL头不符，疑似混入新一代WAL')
        if not 1<=pgno<=MAX_PAGE_NUMBER:raise ValueError(f'第{index}个frame页号越界')
        if dbsz:
            if dbsz>max_pages:raise ValueError(f'第{index}个frame的提交页数越过max_pages')
            if pgno>dbsz:raise ValueError(f'第{index}个frame页号越过其提交页数')
        s0,s1=checksum(fh[:8],s0,s1,header['byte_order'])
        data=wal[off+FRAME_HEADER_BYTES:off+step]
        s0,s1=checksum(data,s0,s1,header['byte_order'])
        if (s0,s1)!=(_u32(fh[16:20]),_u32(fh[20:24])):raise ValueError(f'第{index}个frame校验和不符')
        out.append({'index':index,'offset':off,'page':pgno,'commit_pages':dbsz,'data_offset':off+FRAME_HEADER_BYTES,'sha256':hashlib.sha256(data).hexdigest()})
        off+=step
    tail=prefix-off
    return out,({'offset':off,'bytes':tail} if tail else None)
