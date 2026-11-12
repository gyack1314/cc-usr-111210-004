"""提交边界采集、页来源与各类异常清单检查。"""
import hashlib,json,os,sqlite3,stat,tempfile,unittest
from pathlib import Path
from source_data import wal as walmod
from source_data.collect import collect

ROOT=Path('data')
NORMAL=json.loads((ROOT/'normal.json').read_text(encoding='utf8'))
STORE_DB=(ROOT/'store.db').read_bytes()
STORE_WAL=(ROOT/'store.wal').read_bytes()
PAGE=1024
NORMAL_ROWS=[['v0','hotel',1],['v1','clean',3]]

def wal_page(wal_bytes,index,page_size=PAGE):
    off=32+(index-1)*(24+page_size)+24
    return wal_bytes[off:off+page_size]

def build_wal(frames,page_size=PAGE,salt1=0x11111111,salt2=0x22222222,byte_order='little',version=walmod.FORMAT_VERSION):
    magic=walmod.MAGIC_LITTLE if byte_order=='little' else walmod.MAGIC_BIG
    hdr=b''.join(x.to_bytes(4,'big') for x in (magic,version,page_size,1,salt1,salt2))
    s0,s1=walmod.checksum(hdr,0,0,byte_order)
    out=hdr+s0.to_bytes(4,'big')+s1.to_bytes(4,'big')
    for pgno,dbsz,data in frames:
        fh=b''.join(x.to_bytes(4,'big') for x in (pgno,dbsz,salt1,salt2))
        s0,s1=walmod.checksum(fh[:8],s0,s1,byte_order)
        s0,s1=walmod.checksum(data,s0,s1,byte_order)
        out+=fh+s0.to_bytes(4,'big')+s1.to_bytes(4,'big')+data
    return out

def append_frame(wal_bytes,pgno,dbsz,data,salt1,salt2):
    """在既有WAL的校验链末尾追加一个完整frame，salt可任意指定。"""
    hdr=walmod.wal_header(wal_bytes)
    order=hdr['byte_order'];size=hdr['page_size']
    s0,s1=hdr['checksum'];off=32
    while off+24+size<=len(wal_bytes):
        s0,s1=walmod.checksum(wal_bytes[off:off+8],s0,s1,order)
        s0,s1=walmod.checksum(wal_bytes[off+24:off+24+size],s0,s1,order)
        off+=24+size
    fh=b''.join(x.to_bytes(4,'big') for x in (pgno,dbsz,salt1,salt2))
    s0,s1=walmod.checksum(fh[:8],s0,s1,order)
    s0,s1=walmod.checksum(data,s0,s1,order)
    return wal_bytes[:off]+fh+s0.to_bytes(4,'big')+s1.to_bytes(4,'big')+data

class CollectTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.work=Path(self.tmp.name)
        self.count=0
    def tearDown(self):
        self.tmp.cleanup()
    def fresh_out(self):
        self.count+=1
        return self.work/f'out{self.count}'
    def freeze(self,db_bytes=STORE_DB,wal_bytes=STORE_WAL,**extra):
        (self.work/'m.db').write_bytes(db_bytes)
        (self.work/'m.wal').write_bytes(wal_bytes)
        def ref(name,data):return {'path':name,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        m={'database':ref('m.db',db_bytes),'wal':ref('m.wal',wal_bytes),'prefix_bytes':len(wal_bytes),'commit':'last','table':'vouchers','max_pages':10000}
        m.update(extra)
        return m
    def collect_data(self,manifest_name,out=None):
        out=out or self.fresh_out()
        return collect(json.loads((ROOT/manifest_name).read_text(encoding='utf8')),ROOT,out),out
    def collect_frozen(self,manifest):
        out=self.fresh_out()
        return collect(manifest,self.work,out),out

    def test_normal_matches_sqlite_replay(self):
        r,out=self.collect_data('normal.json')
        self.assertEqual(r['boundary'],{'requested':'last','commit_sequence':2,'frame':3,'database_pages':3})
        self.assertEqual([(p['page'],p['origin'],p.get('frame')) for p in r['pages']],[(1,'main-db',None),(2,'wal-frame',3),(3,'wal-frame',2)])
        self.assertEqual([f['state'] for f in r['frames']],['superseded','applied','applied'])
        self.assertEqual(r['verification']['rows'],NORMAL_ROWS)
        self.assertEqual(r['verification']['integrity_check'],'ok')
        oracle=self.work/'oracle.db'
        oracle.write_bytes(STORE_DB);Path(str(oracle)+'-wal').write_bytes(STORE_WAL)
        con=sqlite3.connect(oracle)
        try:self.assertEqual([list(x) for x in con.execute('SELECT id,service,amount FROM vouchers ORDER BY id')],NORMAL_ROWS)
        finally:con.close()
        report=json.loads((out/'snapshot.report.json').read_text(encoding='utf8'))
        self.assertEqual(report['output']['sha256'],r['output']['sha256'])

    def test_earlier_commit_selection(self):
        r,_=self.collect_data('earlier-commit.json')
        self.assertEqual((r['boundary']['commit_sequence'],r['boundary']['frame']),(1,2))
        self.assertEqual([(p['page'],p['origin'],p.get('frame')) for p in r['pages']],[(1,'main-db',None),(2,'wal-frame',1),(3,'wal-frame',2)])
        self.assertEqual(r['frames'][2]['state'],'commit-not-selected')
        self.assertEqual(r['verification']['rows'],[['v0','hotel',1],['v1','clean',2]])
        self.assertNotEqual(r['output']['sha256'],self.collect_data('normal.json')[0]['output']['sha256'])

    def test_uncommitted_tail_ignored(self):
        r,_=self.collect_data('uncommitted-tail.json')
        self.assertEqual([f['state'] for f in r['frames']],['superseded','applied','applied','uncommitted'])
        self.assertEqual(r['output']['sha256'],self.collect_data('normal.json')[0]['output']['sha256'])
        self.assertEqual(r['verification']['rows'],NORMAL_ROWS)

    def test_partial_tail_marked_not_error(self):
        r,_=self.collect_data('partial-tail.json')
        self.assertEqual(r['truncated_tail'],{'offset':3176,'bytes':11})
        self.assertEqual(r['output']['sha256'],self.collect_data('normal.json')[0]['output']['sha256'])

    def test_corrupt_frame_fails_unlike_truncation(self):
        bad=bytearray(STORE_WAL);bad[1080+24+10]^=0xFF
        out=self.fresh_out()
        with self.assertRaisesRegex(ValueError,'校验和不符'):collect(self.freeze(wal_bytes=bytes(bad)),self.work,out)
        self.assertFalse(out.exists())

    def test_salt_mismatch_fails(self):
        mixed=append_frame(STORE_WAL,2,0,wal_page(STORE_WAL,1),0xCC751F36^1,0x0F923C1B)
        with self.assertRaisesRegex(ValueError,'salt'):self.collect_frozen(self.freeze(wal_bytes=mixed))

    def test_new_generation_not_spliced_with_old_db(self):
        new_gen=build_wal([(2,3,wal_page(STORE_WAL,3))],salt1=0xDEAD,salt2=0xBEEF)
        m=self.freeze(wal_bytes=new_gen)
        m['wal']['sha256']=hashlib.sha256(STORE_WAL).hexdigest()
        with self.assertRaisesRegex(ValueError,'原文件身份不符'):self.collect_frozen(m)

    def test_growth_hole_refuses_zero_fill(self):
        wal_bytes=build_wal([(2,4,wal_page(STORE_WAL,3))])
        out=self.fresh_out()
        with self.assertRaisesRegex(ValueError,'第4页.*均无来源'):collect(self.freeze(wal_bytes=wal_bytes),self.work,out)
        self.assertFalse(out.exists())

    def test_big_endian_checksum_magic(self):
        frames=[(2,0,wal_page(STORE_WAL,1)),(3,3,wal_page(STORE_WAL,2)),(2,3,wal_page(STORE_WAL,3))]
        wal_bytes=build_wal(frames,salt1=0xCC751F36,salt2=0x0F923C1B,byte_order='big')
        r,_=self.collect_frozen(self.freeze(wal_bytes=wal_bytes))
        self.assertEqual(r['wal']['checksum_byte_order'],'big')
        self.assertEqual(r['verification']['rows'],NORMAL_ROWS)
        self.assertEqual(r['output']['sha256'],self.collect_data('normal.json')[0]['output']['sha256'])

    def test_invalid_config_prefix_beyond_file(self):
        with self.assertRaisesRegex(ValueError,'冻结前缀越过文件尾'):self.collect_data('invalid-config.json')

    def test_commit_selection_validated(self):
        for bad,msg in [(5,'越出'),(0,'整数越界'),(True,'整数越界')]:
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(ValueError,msg):self.collect_frozen(self.freeze(commit=bad))
        with self.assertRaisesRegex(ValueError,'没有任何已提交事务'):
            self.collect_frozen(self.freeze(prefix_bytes=32))
        with self.assertRaisesRegex(ValueError,'不足WAL头'):
            self.collect_frozen(self.freeze(prefix_bytes=20))

    def test_pairing_consistency(self):
        db=bytearray(STORE_DB);db[16:18]=(512).to_bytes(2,'big')
        with self.assertRaisesRegex(ValueError,'页大小与主库不符'):
            self.collect_frozen(self.freeze(db_bytes=bytes(db)))
        db=bytearray(STORE_DB);db[18]=1
        with self.assertRaisesRegex(ValueError,'不在WAL模式'):
            self.collect_frozen(self.freeze(db_bytes=bytes(db)))
        wal_bytes=build_wal([(2,3,wal_page(STORE_WAL,3))],version=3007001)
        with self.assertRaisesRegex(ValueError,'3007000'):
            self.collect_frozen(self.freeze(wal_bytes=wal_bytes))

    def test_table_name_validated(self):
        for bad in ('vouchers;DROP TABLE vouchers','x" OR 1=1--',''):
            with self.subTest(bad=bad):
                with self.assertRaisesRegex(ValueError,'表名'):self.collect_frozen(self.freeze(table=bad))

    def test_output_readonly_atomic_no_overwrite(self):
        r,out=self.collect_data('normal.json')
        mode=stat.S_IMODE(os.stat(out/'snapshot.db').st_mode)
        self.assertEqual(mode,0o444)
        with self.assertRaisesRegex(ValueError,'拒绝覆盖'):
            collect(json.loads((ROOT/'normal.json').read_text(encoding='utf8')),ROOT,out)
        self.assertEqual(sorted(p.name for p in out.iterdir()),['snapshot.db','snapshot.report.json'])

    def test_sources_untouched(self):
        before={p.name for p in ROOT.iterdir()}
        digests={n:hashlib.sha256((ROOT/n).read_bytes()).hexdigest() for n in ('store.db','store.wal','uncommitted.wal','partial.wal')}
        self.collect_data('normal.json')
        self.assertEqual({p.name for p in ROOT.iterdir()},before)
        for n,d in digests.items():self.assertEqual(hashlib.sha256((ROOT/n).read_bytes()).hexdigest(),d)

if __name__=='__main__':unittest.main()
