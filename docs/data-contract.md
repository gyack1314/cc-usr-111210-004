# 冻结门店文件

主库文件是同代WAL开始之前的已checkpoint状态，清单给外部冻结代号。magic 0x377f0682用小端32位字计算checksum，0x377f0683用大端，WAL中checksum数值本身均大端。WAL头校验覆盖前24字节，从(0,0)开始；每frame从上一校验累加其前8字节与整页。按成对32位字更新s0=(s0+x0+s1) mod 2^32，s1=(s1+x1+s0) mod 2^32。salt须等于头，frame页号从1开始，提交db-size非零。

commit取last或从1起的提交序号。数据库page size从SQLite头解析，1代表65536，WAL页大小须相符；头格式3007000。前缀中的所有完整frame都须通过校验，包括所选较早提交之后的完整frame；不完整尾部仅标截断。输出截到所选提交页数，主库或已应用页无法覆盖的洞失败。

正常表vouchers只含虚构服务券。查询固定为SELECT id,service,amount FROM vouchers ORDER BY id，不执行清单或源文件提供的任意SQL。源db与wal均不以SQLite可写模式打开，验证只针对新建副本。已发布副本需保留原始头格式并由只读immutable方式检查，避免遗留wal被自动重放。
