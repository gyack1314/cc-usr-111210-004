# 门店SQLite离线副本的WAL提交前缀采集

这是商品与服务履约资料的独立Python工具。`data/`保存合成报文、导出文件或解码服务响应，`source_data.io`读取清单并核对原文件身份，`source_data.domain`提供原始资料概况，`source_data.replay`按记录返回响应，`source_data.cli`显示JSON摘要。字段与来源格式见[资料约定](docs/data-contract.md)。不接入真实商家、支付账户或在线平台。

Python 3.11及以上，无第三方Python依赖。在项目根目录使用：

```sh
python -m unittest discover -s tests -v
python -m compileall -q source_data tests
python -m source_data.cli inspect data/normal.json
```

`inspect`仅读取清单、原始字节身份和基本配置，不覆盖资料。`invalid-config.json`是配置反例，其他清单有不同的源边界，边界报文可以保持合法清单但源内容不完整。资料中的姓名、编号、订单和站点均为虚构。

## 提交边界采集

`collect`把一对已冻结的主库与同代WAL还原成指定提交边界的可查询副本，逐页记录来源：

```sh
python -m source_data.cli collect data/normal.json out/normal
```

输出目录原子发布`snapshot.db`（只读0444，已存在则拒绝覆盖）与`snapshot.report.json`，报告同时打印到stdout。采集器只读源文件字节，从不以SQLite打开源db/wal，也不读取shm；发布前复核源身份确认采集期间未被改动。WAL头与每个完整frame的salt、页号、滚动校验和全部验证，校验字节序由magic决定；前缀末尾的不完整frame只记为截断，损坏的完整frame直接失败，不会跳过它去找后面的提交。边界之后每页取最后一个已提交frame，输出按提交db-size裁到准确页数，主库与已应用frame都覆盖不到的页报错而不是补零。副本以只读immutable方式通过`integrity_check`并读取约定的vouchers核对行，查询固定为`SELECT id,service,amount FROM vouchers ORDER BY id`，不执行清单或源文件提供的任意SQL。

`data/`内清单演示了各场景的边界差异：`normal.json`（last提交，页2被frame 1与3两次覆写，取后者）、`earlier-commit.json`（选第1个提交，页2改取frame 1，券行回到较早状态）、`uncommitted-tail.json`（最后事务未提交，frame 4不应用，副本与normal逐字节一致）、`partial-tail.json`（前缀末尾11字节不完整frame，只记截断）。测试里构造了损坏frame、salt不符、新一代WAL拼旧主库、扩库洞等拒绝场景。采集只反映已提交事务，不修复数据库，"最后已提交"不代表所有营业操作均已落盘。

Dockerfile固定Linux基础镜像。`docker build -t commerce-source .`构建，`docker run --rm commerce-source`读取正常清单；`docker run --rm commerce-source python -m unittest discover -s tests -v`运行已有检查。运行资料均在本地，不需要账号、安装脚本或生产系统。
