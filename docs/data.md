# 数据工具

从项目根运行 `uv run --locked --no-sync main.py <工具> --help`。初次使用先按 [README](../README.md) 安装锁定依赖；K 线读写需要 `data` 依赖组。以下帮助、审计和离线阅读不请求行情，`fetch` 等获取操作会访问公开网络并写入所选目录。

## K 线与产品

```sh
uv run --locked --no-sync main.py candle-data products --help
uv run --locked --no-sync main.py candle-data fetch --help
uv run --locked --no-sync main.py candle-data audit --root .data/candles
```

`audit` 只适用于已有规范 manifest 的目录。采集时明确产品、分辨率、起止时间、请求数与时间预算；根目录必须留在项目内，不能用绝对路径或 `..`。

使用 `--universe research` 识别 OKX 当前真实资产类别、现货/永续/交割、计价与结算币及合约乘数。目录支持不等于每项产品历史都已验证。`products` 可保存目录，后续 `fetch --catalog` 复用；不能按相似 ticker 替代产品或把股票永续当现金股票。

请求窗口是明确的半开区间，未收盘桶不当完整数据；缺口不填充，冲突不按最后一条覆盖。已保存的有限窗口可以续取与跨批次读取，不默认全历史加载。完整性结果只对返回的产品、窗口和分辨率成立。

Python 使用 `msharesift.candles.history.load_histories` 读取单个规范根，使用 `msharesift.candles.collection.load_collection` 合并选定根；传入 `inst_ids`、`start`、`end`，只读取所需分片。日线由完整小时源按 UTC 聚合，不等于美股现金交易日。产品上市前缺少历史不补造。

`msharesift.candles.frame` 拥有入口清理、冲突与完整桶语义；研究代码直接消费已解析对象，不再增加一层相同检查。旧压缩快照格式的导入仍由 `snapshot` 解码，格式标识保留其真实含义，不因包更名而改写已存文件。

`inventory`、`probe` 和会话命令服务明确的历史采集任务，不是默认研究步骤。容量上界与边界探测都不能证明已拥有连续全量历史。可用参数以各子命令帮助为准。

## 市场数据

```sh
uv run --locked --no-sync main.py market-data --help
uv run --locked --no-sync main.py market-data oi-report --help
```

`fetch` 获取选定数据，`verify` 检查已存结果；批量归档入口为 `bulk-fetch`、`bulk-verify`。OI 由 `oi-snapshot` 记录、`oi-report` 汇总：点时观察不是历史 OI。资金费保留已结算时间与费率，不作为未来成本预测；成交量、基金申赎、资金费和持仓量不互相替代。

## 宏观

```sh
uv run --locked --no-sync main.py macro-data fetch --root .data/macro --series DGS2 DGS10 DFII10 BAMLH0A0HYM2 --start 2026-10-01 --end 2026-10-31
```

这是固定历史窗口示例，不代表最新数据。`--policy` 同时取得美联储政策发布索引。来源以 FRED CSV 为主，结果保存在 `macro.json`；观察期、取得时间、单位和频率分别保留。CSV 是取得时的修订历史，不是历史当时可知版本；未知序列的单位或频率不猜测。

同次研究复用内存快照，成功来源保留，部分失败明确报告；不要把所有序列最后一个值冒充同日观察。资产负债表代理不等于银行准备金或风险资产买盘。

## 官方与文本来源

```sh
uv run --locked --no-sync main.py public-data fetch --root .data/reading --source bea coinmetrics hayes
uv run --locked --no-sync main.py public-data read --root .data/reading --catalog
uv run --locked --no-sync main.py public-data read --help
```

`fetch` 将本次响应提供的内容保存在 `sources.json`；`read` 从快照离线选择 URL、作者、主题或文字范围。作者/主题筛选服务后续追读，不预先限制发现。`--page` 取选定原文，`--feed` 添加 RSS/Atom，`--retry-failed` 仅重试明确失败项。SEC 请求使用能识别联系人的 `--sec-user-agent`。

来源正文、引用、回复关系与链接尽量保留；摘要、截断或图表入口不等于已经读过原图。部分失败保留成功结果并返回非成功状态，消费时检查实际范围。访问和权利边界见 [来源限制](sources.md)。
