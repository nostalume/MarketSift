# API使用：从材料到可追问的比较

供研究Agent操作当前MarketSift。命令在项目根目录运行，复用已有环境：Windows用 `.venv/Scripts/marketsift.exe`，其他平台用 `.venv/bin/marketsift`，或 `uv run --locked --no-sync marketsift`。下文的 `marketsift` 指这个项目入口。默认数据库为 `data/marketsift.sqlite`，CLI用 `--database <路径>`、Python用 `database=...` 选择其他库。

## 入口与返回

| 入口 | 操作 | 结果 |
|---|---|---|
| `sources` / `api.sources()` | 读取静态能力 | `sources`下的来源、datasets、filters、options等；不读取得历史 |
| `collect --requests -` / `api.collect(requests, database=..., timeout=15)` | 显式取得并写入连续库 | 总体status、添加数量、各来源批次/read_ids与failures |
| `query --spec -` / `api.query(selections=None, *, relations=None, calculations=None, emit=None, database=..., as_of=None)` | 本地只读选择→关系→计算→投影 | `as_of`与`results.<命名结果>`；不自动补采 |

两个stdin入口读UTF-8 JSON，`-`也可替换为文件路径。collect接请求数组，query接对象；CLI的database来自命令参数，as_of放spec或`--as-of`其中一处。需要查看人可读JSON时用`--pretty`。旧context/info/prices等公共读取已由query取代。

下面是查询取得记录的普通JSON，可存为query.json后运行`marketsift query --spec query.json`，或传给stdin；日期按实际问题替换：

```json
{
  "selections": {
    "activity": {
      "dataset": "acquisitions", "where": {"sources": ["sina"]},
      "time": {"axis": "received", "since": "2026-11-03T09:00+08:00", "until": "2026-11-04T00:00+08:00"}
    }
  }
}
```

## 按需求取得资料

先用sources确认本次来源的参数，再显式取得缺少的范围。例如以下为固定历史示例，使用时换成所问时段：

```python
from marketsift import api

requests = [{
    "source": "okx", "datasets": ["prices", "series", "trades"],
    "instruments": ["OKX:SWAP:BTC-USDT-SWAP"],
    "since": "2026-11-03T08:00Z", "until": "2026-11-03T10:00Z",
    "options": {"metrics": ["funding_history", "open_interest", "ticker"]},
}]
# 联网并写库时调用：api.collect(requests)
```

请求可含source、datasets、instruments、series_ids、urls、since/until、interval、basis、options，实际支持项归该来源。瞬时时间带UTC偏移，原生日期范围使用日期；options容纳来源专用参数。`collect --source ...`等平铺参数适合简单采集，不能与`--requests`混用。

采集名`info`写正文，`prices`写bar，`series`写定义与标量，`trades`写成交，`instruments`写身份及参考。它们与下表的读取粒度不同。原生历史导入可用`collect --source sqlite --input <旧A股库> --since ... --until ...`或`collect --source archive --input <普通资料JSON>`，原文件只读接续。

每批直接提交，失败前的有效资料保留。相同事实不重建版本，新的取得尝试仍进入acquisitions；修订才产生新版本。继续使用同一库，按实际缺口重试对应请求。

## 选择事实与时间

每个selection有两个模式：

- 精确版本：`{"dataset":"documents","ids":[已返回的本地ID]}`，只受全局as_of约束，不能混where/time/versions。ID属于该dataset，来源native_id另存；缺失或当时不可见的ID列入missing_ids。
- 条件选择：`dataset`、可选`where`、`time`、`versions`。版本化数据默认latest，先按as_of与完整自然身份选版本，再筛可能已经更正的文字/时间；all显式读取历史。instruments和acquisitions不接受版本模式。

| dataset | 读取粒度与where | time.axis |
|---|---|---|
| documents | 正文版本：sources、text、urls、document_ids、native_ids、mentions、read_ids | published、observed |
| bars | K线版本：sources、instruments、interval、basis、sessions、session_roles、read_ids | event、observed |
| series | 标量版本：sources、instruments、series_ids、definition_ids、native_ids、read_ids | period、published、observed |
| trades | 逐笔版本：sources、instruments、native_ids、read_ids | event、observed |
| instruments | 稳定身份索引：instruments、text、kinds、venues | 无 |
| references | 每产品/来源的名称与规则版本：sources、instruments、text、read_ids | observed |
| series_definitions | 标量定义版本：sources、instruments、series_ids、read_ids | observed |
| acquisitions | 每次取得尝试：sources、status | received |

text传字符串，其他条件通常传数组，interval/basis等单值也可传字符串。where是明确谓词，不接收任意SQL。instruments.text可以从截止时可见的名称/别名发现身份，进一步选references读名称与规则；身份索引本身不认证历史可交易性。documents.mentions是字面候选过滤，使用的匹配与literal_mentions一致。

`time={"axis":...,"since":...,"until":...}`。点范围左闭右开，区间按重叠选择且保留原界限；统计日/月等使用日期范围，instant使用带时区时间。sessions/session_roles是来源已有标签，由调用者明确选择；工具没有完整交易日历。

as_of限制本地已观察版本。published问公开时间，observed问版本首次取得，received问采集尝试；event/period分别问行情或原生统计期。未知公开时间不借观察时间，无法定位的材料放unplaced；有来源粗精度区间但跨查询边界的，也单列。后来补取的历史不成为过去当时已知。

同刻冲突保留，等值的不同出处可带equivalent_version_ids；后续观察可以解除冲突，旧版本仍可精确追问。数值计算排除冲突、形成中或截止前未结束的bar；all选择中的同一bar多版本也不会隐式累加成成交量。

## 一次调用内发现并展开原文

以下Python示例均只读已有库，日期、关键词、对象和分组是示例参数，按实际问题替换。selections命名输入供关系与计算复用；关系引用选择，计算引用选择或关系，同阶段不相互依赖。名称在整个调用内唯一，所有操作共享一个SQLite快照和as_of。

```python
from marketsift import api

discovery = api.query(
    selections={"news": {
        "dataset": "documents", "where": {"sources": ["sina"], "text": "消费"},
        "time": {"axis": "published", "since": "2026-11-03T09:00+08:00",
                 "until": "2026-11-03T15:00+08:00"},
    }},
    relations={"mentions": {"kind": "literal_mentions", "input": "news"}},
    calculations={"groups": {"kind": "cooccurrence", "input": "mentions"}},
    emit={"news": ["id", "url", "title", "published_at"], "groups": None},
)
news = discovery["results"]["news"]
version_ids = [r["id"] for r in news["rows"]] + [r["row"]["id"] for r in news["unplaced"]]
full_texts = api.query(
    selections={"texts": {"dataset": "documents", "ids": version_ids}},
    as_of=discovery["as_of"],
)
```

选择结果有dataset、rows、unplaced、missing_ids。文档默认给有效原文与出处，目录只是列投影；显式选raw_text或metadata可展开来源提供的原始文本/原生表格。未知时间的材料与范围关系仍待解释，读到原文并不补足其发布时间。

emit省略时返回全部命名结果；数组选择结果，映射按列投影各结果的rows及unplaced中的row，None表示默认列。计算在投影前完成，emit不改变输入与数值；计算结果保留其领域结构，例如cooccurrence返回documents/groups/instruments，compare返回comparisons。

| 关系kind | 用途与参数 |
|---|---|
| literal_mentions | input为文档选择；返回带mentions的rows与字面边，保留歧义 |
| replies / thread | 直接原生回复 / 同一原生主题，二者分开 |
| outbound_link / same_url | 明确外链 / 相同已存URL，后者不承诺自动规范化不同URL |
| documents_for | input为含instrument的选择，scope命名文档选择；从对象反查字面相关原文 |
| underlying / instrument_series | input分别为references / series_definitions，返回来源声明关系 |

replies/thread/outbound_link/same_url支持direction=out/in/both；scope可命名文档选择，或给其where/time/versions等选择参数。种子与目标范围分开。缺目标时先检查范围和实际取得记录；关系只展开本地内容，未取得的目标/附件仍是缺口。

## 量价比较

先选所需历史一次，再计算显式窗口。例如比较两日午后，组别和basis表示本轮研究指定的成员及依据：

```python
from marketsift import api

pool = ["SH603477", "SZ002840"]
windows = {
    "prior": {"since": "2026-11-02T13:00+08:00", "until": "2026-11-02T15:00+08:00"},
    "current": {"since": "2026-11-03T13:00+08:00", "until": "2026-11-03T15:00+08:00"},
}
comparison = api.query(
    selections={
        "prices": {"dataset": "bars", "where": {"sources": ["tdx"],
            "instruments": pool + ["SH000001"], "interval": "5m", "basis": "trade"},
            "time": {"axis": "event", "since": windows["prior"]["since"],
                     "until": windows["current"]["until"]}},
        "news": {"dataset": "documents", "where": {"sources": ["sina"], "text": "猪肉"},
            "time": {"axis": "published", "since": windows["prior"]["since"],
                     "until": windows["current"]["until"]}},
    },
    relations={"related_texts": {"kind": "documents_for", "input": "prices", "scope": "news"}},
    calculations={
        "summary": {"kind": "summarize", "input": "prices", "window": windows["current"]},
        "rotation": {"kind": "compare", "input": "prices", "pool": pool,
            "groups": {"study": {"members": pool, "basis": "研究指定的同组对照"}},
            "benchmark": "SH000001", "windows": windows,
            "pairs": [["prior", "current"]], "alignment": "window_offset"},
    },
    emit=["summary", "rotation", "related_texts"],
)
```

summarize可给window、instruments、paths，返回rows、excluded、boundary_bars和缺对象。默认统计完整落入窗口的已结束bar，边界bar不插值。窗口收益为末收/首开−1，不是昨收涨幅；window_return需要正确端点，中间缺bar不抹掉端点收益。amount是观察小计，window_amount及完整收盘路径回撤要求连续覆盖，未知单位或缺值不变成零。coverage.expected_session_slots=None表示缺少认证日历；午休/休市宜分别给窗口。

compare按source/interval/basis及价格币种分开。alignment=window_offset比较显式窗口的相对位置，timestamp比较实际时点；pairs明确所比窗口，字典顺序不生成业务比较。不同窗口、成员、组和基准各在其所需参与者内对齐，缺C不取消独立A/B；无基准只限制相对基准的指标。

在comparisons中先看每窗口members.observed、groups的有效成员、common_slots及pool分母，再解释changes。份额按完整显式池的共同观察槽算，缺成员/不兼容金额单位时分母未知；共同槽成交不偷换为完整请求窗口成交。已知绝对观察继续保留，组可重叠。local_currency=True明确比较本币收益，仍不混加不同成交额单位。成交额与份额描述活动，不代表净流入、资金身份或可成交盈利。

## 事件前后与反向追问

需要事件响应时，给文档和行情的命名输入，再用event_response连接。下面沿用上一例的selections；精确事件也可另用ids选取：

```python
reaction_spec = {
    "kind": "event_response", "events": "news", "prices": "prices",
    "anchor": "published", "before": "20m", "after": "20m",
    "subjects": pool, "benchmark": "SH000001",
}
# 将reaction_spec放入同一次api.query的calculations，例如 {"reaction": reaction_spec}。
```

subjects明确指定对象；若省略，events需引用literal_mentions关系等带mentions的输入。可给groups，以及指向所选标量的context。published/observed锚点、前后窗口、原文和bar引用组成可追问关系；未知/粗精度锚点、冲突、尚未发生的后窗口和缺行情分别说明。盘后材料没有当日发布后价格，不能据此宣布“没有响应”。共用bar的多个事件也不是独立证据，更不由时间相近证明因果。

反向从量价变化追问时，用同一次选择中的bars和documents做documents_for，再读相关原文/链接。示例中的主题原文可能没有股票字面提及，反查为空不否定主题关系；研究指定的subjects需要Agent阅读后提出依据。对象范围可来自历史或新发现；语义主题及影响关系由Agent解释。

## 标量期间与当时已知背景

series是标量值版本，series_definitions是定义。`change`接input和axis：period在完整原生身份内选各统计期的最新版本后比较同口径期间；revision仅比较定义、native_id、period_kind/start/end都相同的观察，需用versions=all选修订史。单位/定义改变、不同SEC申报或月值与全年值保留区别；缺失/冲突变化不算成零。

`available_asof`给input和at，返回该时点最后取得的标量批次、age_seconds和原生期间；一个批次可能含多个期间或估算。它问“当时已取得什么”，不按统计月份前填。event_response的context复用这一操作。当时尚未取得所选值会返回空行，这不等于当时没有发布；只有一个版本的统计期也不会凭空产生修订。

```python
from marketsift import api

macro = api.query(
    selections={"values": {"dataset": "series", "where": {"sources": ["fred"]},
        "time": {"axis": "period", "since": "2026-08-01", "until": "2026-11-03"},
        "versions": "all"}},
    calculations={
        "revisions": {"kind": "change", "input": "values", "axis": "revision"},
        "known": {"kind": "available_asof", "input": "values", "at": "2026-11-03T12:00Z"},
    },
    emit=["revisions", "known"],
)
```

## 取得记录与恢复

以acquisitions按source和received区间查实际尝试，读status、detail、metadata、datasets范围和read_id，联系事实中的read_id追问。最后一次成功只说明这次请求，不覆盖以前失败的其他类型或时段。来源内容范围见[来源参考](sources.md)。

已有库无匹配返回空行；缺库报错且不创建空库。调用参数/程序错误应定位原调用，数据缺口按相应结果解释。CLI stdout为JSON，stderr为诊断；exit0成功，collect有partial/failed时exit1，调用或本地读写错误exit2。检查已提交批次和实际缺项，再重试明确请求，随后用同一库/新as_of复评。
