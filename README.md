# MshareSift

Agent 辅助的宏观与跨资产预测研究工具。使用者调用公开数据工具、搜索和追读原文，形成有主次、可判错的预测与多空持仓建议，由使用者执行交易。

行情和合约规则来自 OKX；宏观及分析证据优先免费公开来源。项目不执行交易，不运行定时扫描或通知。

## 开始

需要 Python 3.12 或 3.13、uv。在项目根运行：

```sh
uv sync --locked --no-dev --group data
uv run --locked --no-sync main.py --help
uv run --locked --no-sync main.py candle-data --help
```

| 工具 | 用途 |
|---|---|
| `candle-data` | OKX 产品规则、有限窗口采集、读取与 UTC 聚合 |
| `market-data` | 资金费、成交和 OI |
| `macro-data` | 宏观序列和政策发布索引 |
| `public-data` | 官方披露、新闻、分析、论坛、KOL 和选定原文 |

## 研究 skill

[市场预测与持仓研究](skills/market-position-research/SKILL.md) 是正式交付。让支持 skill 的 Agent 显式加载该文件，或按所用 Agent 的机制安装；`skills/` 不会自动注册到本机。

从整体局势与历史演变出发，辨认主导关系及反馈，给出明确预测，比较独立多头、空头、多空表达与不新增，并说明兑现、撤回和推翻条件。日线服务持仓判断，小时线观察传导与执行。预测核对期限不规定持仓天数，研究结论不等于已验证的收益优势。

- [研究使用](docs/research.md)：如何开始、取证和复核。
- [数据工具](docs/data.md)：命令与数据语义。
- [来源限制](docs/sources.md)：覆盖、访问和第三方内容边界。

## 开发

```sh
uv sync --locked
uv run --locked --no-sync pytest
uv run --locked --no-sync ruff check --no-cache --select F,I,RUF100 main.py src tests
```

源码位于 `src/msharesift/`，本地数据放在 `.data/`，研究输出放在 `outputs/`；二者及开发用 `.agents/` 均不提交。

## 许可

项目代码、文档和研究 skill 使用 [MIT](LICENSE)。第三方文章、图表及数据遵守各自许可与使用条件；项目许可不替它们授予再分发权。
