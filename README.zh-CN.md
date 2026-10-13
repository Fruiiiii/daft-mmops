# daft-mmops

> Bilingual edition of [README.md](README.md): the English original comes first,
> followed by its Chinese translation. If the two differ, the English original
> prevails.
>
> 本文是 [README.md](README.md) 的中英对照版：英文原文在前，中文翻译在后；如两
> 者不一致，以英文原文为准。

`daft-mmops` is a multimodal data processing operator library for Daft. It is
intended for reusable text, image, audio, video, document, and cross-modal
preprocessing, enrichment, model inference, and data annotation.

`daft-mmops` 是一个面向 Daft 的多模态数据处理算子库，面向可复用的文本、图像、
音频、视频、文档以及跨模态的预处理、富化、模型推理与数据标注。

The current release runs on Daft and provides:

当前版本基于 Daft 运行，并提供：

- a growing operator catalog under `mmops.operators`;

- 位于 `mmops.operators` 的持续扩充的算子目录；

- `@mmops.row`, `@mmops.batch`, and `@mmops.operator` for developing operators;

- 用于开发算子的 `@mmops.row`、`@mmops.batch` 与 `@mmops.operator`；

- Python-scalar row processing and PyArrow-native batch processing;

- Python 标量行处理与 PyArrow 原生批处理；

- stateless/stateful and synchronous/asynchronous execution;

- 无状态/有状态与同步/异步执行；

- Daft resource, concurrency, retry, and error-policy integration.

- Daft 资源、并发、重试与错误策略集成。

For implementation rules and complete templates, see the
[Multimodal Operator Development Guide](docs/operator-development.md).
Repository-wide automation rules are defined in [AGENTS.md](AGENTS.md).

实现规则与完整模板见[多模态算子开发指南](docs/operator-development.zh-CN.md)。
仓库级自动化规则定义于 [AGENTS.zh-CN.md](AGENTS.zh-CN.md)。

## Installation 安装

Install only the released modality you need:

只安装你需要的已发布模态：

```bash
python -m pip install 'daft-mmops[text]'
```

Available extras:

可用 extras：

| Extra | Purpose |
|---|---|
| `text` | Daft runtime and all released text operators |
| `image` | reserved for image operators; currently empty |
| `audio` | reserved for audio operators; currently empty |
| `video` | reserved for video operators; currently empty |
| `document` | reserved for document operators; currently empty |
| `multimodal` | reserved for cross-modal operators; currently empty |
| `all` | dependency union of `text`, `image`, `audio`, `video`, `document`, and `multimodal`; currently equivalent to `text` |
| `daft` | Daft runtime for SDK users who only define custom operators |
| `test` | dependencies needed to run pytest |
| `dev` | complete contributor toolchain: tests, formatting, build, and metadata checks |

| Extra | 用途 |
|---|---|
| `text` | Daft 运行时与所有已发布的文本算子 |
| `image` | 预留给图像算子；当前为空 |
| `audio` | 预留给音频算子；当前为空 |
| `video` | 预留给视频算子；当前为空 |
| `document` | 预留给文档算子；当前为空 |
| `multimodal` | 预留给跨模态算子；当前为空 |
| `all` | `text`、`image`、`audio`、`video`、`document` 与 `multimodal` 的依赖并集；当前等价于 `text` |
| `daft` | 供只定义自定义算子的 SDK 用户使用的 Daft 运行时 |
| `test` | 运行 pytest 所需的依赖 |
| `dev` | 完整的贡献者工具链：测试、格式化、构建与元数据检查 |

From a source checkout, add `-e`, for example
`python -m pip install -e '.[text]'`. Python 3.10 or newer and PyArrow 12 or
newer are required. Version `0.1` targets the public Daft 0.7 UDF API.

从源码检出安装时请加 `-e`，例如 `python -m pip install -e '.[text]'`。需要
Python 3.10 或更新版本以及 PyArrow 12 或更新版本。版本 `0.1` 面向 Daft 0.7
公共 UDF API。

All planned modality names are reserved as installation extras. An empty extra
does not mean that the modality is implemented; it provides a stable install
name until its first production operator adds runtime dependencies. The `all`
extra is always the flattened dependency union of all six modality extras, so
empty modalities currently contribute no dependencies.

所有规划中的模态名称均已保留为安装 extra。空的 extra 不代表该模态已实现；在
其首个生产算子加入运行时依赖之前，它提供一个稳定的安装名。`all` extra 始终是
全部六个模态 extras 的扁平依赖并集，因此空模态目前不引入任何依赖。

## Quick start 快速开始

Built-in operators are ordinary Daft expressions:

内置算子就是普通的 Daft 表达式：

```python
import daft

from mmops.operators import KeywordMatcher, normalize_text

df = daft.from_pydict(
    {
        "text": ["  ROBOT navigation  ", None, "manipulation"],
    }
)

normalized = df.with_column(
    "normalized",
    normalize_text(df["text"], lowercase=True),
)
matcher = KeywordMatcher(
    ["robot", "navigation", "manipulation"],
    whole_word=True,
)
result = normalized.with_column(
    "keywords",
    matcher.match_batch(normalized["normalized"]),
)
```

Define a lightweight row operator with Python scalar types:

用 Python 标量类型定义一个轻量级行算子：

```python
import mmops


@mmops.row
def normalize(text: str) -> str:
    return text.strip().lower()


df = df.with_column("normalized", normalize(df["text"]))
```

Batch and stateful operators use the same expression style. See the
[development guide](docs/operator-development.md#choose-an-execution-shape) for
Arrow batch and model-backed class templates.

批处理与有状态算子使用相同的表达式风格。Arrow 批处理与模型类模板见
[开发指南](docs/operator-development.zh-CN.md#choose-an-execution-shape-选择执行形态)。

## Text operators 文本算子

The first text package contains one stateless and one stateful operator:

首个文本包包含一个无状态算子与一个有状态算子：

| Operator | State and entrypoints | Behavior | Framework contract covered |
|---|---|---|---|
| `normalize_text` | stateless batch | Unicode, lowercase, whitespace collapse, and trim options | scalar options, Arrow kernels, null and empty batches |
| `KeywordMatcher` | worker-local row + batch | compiled keyword matching with case and whole-word options | constructor options, row/batch parity, Arrow list output, null and pickle |

| 算子 | 状态与入口 | 行为 | 覆盖的框架契约 |
|---|---|---|---|
| `normalize_text` | 无状态 batch | Unicode、小写、空白折叠与去首尾空格选项 | 标量选项、Arrow 内核、null 与空批次 |
| `KeywordMatcher` | worker 本地 row + batch | 支持大小写与全词选项的编译后关键词匹配 | 构造器选项、row/batch 一致性、Arrow 列表输出、null 与 pickle |

```python
from mmops.operators.text import KeywordMatcher, normalize_text

df = df.with_column(
    "normalized",
    normalize_text(df["text"], lowercase=True),
)

matcher = KeywordMatcher(
    ["robot", "manipulation", "navigation"],
    case_sensitive=False,
    whole_word=True,
)
df = df.with_column("keywords", matcher.match_batch(df["normalized"]))
```

`normalize_text` uses Arrow compute kernels and preserves nulls.
`KeywordMatcher` compiles escaped regex patterns once per Daft worker/actor,
returns each configured keyword at most once in configuration order, and
returns null for null text. Keyword configuration is validated when the first
entrypoint is invoked because Daft initializes operator instances lazily.

`normalize_text` 使用 Arrow compute 内核并保留 null。`KeywordMatcher` 在每个
Daft worker/actor 上将转义后的正则表达式编译一次，按配置顺序对每个配置的关键
词至多返回一次，null 文本返回 null。关键词配置在首次调用 entrypoint 时校验，
因为 Daft 会惰性初始化算子实例。

## Current execution scope 当前执行范围

Version 1 supports strict 1:1 Python UDF execution on Daft:

版本 1 支持基于 Daft 的严格 1:1 Python UDF 执行：

| | row | batch |
|---|---|---|
| stateless sync | supported | supported |
| stateless async | supported | supported |
| stateful sync | supported | supported |
| stateful async | supported | supported |

| | row | batch |
|---|---|---|
| 无状态同步 | 支持 | 支持 |
| 无状态异步 | 支持 | 支持 |
| 有状态同步 | 支持 | 支持 |
| 有状态异步 | 支持 | 支持 |

Batch functions accept one or more `pa.Array` inputs and return one `pa.Array`.
Use `pa.StructArray` with `unnest=True` for multiple output columns. Daft owns
planning, partitioning, local or distributed execution configured through
Daft, resource scheduling, async concurrency, retry handling, and backpressure.

batch 函数接受一个或多个 `pa.Array` 输入并返回一个 `pa.Array`。多个输出列可使
用 `pa.StructArray` 配合 `unnest=True`。Daft 负责规划、分区、通过 Daft 配置的
本地或分布式执行、资源调度、异步并发、重试处理与背压。

Generator/flat-map UDFs, UDAFs, dynamic schemas, distributed strong state, and
execution directly through other data engines are outside version 1.

生成器/flat-map UDF、UDAF、动态 schema、分布式强一致状态，以及直接通过其他
数据引擎执行，均不在版本 1 范围内。

## Development 开发

Run the formal text pipeline example:

运行正式的文本流水线示例：

```bash
python examples/text_pipeline.py
```

Install the contributor environment and run the test suite:

安装贡献者环境并运行测试套件：

```bash
python -m pip install -e '.[dev]'
pytest -q
```

Before adding or reviewing an operator, read
[docs/operator-development.md](docs/operator-development.md) for the data
contract, state semantics, dependency policy, test matrix, and review checklist.

在新增或评审算子之前，请阅读
[docs/operator-development.zh-CN.md](docs/operator-development.zh-CN.md)，了解
数据契约、状态语义、依赖策略、测试矩阵与评审清单。
