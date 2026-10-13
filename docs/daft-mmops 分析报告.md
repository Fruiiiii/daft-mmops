# daft-mmops 代码分析报告

> 分析对象：`daft-mmops` 工程内的全部 Python 代码（`src/`、`tests/`、`examples/`）
> 分析方式：仅基于代码，未参考仓库内任何 Markdown 文档。

---

## 1. 项目概述

`daft-mmops`（包名 `daft-mmops`，模块名 `mmops`，版本 `0.1.0`）是一个**多模态数据处理与标注算子库（operator library）**。其核心价值是：让用户用「纯 Python 标量」或「PyArrow」接口编写业务算子，再由内部后端把算子编译并提交到 **Daft** 分布式 DataFrame 引擎上执行。

从代码层面可确证的关键事实：

- **当前唯一执行引擎是 Daft**。后端层只有 `src/mmops/backends/daft.py`，不存在其它引擎绑定（`backends/__init__.py` 仅一行说明，无 Ray/Ray Data 实现）。
- **业务代码与引擎解耦**。算子业务代码（`operators/`）只接触 `pyarrow`，从不触碰 `daft.Series` / `daft.DataType`。
- **运行时依赖极轻**。`[project]` 声明的基础依赖只有 `pyarrow>=12`；Daft 是可选依赖（`daft`、`text` extras 提供）。
- **模态以 extras 预留**：`image`/`audio`/`video`/`document`/`multimodal` 均为空 extras，仅 `text` 有依赖且已有实现。

---

## 2. 技术栈与依赖

| 类别 | 内容 |
|------|------|
| 语言 | Python（`requires-python >= 3.10`，使用 `from __future__ import annotations`） |
| 构建 | setuptools（`src` 布局，`[tool.setuptools.packages.find] where=["src"]`） |
| 硬依赖 | `pyarrow>=12` |
| 可选依赖 | `daft>=0.7`（`daft`、`text`、`all` extras） |
| 开发依赖 | `pytest`、`black`、`build`、`twine` |
| 类型与校验 | `pyarrow` 类型系统、`dataclasses(frozen=True)`、`typing` |

代码风格统一：模块级 `from __future__ import annotations`、类型标注完整、frozen dataclass 承载配置、`frozenset`/`Literal` 约束枚举。

---

## 3. 代码结构与分层

```
src/mmops/
├── __init__.py                # 公共 SDK 出口（装饰器 + 定义类型 + 异常）
├── _definitions.py            # 数据定义层：异常、冻结容器、Resources/EntrypointDefinition/OperatorDefinition
├── _decorators.py             # 装饰器层：@operator/@row/@batch + Function/Operator/Instance 三个 Facade
├── _arrow.py                  # Arrow 边界层：参数转换 + 批量输出校验
├── backends/
│   ├── __init__.py
│   └── daft.py                # 后端绑定层：把定义编译为 Daft UDF/类
└── operators/
    ├── __init__.py            # 算子目录出口
    └── text/
        ├── __init__.py
        ├── normalize.py       # normalize_text（批处理算子）
        └── keywords.py        # KeywordMatcher（有状态算子）
```

整体呈清晰的三层架构：

```
┌─────────────────────────────────────────────┐
│  调用侧 / 算子目录 (operators, examples)      │  用户业务代码（pyarrow / Python 标量）
├─────────────────────────────────────────────┤
│  定义层 (_definitions.py)                     │  不可变、可序列化的算子契约
│  装饰器层 (_decorators.py)                    │  Facade 懒编译、属性转发
│  Arrow 边界层 (_arrow.py)                     │  类型/形状契约校验
├─────────────────────────────────────────────┤
│  后端层 (backends/daft.py)                    │  编译为 Daft 公共 API UDF
└─────────────────────────────────────────────┘
```

---

## 4. 核心模块逐项分析

### 4.1 `_definitions.py` —— 契约与数据类型

**职责**：定义算子契约的不可变数据模型、异常体系、资源与校验逻辑。

- **异常体系**（公开）：
  - `MmopsError`：公共根异常。
  - `DefinitionError(MmopsError, ValueError)`：算子定义非法（如 batch 缺 return_dtype、非法 batch_size）。
  - `BatchOutputError(MmopsError, ValueError)`：批量输出违反 Arrow 契约（非数组 / dtype 不符 / 行数不符）。
  - `DaftCompatibilityError(MmopsError, RuntimeError)`：后端无法表达所请求选项（如旧版 Daft 不支持某选项）。
- **冻结容器**（保证可 pickle 且深不可变）：
  - `FrozenDict(Mapping)`、`FrozenList(Sequence)`、`FrozenSet(frozenset)`；三者均实现 `__reduce__` 以支持跨进程序列化。
  - `_freeze(value)` 递归冻结 Mapping/list/tuple/set/frozenset；`_frozen_mapping` 是构造入口。
- **校验函数**：`_validate_resource_number`（有限非负实数）、`_validate_non_negative_int`、`_validate_positive_int`、`_validate_on_error`（`None|raise|log|ignore`）。
- **`Resources`**（frozen）：`cpus: float|None`、`gpus: float|None`、`memory_bytes: int|None`，`__post_init__` 中做数值校验。
- **`EntrypointDefinition`**（frozen）：单行/批处理入口契约。字段 `original`(原始可调用)、`kind`("row"/"batch")、`return_dtype`、`resources`、`unnest`、`batch_size`、`max_retries`、`on_error`、`use_process`、`backend_options`。`__post_init__` 强制：batch 必须给 `return_dtype`、row 不得给 `batch_size`、batch_size 为正整数、`backend_options` 冻结。
- **`OperatorDefinition`**（frozen）：有状态算子类契约。字段 `original`(类)、`entrypoints`(名称→EntrypointDefinition)、`resources`、`max_concurrency`、`max_retries`、`on_error`、`use_process`、`name`、`backend_options`。强制至少一个入口，`entrypoints`/`backend_options` 冻结。

**设计要点**：契约即配置，深不可变保证在 Daft 分布式 worker 之间安全序列化。

### 4.2 `_decorators.py` —— 装饰器与 Facade

**职责**：提供 `@row`/`@batch`/`@operator` 三个公开装饰器，以「懒编译门面」隔离 Daft。

三个门面类：

- **`FunctionFacade`**：
  - 持有 `EntrypointDefinition`，首次 `__call__` 时经 `_compile()` 惰性调用 `compile_function`（此时才 `import daft`）。
  - 用 `functools.update_wrapper` 与 `inspect.signature` 保留 `__name__`/`__doc__`/`__signature__`。
  - 自定义 `__getstate__`/`__setstate__`：序列化时丢弃 `_compile_lock` 与 `_compiled`，反序列化后重建锁——保证可 pickle（测试 `test_definitions.py`、`test_daft_operators.py` 已验证往返）。
- **`OperatorFacade`**：
  - 持有 `OperatorDefinition`，调用时经 `compile_operator` 编译出一个「继承原类」的适配类并实例化。
  - 实例化结果包成 `OperatorInstanceFacade`。
- **`OperatorInstanceFacade`**：
  - 通过 `__getattr__` 仅暴露 `entrypoints` 中显式声明的入口，隐藏 `helper`/`_match_text` 等私有方法（测试验证 `AttributeError`）。
  - `__call__` 仅当声明了 `__call__` 入口时可用。

装饰器契约：

- `@row(...)` / `@batch(...)`：支持裸用与带参两种形态（`@overload`），共同走 `_entrypoint(kind, fn, ...)` 工厂。
- `@operator(...)`：扫描 `vars(original)` 中所有 `FunctionFacade` 成员作为入口集合，构造 `OperatorDefinition`；无入口会抛 `DefinitionError`。

**设计要点**：懒编译令 `import mmops` 不触发 `import daft`（测试用子进程断言 `daft not in sys.modules`），可选重依赖被完全隔离。

### 4.3 `_arrow.py` —— Arrow 边界契约

**职责**：把 Daft 侧值转换为 Arrow，并对批量输出做严格校验。

- **`to_arrow_argument(value)`**：
  - `pa.Array` 原生直通（零拷贝）；
  - 有 `to_arrow()` 方法的对象（Daft Series 的公共边界）调用它，且强制返回 `pa.Array`；
  - 标量字面量直通。
- **`_types_compatible(expected, actual)`**：数据类型相等即兼容；对 `string/large_string`、`binary/large_binary` 这两组「仅偏移宽度不同」的类型放宽为逻辑兼容。
- **`validate_batch_output(output, definition, expected_length)`**：
  1. 必须是 `pa.Array`（否则 `BatchOutputError`）；
  2. 实际 dtype 与声明 `return_dtype` 兼容（否则 `BatchOutputError`）；
  3. 长度必须与输入行数严格一致（1:1 基数，否则 `BatchOutputError`）。

**设计要点**：V1 版本严格执行 1:1 基数与类型契约，杜绝批量路径静默变形；字符串/二进制偏移宽度兼容是对 Arrow 语义的合理放宽（`test_arrow_boundary.py` 逐条覆盖）。

### 4.4 `backends/daft.py` —— Daft 编译绑定

**职责**：把 MMops 定义翻译为 Daft 公共 API 的 UDF/类，并做「选项不静默丢弃」的严格校验。

关键机制：

1. **选项白名单**：`_ENTRYPOINT_OPTION_KEYS` 与 `_OPERATOR_OPTION_KEYS` 两套 `frozenset`，界定「顶层已管理、后端不得覆盖」的保留键。
2. **命名空间选项**：`_namespaced_options` 取出 `backend_options["daft"]`；`_thaw` 将冻结容器还原为可序列化/可传递的原生类型（`_freeze` 的逆操作）。
3. **合并与防覆盖**：`_merge_backend_options` 若发现 `backend_options["daft"]` 与保留键冲突则抛 `DaftCompatibilityError`，提示改用顶层选项。
4. **能力探测**：`_supported_options(decorator, options, context)` 用 `inspect.signature` 检查目标 Daft 装饰器是否支持每个键，不支持即抛错——**绝不静默丢弃用户显式请求的选项**。
5. **类型转换**：`_arrow_return_dtype` 通过公共 API `daft.DataType.from_arrow_type` 把 `pa.DataType` 转成 `daft.DataType`。
6. **批次参数转换**：`_convert_batch_arguments` 确保至少一个 `pa.Array` 入参、所有数组等长，返回期望长度。
7. **批次桥接**：`_batch_bridge` 按 original 是否协程分别生成 sync/async 包装，内部完成参数转换 + 调原函数 + `validate_batch_output`。
8. **四类编译入口**：
   - `compile_function`：独立函数。batch→`daft.func.batch`，row→`daft.func`。
   - `_compile_method`：类方法。batch→`daft.method.batch`，row→`daft.method`。
   - `compile_operator`：用 `type(...)` 动态创建 `(original,)` 的子类，把各入口替换为编译后的方法，再套 `daft.cls(adapter_class, **options)`。

**设计要点**：只使用 Daft 公共 API（`daft.func`/`daft.func.batch`/`daft.method`/`daft.method.batch`/`daft.cls`/`daft.DataType.from_arrow_type`），不触碰私有 `daft.udf.*`。

### 4.5 `operators/text/` —— 算子目录

#### 4.5.1 `normalize.py`

- `_NORMALIZATION_FORMS = frozenset({"NFC","NFKC","NFD","NFKD"})` 约束合法形式。
- `@batch(return_dtype=pa.string(), batch_size=1024)` 修饰 `normalize_text(text, form="NFKC", lowercase=False, collapse_whitespace=True, strip=True)`。
- 纯 Arrow 内核实现：`pc.utf8_normalize` →（可选）`pc.utf8_lower` →（可选）`pc.replace_substring_regex(r"\s+", " ")` →（可选）`pc.utf8_trim_whitespace`。
- 非法 `form` 抛 `ValueError`（运行时参数校验）；null 值在 Arrow 内核下天然保持为 null。

#### 4.5.2 `keywords.py`

- `KEYWORD_MATCH_DTYPE = pa.list_(pa.string())`。
- `@operator class KeywordMatcher`：
  - `__init__` 校验非空、去空串、按 `casefold`（大小写不敏感时）查重；`re.escape` 编译正则，`whole_word` 时加 `(?<!\w)...(?!\w)` 边界；配置为 `re.IGNORECASE` 可选。
  - `_match_text` 返回命中关键字（保持配置顺序、去重）。
  - `@row(return_dtype=KEYWORD_MATCH_DTYPE) def match(...)`：单行 Python 标量处理。
  - `@batch(return_dtype=KEYWORD_MATCH_DTYPE, batch_size=256) def match_batch(...)`：`pa.Array` 进、把每行 `as_py()` 后走同一 `_match_text`，再 `pa.array(matches, type=...)` 出，保证 row/batch 语义一致。
- 有状态算子的典型范式：`__init__` 里初始化「重对象」（编译正则在局部 worker），入口方法复用状态。

### 4.6 出口 `__init__.py`

- 顶层 `mmops/__init__.py` 导出：装饰器 `batch`/`operator`/`row`，异常与定义 `BatchOutputError`/`DaftCompatibilityError`/`DefinitionError`/`EntrypointDefinition`/`MmopsError`/`OperatorDefinition`/`Resources`。
- `mmops/operators/__init__.py` 从 `text` 再导出 `KEYWORD_MATCH_DTYPE`、`KeywordMatcher`、`normalize_text`。

---

## 5. 执行流程（以两个算子为例）

**独立批处理函数 `normalize_text`：**

```
daft.from_pydict(...).select(normalize_text(df["text"], lowercase=True))
        │
        ▼
FunctionFacade.__call__ → _compile() → backends.daft.compile_function
        │  (第一次才 import daft)
        ▼
_entrypoint_options 组装 return_dtype/batch_size 等 → _supported_options 校验
        │
        ▼
daft.func.batch(batch_size=1024, return_dtype=...)(_batch_bridge(original))
        │
        ▼  Daft 执行到每个批次
_batch_bridge: to_arrow_argument(df chunk) → original(pa.Array) → validate_batch_output
```

**有状态算子 `KeywordMatcher`：**

```
matcher = KeywordMatcher(["robot", ...], whole_word=True)   # __init__ 局部编译正则
        │
        ▼
OperatorFacade.__call__ → compile_operator → type("KeywordMatcher", (原类,), ns)
        │  ns 中各入口方法替换为 _compile_method 产物
        ▼
daft.cls(adapter_class, **options)，实例化包成 OperatorInstanceFacade
        │
        ▼
matcher.match_batch(df["text"])
  → OperatorInstanceFacade.__getattr__("match_batch")  # 仅显式入口可见
  → daft.method.batch(...)(_batch_bridge(match_batch 原方法))
```

---

## 6. 质量与测试覆盖分析

测试布局与源码镜像，覆盖全面：

| 测试文件 | 覆盖点 |
|----------|--------|
| `test_definitions.py` | 装饰器裸用/带参、元数据保留、冻结与 pickle、资源/参数校验、入口收集、异常 |
| `test_arrow_boundary.py` | `to_arrow_argument` 直通/`to_arrow()`/标量、批量输出类型/长度/窄宽类型兼容、嵌套类型精确匹配 |
| `test_daft_functions.py` | 独立 row/batch 函数在真实 Daft DataFrame 中运行、同步/异步、空表契约 |
| `test_daft_operators.py` | 有状态算子、row+batch 双入口、async 方法、`__call__` 转发、Daft pickle 往返 |
| `test_daft_contract.py` | struct unnest、非数组/错 dtype/错基数报错、不静默丢弃选项、后端选项命名空间、类型保留、防覆盖、计划透传、未设 GPU 选项不转发 |
| `test_packaging.py` | `all` extra 是各模态依赖精确并集、未发布模态 extras 为空、dev 覆盖验证命令 |
| `test_examples.py` | 示例流水线在 Daft 上端到端跑通 |
| `operators/text/test_normalize.py` | 目录出口、Arrow 内核行为、null 保持、选项显式、非法 form、空数组、真实 Daft |
| `operators/text/test_keyword_matcher.py` | 配置校验/去重、row 顺序、大小写/整词、batch 与 row 语义一致、真实 Daft、pickle 与隐藏私有方法 |

值得注意的质量信号：

- **懒加载经子进程验证**：`test_importing_mmops_does_not_import_daft` 用 `subprocess` 独立进程断言，而非同进程已经污染的状态。
- **契约的「负向」测试完备**：非法输出、非法定义、不支持选项、覆盖冲突均有针对性用例。
- **「不静默丢弃」可验证**：`test_explicit_unsupported_daft_option_is_never_silently_dropped`、`test_explicit_resource_unsupported_by_installed_daft_is_reported` 通过 monkeypatch 模拟旧版 Daft 验证。
- **pickle 往返**：门面、定义、算子句柄均验证可序列化。

---

## 7. 亮点与潜在风险

**亮点（设计上值得肯定）：**

1. **关注点分离**：定义层 / 门面层 / Arrow 边界层 / 后端层职责单一，新增引擎（如未来 Ray Data）只需新增后端模块，不必改写算子。
2. **强契约校验**：`return_dtype`、1:1 基数、`pa.Array` 类型在边界处被强制，错误带清晰上下文。
3. **显式优于隐式**：用户显式请求的选项绝不会被静默丢弃，异常信息可定位。
4. **懒编译 + 轻依赖**：导入 `mmops` 不触发 `import daft`。
5. **可序列化设计**：全链路（定义→门面→运营商句柄）支持 pickle，适配分布式 worker。

**潜在风险 / 可改进点：**

1. **无可观测性/日志**：`on_error="log"` 已作为选项传递到 Daft，但代码库自身未实现统一日志抽象；错误诊断依赖异常消息。
2. **`match_batch` 跨行回到 Python 标量**：正则匹配逐行 `as_py()` 回到 Python（作者注释已说明是 Python `re` 引擎的必然），对超大数据集吞吐可能有限；这是「正确性优先」的取舍。
3. **`_convert_batch_arguments` 的标量处理**：非 `pa.Array` 的标量字面量（如 `offset: int`）会保留在 `kwargs` 中，Daft 是否逐批次广播标量依赖后端语义，边界校验只覆盖数组参数。
4. **无运行时类型注解强制**：`return_dtype` 与函数返回类型注解之间无编译期校验，依赖测试与运行时校验兜底。
5. **`name_override`/`use_process` 等键依赖 Daft 版本**：靠 `_supported_options` 探测规避，但探测结果依赖当前环境，不同环境行为可能不同（这是能力探测的固有代价）。

---

## 8. 结论

`daft-mmops` 是一个**结构清晰、契约严格、关注点分离良好的算子 SDK**。其核心工程思想是：用「不可变契约 + 懒编译门面 + 严格 Arrow 边界校验 + 单一 Daft 后端」把业务算子作者与分布式引擎细节隔离，并在此前提下守住类型安全、1:1 基数、选项显式、依赖按需加载等底线。当前实现范围集中在文本模态（一个无状态批处理算子 + 一个有状态两入口算子），但 SDK 层已为图像/音频/视频/文档/多模态模态与未来多引擎预留了明确的扩展位（空 extras、独立 backend 目录、定义层与引擎无关）。

配套的模块设计文档见《daft-mmops 模块设计文档.md》。