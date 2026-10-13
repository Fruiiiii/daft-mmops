# daft-mmops 模块设计文档

> 基于工程源码（`src/mmops/**`）还原的设计说明，未参考仓库 Markdown 文档。

---

## 1. 设计目标与原则

### 1.1 定位

多模态数据处理与标注算子库。用户用 Python 标量 / PyArrow 接口编写算子，SDK 负责把算子编译并提交到 Daft 分布式引擎执行。

### 1.2 非协商约束（来自代码结构所体现的设计意图）

| 原则 | 代码体现 |
|------|----------|
| 业务代码只用 Python 标量或 PyArrow，不用 `daft.Series`/`daft.DataType` | `operators/` 仅 import `pyarrow`；Daft 类型只出现在 `backends/daft.py` |
| 引擎绑定内部隔离，未来多引擎不重写算子 | 定义层不含 Daft；`backends/` 独立包，仅 `daft.py` |
| 只用 Daft 公共 API | 只用 `daft.func`/`daft.method`/`daft.cls`/`daft.DataType.from_arrow_type` |
| V1 严格 1:1 基数 | `validate_batch_output` 强校验长度 |
| 不静默丢弃显式选项 | `_supported_options`/`_merge_backend_options` 抛 `DaftCompatibilityError` |
| 敏感信息不进代码/日志/序列化元数据 | 配置仅 `backend_options`，无凭据路径 |
| 可选重依赖不拖累基础导入 | 懒编译，`import mmops` 不 `import daft` |

---

## 2. 总体架构

```
                  用户代码 / 示例 / 算子目录
   (examples/text_pipeline.py, operators/text/*.py)
                          │ 使用 @row / @batch / @operator
                          ▼
        ┌──────────────────────────────────────┐
        │  装饰器层  _decorators.py              │
        │  FunctionFacade / OperatorFacade /    │
        │  OperatorInstanceFacade（懒编译门面）  │
        └──────────────────┬───────────────────┘
                           │ 持有
                           ▼
        ┌──────────────────────────────────────┐
        │  定义层  _definitions.py               │
        │  EntrypointDefinition / OperatorDefi- │
        │  nition / Resources / 冻结容器 / 异常  │
        └──────────────────┬───────────────────┘
                           │ 编译时
                           ▼
        ┌──────────────────────────────────────┐
        │  后端层  backends/daft.py               │
        │  compile_function / compile_operator  │
        │  选项组装 / 能力探测 / 批次桥接         │
        └──────────────────┬───────────────────┘
                           │ 运行时边界
                           ▼
        ┌──────────────────────────────────────┐
        │  Arrow 边界层  _arrow.py               │
        │  to_arrow_argument / validate_batch_  │
        │  output                              │
        └──────────────────────────────────────┘
```

分层依赖方向为**单向向下**：`operators → decorators → definitions → backend → arrow`；`arrow` 与 `definitions` 不依赖 `daft`，`backends` 在编译时才 `import daft`。

---

## 3. 模块详细设计

### 3.1 定义层 `_definitions.py`

#### 3.1.1 异常体系

```
MmopsError(Exception)
 ├── DefinitionError(MmopsError, ValueError)         # 定义非法
 ├── BatchOutputError(MmopsError, ValueError)        # 批量输出违约
 └── DaftCompatibilityError(MmopsError, RuntimeError) # 后端能力不匹配
```

#### 3.1.2 冻结容器（不可变 + 可 pickle）

| 类型 | 基类 | 说明 |
|------|------|------|
| `FrozenDict[K,V]` | `Mapping` | 递归不可变映射，`__reduce__` 支持 pickle |
| `FrozenList[V]` | `Sequence` | 不可变序列，`__eq__` 与普通序列比较 |
| `FrozenSet[V]` | `frozenset` | 记录原始可变 set |

辅助：
- `_freeze(v)`：Mapping→FrozenDict、list/tuple→tuple、set→FrozenSet、frozenset→frozenset，逐层递归。
- `_frozen_mapping(v)`：`FrozenDict(v)` 构造入口。

#### 3.1.3 校验函数

- `_validate_resource_number(name, value)`：`None` 或有限非负实数（拒绝 bool、`nan`、负数）。
- `_validate_non_negative_int(name, value)`：`None` 或非负整数（拒绝 bool、float）。
- `_validate_positive_int(name, value)`：正整数。
- `_validate_on_error(value)`：`None | "raise" | "log" | "ignore"`。

#### 3.1.4 数据类

**`Resources`（`@dataclass(frozen=True)`）**

| 字段 | 类型 |
|------|------|
| `cpus` | `float \| None` |
| `gpus` | `float \| None` |
| `memory_bytes` | `int \| None` |

**`EntrypointDefinition`（`@dataclass(frozen=True)`）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `original` | `Callable` | 用户原始函数 |
| `kind` | `Literal["row","batch"]` | 入口类型 |
| `return_dtype` | `pa.DataType \| None` | batch 必填 |
| `resources` | `Resources` | 资源需求 |
| `unnest` | `bool` | struct 输出展开为多列 |
| `batch_size` | `int \| None` | 仅 batch |
| `max_retries` | `int \| None` | 非负整数 |
| `on_error` | `"raise"/"log"/"ignore" \| None` | 错误策略 |
| `use_process` | `bool \| None` | 进程隔离 |
| `backend_options` | `Mapping`（冻结） | 后端专属选项 |

`__post_init__` 校验：`return_dtype` 必须 `pa.DataType`；batch 必填；row 不得带 `batch_size`；`batch_size` 正整数；`max_retries` 非负；`on_error` 合法；冻结 `backend_options`。

**`OperatorDefinition`（`@dataclass(frozen=True)`）**

| 字段 | 类型 | 说明 |
|------|------|------|
| `original` | `type` | 用户原始类 |
| `entrypoints` | `Mapping[str, EntrypointDefinition]`（冻结） | 至少一个 |
| `resources` | `Resources` | 实例级资源 |
| `max_concurrency` | `int \| None` | 正整数 |
| `max_retries` | `int \| None` | 非负整数 |
| `on_error` | `ErrorPolicy \| None` | |
| `use_process` | `bool \| None` | |
| `name` | `str \| None` | 覆盖算子名 |
| `backend_options` | `Mapping`（冻结） | |

### 3.2 装饰器层 `_decorators.py`

#### 3.2.1 `FunctionFacade`

- 字段：`definition: EntrypointDefinition`、`_compiled`（懒缓存）、`_compile_lock: Lock`。
- 构造：`functools.update_wrapper(self, original)` + 手动 `__signature__`。
- `_compile()`：双重检查锁定，首次调用 `compile_function(definition)`（此处才 import daft）。
- `__call__(*a, **k)` → `_compile()(*a, **k)`。
- 序列化：`__getstate__` 丢弃锁与 `_compiled`；`__setstate__` 重建锁。

#### 3.2.2 `OperatorFacade`

- 类似懒编译，`_compile()` 调 `compile_operator(definition)` 得到适配类。
- `__call__` 实例化适配类并返回 `OperatorInstanceFacade(definition, backend_instance)`。

#### 3.2.3 `OperatorInstanceFacade`

- `__getattr__(name)`：仅当 `name in definition.entrypoints` 时转发到底层实例，否则 `AttributeError`。
- `__call__`：仅当 `"__call__"` 在入口中时转发。

#### 3.2.4 装饰器工厂

- `_entrypoint(kind, fn, *, return_dtype, resources, unnest, batch_size, max_retries, on_error, use_process, backend_options)` → `FunctionFacade(EntrypointDefinition(...))`。
- `@row`：`_entrypoint("row", ...)`；入参不含 `batch_size`（batch_size 仅 batch 才有意义）。
- `@batch`：`_entrypoint("batch", ...)`；含 `batch_size`。
- `@operator`：`vars(original)` 里 `isinstance(attr, FunctionFacade)` 的成员构成 `entrypoints`。

> `row` 与 `operator` 提供 `@overload` 支持裸用 / 带参两种签名。

### 3.3 Arrow 边界层 `_arrow.py`

| 函数 | 职责 |
|------|------|
| `to_arrow_argument(value)` | `pa.Array` 直通；有 `to_arrow()` 则调用并强制返回 `pa.Array`；标量直通 |
| `_types_compatible(expected, actual)` | 相等 OR （string↔large_string）OR （binary↔large_binary） |
| `validate_batch_output(output, definition, expected_length)` | 类型为 `pa.Array` → dtype 兼容 → 长度 == expected，返回原数组 |

### 3.4 后端层 `backends/daft.py`

#### 3.4.1 常量

- `_ENTRYPOINT_OPTION_KEYS`：`{return_dtype, unnest, cpus, gpus, memory_bytes, batch_size, max_retries, on_error, use_process}`。
- `_OPERATOR_OPTION_KEYS`：`{cpus, gpus, memory_bytes, use_process, max_concurrency, max_retries, on_error, name_override}`。

#### 3.4.2 函数清单

| 函数 | 说明 |
|------|------|
| `_arrow_return_dtype(dtype)` | `daft.DataType.from_arrow_type(dtype)`，None 直通 |
| `_namespaced_options(options)` | 取 `options["daft"]`，校验为 Mapping，返回 `_thaw` 后的 dict |
| `_thaw(value)` | `_freeze` 的逆：FrozenList→list、FrozenSet→set、tuple/frozenset/递归 |
| `_merge_backend_options(options, backend_options, reserved)` | 合并，冲突抛错 |
| `_supported_options(decorator, options, context)` | `inspect.signature` 校验可表达性 |
| `_entrypoint_options(def)` | 组装 row/batch 装饰器选项 |
| `_operator_options(def)` | 组装 `daft.cls` 选项 |
| `_convert_batch_arguments(args, kwargs)` | 转 `pa.Array`、保证至少一个数组、等长，返回期望长度 |
| `_batch_bridge(def)` | 同步/异步包装：转参→调原函数→校验输出 |
| `compile_function(def)` | batch→`daft.func.batch`；row→`daft.func` |
| `_compile_method(def)` | batch→`daft.method.batch`；row→`daft.method` |
| `compile_operator(def)` | 动态子类 + 编译方法 + `daft.cls` |

#### 3.4.3 选项组装流程

```
definition 字段 ──► _entrypoint_options / _operator_options
     │  只放入非 None 值
     ▼
 _merge_backend_options  ← backend_options["daft"]（冲突检查）
     ▼
 _supported_options(目标装饰器 signature，context)
     ▼
 传给 daft.func.* / daft.method.* / daft.cls
```

#### 3.4.4 有状态算子编译

```python
namespace = {"__module__": ..., "__doc__": ..., "__mmops_original__": original}
namespace.update({name: _compile_method(ep) for name, ep in entrypoints.items()})
adapter_class = type(original.__name__, (original,), namespace)
return daft.cls(adapter_class, **options)
```

关键：适配类**继承原类**，使 `__init__` 与私有 helper 原样可用；仅入口方法被替换为 Daft 编译版本。

### 3.5 算子目录 `operators/`

#### 3.5.1 `text/normalize.py`

- 常量 `_NORMALIZATION_FORMS`。
- `normalize_text`：`@batch(return_dtype=pa.string(), batch_size=1024)`。
- 管道：`utf8_normalize → utf8_lower? → 合并空白 regex? → trim?`；纯 Arrow `pc.*` 内核。

#### 3.5.2 `text/keywords.py`

- 常量 `KEYWORD_MATCH_DTYPE = pa.list_(pa.string())`。
- `@operator class KeywordMatcher`：
  - 构造：校验/去重/编译 `re.Pattern`（`re.escape` + 可选整词边界 + 可选 `IGNORECASE`）→ `_patterns: tuple[(keyword, pattern)]`。
  - `_match_text(text)`：None→None；否则返回命中关键字列表（配置序、去重）。
  - `match`：`@row`，Python 标量。
  - `match_batch`：`@batch`，`pa.Array` 进、逐行 `as_py()` 后统一走 `_match_text`，`pa.array(...)` 出。

---

## 4. 关键数据结构总览

| 结构 | 类型 | 用途 |
|------|------|------|
| 入口类型 | `EntrypointKind = Literal["row","batch"]` | 区分标量/批量 |
| 错误策略 | `ErrorPolicy = Literal["raise","log","ignore"]` | 后端错误处理 |
| 资源 | `Resources(cpus,gpus,memory_bytes)` | 每算子实例资源 |
| 入口契约 | `EntrypointDefinition` | 单入口完整描述 |
| 算子契约 | `OperatorDefinition` | 有状态类完整描述 |
| 门面 | `FunctionFacade`/`OperatorFacade`/`OperatorInstanceFacade` | 懒编译与属性收敛 |

---

## 5. 关键时序

### 5.1 独立函数首次调用（懒编译）

```
normalize_text(df["text"])                     # FunctionFacade.__call__
  → _compile()
      → from .backends.daft import compile_function   # 首次 import daft
      → compile_function(definition)
          → _entrypoint_options → _supported_options → daft.func.batch(**opts)(_batch_bridge)
  → _compiled(...) → Daft 惰性建图
  → Daft 执行：_batch_bridge → to_arrow_argument → original(pa.Array) → validate_batch_output
```

### 5.2 有状态算子实例化与调用

```
KeywordMatcher([...])                          # OperatorFacade.__call__
  → _compile() → compile_operator(definition)
      → type(...) 子类 + daft.cls(..., **opts)   # 得到 Daft 类
  → OperatorInstanceFacade(definition, backend_instance)

matcher.match_batch(col)                       # OperatorInstanceFacade.__getattr__
  → backend_instance.match_batch(col)          # daft.method.batch 产物
```

### 5.3 序列化（分布式)

```
pickle.dumps(Facade/Definition/句柄)
  → __getstate__ 去锁、置 _compiled=None
  → 深不可变 (Frozen*) 结构随 pickle 传输
  → __setstate__ 重建 Lock，下次调用重新编译
```

---

## 6. 扩展点设计

| 扩展目标 | 扩展方式 | 无需改动 |
|----------|----------|----------|
| 新增算子 | 在 `operators/<模态>/` 写 `@row`/`@batch`/`@operator` 函数或类，并在相应 `__init__.py` 导出 | 定义层/后端层 |
| 新增模态 | 创建 `operators/<mod>/__init__.py`；在 `pyproject.toml` 增加对应 extra 并同步 `all` | 后端层 |
| 新增执行引擎 | 在 `backends/` 新增模块，实现 `compile_function`/`compile_operator` 等入口 | 定义层、装饰器层、算子目录 |
| 传递后端专属选项 | 使用 `backend_options={"daft": {...}}`，保留键不可覆盖 | — |
| 资源/重试/错误策略 | 通过各装饰器 `resources`/`max_retries`/`on_error` 参数 | — |

---

## 7. 契约与规范（编码时必须遵守）

1. **batch 必填 `return_dtype`**，且返回必须是 `pa.Array`；类型必须兼容（string/large_string、binary/large_binary 视为兼容）；长度必须 1:1 匹配输入。
2. **row 单行标量**；可选 `return_dtype` 用于显式指定 Arrow 输出类型。
3. **运算子的重对象在 `__init__` 初始化**（如编译正则），不得在模块导入时初始化 model/client。
4. **仅暴露显式入口**：`OperatorInstanceFacade` 会隐藏未用 `@row`/`@batch` 标记的方法。
5. **选项显式**：显式请求的选项若后端不支持，必须抛 `DaftCompatibilityError`，不得静默丢弃；`backend_options["daft"]` 不得覆盖保留键。
6. **可序列化**：定义与门面必须 pickle 安全（自定义 `__getstate__`/`__setstate__`、`Frozen*` 容器）。
7. **基础导入零依赖**：`import mmops` 不得触发 `import daft`。
8. **公共 API 优先**：接口（资源、返回类型、批量边界）在 Arrow 层校验，业务代码只面对 `pyarrow` 与 Python 标量。

---

## 8. 模块依赖图

```
operators/text/normalize.py ──► _decorators.batch
operators/text/keywords.py ──► _decorators.{operator,row,batch}

_decorators ──► _definitions (EntrypointDefinition/OperatorDefinition/Resources)
_decorators ──► (懒) backends.daft.{compile_function,compile_operator}

backends/daft ──► _arrow (to_arrow_argument, validate_batch_output)
backends/daft ──► _definitions (DaftCompatibilityError, FrozenList, FrozenSet, ...)

_arrow ──► _definitions (BatchOutputError, EntrypointDefinition)
```

依赖约束：`_definitions` 与 `_arrow` 不依赖 `daft`；`daft` 仅在 `backends/daft.py` 的函数体内 `import`。