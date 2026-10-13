# Multimodal Operator Development Guide 多模态算子开发指南

> Bilingual edition of [operator-development.md](operator-development.md): the
> English original comes first, followed by its Chinese translation. If the
> two differ, the English original prevails.
>
> 本文是 [operator-development.md](operator-development.md) 的中英对照版：英文
> 原文在前，中文翻译在后；如两者不一致，以英文原文为准。

## Purpose 目的

This guide defines how to develop and review operators in `daft-mmops`.
Operators currently run on Daft and turn input records into enriched 1:1
records for multimodal preprocessing, inference, and annotation.

本指南定义如何在 `daft-mmops` 中开发与评审算子。算子当前运行在 Daft 上，将
输入记录转换为富化后的 1:1 记录，用于多模态预处理、推理与标注。

For installation and user-facing examples, see [README.md](../README.md).
Automated contributors must also follow [AGENTS.md](../AGENTS.md).

安装与面向用户的示例见 [README.zh-CN.md](../README.zh-CN.md)。自动化贡献者
还须遵循 [AGENTS.zh-CN.md](../AGENTS.zh-CN.md)。

## Choose an execution shape 选择执行形态

| Workload | Recommended API |
|---|---|
| Lightweight independent transform | standalone `@mmops.row` |
| Arrow-vectorized transform | standalone `@mmops.batch` |
| Reusable model, tokenizer, codec, or client | `@mmops.operator` class |
| Low-latency and high-throughput forms of one model | row and batch methods on one class |
| Network-bound annotation | async row or batch method with bounded concurrency |

| 工作负载 | 推荐 API |
|---|---|
| 轻量级独立变换 | 独立 `@mmops.row` |
| Arrow 向量化变换 | 独立 `@mmops.batch` |
| 可复用的模型、分词器、编解码器或客户端 | `@mmops.operator` 类 |
| 同一模型的低延迟与高吞吐两种形态 | 同一个类上的 row 与 batch 方法 |
| 受网络限制的标注 | 具有有界并发的异步 row 或 batch 方法 |

Use `def` or `async def` to express synchronous or asynchronous behavior. Row
or batch grain belongs on each method, not on `@mmops.operator`; one class may
therefore expose several row and batch entrypoints.

用 `def` 或 `async def` 表达同步或异步行为。行或批的粒度属于每个方法本身，
而不属于 `@mmops.operator`；因此一个类可以同时暴露多个 row 与 batch 入口。

Methods without `@mmops.row` or `@mmops.batch` are internal helpers and are
not exposed to Daft.

未标注 `@mmops.row` 或 `@mmops.batch` 的方法是内部辅助方法，不会暴露给 Daft。

## Public API conventions 公共 API 约定

- Do not declare an input schema on the decorator. The Python signature and
  call site bind column expressions and literal arguments.

- 不要在装饰器上声明输入 schema。Python 签名与调用点负责绑定列表达式与
  字面量参数。

- Row `return_dtype` is optional when Daft can infer it from the return type.

- 当 Daft 能从返回类型推断时，row 的 `return_dtype` 是可选的。

- Batch `return_dtype` is required and must be a `pyarrow.DataType`.

- batch 的 `return_dtype` 是必需的，且必须是 `pyarrow.DataType`。

- Use `return_dtype`, `batch_size`, `max_retries`, and `on_error` names.

- 使用 `return_dtype`、`batch_size`、`max_retries`、`on_error` 这些参数名。

- Use `Resources(cpus=..., gpus=..., memory_bytes=...)` for resource requests.

- 资源请求使用 `Resources(cpus=..., gpus=..., memory_bytes=...)`。

- Put Daft-only escape hatches under `backend_options["daft"]`.

- Daft 专属的逃生舱放在 `backend_options["daft"]` 之下。

### Row template 行级模板

```python
import mmops


@mmops.row
def normalize(text: str) -> str:
    return text.strip().lower()
```

Add an explicit Arrow dtype when the Python annotation cannot express the
required precision, fixed size, nullability, or struct layout:

当 Python 注解无法表达所需的精度、固定长度、可空性或结构体布局时，显式添加
Arrow dtype：

```python
import pyarrow as pa


@mmops.row(return_dtype=pa.list_(pa.float32()))
def embed_one(text: str) -> list[float]:
    return model.embed(text)
```

### Batch template 批处理模板

```python
import pyarrow as pa
import pyarrow.compute as pc

import mmops


@mmops.batch(return_dtype=pa.int64(), batch_size=256)
def text_length(text: pa.Array) -> pa.Array:
    return pc.cast(pc.utf8_length(text), pa.int64())
```

Batch parameters are one or more `pa.Array` values plus ordinary literals.
Batch output must be exactly one `pa.Array` with the declared dtype.

batch 参数是一个或多个 `pa.Array` 值加上普通字面量。batch 输出必须恰好是一个
具有所声明 dtype 的 `pa.Array`。

### Stateful template 有状态模板

```python
import pyarrow as pa

import mmops


@mmops.operator(
    resources=mmops.Resources(gpus=1),
    max_concurrency=4,
)
class Embedder:
    def __init__(self, model_path: str):
        self.model = load_model(model_path)

    @mmops.row(return_dtype=pa.list_(pa.float32()))
    async def embed_one(self, text: str) -> list[float]:
        return await self.model.embed(text)

    @mmops.batch(
        return_dtype=pa.list_(pa.float32()),
        batch_size=64,
    )
    async def embed_batch(self, text: pa.Array) -> pa.Array:
        return await self.model.embed_batch(text)
```

Initialize models and clients in `__init__`. Daft lazily creates and reuses
the instance in a worker/actor. Constructor validation and initialization
errors therefore appear when an entrypoint is first invoked, not when the
client-side operator handle is created.

在 `__init__` 中初始化模型与客户端。Daft 会在 worker/actor 中惰性创建并复用
实例。因此构造器校验与初始化错误会在首次调用 entrypoint 时出现，而不是在
客户端侧创建算子句柄时出现。

## Batch data contract 批处理数据契约

Version 1 has strict 1:1 cardinality: every input row produces one output row.

版本 1 具有严格 1:1 基数：每一条输入行产生一条输出行。

Batch entrypoints must:

batch 入口必须：

- accept `pa.Array`, not `daft.Series`;

- 接受 `pa.Array`，而不是 `daft.Series`；

- return `pa.Array` or its `pa.StructArray` subtype;

- 返回 `pa.Array` 或其子类型 `pa.StructArray`；

- return the same number of rows as the input arrays;

- 返回与输入数组相同的行数；

- return the exact declared dtype;

- 返回与声明完全一致的 dtype；

- preserve documented null semantics;

- 保持已文档化的 null 语义；

- handle empty and final short batches;

- 处理空批次与末尾的短批次；

- avoid whole-batch `to_pylist()` conversion when Arrow kernels or buffer
  interfaces can perform the operation.

- 在 Arrow 内核或缓冲区接口能够完成操作时，避免整批 `to_pylist()` 转换。

Do not return `pa.Table`, `pa.RecordBatch`, `pa.ChunkedArray`, Pandas objects,
NumPy arrays, Python lists, or `daft.Series`. If a model or external service
requires Python objects, document the conversion and its expected cost.

不要返回 `pa.Table`、`pa.RecordBatch`、`pa.ChunkedArray`、Pandas 对象、
NumPy 数组、Python 列表或 `daft.Series`。如果模型或外部服务需要 Python 对象，
请将转换及其预期开销写入文档。

The framework treats top-level `string`/`large_string` and
`binary`/`large_binary` as logically compatible because Daft may normalize
Arrow offset widths. Nested types and all other dtypes must match exactly.

框架将顶层 `string`/`large_string` 与 `binary`/`large_binary` 视为逻辑兼容，
因为 Daft 可能会规范化 Arrow 偏移宽度。嵌套类型及所有其他 dtype 必须精确匹配。

### Multiple output columns 多输出列

One UDF invocation returns one logical Arrow array. Use a struct dtype and
`unnest=True` for several named output columns:

一次 UDF 调用返回一个逻辑 Arrow 数组。要产生多个命名的输出列，请使用结构体
dtype 与 `unnest=True`：

```python
result_type = pa.struct(
    [
        pa.field("embedding", pa.list_(pa.float32())),
        pa.field("score", pa.float32()),
    ]
)


@mmops.batch(return_dtype=result_type, unnest=True)
def analyze(text: pa.Array) -> pa.StructArray:
    embeddings, scores = run_model(text)
    return pa.StructArray.from_arrays(
        [embeddings, scores],
        fields=list(result_type),
    )
```

Treat field names and types as public API. Names must describe the computed
value accurately.

将字段名与类型视为公共 API。字段名必须准确描述其计算值。

## Row and batch consistency 行与批一致性

When the same capability has row and batch entrypoints, align:

当同一能力同时具有 row 与 batch 入口时，需要对齐：

- output dtype and field names;

- 输出 dtype 与字段名；

- null and empty-input behavior;

- null 与空输入行为；

- error behavior;

- 错误行为；

- literal option defaults;

- 字面量选项默认值；

- ordering guarantees.

- 顺序保证。

Do not rely on global row order for asynchronous Daft execution unless the
operator explicitly guarantees it.

除非算子显式保证，否则不要依赖异步 Daft 执行的全局行顺序。

## State semantics 状态语义

Operator state is local to one Daft worker/actor instance. It is suitable for
expensive initialization and connection reuse, not for:

算子状态局限于单个 Daft worker/actor 实例。它适合昂贵的初始化与连接复用，
不适合：

- cross-worker shared state;

- 跨 worker 的共享状态；

- keyed routing or ordering;

- 按键路由或排序；

- checkpoint/restore;

- 检查点/恢复；

- exactly-once processing;

- 精确一次（exactly-once）处理；

- a guaranteed teardown callback.

- 有保证的析构回调。

Keep credentials, access tokens, and environment-specific endpoints out of
source code and serialized operator metadata.

将凭证、访问令牌与环境特定的端点排除在源代码和序列化的算子元数据之外。

## Daft integration boundary Daft 集成边界

Operator business code uses Python scalars and PyArrow arrays. Daft
integration is implemented centrally in `src/mmops/backends/daft.py` with
public APIs only:

算子业务代码使用 Python 标量与 PyArrow 数组。Daft 集成集中在
`src/mmops/backends/daft.py` 中实现，且只使用公共 API：

- `daft.func` and `daft.func.batch`;

- `daft.func` 与 `daft.func.batch`；

- `daft.cls`;

- `daft.cls`；

- `daft.method` and `daft.method.batch`.

- `daft.method` 与 `daft.method.batch`。

Do not use private APIs such as `Func._from_func`, `mark_cls_method`, or
`daft.udf.udf_v2` internals. Never silently discard an explicitly requested
execution option; raise `DaftCompatibilityError` when the installed Daft
version cannot represent it.

不要使用私有 API，例如 `Func._from_func`、`mark_cls_method` 或
`daft.udf.udf_v2` 内部实现。绝不静默丢弃显式请求的执行选项；当已安装的
Daft 版本无法表示某个选项时，抛出 `DaftCompatibilityError`。

Definitions and constructed operator handles must remain pickleable across
Daft worker boundaries without serializing locks or client-side caches.

定义与已构造的算子句柄必须保持可 pickle，能够跨越 Daft worker 边界，且不
序列化锁或客户端侧缓存。

The current release executes only through Daft. Keep execution bindings
isolated internally so another engine can be evaluated later, but do not
expose or document an unimplemented engine as supported.

当前版本仅通过 Daft 执行。在内部保持执行绑定隔离，以便未来评估其他引擎，
但不要把未实现的引擎暴露或记录为已支持。

## Package and dependency organization 包与依赖组织

Reusable operators live under `src/mmops/operators/`. As the catalog grows,
group cohesive capabilities by domain:

可复用算子位于 `src/mmops/operators/` 之下。随着目录增长，按领域组织内聚的
能力：

```text
src/mmops/operators/
├── text/
├── image/
├── audio/
├── video/
├── document/
└── multimodal/
```

Do not add empty directory scaffolding. Create a domain package when real
operators form a useful group.

不要添加空目录脚手架。当真实算子构成一个有用的组时，再创建领域包。

Keep the core package lightweight. Heavy model, media, codec, or service SDKs
belong in optional extras and should be imported only by the relevant domain
module or execution path. Users must not need to rebuild Daft or edit
installed Daft source code.

保持核心包轻量。重型模型、媒体、编解码器或服务 SDK 应放在可选 extras 中，
并且只由相关领域模块或执行路径导入。用户不应需要重新构建 Daft 或修改已安装
的 Daft 源代码。

Each planned modality has a same-named extra in `pyproject.toml`. Extras for
modalities without production operators remain empty and are documented as
reserved, not supported. When the first operator is released, add the Daft
runtime and any modality dependencies to that extra. The `all` extra must
equal the flattened dependency union of `text`, `image`, `audio`, `video`,
`document`, and `multimodal`; PEP 621 extras cannot reference one another by
name. Update the modality extra, `all`, README installation table, and
packaging tests in the same change as a new operator dependency.

每个规划中的模态在 `pyproject.toml` 中都有一个同名 extra。尚无生产算子的
模态 extras 保持为空，并被记录为“预留”而非“已支持”。当首个算子发布时，将
Daft 运行时及该模态的依赖加入对应 extra。`all` extra 必须等于 `text`、
`image`、`audio`、`video`、`document` 与 `multimodal` 的扁平依赖并集；PEP 621
的 extras 不能按名称相互引用。在新增算子依赖的同一变更中，同步更新模态
extra、`all`、README 安装表与打包测试。

Each production operator must document:

每个生产算子必须记录：

- accepted input representation;

- 接受的输入表示；

- output dtype and field meanings;

- 输出 dtype 与字段含义；

- null and failure behavior;

- null 与失败行为；

- optional dependencies, model, or service requirements;

- 可选依赖、模型或服务要求；

- CPU, GPU, memory, batch-size, and concurrency expectations;

- CPU、GPU、内存、batch size 与并发预期；

- runtime credentials or configuration without exposing their values.

- 运行时凭证或配置（不暴露其值）。

## Testing requirements 测试要求

Every new operator needs:

每个新算子需要：

- eager Python or Arrow tests;

- eager 的 Python 或 Arrow 测试；

- a real Daft DataFrame integration test;

- 真实 Daft DataFrame 集成测试；

- null behavior coverage;

- null 行为覆盖；

- empty input coverage when applicable;

- 适用时的空输入覆盖；

- final-short-batch coverage when `batch_size` is set;

- 设置了 `batch_size` 时对末尾短批次的覆盖；

- output dtype and field-name assertions;

- 输出 dtype 与字段名断言；

- row/batch consistency coverage when both forms exist;

- 同时存在两种形态时的 row/batch 一致性覆盖；

- error-path coverage for the public contract.

- 公共契约的错误路径覆盖。

Run the repository verification commands listed in
[AGENTS.md](../AGENTS.md#tests-and-verification) before completion.

完成之前，运行
[AGENTS.zh-CN.md](../AGENTS.zh-CN.md#tests-and-verification-测试与验证)中列出
的仓库验证命令。

## Review checklist 评审清单

- The operator solves a reusable multimodal processing or annotation task.

- 算子解决的是可复用的多模态处理或标注任务。

- Its public name and output fields accurately describe its behavior.

- 其公共名称与输出字段准确描述其行为。

- Row and batch signatures follow the scalar/Arrow contract.

- row 与 batch 签名遵循标量/Arrow 契约。

- Batch output has the declared dtype and preserves 1:1 cardinality.

- batch 输出具有声明的 dtype 并保持 1:1 基数。

- Null, empty, error, and ordering semantics are explicit and tested.

- null、空、错误与顺序语义是显式的并经过测试。

- Expensive resources initialize in `__init__`, not at module import.

- 昂贵资源在 `__init__` 中初始化，而不是在模块导入时。

- Optional dependencies do not affect unrelated imports.

- 可选依赖不影响无关导入。

- Secrets are supplied at runtime and are not logged or serialized.

- 凭证在运行时提供，不被记录日志或序列化。

- Resource and concurrency expectations are documented.

- 资源与并发预期已写入文档。

- Public exports, README/operator docs, and tests change together.

- 公共导出、README/算子文档与测试一起变更。
