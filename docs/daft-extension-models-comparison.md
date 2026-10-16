# Daft 官方扩展模型对比：Python UDF 扩展 vs Native ABI 扩展

> 调研日期：2026-10-13（数据以当日官方文档与博客为准）
> 方法：官方文档（P0）+ 官方博客（P1）交叉验证；每条结论附引用标记 `[cite:N]`；无法验证处显式标注 `[未验证]` 或 `[推断]`。

---

## 1. 执行摘要

Daft 自 2026 年 5 月起正式提供两条扩展路线，并配套社区扩展注册表 [cite:1][cite:4]：

| | Python UDF 扩展 | Native ABI 扩展 |
|---|---|---|
| 一句话定位 | 用 Daft 现有 UDF API 打包的纯 Python 库 | 用 Rust/C++ 写的、经 Arrow C Data Interface 注入引擎的共享库 |
| 成熟度 | 稳定（基于长期公开的 UDF API） | **实验性**（官方标注 may change）[cite:3] |
| 性能 | 常规（热路径含 Python） | 官方实测较 Python UDF 快 **3–16x**（daft-h3）[cite:4] |
| 当前能力边界 | 函数/批函数/有状态类/异步全覆盖 | **仅标量函数**，聚合、有状态、异步均未支持 [cite:3] |
| 典型代表 | daft-lance、daft-qdrant、daft 自家 `daft.functions.ai` [cite:1][cite:2] | daft-h3、daft-html、daft-geo [cite:2] |

针对本仓库（daft-mmops）的三条结论：

1. mmops 当前形态**已经就是**官方 Python UDF 扩展模型，无需任何架构改动 [cite:1]；
2. 现有算子中仅 `KeywordMatcher.match_batch`（逐行 Python 循环）符合 Native ABI 的下沉收益模型，`normalize_text`（PyArrow C++ kernel 编排）不值得下沉；
3. Native ABI 当前能力边界（无状态、无执行选项、无异步）与 mmops 的算子契约（有状态配置、batch_size/资源/重试选项、async 支持）存在**真实功能缺口**，短期不宜切换。

---

## 2. 背景：官方 Extensions 生态

- 官方文档设有专门章节：Overview / Community Extensions 注册表 / Authoring Guide [cite:1][cite:2][cite:3]。
- 2026-05-04 官方博客正式宣布该模式，GitHub Discussion #6852 为官方公告 [cite:4][cite:5]。
- 治理模式为 **"You Build It, You Own It"**：扩展在各自仓库独立维护，PyPI 命名遵循 `daft-<domain>` 约定，通过 PR 进入官方注册表；采用度高、维护活跃的项目可迁入 `daft-engine` 组织 [cite:4]。
- 注册表现有 5 个扩展：daft-h3（Native）、daft-html（Native）、daft-geo（Native，含扩展数据类型）、daft-lance（Python UDF）、daft-qdrant（Python UDF）[cite:2]。

---

## 3. 两种模型的定义

### 3.1 Python UDF 扩展

官方定义：**基于 Daft 自定义代码 API（`@daft.func`、`@daft.func.batch`、`@daft.cls`、`@daft.method.batch`）构建的 Python 包**，把 UDF 藏在表达式 API 背后，面向 AI/媒体/多模态处理场景 [cite:1]。用户安装后直接 `import` 扩展包并调用，无需向 Daft 注册任何东西 [cite:1]。

### 3.2 Native ABI 扩展

官方定义：以 **Arrow C Data Interface**（`ArrowSchema`/`ArrowArray` 纯 C 结构体）为 ABI 边界的原生共享库，用 Rust（`daft-ext` crate）或 C++（raw ABI）编写，在运行时经 `dlopen` 载入 Daft 引擎 [cite:3]。ABI 与 Daft 内部使用的 arrow-rs 版本**解耦**（`arrow-56/57/58` feature flag + `from_raw/as_raw` 逃生舱）[cite:3]。

---

## 4. 核心对比矩阵

图例：✅ 支持｜⚠️ 部分/有条件｜❌ 不支持｜🔜 官方在做但未发布

| 维度 | Python UDF 扩展 | Native ABI 扩展 |
|---|---|---|
| 开发语言 | Python | Rust（推荐）/ C++ |
| 标量函数 | ✅ `@daft.func` | ✅ `DaftScalarFunction` |
| 批函数（向量化） | ✅ `@daft.func.batch` | ✅（标量函数本身按列数组运算）[cite:3] |
| 返回嵌套类型（List/Struct） | ✅ | ⚠️ 教程仅示例 Utf8 返回；List 返回未见官方示例 [未验证] |
| 有状态类（worker 侧 `__init__`） | ✅ `@daft.cls` | ❌ 无等价机制 |
| 异步函数 | ✅（协程 UDF） | ❌ |
| 聚合函数 | ❌（Python 侧也无） | ❌（"actively working on additional features"）[cite:3] |
| 扩展数据类型 | ❌ | ✅（daft-geo 注册 geo 类型）[cite:2] |
| 执行选项（batch_size/cpus/gpus/memory_bytes/max_retries/on_error/use_process） | ✅ 装饰器参数 | ❌ 引擎默认调度，无逐函数选项 [cite:3] |
| 性能（热路径） | 含 Python 开销 | **零 Python 开销**（"All data flows through Arrow arrays — no per-row Python overhead"）[cite:3] |
| 实测提升 | 基线 | 3–16x（daft-h3 vs h3-py batch UDF）[cite:4] |
| 打包 | 纯 Python wheel（pyproject 即可） | setuptools-rust + cdylib，按平台出 wheel [cite:3] |
| 加载方式 | `import 扩展包` | `import` + `daft.load_extension(pkg)` [cite:3] |
| 作用域 | 常规 Python 命名空间 | 函数名**会话内全局**，需前缀防冲突；`Session` 作用域隔离 [cite:3] |
| 版本稳定性 | 稳定 API | **实验性**，may change [cite:3] |
| 对 Arrow 实现的耦合 | pyarrow（用户代码侧） | 无耦合（C ABI）[cite:3] |
| 可被 SQL 直接调用 | ❌（需 Python 包装） | ✅（`daft.get_function` 仅是 Python 语法糖，SQL 可无 Python 解析）[cite:3] |

---

## 5. 逐维度详解

### 5.1 编程语言与技能栈

- **Python UDF**：只要会 Python 与 pyarrow 即可；mmops 的 `@batch` 契约（Arrow 进/Arrow 出）正好是这一模型的惯用形态。
- **Native ABI**：需要 Rust 工具链（官方教程基于 `cargo init --lib` + `uv`），理解 FFI 边界、null 位图、偏移类型陷阱（Daft 内部字符串是 `LargeUtf8`/i64 偏移，误用 `as_string::<i32>()` 会运行时 panic）[cite:3]。C++ 走 raw ABI 可行但无官方一手示例。

### 5.2 API 形态与表达能力

Python UDF 侧能力全面：顶层函数（daft-lance 的 `compact_files` 等）、DataFrame 方法（daft-qdrant 的 `df.write_qdrant`）、有状态类与异步均在官方生态内有实例 [cite:2]。

Native ABI 侧核心是两个 trait [cite:3]：

```rust
impl DaftExtension for MyExtension {
    fn install(session: &mut dyn DaftSession) {
        session.define_function(Arc::new(MyFn));   // 载入时注册一次
    }
}
impl DaftScalarFunction for MyFn {
    fn name(&self) -> &CStr { c"myext_myfn" }                 // 会话内全局名
    fn return_field(&self, args: &[ArrowSchema]) -> DaftResult<ArrowSchema>;  // 类型检查
    fn call(&self, args: Vec<ArrowData>) -> DaftResult<ArrowData>;            // 列式求值
}
```

关键限制：官方明确**"Today we support authoring native scalar functions"**，聚合、有状态类、异步均不在当前能力内 [cite:3]。

### 5.3 性能模型与开销路径

- Python UDF 的开销构成：每 batch 一次 Arrow→Python 边界穿越（`to_arrow()`/`pa.array` 重建），行级 UDF 还有逐行对象化；计算本体若是纯 Python 循环则进一步放大（mmops 的 `KeywordMatcher` 即如此：逐行 `as_py()` + Python `re`）。
- Native ABI 的收益来源：计算本体直接在 arrow-rs 数组上运行，热路径零 Python；daft-h3 实测对"把 h3-py 包成 batch UDF"的方案快 **3–16x** [cite:4]。
- 边界情形：若 Python UDF 本身只是 PyArrow compute kernel 的编排（如 mmops 的 `normalize_text` 全部走 `pc.utf8_normalize/utf8_lower/replace_substring_regex/utf8_trim_whitespace`），数据从不逐行过 Python，Native 化收益趋近于每 batch 一次函数调用的开销——**不值得下沉**。

### 5.4 打包与分发

| | Python UDF | Native ABI |
|---|---|---|
| 构建后端 | 任意（setuptools/hatchling 均可） | setuptools + setuptools-rust（强制）[cite:3] |
| 产物 | 纯 wheel，跨平台 | cdylib（`.so`/`.dylib`/`.dll`）嵌入包目录，**按 OS×架构 出平台 wheel** [cite:3] |
| 构建配置 | `pyproject.toml` | 另需 `setup.py`：`RustExtension("pkg.libpkg", binding=Binding::NoBinding, strip=True)`；crate-type=`["cdylib"]` [cite:3] |
| CI 复杂度 | 低 | 高（交叉编译/多平台 runner） |

### 5.5 加载、作用域与命名

- Python UDF：装完 `import` 即用；命名就是普通 Python 命名空间，无冲突问题。
- Native ABI：`import` 之后必须 `daft.load_extension(pkg)`；`dlopen` 每进程仅一次；**函数名在会话内全局**，官方建议 `myext_greet` 式前缀防冲突；函数只在载入了扩展的 `Session` 中可见，可用 `with sess:` 作用域隔离 [cite:3]。

### 5.6 版本兼容与稳定性

- Python UDF：依赖 daft 公共 API 的稳定面；升级风险与普通库一致。
- Native ABI：官方标注**实验性**，行为与 ABI 可能变化 [cite:3]；但 Arrow 边界经 C Data Interface 解耦，arrow-rs 版本不与 Daft 锁死（feature flag 或 raw 逃生舱）[cite:3]。

### 5.7 错误处理

- Native ABI 侧约定：`return_field` 中 schema 违规返回 `DaftError::TypeError`，`call` 中执行失败返回 `DaftError::RuntimeError` [cite:3]。
- Python UDF 侧异常直接沿 Python 语义传播（mmops 的 `BatchOutputError`/`DaftCompatibilityError` 均如此）。

### 5.8 测试

官方教程给出 Session 作用域的 pytest 范式（`Session()` + `sess.load_extension(hello)` + `with sess:`），并对 null 传播显式断言 [cite:3]。Python UDF 扩展测试即普通 Daft 测试（mmops 的 tests/ 目录即此形态）。

### 5.9 维护成本

Native ABI 长期成本显著更高：Rust 代码审查、平台 wheel 矩阵、上游 `daft-ext` API 演进（实验期）跟进、语义对齐测试（Python 参考实现 vs Rust kernel）。Python UDF 扩展的成本与普通 Python 包相当。

---

## 6. 社区扩展实例与选型逻辑

| 扩展 | 模型 | 形态 | 选型逻辑 |
|---|---|---|---|
| daft-h3 | Native | 9 个标量函数 [cite:4] | H3 索引计算是逐元素纯计算，Python 包装（h3-py）逐行开销大 → Native 收益 3–16x [cite:4] |
| daft-html | Native | 标量函数 | HTML 解析为逐元素 CPU 密集计算 |
| daft-geo | Native | 函数 + 扩展数据类型 [cite:2] | 需要引入新数据类型，仅 Native 路线支持 |
| daft-lance | Python UDF | 顶层维护函数（compact_files 等）[cite:2] | I/O 与表管理操作，无逐行计算热点；依赖 Lance Python 客户端 |
| daft-qdrant | Python UDF | DataFrame 方法（`df.write_qdrant`）[cite:2] | 外部系统写入，网络 I/O 主导；Python UDF 路线的 accessor 形态 |
| `daft.functions.ai`（官方内置） | Python UDF | `prompt`/`embed_text`/`classify_image` [cite:1] | 模型调用与异步 I/O，Native 路线无异步能力 |

规律清晰：**逐元素 CPU 密集计算 → Native；I/O 主导 / 需要状态 / 需要异步 → Python UDF**。

---

## 7. 选型决策准则

1. 热点是否为**逐元素/逐行的 Python 计算**？（是 → Native 候选；是 PyArrow kernel 编排或 I/O → 留在 Python UDF）
2. 是否需要**有状态初始化**（模型加载、编译正则、连接池）？（是 → 只能 Python UDF `@daft.cls`）
3. 是否需要**异步**（调用外部 API）？（是 → 只能 Python UDF）
4. 是否需要引入**新数据类型**或被 **SQL 直接调用**？（是 → 只能 Native）
5. 是否能承担 **Rust 技能栈 + 多平台 wheel + 实验期 API 演进**的维护成本？（否 → Python UDF）
6. 性能是否真的是当前瓶颈？（先用 profiling 验证，避免为不存在的热点下沉）

---

## 8. 对 mmops 的适配分析

### 8.1 当前定位

mmops 的 [backends/daft.py](../src/mmops/backends/daft.py) 使用的正是 `daft.func`/`daft.func.batch`/`daft.method`/`daft.method.batch`/`daft.cls`——**完全落在 Python UDF 扩展模型内** [cite:1]，`daft-mmops` 命名也符合 `daft-<domain>` 约定 [cite:4]。转正动作仅剩向注册表提 PR [cite:2]。

### 8.2 算子级适配

| 算子 | 下沉判定 | 依据 |
|---|---|---|
| `normalize_text` | ❌ 不下沉 | 全部逻辑为 PyArrow compute kernel 编排，无逐行 Python 热点（见 §5.3） |
| `KeywordMatcher.match_batch` | ✅ 唯一候选 | 逐行 `as_py()` + Python `re` 逐行搜索，是典型"逐元素 Python 计算"热点 |

`KeywordMatcher` 走 Native 的前置设计问题（均为推断，需 PoC 验证）：

- **有状态配置**：Native ABI 无 `@daft.cls` 等价物 [cite:3]；per-instance 的关键词配置需改为"注册期烘焙"（按配置生成具名 kernel）或 kernel 内按配置哈希缓存自动机 `[推断]`；
- **List 返回类型**：输出契约为 `List<Utf8>`，官方教程仅示例 Utf8 返回，List 返回是否受支持 `[未验证]`；
- **语义对齐**：`(?<!\w)...(?!\w)` 词边界、`re.IGNORECASE` 的 Unicode 语义、配置顺序输出与"至多一次"去重，需 conformance 测试钉死（对应 [native-kernel-design.md](native-kernel-design.md) G3）；
- **执行选项丢失**：Native 函数无 batch_size/cpus/max_retries 等选项 [cite:3]，mmops 的算子契约需降级或文档说明。

### 8.3 未来多模态算子的含义

image/audio/video 算子的解码层走 FFmpeg/OpenCV 等 C/C++ 库，属于"绑定既有原生库"而非"重写 Python 逻辑"；而涉及外部 API 调用的算子（如向模型服务发请求）依赖异步能力，Native 路线当前不支持 [cite:3]。混合策略（Python UDF 编排 + 局部 Rust kernel）是可预见的组合形态 `[推断]`。

---

## 9. 结论

1. 两条路线是**互补**而非替代：官方生态的实例分布（§6）显示社区按"计算热点类型"自然分流。
2. Python UDF 路线是**默认起点**：能力全覆盖、打包简单、API 稳定；本仓库已在此路线上。
3. Native ABI 是**针对性武器**：仅当 profiling 证实逐元素 Python 热点、且能接受实验期与原生维护成本时启用；daft-h3 的 3–16x 证明了上限，也划清了适用面（纯计算函数）。
4. 对 mmops：短期动作是注册表转正（零代码）；中期观察项是 `KeywordMatcher` 的 Native PoC 与官方对聚合/有状态/异步能力的路线图。

---

## 引用来源

1. [Daft Extensions — Overview（官方文档）](https://docs.daft.ai/en/stable/extensions/overview/)
2. [Community Extensions 注册表（官方文档）](https://docs.daft.ai/en/stable/extensions/community/)
3. [Daft Extensions — Authoring Guide（官方文档，Native ABI 完整教程）](https://docs.daft.ai/en/stable/extensions/)
4. [Daft Extensions Featuring daft-h3: Native Rust Performance, Community Owned（官方博客，2026-05-04）](https://www.daft.ai/blog/daft-extensions-ecosystem)
5. [Community Extensions 公告（GitHub Discussion #6852）](https://github.com/Eventual-Inc/Daft/discussions/6852)
6. [Daft 官网（Rust 内核、生产用户背景）](https://www.daft.ai/)
