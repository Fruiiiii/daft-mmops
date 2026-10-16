# Operator Detailed Design: file_sha256 文件哈希与一致性校验算子细化设计

状态：**设计稿（未实现）**。本文是 [video-module-design.md](../video-module-design.md) §3 算子目录中第二个细化到可实现程度的规格，模板与 [video-file-metadata.md](video-file-metadata.md) 一致；对外能力承诺以 [README.md](../../README.md) 为准。

## 1. 功能名称

`file_sha256`（函数，导出于 `mmops.operators.video`）。

## 2. 功能概述

流式读取文件全部字节，产出一行一个 struct 的 1:1 摘要列：SHA-256 十六进制摘要 + 文件字节数。

- **职责**：计算 `sha256` 与 `size_bytes` 两个**事实字段**；本算子只产出事实，不消费事实——传输错误检测与完全重复文件检测都是下游对事实的用法（见 §3.7）。
- **检测传输错误**：`size_bytes` 先行预筛（截断/多传文件秒级识别），`sha256` 精确判定——下游与源端 checksum 清单 join 对比即可，算子不内置"比对模式"参数。
- **完全重复文件**：SHA-256 相同即字节内容完全相同；下游按 `sha256` 分组（可复合 `size_bytes` 预筛）即可找出完全重复的副本，算子不输出"是否重复"这类关系型结论。
- **字段来源**：整文件字节流——与 [video_file_metadata.md](video-file-metadata.md) 的本质区别在于后者只读容器头（KB 级），本算子必须读全量（O(file size)），两算子组成"先元数据粗筛、再哈希核验"的成本阶梯。
- **开销特征**：O(file size) 顺序流式读，单 pass、无 seek；摘要计算走 OpenSSL（x86 SHA-NI / aarch64 SHA2 加密扩展均有硬件路径），通常仍受存储带宽而非 CPU 限制。
- **不做的事**：不校验容器结构与码流（`container_integrity`/`stream_integrity` 的职责）、不读像素、不缓存副本、不提供 md5/crc32 等其他摘要算法（保持零选项签名）。

## 3. 实现设计

### 3.1 模块位置与依赖

```text
src/mmops/operators/video/
├── __init__.py      # from .sha256 import FILE_SHA256_DTYPE, file_sha256
└── sha256.py        # 算子实现；仅标准库 hashlib，无第三方依赖
```

- **零新增依赖**：`hashlib` 为标准库（OpenSSL 后端），不进任何 extra，不影响 `all` 扁平并集与打包测试；这是目录首批 10 算子中唯一无新依赖者。
- 无状态函数 facade：无 `__init__` 状态，天然 pickleable。

### 3.2 执行形态与资源

- `@mmops.batch`，`batch_size=8`（每行一次文件打开；1 文件 1 分区下每批通常 1–2 行）。
- 内存 O(分块大小)（常数级，与文件大小无关），无 GPU，不设特殊 `Resources`。
- I/O 带宽型负载；v1 同步实现，Daft 分区级并行已足够（算子不自行并行）。

### 3.3 计算流程

```python
_CHUNK_SIZE = 1 << 20  # 1 MiB

@batch(return_dtype=FILE_SHA256_DTYPE, batch_size=8)
def file_sha256(uri: pa.Array) -> pa.StructArray:
    rows = [None if value is None else _digest(value.as_py()) for value in uri]
    return pa.array(rows, type=FILE_SHA256_DTYPE)


def _digest(uri: str) -> dict:
    h = hashlib.sha256()
    size = 0
    with _open_stream(uri) as fp:
        while chunk := fp.read(_CHUNK_SIZE):
            h.update(chunk)
            size += len(chunk)
    return {"sha256": h.hexdigest(), "size_bytes": size}
```

- 逐行循环是文件句柄边界所需（同 `KeywordMatcher.match_batch` 模式），不做整批额外物化。
- `_open_stream(uri)`：v1 承诺本地路径与 `http`/`https`（urllib 流式读，不依赖 Range）；对象存储凭证通道（S3/OSS 经 `IOConfig`）为后续增强，与 video_file_metadata §6 同节奏。
- `size` 累计实际读到的字节数，不取 `Content-Length`/`os.stat`——传输链路谎报长度时以字节事实为准。
- 单 pass 顺序读，不 seek、不重读；全程不解析内容。

### 3.4 null 语义

- 输入 URI 为 null → 输出整行 null（对齐 `KeywordMatcher` null 进 null 出先例）。
- 空文件是合法输入：`size_bytes = 0`，`sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`（SHA-256 空串标准向量），行非 null。

### 3.5 错误语义

- 文件不存在、读中断、协议错误：`_digest` 内**自然抛异常**，不吞错、不在 struct 里造 `ok`/`error` 字段（问题报告是 `container_integrity` 的职责）。
- 失败行处置交给框架既有 `on_error` 策略：`raise`（默认，分区失败可重试）/ `log`（记录后该行输出 null）/ `ignore`（该行输出 null）。算子代码对三种策略零感知。

### 3.6 性能与 IO 特征

- 顺序流式 1 MiB 分块：内存常数、对对象存储友好（单连接顺序 GET，无 Range 往返）。
- 摘要吞吐（硬件加速后 GB/s 级）通常高于单流存储吞吐，瓶颈在带宽；GPU 无用，CPU 单核即足。
- 无缓存：同一 URI 重复哈希由上游（manifest 物化）负责，算子保持纯函数。

### 3.7 定位与职责边界

- **事实与用法分离**：本算子输出 `(sha256, size_bytes)` 事实；"与源端对比找传输错误"、"按哈希分组找完全重复文件"均为下游 join/groupby 表达式，不进算子签名——保证零选项、可组合。
- **成本定位**：全量读是本算子的固有成本，治理流水线应置于 `video_file_metadata` 粗筛之后（video-module-design §4.5 两段式），避免为注定淘汰的文件付哈希成本。
- **同哈希即同内容**的适用范围：SHA-256 抗碰撞前提下成立；本算子不做"部分哈希/抽样哈希"等近似模式。

## 4. 接口设计

### 4.1 接口名称

`file_sha256`，`from mmops.operators.video import file_sha256`。

### 4.2 接口描述

对 URI string 列逐行流式计算 SHA-256 摘要与文件字节数，返回同长度的 `FILE_SHA256_DTYPE` struct 列；null URI 映射为 null 行，读取失败按 `on_error` 策略处置。

### 4.3 接口类型

| 项 | 值 |
| --- | --- |
| 定义形态 | 独立函数 `@mmops.batch`（无状态 facade） |
| `return_dtype` | `FILE_SHA256_DTYPE`（`pa.struct`，见 §4.5） |
| `unnest` | `False`（单 struct 列；下游可用嵌套访问 `df["digest"]["sha256"]` 触发谓词下推，或自行 unnest） |
| `batch_size` | `8` |
| `max_retries` | 调用方按需设置（建议 I/O 场景 `2`） |
| `on_error` | 默认 `raise`；批量治理场景可显式 `log` |
| `Resources` | 默认（无 GPU/内存特殊要求） |

### 4.4 input 参数

| 参数 | Arrow 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `uri` | `pa.Array`（`string`/`large_string` 逻辑兼容） | 是 | 文件 URI。v1 承诺本地路径与 `http`/`https`；对象存储凭证通道为后续增强。null → null 行 |

无可选参数：哈希计算不需要任何配置项，保持零选项签名。

### 4.5 output 参数

输出为单列 `pa.StructArray`，行数与输入严格相等（1:1），dtype 与下表逐字段一致（公共 API）：

```python
FILE_SHA256_DTYPE = pa.struct(
    [
        pa.field("sha256", pa.string()),     # 64 位小写十六进制摘要；空文件为标准空串向量
        pa.field("size_bytes", pa.int64()),  # 实际读取的字节数；空文件为 0
    ]
)
```

| 字段 | 类型 | 含义 | 可空 |
| --- | --- | --- | --- |
| `sha256` | string | 文件字节流的小写十六进制 SHA-256 摘要 | 否（行非 null 时恒有值） |
| `size_bytes` | int64 | 实际读取的字节数（非 stat/Content-Length 谎报值） | 否（行非 null 时恒有值） |

### 4.6 用法示例

```python
import daft
from mmops.operators.video import file_sha256

manifest = daft.read_parquet("s3://lake/videos/manifest")
hashed = manifest.with_column("digest", file_sha256(manifest["uri"]))

# 检测传输错误：size 先行预筛（截断文件秒级识别），sha256 精确判定
corrupted = hashed.where(
    (daft.col("digest")["size_bytes"] != daft.col("expected_size"))
    | (daft.col("digest")["sha256"] != daft.col("expected_sha256"))
)

# 完全重复文件：SHA-256 相同即字节内容完全相同
dup_counts = hashed.groupby(daft.col("digest")["sha256"]).count()
```

## 5. 测试计划

按 [operator-development.md](../operator-development.md#testing-requirements) 测试矩阵，fixture 用现场写入的真实文件（非 mock）：

1. **eager/Arrow**：NIST 标准向量（`"abc"` → `ba7816bf...`）、空文件（空串向量 + `size_bytes=0`）、跨 1 MiB 分块的大文件、恰好 1 MiB 与 ±1 字节边界文件；断言 dtype 逐字段一致、1:1 行数。
2. **真实现对拍**：`sha256` 与 `hashlib` 单次全读结果一致、`size_bytes` 与 `os.path.getsize` 一致（覆盖本地与 HTTP fixture）。
3. **null 与边界**：null URI → null 行；空批 → 空输出；末尾短批。
4. **错误路径**：不存在 URI / 读取中断在 `on_error` 三策略下的行为（raise 抛出 / log、ignore 出 null 行）。
5. **真 Daft 集成**：本地目录端到端——with_column + 嵌套字段谓词下推（按 `digest["sha256"]` 过滤）。
6. **打包**：断言**零新增依赖**——不装任何 extra 时 `from mmops.operators.video import file_sha256` 可用，extras 联动测试不受影响。

## 6. 边界与非目标

- 只产出 `(sha256, size_bytes)` 事实；比对、去重、报告逻辑均在下游，算子不内置任何模式参数。
- 不校验容器结构与码流完整性（`container_integrity`/`stream_integrity` 职责）；哈希一致不代表内容合法——损坏但完整传输的文件哈希同样匹配。
- 单一 SHA-256 定位：不提供 md5/crc32/blake2 等算法选项；如未来需要另立算子，不复用本签名。
- 全量读是固有成本：对象存储上等于一次完整下载，务必置于元数据粗筛之后；算子不做"抽样/部分哈希"近似模式。
- v1 不承诺对象存储 URI 的凭证通道（S3/OSS 经 `IOConfig` 为后续增强）；先落本地与 HTTP 场景。
- 输出 schema 是公共 API：增删字段需同步本规格、[video-module-design.md](../video-module-design.md) 与测试。
