# Operator Detailed Design: video_file_metadata 视频元数据算子细化设计

状态：**设计稿（未实现）**。本文是 [video-module-design.md](../video-module-design.md) §3 算子目录中第一个细化到可实现程度的规格，并作为后续算子规格的模板；对外能力承诺以 [README.md](../../README.md) 为准。

## 1. 功能名称

`video_file_metadata`（函数，导出于 `mmops.operators.video`）。

设计稿原用名 `VideoFile_metadata`；本规格统一为仓库既有 snake_case 约定（对齐 `normalize_text`/`KeywordMatcher`）。

## 2. 功能概述

读取视频容器的**头元数据**（mp4 的 moov 在文件尾时经自动 seek 读取，仍属头数据），不解码任何帧，产出一行一个 struct 的 1:1 元数据列。

- **基线（继承）**：完整继承 Daft 核心库 [VideoFile.metadata()](file:///C:/Users/Administrator/Documents/GitHub/Daft/daft/file/video.py#L45-L103) 已提取的 6 个字段——`width`/`height`/`fps`/`duration`/`frame_count`/`time_base`，名称、类型与回退链逐项对齐（见 §3.10）；输出 schema 是 `VideoMetadata` 的超集，既有调用方可零改码迁移。
- **新增**：容器格式 `container_format`、编码 `codec`、像素格式 `pix_fmt`、色彩四元组 `color_space`/`color_primaries`/`color_transfer`/`color_range`、音轨清单 `audio_tracks`（index/codec/sample_rate/channels/language）、旋转 `rotation`（双路探测，见 §3.5）与标签 `tags`（map\<string,string\>）。

- **字段来源**：全部字段取自容器头/流头数据（容器格式头、流 codec parameters、显示矩阵 side data、metadata 标签，见 §3.9），无一来自解码输出。
- **定位**：视频库治理的第一跳。两段式流水线的第一阶段（见 [video-module-design.md](../video-module-design.md) §4.5）——先用本算子扫描 manifest，谓词下推淘汰不合格分区（如时长 < 5s），再进入重解码算子，避免为被淘汰文件付出解码成本。
- **开销特征**：O(容器元数据大小)。mp4 的 moov box 常在文件尾部，FFmpeg 会自动发两次 Range 请求（读头 + seek 尾部）；faststart 布局单次读头即完成。
- **不做的事**：不校验码流完整性（`container_integrity` 的职责）、不逐包审计时间戳（`timestamp_audit` 的职责）、不读取帧像素、不读文件系统 stat（文件体积属 `file_sha256`，见 §6）。

## 3. 实现设计

### 3.1 模块位置与依赖

```text
src/mmops/operators/video/
├── __init__.py      # from .metadata import VIDEO_METADATA_DTYPE, video_file_metadata
└── metadata.py      # 算子实现；PyAV 仅在本模块 import
```

- 依赖 `av>=13.1`（DISPLAYMATRIX side data 读取端与流级色彩属性在该线确认可用），属重依赖，只进 `video` extra；实现落地时同步填充 `all` 扁平并集、README 安装表与打包测试（见 [operator-development.md](../operator-development.md) Package and dependency organization）。
- 无状态函数 facade：无 `__init__` 状态，天然 pickleable。

### 3.2 执行形态与资源

- `@mmops.batch`，`batch_size=8`（每行一次文件打开，小批控内存与句柄压力；1 文件 1 分区下每批通常 1–2 行）。
- 无 GPU；内存 O(头部长度)（KB 级），不设特殊 `Resources`。
- I/O 等待型负载；v1 用同步实现，Daft 分区级并行已足够（算子不自行并行，见设计稿 §4.2）。

### 3.3 探测流程

```python
@batch(return_dtype=VIDEO_METADATA_DTYPE, batch_size=8)
def video_file_metadata(uri: pa.Array) -> pa.StructArray:
    rows = [None if value is None else _probe(value.as_py()) for value in uri]
    return pa.array(rows, type=VIDEO_METADATA_DTYPE)
```

- 逐行循环是 PyAV 文件对象边界所需（同 `KeywordMatcher.match_batch` 模式），只取每行一个标量，不做整批 `to_pylist()` 之外的额外物化。
- `_probe(uri)` 单文件流程（镜像并扩展 Daft `VideoFile.metadata()` 的探测逻辑，关系见 §3.10）：
  1. `av.open(uri)`——URI 直传 FFmpeg 协议层（`file`/`http`/`https` 内建支持，HTTP 走 Range 流式）。
  2. 读容器级字段：`duration`（µs→s，>0 校验，回退链见 §3.5）、`bit_rate`、`format.name`；`container.metadata` → 容器级 `tags`。
  3. 遍历 `container.streams`：按 §3.4 判定主视频流（顶层字段唯一来源，其余视频流不输出）；音频流收集为 `audio_tracks`；编解码与色彩四元组从 `codec_context` 取（`name`/`pix_fmt`/`colorspace`/`color_range`/`color_primaries`/`color_trc`，音频流另取 `sample_rate`/`channels`，均为流头数据，不触发解码）；`time_base`、`frames`、disposition 一并收集。
  4. 主视频流按 §3.5 双路探测 `rotation`：优先 DISPLAYMATRIX side data，回退流级/容器级 `rotate` tag。
  5. `container.close()`；全程不调用 demux/decode。

### 3.4 主视频流判定规则

mp4 封面图也是一条 video 流，顶层便捷字段（`codec`/`pix_fmt`/`fps`/`width`/`height`/`rotation`/色彩四元组）需要明确选择规则：

> **主视频流** = 全部视频流中第一条 disposition 未标记 `attached_pic` 的视频流；不存在时顶层字段为 null。

Daft `VideoFile.metadata()` 直接取第一条 video 流（不排除封面图，封面图文件会选中 mjpeg 封面流）；本算子按上述规则细化，属继承之上的有意差异，见 §3.10。

### 3.5 单位与换算

| PyAV 原始值 | 换算 | 输出字段 |
| --- | --- | --- |
| `container.duration`（µs） | `> 0` 时 `/ 1_000_000`，否则回退流级换算（Daft 同款回退链，见 §3.10） | `duration` |
| `stream.duration`（time_base ticks） | `× float(stream.time_base)` | 流级 `duration` |
| `stream.average_rate`，缺失退回 `guessed_rate` | 直接转 float | `fps` |
| `stream.frames` | `0` 表示未知；未知时回退 `round(duration × fps)` 估算（Daft 同款） | `frame_count` |
| `stream.time_base`（Fraction） | `float(...)`，如 1/16000 → `6.25e-05`（Daft 对齐，不字符串化） | `time_base` |
| `codec_context.colorspace`/`color_range`/`color_primaries`/`color_trc`（枚举） | 映射为 FFmpeg 枚举名（如 `"bt709"`/`"tv"`）；未标注（unspecified）→ null | `color_space`/`color_range`/`color_primaries`/`color_transfer` |
| `side_data` 的 `DISPLAYMATRIX`（9×int32 矩阵，路径 A） | 按 FFmpeg `av_display_rotation_get` 语义：逆时针度数，范围 [-180, 180]；矩阵缺失或奇异（NaN）→ 走路径 B | `rotation`（主视频流顶层） |
| 流级/容器级 `rotate` tag（路径 B） | legacy tag 为顺时针语义，取负归一化到 [-180, 180]；仅路径 A 缺失时生效 | `rotation` |
| `stream.metadata` 的 `language` 键 | 原样透传（如 `"eng"`/`"chi"`）；缺失 → null | `audio_tracks[].language` |
| `container.metadata`（str→str） | 原样转 map（str→str）；空 dict 视为未提供 → null | `tags`（容器级） |

所有"容器/流未提供"的字段一律输出 null，不造默认值（0 会污染下游过滤）。`rotation` 输出统一为 FFmpeg `av_display_rotation_get` 的逆时针约定（[-180, 180]，float64）；legacy rotate tag 的顺时针语义在路径 B 内归一化，输出侧无符号翻转歧义。

**旋转双路探测**（`rotation` 取值规则）：

1. **路径 A（DISPLAYMATRIX side data，优先）**：读 `Stream.side_data` 中的 `DISPLAYMATRIX`（PyAV ≥ 13.1 暴露读取端），按 `av_display_rotation_get` 换算逆时针角度。FFmpeg demux 层自 2017 年起已把 mp4/mov 的 legacy `rotate` tag 转写为该矩阵，路径 A 天然覆盖绝大多数 mp4/mov 场景。
2. **路径 B（`rotate` tag，回退）**：路径 A 无矩阵或矩阵奇异时，查流级再容器级 metadata 的 `rotate` tag（MKV/TS 等未转写容器常见）。tag 值为顺时针度数，取负归一化到 [-180, 180] 后输出。
3. **优先级**：路径 A 恒优先——两路并存时取 A（正常封装下二者一致）；双路均无 → `rotation` 为 null。

### 3.6 null 语义

- 输入 URI 为 null → 输出整行 null（对齐 `KeywordMatcher` null 进 null 出先例）。
- 字段级缺失（如 TS 流无 `duration`/`frame_count`/`bit_rate`）→ 对应字段 null，行非 null。

### 3.7 错误语义

- 文件不存在、容器损坏、协议错误：`_probe` 内**自然抛异常**，不吞错、不在 struct 里造 `ok`/`error` 字段（问题报告是 `container_integrity` 的职责）。
- 失败行处置交给框架既有 `on_error` 策略：`raise`（默认，分区失败可重试）/ `log`（记录后该行输出 null）/ `ignore`（该行输出 null）。算子代码对三种策略零感知。

### 3.8 性能与 IO 特征

- mp4 moov 在尾部：FFmpeg 自动 seek，对象存储上表现为两次 Range 请求；faststart 布局单次读头。文档提示用户治理源头布局，不在算子内做双 pass。
- 无缓存：同一 URI 重复扫描由上游（manifest 物化）负责，算子保持纯函数。

### 3.9 字段来源与"仅容器头"边界

输出 schema 每个字段组的容器头来源（PyAV 读取路径）：

| 字段组 | 头数据来源（PyAV 路径） | 典型容器对应物 |
| --- | --- | --- |
| `duration`/`bit_rate`/`container_format`（容器级） | demuxer 格式头 | mp4 `mvhd`；MKV `Info`/`Duration`；TS 由 pts 估算 |
| `tags`（容器级） | `container.metadata` | mp4 `udta`/`ilst`；MKV `Tags`；TS 通常无 |
| `codec`/`pix_fmt` + 色彩四项（主视频流顶层） | 流 codec parameters（`codec_context`，不触发解码） | mp4 `stsd`/`colr`；MKV `CodecPrivate`/`Colour` |
| `width`/`height`/`fps`/`frame_count`/`time_base`（主视频流） | 流头 | mp4 `tkhd`/`mdhd`/`stts`；MKV `TrackEntry`/`TimestampScale` |
| `rotation` | `side_data` 的 `DISPLAYMATRIX`（demuxer 从 mp4 `tkhd` 矩阵转写）+ 流级/容器级 `rotate` tag 双路探测（§3.5） | 手机竖拍视频；两路均无 → null |

唯一被排除的常用字段是文件体积 `size_bytes`——它来自文件系统 stat 而非容器头，按"全部字段来自容器头"约束移出本算子（体积需求由 `file_sha256` 或文件系统层覆盖，见 §6）。

### 3.10 与 Daft `VideoFile.metadata()` 的关系

本算子**镜像并扩展** Daft 核心库 [VideoFile.metadata()](file:///C:/Users/Administrator/Documents/GitHub/Daft/daft/file/video.py#L45-L103)，而非包装 `VideoFile` 类——算子业务层禁止 `import daft`（引擎无关约束，见 [operator-development.md](../operator-development.md)），故在 PyAV 层逐项复刻其探测逻辑并叠加新增字段。

**继承对齐**（6 字段逐项对齐，输出 schema 是 `VideoMetadata` 的超集）：

| 字段 | Daft `VideoMetadata` 语义 | 本算子对齐点 |
| --- | --- | --- |
| `width`/`height` | 主视频流宽高（int） | 同源取值；主流判定差异见下 |
| `fps` | `average_rate` → `guessed_rate` 回退 | 回退链逐项一致 |
| `duration` | `container.duration / 1e6`（> 0 校验）→ `video.duration × time_base` | 回退链逐项一致 |
| `frame_count` | `video.frames`（0=未知）→ `round(duration × fps)` | 回退链逐项一致 |
| `time_base` | `float(video.time_base)` | 同为 float64，不字符串化 |

**有意差异**（均记录在案，不破坏 6 字段迁移）：

1. 主视频流判定：Daft 直接取第一条 video 流（不排除封面图，封面图文件会选中 mjpeg 封面流）；本算子按 §3.4 排除 `attached_pic`。
2. I/O 通道：Daft 经自家 IO 层（io_config 凭证）调用 `self.open()`；本算子 URI 直传 FFmpeg 协议层，对象存储凭证为 §6 后续增强。
3. 新增字段：`container_format`/`bit_rate`/`codec`/`pix_fmt`/色彩四元组/`audio_tracks`/`rotation`/`tags` 为 Daft `VideoMetadata` 所无。

**兼容承诺**：继承的 6 字段不单方面改名或改型；Daft 侧若演进回退链，本算子评估跟进并同步本表。

## 4. 接口设计

### 4.1 接口名称

`video_file_metadata`，`from mmops.operators.video import video_file_metadata`。

### 4.2 接口描述

对 URI string 列逐行读取视频容器元数据，返回同长度的 `VIDEO_METADATA_DTYPE` struct 列；null URI 映射为 null 行，探测失败按 `on_error` 策略处置。

### 4.3 接口类型

| 项 | 值 |
| --- | --- |
| 定义形态 | 独立函数 `@mmops.batch`（无状态 facade） |
| `return_dtype` | `VIDEO_METADATA_DTYPE`（`pa.struct`，见 §4.5） |
| `unnest` | `False`（单 struct 列；下游可用嵌套访问 `df["meta"]["duration"]` 触发谓词下推，或自行 unnest） |
| `batch_size` | `8` |
| `max_retries` | 调用方按需设置（建议 I/O 场景 `2`） |
| `on_error` | 默认 `raise`；批量治理场景可显式 `log` |
| `Resources` | 默认（无 GPU/内存特殊要求） |

### 4.4 input 参数

| 参数 | Arrow 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `uri` | `pa.Array`（`string`/`large_string` 逻辑兼容） | 是 | 视频文件 URI。v1 承诺本地路径与 FFmpeg 内建协议（`http`/`https`）；对象存储凭证通道（S3/OSS 经 `IOConfig`）为后续增强。null → null 行 |

无可选参数：探测不需要任何配置项，保持零选项签名。

### 4.5 output 参数

输出为单列 `pa.StructArray`，行数与输入严格相等（1:1），dtype 与下表逐字段一致（公共 API）：

```python
_TAGS = pa.map_(pa.string(), pa.string())  # 标签：键值对 map<string,string>；无标签 → null，不造空 map

VIDEO_METADATA_DTYPE = pa.struct(
    [
        # —— 继承自 Daft VideoFile.metadata() 的 6 字段（对齐关系见 §3.10）——
        pa.field("width", pa.int32()),               # 主视频流宽（像素，§3.4 规则）；无视频流 → null
        pa.field("height", pa.int32()),              # 主视频流高（像素，§3.4 规则）；无视频流 → null
        pa.field("fps", pa.float64()),               # 主视频流帧率（average_rate → guessed_rate）
        pa.field("duration", pa.float64()),          # 容器时长（秒）；容器未提供 → null
        pa.field("frame_count", pa.int64()),         # 主视频流帧数；未知 → 估算，再未知 → null
        pa.field("time_base", pa.float64()),         # 主视频流时间基准（如 1/16000 → 6.25e-05）
        # —— 新增字段 ——
        pa.field("container_format", pa.string()),   # FFmpeg demuxer 名，如 "mov,mp4,m4a,3gp,3g2,mj2"
        pa.field("bit_rate", pa.int64()),            # 容器总码率（bps）；未提供 → null
        pa.field("codec", pa.string()),              # 主视频流编解码器（§3.4 规则）
        pa.field("pix_fmt", pa.string()),            # 主视频流像素格式，如 "yuv420p"（§3.4 规则）
        pa.field("rotation", pa.float64()),          # 主视频流旋转角：逆时针 [-180,180]，双路探测（§3.5）
        pa.field("color_space", pa.string()),        # 如 "bt709"；未标注 → null
        pa.field("color_primaries", pa.string()),    # 如 "bt709"；未标注 → null
        pa.field("color_transfer", pa.string()),     # 如 "smpte2084"（PQ）；未标注 → null
        pa.field("color_range", pa.string()),        # "tv" / "pc"；未标注 → null
        pa.field("tags", _TAGS),                     # 容器级标签原样透传；无标签 → null
        pa.field(
            "audio_tracks",
            pa.list_(
                pa.struct(
                    [
                        pa.field("index", pa.int32()),        # 容器内流序号
                        pa.field("codec", pa.string()),       # aac / opus / mp3 ...
                        pa.field("sample_rate", pa.int32()),  # Hz
                        pa.field("channels", pa.int32()),
                        pa.field("language", pa.string()),    # 流 metadata 的 language 键，如 "eng"；缺失 → null
                    ]
                )
            ),
        ),
    ]
)
```

顶层字段含义速查（`codec`/`fps`/`width`/`height`/`rotation`/色彩四元组为 §3.4 主视频流便捷字段；标 ◆ 的为继承自 Daft `VideoMetadata` 的 6 字段）：

| 字段 | 类型 | 含义 | 可空 |
| --- | --- | --- | --- |
| `width` ◆ / `height` ◆ | int32 / int32 | 主视频流宽高（§3.4 规则） | 是 |
| `fps` ◆ | float64 | 主视频流帧率（§3.4 规则） | 是 |
| `duration` ◆ | float64 | 容器时长（秒） | 是 |
| `frame_count` ◆ | int64 | 主视频流帧数（未知时估算） | 是 |
| `time_base` ◆ | float64 | 主视频流时间基准（如 6.25e-05） | 是 |
| `container_format` | string | FFmpeg demuxer 名 | 否 |
| `bit_rate` | int64 | 容器总码率（bps） | 是 |
| `codec` | string | 主视频流编解码器（§3.4 规则） | 是 |
| `pix_fmt` | string | 主视频流像素格式，如 "yuv420p"（§3.4 规则） | 是 |
| `rotation` | float64 | 主视频流旋转角（逆时针 [-180,180]，双路探测 §3.5） | 是 |
| `color_space` / `color_primaries` / `color_transfer` / `color_range` | string | 主视频流色彩四元组；未标注 → null | 是 |
| `tags` | map\<string,string\> | 容器级标签原样透传；无标签 → null | 是 |
| `audio_tracks` | list\<struct\> | 全部音轨清单（index/codec/sample_rate/channels/language） | 否（可为空 list） |

### 4.6 用法示例

```python
import daft
from mmops.operators.video import video_file_metadata

manifest = daft.read_parquet("s3://lake/videos/manifest")
meta = manifest.with_column("meta", video_file_metadata(manifest["uri"]))
# 嵌套字段访问触发谓词下推：短视频、低帧率源在进入 GPU 解码前被淘汰
stage1 = meta.where((daft.col("meta")["duration"] > 5) & (daft.col("meta")["fps"] >= 24))
```

## 5. 测试计划

按 [operator-development.md](../operator-development.md#testing-requirements) 测试矩阵，fixture 用 PyAV 现场封装生成（秒级小文件，非 mock 解码）：

1. **eager/Arrow**：mp4（moov 前置 + moov 后置两种布局）、含封面图流的 mp4（验证 §3.4 主流判定）、TS 流（字段缺失 → null 路径）；断言 dtype 逐字段一致、1:1 行数。
2. **Daft 继承对齐**：与 `VideoFile.metadata()` 对同一批 fixture 对拍——6 字段（`width`/`height`/`fps`/`duration`/`frame_count`/`time_base`）逐项相等，含各自回退路径（容器 duration 缺失、`frames` 未知的文件）。
3. **色彩**：带 `colr` box 的 mp4（断言色彩四项映射为 FFmpeg 枚举名）；未标注色彩的文件（四项 → null）。
4. **旋转（双路探测）**：带 DISPLAYMATRIX 的 mp4（路径 A，断言逆时针角度）；仅带 legacy `rotate` tag 的 MKV（路径 B，断言顺时针值取负归一化）；双路并存的文件（路径 A 胜出）；两路均无（→ null）。矩阵 fixture 优先用 PyAV 写端 API（`set_display_rotation`，较新版本提供）现场封装，否则用预生成的最小 tkhd 测试资产。
5. **标签与音轨**：带 `udta`/`ilst` 的 mp4 与带 `Tags` 的 MKV（`tags` 逐项透传为 map）；无标签文件（`tags` → null）；多语言音轨文件（`audio_tracks` 的 `language` 透传，缺失 → null）。
6. **null 与边界**：null URI → null 行；空批 → 空输出；末尾短批。
7. **错误路径**：不存在 URI / 截断文件在 `on_error` 三策略下的行为（raise 抛出 / log、ignore 出 null 行）。
8. **真 Daft 集成**：本地目录端到端——with_column + 嵌套字段谓词下推，验证淘汰后分区数。
9. **打包**：`video` extra 填充后 extras 联动测试 + wheel 外装真 Daft 冒烟。

## 6. 边界与非目标

- 不解码帧、不读像素；完整性检测与时间戳审计分属 `container_integrity`/`timestamp_audit`。
- 不读文件系统 stat：`size_bytes` 已按"全部字段来自容器头"约束移出本算子，体积需求由 `file_sha256`（`sha256` + `size_bytes`）或文件系统层覆盖。
- `tags` 键值原样透传，不做清洗、翻译或白名单过滤；无标签输出 null 而非空 map。
- `rotation` 输出统一为 FFmpeg `av_display_rotation_get` 语义（逆时针为正，[-180,180]，float64）；legacy `rotate` tag 的顺时针语义在路径 B 内取负归一化，输出侧无符号翻转歧义。
- 兼容承诺：输出 schema 是 Daft `VideoMetadata` 的超集（§3.10），继承的 6 字段不单方面改名或改型。
- v1 不承诺对象存储 URI 的凭证通道（S3/OSS 经 `IOConfig` 解析文件句柄为后续增强）；先落本地与 HTTP 场景。
- 输出 schema 是公共 API：增删字段需同步本规格、README 与测试。
