# daft-mmops PR #2 分析报告

> 分析对象:[daft-engine/daft-mmops#2](https://github.com/daft-engine/daft-mmops/pull/2) —— `feat(robotics): add MCAP to MP4 extractor`
> 分析方式:基于 PR 全量 diff(11 个文件,+2478/−30)、PR 描述与元数据,并与本地 main 分支代码、AGENTS.md 约束交叉核对;算子所依赖的 `_decorators` 契约已对照 main 分支源码确认。
> 状态快照:2026-10-16。PR 为 **Open**,无 reviewer、无评审评论、无任何 CI 检查记录(combined status 为 pending 且 0 条 status)。

---

## 1. PR 概况

| 项目 | 内容 |
| --- | --- |
| 标题 | feat(robotics): add MCAP to MP4 extractor |
| 作者 | `dujl`(fork 分支 `dujl:feat/mcap-to-mp4-extractor` → `daft-engine:main`) |
| head / base | head `28ce5e2`,base `e64ca4a`;base 最后推送(2026-10-13)早于 PR 创建(2026-10-15),暂无已知冲突信号 |
| 规模 | 11 个文件,**+2478 / −30** |
| 时间 | 创建于 2026-10-15 09:31 UTC,最后更新同日 14:12 UTC |
| 主题 | 新增首个 `robotics` 域算子 `McapVideoExtractor`:把一段 MCAP 机器人相机数据(本地 / `tos://` / HTTP(S))转换为 MP4 对象(本地 / `tos://`) |

PR 自述的四项要点:

1. `McapVideoExtractor` 为 Daft 行级 UDF;H.264 免转码 remux,Foxglove JPEG 可编码为 H.264 或 remux 为 MJPEG。
2. 本地与 TOS 读写统一走 fsspec;可在构造器注入可 pickle 的文件系统,否则各 worker 按环境自行构造;流适配器处理"后端多返回字节"的情况。
3. 发布前校验包数、全量解码数、编解码器与时长;对象存储输出采用唯一 staging 对象 + 内容校验 + 条件不覆盖复制 + 终验 + 保守清理。
4. 按 `docs/operators/robotics/` 组织算子使用指南。

PR 描述给出的验证证据(均未在 CI 中留痕,合并前需复跑):

- `pytest -q`:**127 passed**(含真实 Daft 执行与 fsspec 多返回字节回归);
- `black --check`、`compileall`、`git diff --check` 通过;
- sdist/wheel 构建 + `twine check` 通过,指南随 sdist 分发;
- wheel 安装到仓库外完成真实 Daft 执行与句柄序列化冒烟;
- 两次真实对象存储验收任务(H.264 remux、JPEG→H.264)独立复核包数/解码数/编解码器/时长/大小/摘要,均清理 staging 且无 OOM。

---

## 2. 变更清单

| 状态 | +/− | 文件 | 作用 |
| --- | --- | --- | --- |
| modified | +4/−3 | [AGENTS.md](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/AGENTS.md) | 措辞升级 modality → domain,extras 并集纳入 `robotics` |
| modified | +47/−8 | [README.md](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/README.md) | 安装表新增 `robotics` extra;新增 Robotics operators 快速上手段 |
| modified | +33/−6 | [docs/operator-development.md](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/docs/operator-development.md) | 新增"发布型算子(有外部副作用)"通用政策;目录树加 `robotics/`;惰性导出规则成文 |
| added | +162 | [docs/operators/robotics/mcap-video-extractor.md](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/docs/operators/robotics/mcap-video-extractor.md) | 算子用户指南(文件系统注入、凭据、编解码选项、发布保障) |
| modified | +27/−2 | [pyproject.toml](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/pyproject.toml) | 新增 `robotics` extra;`all`/`test`/`dev` 同步为扁平并集 |
| modified | +32/−1 | [src/mmops/operators/\_\_init\_\_.py](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/src/mmops/operators/__init__.py) | 根目录改为 PEP 562 惰性导出 |
| added | +5 | [src/mmops/operators/robotics/\_\_init\_\_.py](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/src/mmops/operators/robotics/__init__.py) | 域目录出口 |
| added | +888 | [src/mmops/operators/robotics/mcap_to_mp4.py](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/src/mmops/operators/robotics/mcap_to_mp4.py) | **核心算子** |
| added | +1153 | [tests/operators/robotics/test_mcap_video_extractor.py](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/tests/operators/robotics/test_mcap_video_extractor.py) | 算子测试(40 个测试函数) |
| added | +57 | [tests/test_operator_catalog.py](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/tests/test_operator_catalog.py) | 根目录惰性导出隔离守护(子进程探针) |
| modified | +70/−10 | [tests/test_packaging.py](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/tests/test_packaging.py) | extras 一致性测试扩展到 robotics 域 |

变更结构与仓库既定分层完全对应:业务算子进 `src/mmops/operators/robotics/`,引擎绑定不加一行(复用既有 `_decorators` → `backends/daft.py`),打包/文档/测试同变更更新。

---

## 3. 核心算子设计(`mcap_to_mp4.py`)

### 3.1 端到端数据流

```
MCAP (本地 / tos:// / http(s))
        │  fsspec 文件 或 urllib 响应
        ▼
_NonSeekingStream(顺序读适配,处理多返回字节)
        ▼
mcap + mcap-ros2 解码(ROS2 CompressedImage=h264 / Foxglove CDR CompressedImage=jpeg)
        ▼
PyAV 写本地临时 MP4(h264 remux:包级 pts/dts 重排;mjpeg:重编码或 remux)
        ▼
发布前校验(demux 包数 + 全量解码数 + 编解码器 + 时长 + B 帧拒绝)
        ▼
sha256 / size
        ▼
发布(本地:hidden 临时文件 + 原子 link;TOS:staging + 条件复制 + 终验)
        ▼
返回 pa.struct{output_uri, topic, frame_count, duration_ns, size_bytes, sha256, codec}
```

### 3.2 对外契约

- **入口点**:仅 `extract`(`@row(return_dtype=MCAP_VIDEO_RESULT_DTYPE)`),刻意不提供批量路径——每行产物是一次完整的转码/发布事务,批量路径暂无收益。
- **1:1 基数**:`mcap_uri` 与 `mp4_uri` 成对 null → 返回 `None`;单边 null → `ValueError`。符合"版本 1 严格 1:1"。
- **返回结构**:7 字段全部 `nullable=False`,与 PR 描述一致。
- **构造参数**:`filesystem`(可选,须可 pickle)、`temp_dir`、`output_codec`(`"source"|"h264"`,默认 `h264`)、`h264_crf=23`(0–51,显式拒绝 bool)、`h264_preset="medium"`(9 个 x264 预设白名单)。配置在 `__init__` 即校验失败,不在执行期才暴露。

### 3.3 文件系统抽象

- 本地路径也经 fsspec(`file` 协议),`tos://` 经 `tosfs`(懒注册 `fsspec.register_implementation`);HTTP(S) 用 `urllib.urlopen` 作为不可 seek 流,不整体下载进内存。
- 凭据只从 worker 环境读取(`TOS_ENDPOINT/TOS_REGION/TOS_ACCESS_KEY/TOS_SECRET_KEY/TOS_SECURITY_TOKEN`),缺失即 fail-closed;凭据不进代码、不进列数据、不进序列化元数据。
- 注入文件系统按 `protocol` 属性匹配;不匹配时,仅当任务整体已有 TOS 语境才允许回退默认本地文件系统,否则报错——避免静默用错后端。

### 3.4 编码路径

| 源 | `output_codec` | 路径 | 产物 |
| --- | --- | --- | --- |
| H.264(ROS2 CompressedImage) | 任意 | 包级 remux,重排 pts/dts/time_base,不重编码 | h264 |
| Foxglove JPEG | `source` | 包级 remux | mjpeg |
| Foxglove JPEG | `h264` | 逐帧解码 → reformat yuv420p → libx264(`bframes=0`) | h264 |

- 时间戳要求严格递增(违反即 `ValueError`),相对首帧以纳秒为 time_base(1/1e9)。
- H.264 源在产物校验阶段逐帧拒绝 B 帧重排(`reject_b_frames=source_is_h264`)——remux 路径按 dts=pts 直写,存在 B 帧时时间轴本就不成立,因此以"先写后验"的方式 fail-closed。
- 尾帧时长取前一帧间隔,单帧视频兜底 1ns(见 §9.4)。

### 3.5 发布协议(本 PR 最具复用价值的部分)

- **本地输出**:目标目录内隐藏临时文件(`.{name}.{uuid}.tmp`)→ 校验 → `link()` 硬链接到最终名;`FileExistsError` 转译为"输出已存在"。发布原子性由 `link` 的不可覆盖语义保证。
- **TOS 输出**(fsspec 无原子条件复制,故下沉到 tosfs 后端客户端):
  1. 唯一 staging key(`{key}.mmops-staging-{uuid}`)上传;
  2. 回读校验 size + sha256;
  3. `copy_object(copy_source_if_match=etag, forbid_overwrite=True)` 条件复制到最终 key;
  4. 拒绝任何版本化 bucket(`get_bucket_version` 非"从未启用"即失败);
  5. 若复制返回了 `version_id`(说明发布途中 bucket 版本化状态变化),**只删除精确返回的 version** 并整体失败;
  6. 终验 size + sha256;staging 清理失败仅 `warnings.warn`,不吞掉主流程结果。
- 该协议随后被抽象成 `docs/operator-development.md` 的通用政策:"发布型算子仍须是 1:1 行变换,但外部写是副作用——写临时目标、验证、不静默覆盖、成功后才返回;失败时绝不删除语义含糊的无版本 final key(并发写者可能已替换它),只有精确返回的 version 标识才可删。"把单点实现经验沉淀为规则,是这个 PR 文档层面的最大亮点。

### 3.6 与框架契约的衔接

- 算子文件**零 `daft` 导入**:dtype 用 `pa.struct` 定义,绑定经由 `..._decorators` 的 `@operator/@row`,由 `backends/daft.py` 懒编译(已对照 main 分支 `_decorators.py` 确认)。
- 重依赖(`mcap`、`av`、`fsspec`、`tosfs`)全部在执行路径内延迟 import,满足"可选重依赖不得破坏无关导入"。
- 实例状态只有配置 + 可选文件系统,满足 worker 序列化(可 pickle)要求;TOS/本地文件系统缓存在 worker 侧首次执行时才填充。

---

## 4. 惰性算子目录(`operators/__init__.py`)

由于 robotics 带重依赖,根目录导入不能连带加载域包。PR 把 `mmops.operators` 改为 PEP 562 `__getattr__` 惰性导出:

- 导入根目录 → 不加载任何域包;
- 访问 `KeywordMatcher` → 只加载 `.text`;
- 访问 `McapVideoExtractor` → 只加载 `.robotics`;
- 未知名 → 标准 `AttributeError`。

配套 [test_operator_catalog.py](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/tests/test_operator_catalog.py) 用**子进程探针**(隔离 import 状态)逐一验证四种场景,并把"新增根导出不得急切导入域包、导入一个域不得导入另一个域"写进了开发指南。这是本 PR 在架构层面最有长期价值的改动之一,后续每个重域都能直接受益。

---

## 5. 打包与依赖

| extra | 内容 |
| --- | --- |
| `robotics`(新增) | `daft>=0.7`、`mcap>=1.3,<2`、`mcap-ros2-support>=0.5,<1`、`av>=15,<17`、`fsspec>=2023.5`、`tosfs>=2026.7,<2027` |
| `all` | 按规则重新展开为七个域 extras 的扁平并集(PEP 621 extras 不能互相引用) |
| `test` / `dev` | 同步纳入 robotics 依赖,保证套件可完整运行 |

- 版本上界收敛(mcap<2、mcap-ros2-support<1、av<17、tosfs<2027),与"钉住发布协议所依赖的后端行为"的策略一致。
- `tests/test_packaging.py` 同步断言:robotics 依赖集合、`all` 精确等于并集、指南链接存在且五个 TOS 环境变量成文、空域仍为空。

---

## 6. 测试分析

规模:40 个测试函数(含 6 组参数化),全部为真实行为测试——自造 H.264/JPEG 压缩包、真实 MCAP 容器写入、本地 HTTP 服务器、内存版 TOS 文件系统;3 个用例跑**真实 Daft DataFrame 端到端**。无 mock 框架。

| 分组 | 覆盖点 |
| --- | --- |
| 目录导出 | 双目录导出、无 legacy 别名、facade 仅暴露 `extract` |
| URI 校验 | 不支持的输入 scheme(s3/ftp)、不可发布的输出 scheme(http/https/s3)、无 host 的 HTTP |
| 编码路径 | 本地 H.264 remux、HTTP 流式、Foxglove JPEG→mjpeg remux、JPEG→h264 编码 |
| 配置校验 | 非法 `output_codec`/`h264_crf`(含 bool)/`h264_preset` |
| 文件系统 | TOS 配置对本地路径惰性、注入 fs 生效、注入 fs 协议不匹配不被忽略 |
| TOS 读写 | fsspec 读 TOS 流、**多返回字节回归**、读+上传全流程 |
| TOS 发布协议 | 损坏发布拒绝、版本化 bucket 拒绝(Enabled/Suspended)、上传提交后异常仍清理 staging、发布中途版本化变化精确回滚、歧义 final key 永不删除 |
| 真实 Daft | 本地路径执行、注入 TOS fs 执行、JPEG→h264 执行 |
| 语义拒绝 | 多话题要求显式 topic、非 h264 ROS2 图像拒绝、混合格式自动选择唯一 h264 话题、不可解码 H.264 拒绝、B 帧重排拒绝 |
| 保护语义 | 本地输出不覆盖、成对 null/单边 null |

**数量自洽性**:本地 main 套件基线为 77 条(见 [test-suite-analysis.md](test-suite-analysis.md)),本 PR 参数化展开后新增约 50 条,77 + 50 = 127,与 PR 描述"127 passed"吻合。

---

## 7. 与仓库约束符合性核对

逐条对照 AGENTS.md 非协商约束与评审门槛:

| 约束 | 结论 |
| --- | --- |
| 只声明 Daft 引擎支持 | ✅ 未引入任何新运行时/调度器 |
| 引擎绑定隔离 | ✅ 算子零 daft 导入,复用既有 `_decorators`/`backends.daft` |
| 业务代码只用 Python 标量 / PyArrow | ✅ dtype 全为 `pa.*` 常量 |
| 只用公共 Daft API | ✅ 无 `daft.udf.udf_v2` 私有符号 |
| 1:1 基数 | ✅ 严格行级,成对/单边 null 语义明确并成文 |
| 可选重依赖不破坏无关导入 | ✅ 全部执行路径内延迟 import + 惰性目录 |
| 模型/客户端不在模块导入期初始化 | ✅ 文件系统在 `__init__`/worker 首用时才构造 |
| worker 句柄可 pickle | ✅ 实例仅持配置与可选 fs;含真实 Daft 序列化冒烟证据 |
| 凭据不进代码/日志/序列化元数据 | ✅ 仅环境变量,fail-closed |
| extras 并集规则 | ✅ `all`/`test`/`dev`/README/打包测试/AGENTS.md 六处同变更更新 |
| 文档同变更更新 | ✅ README + 开发指南 + 新指南 + 打包测试断言成文 |
| 演示不进算子目录 | ✅ 示例只在 README 与 tests |
| 不假设全局行序 | ✅ 纯 1:1 行算子,无跨行依赖 |
| 测试覆盖真实行为而非仅 mock | ✅ 真实 MCAP/真实 Daft/真实发布协议仿真 |

唯一在"内部边界"之外的用法:TOS 发布路径经 `getattr(fs, "tos_client")` 使用 tosfs 的后端客户端(见 §9.2)。

---

## 8. 评审发现与风险

按严重程度排列,均为建议项,无阻断性正确性缺陷:

1. **HTTP 拉流无超时**(`mcap_to_mp4.py` `urlopen(uri)`,约 [L337](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/src/mmops/operators/robotics/mcap_to_mp4.py#L337)):源端卡住会让 Daft task 无限挂起。建议增加可配置 timeout,并考虑透传认证头/重试语义。
2. **tosfs 内部属性耦合**([`tos_client`](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/src/mmops/operators/robotics/mcap_to_mp4.py#L717) 经 `getattr` 访问):有注释解释(fsspec 无原子条件复制)且缺失时 fail-closed,但仍依赖 tosfs 实现细节;建议在指南中把 `tosfs>=2026.7,<2027` 的上界钉住原因写明,升级 tosfs 时纳入回归。
3. **发布前全量 demux + 全量解码**(`_verify_mp4`):每行产物都做两遍容器遍历(含逐帧 B 帧检查),CPU 成本高。这是"先验证后发布"的有意设计,v1 正确性优先合理,但吞吐敏感场景需在指南中量化说明(现有指南已描述行为,建议补成本提示)。
4. **单帧视频尾帧时长兜底 1ns**([`last_frame_duration_ns or 1`](https://github.com/daft-engine/daft-mmops/blob/28ce5e2/src/mmops/operators/robotics/mcap_to_mp4.py#L617)):通过时长校验(>0)但语义上偏怪,可考虑用时间基最小粒度或显式约定。
5. **本地输出 `exists()` 预检存在竞态**:仅为快速失败提示,真正保护是 `link` 的原子失败——正确,但值得在指南注明"预检不可依赖"。
6. **流程缺口**:PR 无 reviewer、无 CI 检查记录,验证证据均来自 PR 描述。按 AGENTS.md,合并前应在干净环境复跑全套命令(`pip install -e '.[dev]'` → `pytest -q` → `black --check` → `compileall` → `python -m build` → `twine check` → `git diff --check`),并保留 wheel 仓库外安装 + 真实 Daft 冒烟记录。
7. **产品限制需确认**:`_require_unversioned_tos_bucket` 把"从未启用版本化"设为发布前提。这是 fail-closed 的保守决策(正确),但对已开版本化的用户是硬性不可用——建议在指南显著位置声明该限制及原因(版本化会破坏 `forbid_overwrite` 的 no-overwrite 语义)。

---

## 9. 结论

**总评:完成度很高、基本达到可合并状态的特性 PR。**

- 架构决策(惰性目录、非 seek 流适配、staging 发布协议)有明确动机,且把实现经验反向沉淀为 `docs/operator-development.md` 的通用政策,属于超出单点需求的贡献;
- 与仓库全部非协商约束和评审门槛逐条自洽,打包/文档/测试六处联动更新无遗漏;
- 测试以真实行为为主(真实 Daft 端到端、真实容器与编解码数据),数量与 PR 证据自洽。

**建议合并前处理**:§8.1(HTTP 超时,建议必改)、§8.2(耦合说明)、§8.6(复跑验证 + 争取至少一名 reviewer 实审)。其余为可选优化。

**对目录的影响**:合并后公开算子由 2 个入口/3 个导出(text 域)扩展至 4 个入口(text 3 + robotics 1),`robotics` 成为第一个带重依赖的域,根目录惰性导出模式自此成为重域接入的标准路径。
