# AGENTS.md

> Bilingual edition of [AGENTS.md](AGENTS.md): the English original comes
> first, followed by its Chinese translation. If the two differ, the English
> original prevails.
>
> 本文是 [AGENTS.md](AGENTS.md) 的中英对照版：英文原文在前，中文翻译在后；如
> 两者不一致，以英文原文为准。

## Scope and source of truth 适用范围与权威来源

These instructions apply to the entire repository.

以下说明适用于整个仓库。

- [README.md](README.md) is the user-facing source for installation, current
  capabilities, operators, and examples.

- [README.zh-CN.md](README.zh-CN.md) 是面向用户的安装、当前能力、算子与示例的
  权威来源。

- [docs/operator-development.md](docs/operator-development.md) is the source
  of truth for operator APIs, data contracts, state semantics, dependencies,
  testing requirements, and review criteria.

- [docs/operator-development.zh-CN.md](docs/operator-development.zh-CN.md) 是
  算子 API、数据契约、状态语义、依赖、测试要求与评审标准的权威来源。

- This file contains execution rules for automated contributors. Do not copy
  the detailed development guide back into this file.

- 本文件包含自动化贡献者的执行规则。不要把详细开发指南复制回本文件。

## Non-negotiable project constraints 不可妥协的项目约束

- The product is a multimodal data processing and annotation operator library.

- 本产品是一个多模态数据处理与标注算子库。

- The current release executes operators only through Daft.

- 当前版本仅通过 Daft 执行算子。

- Do not claim that Ray Data or another engine is implemented or supported.

- 不要声称已实现或支持 Ray Data 或其他引擎。

- Keep engine bindings isolated internally so future implementations do not
  require rewriting every operator.

- 在内部保持引擎绑定隔离，使未来的实现无需重写每个算子。

- Keep the planned modality extras declared. Unreleased modalities remain
  empty. `all` must be the exact flattened dependency union of `text`,
  `image`, `audio`, `video`, `document`, and `multimodal`; update it,
  installation docs, and packaging tests whenever any modality dependency
  changes.

- 保持规划中的模态 extras 声明。未发布的模态保持为空。`all` 必须是 `text`、
  `image`、`audio`、`video`、`document` 与 `multimodal` 的精确扁平依赖并集；
  任何模态依赖变化时，同步更新它、安装文档与打包测试。

- Use only public Daft APIs. Never import private `daft.udf.udf_v2` helpers
  such as `Func._from_func` or `mark_cls_method`.

- 只使用 Daft 公共 API。绝不导入私有 `daft.udf.udf_v2` 辅助工具，例如
  `Func._from_func` 或 `mark_cls_method`。

- Operator business code must use Python scalar or PyArrow interfaces, not
  `daft.Series` or `daft.DataType`.

- 算子业务代码必须使用 Python 标量或 PyArrow 接口，而不是 `daft.Series` 或
  `daft.DataType`。

- Version 1 has strict 1:1 cardinality.

- 版本 1 具有严格 1:1 基数。

- Never silently drop an explicitly requested execution option.

- 绝不静默丢弃显式请求的执行选项。

- Keep secrets and environment-specific credentials out of code, logs, and
  serialized metadata.

- 将机密与环境特定的凭证排除在代码、日志与序列化元数据之外。

## Change workflow 变更工作流

Before editing:

编辑之前：

1. Read the relevant source, tests, README section, and
   `docs/operator-development.md`.

   阅读相关源代码、测试、README 章节与 `docs/operator-development.zh-CN.md`。

2. Confirm the change belongs in the operator catalog, public SDK, or Daft
   integration layer.

   确认变更属于算子目录、公共 SDK 或 Daft 集成层。

3. Preserve unrelated user changes and keep the diff scoped to the request.

   保留无关的用户改动，将 diff 限定在请求范围内。

4. Work on a feature branch, not directly on `main`.

   在特性分支上工作，不要直接在 `main` 上工作。

While implementing:

实现期间：

1. Add a focused failing test and verify the expected failure.

   添加一个聚焦的失败测试，并验证其按预期失败。

2. Implement the minimum behavior needed to pass it.

   实现使其通过所需的最小行为。

3. Keep reusable operators under `src/mmops/operators/` and Daft binding
   logic under `src/mmops/backends/daft.py`.

   可复用算子放在 `src/mmops/operators/` 下，Daft 绑定逻辑放在
   `src/mmops/backends/daft.py` 下。

4. Follow the API, Arrow ABI, state, dependency, null, schema, and test rules
   in [the development guide](docs/operator-development.md).

   遵循[开发指南](docs/operator-development.zh-CN.md)中的 API、Arrow ABI、
   状态、依赖、null、schema 与测试规则。

5. Update public exports and user/developer documentation in the same change
   as public behavior.

   在与公共行为相同的变更中更新公共导出与用户/开发者文档。

Do not add speculative runtimes, schedulers, state stores, empty domain
packages, aliases, or compatibility layers without a concrete requirement.

没有具体需求时，不要添加投机性的运行时、调度器、状态存储、空领域包、别名或
兼容层。

## Review gates 评审关卡

Reject or revise a change when any of the following is true:

出现以下任一情况时，拒绝或要求修改变更：

- a field name does not accurately describe the computed value;

- 字段名不能准确描述其计算值；

- analogous row and batch paths differ in dtype, null, error, or field
  semantics without documentation;

- 类似的 row 与 batch 路径在 dtype、null、错误或字段语义上不一致且未文档化；

- batch output is not a `pa.Array`/`pa.StructArray`, has the wrong dtype, or
  changes row count;

- batch 输出不是 `pa.Array`/`pa.StructArray`、dtype 错误或改变行数；

- an avoidable whole-batch Python-object conversion replaces an Arrow-native
  path;

- 用可避免的整批 Python 对象转换替代 Arrow 原生路径；

- a model/client initializes at module import rather than in operator
  `__init__`;

- 模型/客户端在模块导入时初始化，而不是在算子 `__init__` 中；

- an optional heavy dependency breaks unrelated imports;

- 可选重型依赖破坏无关导入；

- private Daft APIs are used;

- 使用了 Daft 私有 API；

- worker-bound definitions or handles are no longer pickleable;

- worker 绑定的定义或句柄不再可 pickle；

- documentation describes unimplemented execution support;

- 文档描述了未实现的执行支持；

- tests cover only helpers or mocks instead of real operator behavior.

- 测试只覆盖辅助函数或 mock 而非真实算子行为。

## Tests and verification 测试与验证

Follow the test matrix in
[docs/operator-development.md](docs/operator-development.md#testing-requirements).
Do not assume global row order for asynchronous Daft execution unless the
operator contract guarantees it.

遵循
[docs/operator-development.zh-CN.md](docs/operator-development.zh-CN.md#testing-requirements-测试要求)
中的测试矩阵。除非算子契约保证，否则不要假设异步 Daft 执行的全局行顺序。

Before claiming completion, run from the repository root:

在声称完成之前，从仓库根目录运行：

```bash
python -m pip install -e '.[dev]'
pytest -q
black --check src tests examples
python -m compileall -q src tests examples
python -m build
python -m twine check dist/*
git diff --check
```

When package contents or worker serialization changes, install the built
wheel outside the repository and run a real Daft smoke test. Distinguish
inherited environment dependency problems from mmops failures.

当包内容或 worker 序列化发生变化时，在仓库之外安装构建出的 wheel 并运行真实
的 Daft 冒烟测试。区分继承自环境的依赖问题与 mmops 自身的失败。

## Documentation and delivery 文档与交付

- Keep README concise and user-facing; link to the development guide for
  authoring details.

- 保持 README 简洁且面向用户；编写细节链接到开发指南。

- Keep detailed operator rules in `docs/operator-development.md` rather than
  duplicating them here or in README.

- 将详细的算子规则保留在 `docs/operator-development.zh-CN.md` 中，不要复制到
  本文件或 README。

- Describe only implemented behavior as supported.

- 只把已实现的行为描述为受支持。

- Do not publish reference/demo functions in the operator catalog. Keep
  demonstrations under `examples/` or `tests/`.

- 不要在算子目录中发布参考/演示函数。演示放在 `examples/` 或 `tests/` 下。

- Confirm repository, branch, target remote, and clean status before push.

- 推送前确认仓库、分支、目标远程与干净状态。

- Do not use destructive git commands.

- 不要使用破坏性 git 命令。

- PR summaries must state behavior, schema, dependencies, Daft execution
  scope, and exact verification evidence.

- PR 摘要必须说明行为、schema、依赖、Daft 执行范围与确切的验证证据。
