# mmops 测试套件分析

> 统计口径：`pytest --collect-only`（pytest 9.1.1 / Python 3.14 / daft 0.7.26 / Windows）。
> 本文描述测试现状，不新增约束；算子开发规则见 [operator-development.md](operator-development.md)。

## 1. 总体概览

| 指标 | 数值 |
| --- | --- |
| 测试文件 | 9 个（`tests/` 根 7 个 + `tests/operators/text/` 2 个） |
| 收集用例 | 77 条 |
| 测试函数 | 64 个 |
| 参数化组 | 6 组，组大小 4 / 4 / 4 / 3 / 2 / 2，参数化展开贡献 +13 条 |
| conftest.py / 自定义 fixture | 无（辅助逻辑以模块内函数与测试替身实现） |
| 被测对象 | Arrow 边界、定义层、Daft 绑定层、两个 text 算子、打包声明、示例 |

测试通过 [pyproject.toml](../pyproject.toml) 的 `[tool.pytest.ini_options]` 配置：

```toml
addopts = "-ra"            # 摘要中显示失败/跳过/xfail 等非通过项
pythonpath = [".", "src"]  # src 布局下直接导入 mmops，无需安装
testpaths = ["tests"]      # 默认收集范围
```

运行方式：仓库根目录 `pytest -q`。全部用例无跳过标记、无网络依赖；除一条子进程导入守护外，均基于真实 daft 运行时（含 `daft.from_pydict` 端到端执行），不使用 mock 框架。

## 2. 测试矩阵

| 文件 | 用例 | 函数 | 被测对象 | 层级 |
| --- | --- | --- | --- | --- |
| [test_arrow_boundary.py](../tests/test_arrow_boundary.py) | 14 | 8 | `mmops._arrow.to_arrow_argument` / `validate_batch_output` | 单元（Arrow ABI 边界） |
| [test_daft_contract.py](../tests/test_daft_contract.py) | 12 | 12 | `mmops.backends.daft` 绑定层 + 公共 facade 契约 | 契约 |
| [test_daft_functions.py](../tests/test_daft_functions.py) | 6 | 6 | row/batch 函数经真实 Daft 执行 | 端到端 |
| [test_daft_operators.py](../tests/test_daft_operators.py) | 5 | 5 | `@mmops.operator` 类语义经真实 Daft 执行 | 端到端 |
| [test_definitions.py](../tests/test_definitions.py) | 22 | 15 | `mmops._definitions` 定义校验 + 导入隔离守护 | 单元 / 守护 |
| [test_examples.py](../tests/test_examples.py) | 1 | 1 | [examples/text_pipeline.py](../examples/text_pipeline.py) | 示例回归 |
| [test_packaging.py](../tests/test_packaging.py) | 4 | 4 | pyproject.toml extras 一致性 | 元测试 |
| [test_keyword_matcher.py](../tests/operators/text/test_keyword_matcher.py) | 7 | 7 | `KeywordMatcher` | 单元 + 端到端 |
| [test_normalize.py](../tests/operators/text/test_normalize.py) | 6 | 6 | `normalize_text` | 单元 + 端到端 |

分层意图清晰：**定义层先校验拒绝，Arrow 边界再校验类型，Daft 层最后验证真实执行契约**；两个已发布算子（text）在单元与端到端两层各测一遍。

## 3. 分文件明细

### 3.1 test_arrow_boundary.py — Arrow ABI 边界（14 条）

守护「业务代码只见 Arrow，不见 daft 对象」的转换与校验边界。

| 用例 | 验证点 |
| --- | --- |
| `test_arrow_argument_is_passed_through_without_copy` | pa.Array 入参原样直传，不复制 |
| `test_series_like_argument_uses_its_public_to_arrow_boundary` | SeriesLike 替身：仅调用其公开 `to_arrow()`，且**只调一次** |
| `test_scalar_argument_is_passed_through` | 标量（字面量）参数直传 |
| `test_batch_output_rejects_non_array_containers[invalid0-3]` | parametrize：`[1,2]`、`np.array`、`pa.Table`、`pa.ChunkedArray` 均拒收 |
| `test_string_and_binary_offset_widths_are_logically_compatible[...]` | parametrize：string↔large_string、binary↔large_binary 偏移宽度逻辑兼容 |
| `test_nested_arrow_dtype_must_match_exactly` | 嵌套类型精确匹配：`list<float32>` vs `list<float64>` 报错 |
| `test_empty_array_satisfies_empty_batch_contract` | 空数组满足空批契约 |
| `test_batch_output_length_must_match_input_length` | 输出行数与输入不符报错（1:1 基数） |

### 3.2 test_daft_contract.py — Daft 绑定层契约（12 条）

验证定义层选项如何进入 Daft，以及批输出契约在 eager 求值时的强制执行。

| 用例 | 验证点 |
| --- | --- |
| `test_struct_batch_output_is_unnested_into_named_columns` | struct 输出 + `unnest=True` 展开为命名列 |
| `test_batch_output_must_be_an_arrow_array` | 输出非 pa.Array → `BatchOutputError` |
| `test_batch_output_dtype_is_checked_during_eager_evaluation` | dtype 不符 → `BatchOutputError` |
| `test_batch_output_cardinality_is_checked_during_eager_evaluation` | 行数不符 → `BatchOutputError` |
| `test_explicit_unsupported_daft_option_is_never_silently_dropped` | `backend_options={"daft": {"not_a_real_option": True}}` 显式报错，不静默丢弃 |
| `test_explicit_resource_unsupported_by_installed_daft_is_reported` | monkeypatch 模拟旧版 `daft.func`（无 resources 支持），显式 resources 报告为不支持 |
| `test_invalid_daft_option_namespace_is_reported` | `{"daft": "invalid"}` 报 "must be a mapping" |
| `test_backend_options_preserve_nested_container_types` | 直接调用 `_namespaced_options`：list/tuple/set/frozenset 深层类型保留 |
| `test_backend_options_cannot_override_entrypoint_options` | backend_options 不得覆盖顶层 batch_size / max_concurrency |
| `test_backend_options_cannot_override_operator_options` | 同上，算子选项不可被 backend_options 覆盖 |
| `test_supported_operator_options_reach_daft_plan` | `daft.explain(file=…)` 白盒断言物理计划含 "Scorer" 与 "concurrency = 2" |
| `test_unset_gpu_option_is_not_forwarded_to_daft` | monkeypatch 包装 `daft.cls`：未设置 gpu 时不向 Daft 转发该参数 |

### 3.3 test_daft_functions.py — 真实 Daft 执行：函数（6 条）

| 用例 | 验证点 |
| --- | --- |
| `test_sync_row_function_runs_in_a_real_daft_dataframe` | 同步 row：`text.strip().lower()` 在真实 DataFrame 上执行 |
| `test_row_function_accepts_explicit_arrow_return_dtype` | 显式 `return_dtype=pa.float32()` 落入输出 schema |
| `test_async_row_function_is_awaited_by_daft` | async row 被 Daft 正确 await |
| `test_sync_batch_function_receives_arrow_arrays_and_literal_arguments` | batch 收到 `pa.Int64Array`；`batch_size=2` 分批上限；字面量参数直传（`observed` 列表捕获每次批调用） |
| `test_async_batch_function_is_awaited_and_returns_arrow` | async batch 被 await 且返回 Arrow |
| `test_empty_dataframe_does_not_break_batch_contract` | 空 DataFrame 不破坏批契约 |

### 3.4 test_daft_operators.py — 真实 Daft 执行：算子（5 条）

| 用例 | 验证点 |
| --- | --- |
| `test_stateful_operator_reuses_constructor_state_for_row_entrypoint` | 有状态算子 `Multiplier(3)` 复用构造器状态；未暴露的 helper 被隐藏（AttributeError） |
| `test_one_operator_can_expose_row_and_batch_entrypoints` | 单算子同时暴露 row + batch 入口 |
| `test_operator_supports_async_row_and_batch_methods` | async row / batch 方法 |
| `test_callable_entrypoint_is_forwarded_by_operator_instance` | `__call__` 作为入口：实例可被直接调用 |
| `test_constructed_operator_handle_round_trips_through_daft_pickle` | `daft.pickle.dumps/loads` 往返后仍可执行（worker 序列化契约） |

### 3.5 test_definitions.py — 定义层校验与导入守护（22 条）

参数化最密集的文件（4 组）。

| 用例 | 验证点 |
| --- | --- |
| `test_importing_mmops_does_not_import_daft` | `subprocess.run` 起干净解释器：`import mmops` 后 `sys.modules` 无 daft（可选重依赖不破坏导入） |
| `test_row_supports_bare_decorator_and_preserves_callable_metadata` | 裸装饰器保留 `__name__`/`__doc__`/签名/`definition.original` |
| `test_row_accepts_an_explicit_arrow_return_dtype` | row 显式 return_dtype |
| `test_batch_requires_an_arrow_return_dtype` / `test_batch_rejects_non_arrow_return_dtype` | batch 返回类型必须是 `pa.DataType`，`list[int]` 拒收 |
| `test_batch_keeps_resource_requirements` | row/batch 均保留 resources |
| `test_definition_options_are_deeply_immutable_and_pickleable` | 选项深不可变（外部 mutate 不影响、新增键 TypeError）且可 pickle |
| `test_batch_size_must_be_a_positive_integer[0/-1/1.5/True]` | parametrize：batch_size 四种非法值 |
| `test_invalid_error_policy_is_rejected` | on_error 非法策略拒收 |
| `test_operator_collects_only_explicit_row_and_batch_entrypoints` | 只收集显式入口 `{"predict_one", "predict_batch"}`，helper 不暴露 |
| `test_operator_requires_at_least_one_explicit_entrypoint` | 无入口报错 |
| `test_resource_values_must_be_non_negative` | Resources 负值拒收 |
| `test_resource_values_have_valid_numeric_types[kwargs0-2]` | parametrize：resources 类型校验 |
| `test_max_retries_must_be_a_non_negative_integer[1.5/True]` | parametrize：max_retries 类型校验 |
| `test_max_concurrency_must_be_a_positive_integer[1.5/True]` | parametrize：max_concurrency 类型校验 |

### 3.6 test_examples.py — 示例回归（1 条）

- `test_text_pipeline_example_runs_against_daft`：运行 `run_example()`，断言 text / normalized / keywords 三列输出精确（含 None 语义）。守护 README 示例不腐化。

### 3.7 test_packaging.py — 打包元测试（4 条）

以 `tomllib` 解析 [pyproject.toml](../pyproject.toml)，用代码守护打包声明：

| 用例 | 验证点 |
| --- | --- |
| `test_text_extra_installs_the_current_daft_runtime` | text extra 含 daft |
| `test_all_extra_contains_every_released_modality_dependency` | `all` == 六模态（text/image/audio/video/document/multimodal）依赖并集 |
| `test_unreleased_modality_extras_are_reserved_and_empty` | 未发布模态 extras 为空列表 |
| `test_dev_extra_supports_every_documented_verification_command` | dev extra 覆盖 daft/pytest/black/build/twine |

### 3.8 tests/operators/text/test_keyword_matcher.py — KeywordMatcher（7 条）

| 用例 | 验证点 |
| --- | --- |
| `test_keyword_matcher_is_exported_from_operator_catalog` | catalog 导出一致 |
| `test_keyword_matcher_validates_and_deduplicates_configuration` | 三种 ValueError：至少一个关键词 / 关键词非空 / 大小写归一后重复拒收 |
| `test_keyword_matcher_eager_row_preserves_keyword_order` | eager row：结果保序、无匹配返回 `[]`、None 入 None 出 |
| `test_keyword_matcher_supports_case_sensitive_whole_word_matching` | `"Ray ray XRay ray2"` 在 case_sensitive+whole_word 下只命中 `["Ray", "ray"]` |
| `test_keyword_matcher_eager_batch_matches_row_null_semantics` | eager batch 行列 null 语义一致：`[["Ray","Daft"], None, []]` |
| `test_keyword_matcher_row_and_batch_run_in_real_daft_dataframes` | row/batch 在真实 DataFrame 输出一致 |
| `test_keyword_matcher_handle_is_pickleable_and_hides_helpers` | pickle 往返 + `_match_text` 被隐藏 |

### 3.9 tests/operators/text/test_normalize.py — normalize_text（6 条）

| 用例 | 验证点 |
| --- | --- |
| `test_normalize_text_is_exported_from_operator_catalog` | catalog 导出一致 |
| `test_normalize_text_uses_arrow_kernels_and_preserves_nulls` | Arrow 内核：全角→半角 + 小写 + 压缩空白 + null 保留 |
| `test_normalize_text_options_are_explicit` | 选项全关时原样返回 |
| `test_normalize_text_rejects_unknown_unicode_form` | 非法 `form="invalid"` 拒收 |
| `test_normalize_text_accepts_an_empty_string_array` | 空数组 |
| `test_normalize_text_runs_in_a_real_daft_dataframe` | 真实 DataFrame 执行 |

## 4. 测试技术手法

| 手法 | 位置 | 作用 |
| --- | --- | --- |
| monkeypatch 模拟旧版 `daft.func` | test_daft_contract `test_explicit_resource_…` | 隔离 Daft 版本差异，使 resources 检测断言确定化 |
| monkeypatch 包装 `daft.cls` | test_daft_contract `test_unset_gpu_option_…` | 白盒验证 kwargs 转发边界（未设置不转发） |
| `subprocess.run` 导入隔离 | test_definitions `test_importing_mmops_…` | 干净解释器中验证 `import mmops` 不触发 daft 导入 |
| `pytest.raises(match=…)` | definitions / arrow / keyword_matcher / normalize | 同时断言异常类型与消息关键字 |
| `@pytest.mark.parametrize` | 6 组（arrow 2、definitions 4） | 系统性覆盖非法输入空间 |
| 真实 `daft.from_pydict` 端到端 | daft_functions / daft_operators / 两个 text 算子测试 | 不 mock 引擎，直接验证调度行为 |
| `observed` 列表捕获批调用实参 | test_daft_functions `test_sync_batch_…` | 断言分批上限与字面量直传 |
| `daft.explain` 白盒计划断言 | test_daft_contract `test_supported_operator_options_…` | 断言物理计划含算子名与并发配置 |
| `daft.pickle` 往返 | test_daft_operators / test_keyword_matcher | worker 序列化契约 |
| SeriesLike 测试替身 | test_arrow_boundary | 最小 hand-rolled fake，断言 `to_arrow` 只调一次 |
| tomllib 元测试 | test_packaging | 以可执行测试守护打包声明 |

整体风格：**无 fixture、无 mock 框架、无网络**；替身与捕获逻辑内联在测试文件中，用例即文档（测试函数名完整描述行为契约）。

## 5. 与审查门的对应

测试套件对 [AGENTS.md](../AGENTS.md) 审查门的自动化守护情况：

| 审查门 | 对应测试 |
| --- | --- |
| batch 输出必须是 `pa.Array`、dtype 正确、行数一致 | test_arrow_boundary 全套 + test_daft_contract 三条 `BatchOutputError` |
| 1:1 基数 | 行数不匹配报错、空批契约、空 DataFrame 契约 |
| 显式执行选项不得静默丢弃 | 未知选项报错、深层容器类型保留、entrypoint/operator 选项不可被覆盖 |
| worker 定义/句柄可 pickle | 算子 pickle 往返、KeywordMatcher pickle 往返、定义选项深不可变且可 pickle |
| `all` extras = 模态依赖并集 | test_packaging 并集测试 |
| 模型/客户端在 `__init__` 初始化、重依赖不破坏导入 | `import mmops` 不导入 daft（子进程守护） |
| 行/批路径 null 语义一致 | KeywordMatcher row/batch null 语义测试 |
| 禁用私有 Daft API | 无自动化守护（仅靠 review，见 §6） |

## 6. 覆盖空白观察

以下为基于当前测试代码可证实的观察，供后续补强参考：

1. **on_error 仅测拒收**：只验证非法策略报错（test_definitions），合法策略的运行时行为未在 Daft 执行测试中出现。
2. **struct 输出仅测 `unnest=True`**：struct 返回类型只覆盖展开路径，保留 struct 单列的路径未见。
3. **资源转发依赖版本模拟**：resources 在定义层已校验，但 Daft 层用 monkeypatch 模拟旧版确定化，真实转发路径未做双版本对照。
4. **无共享 conftest.py / fixture**：辅助构造（如 `_definition`、SeriesLike）在各文件内独立实现，跨文件复用为零。当前规模可接受，测试增长后可考虑收敛。
5. **「禁用私有 Daft API」无自动化守护**：该约束目前仅靠 review 执行，没有类似 test_packaging 的静态扫描测试。
6. **异常执行路径未覆盖**：async 取消/超时、Daft 任务失败重试等运行时异常路径无测试。
7. **示例回归单薄但有效**：examples 仅 1 条用例，作为 README 示例的最低守护。
