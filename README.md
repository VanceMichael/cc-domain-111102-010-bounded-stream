# 产线遥测流的有界采集与背压恢复

这个 Python 项目保存采集侧的来源读取工具和合成资料。代码位于 `app`，示例输入位于
`fixtures`，回归与端到端验证位于 `tests`，进程级崩溃演练脚本位于 `scripts`。示例均为
人工生成的传感器记录与压缩字节流，不含设备连接、真实生产信息或选中代码。

## 解决的问题

遥测从每日文件改为压缩流后，采集进程需要一条**有界、可恢复**的链路：

* HTTP 响应可在任意字节边界切块、可串联多个 gzip member，中文与整行 JSON 都能跨块；
* 内存上限必须约束**所有阶段的累计在途字节**，而不是单次 `read` 的大小；
* 源确认位置只能推进到已持久保存的连续记录；
* 坏 JSON、超长行、损坏压缩尾部不能悄悄跳过后继续确认后续 offset；
* 同 offset 同内容识别为重传（幂等），不同内容则停止分区并保留冲突指纹；
* 崩溃后从已持久化位置恢复，“提交后、确认前退出”不产生业务重复。

本项目只负责原始遥测接入，不做设备控制或故障预测。

## 模块结构

| 模块 | 职责 |
| --- | --- |
| `app/buffer.py` | `BufferLedger` 有界内存账本：全局上限 + 分区配额，记录缓冲峰值与每次阻塞原因 |
| `app/codec.py` | 多 member gzip 流式解压、字节级 NDJSON 行装配（半行/半多字节字符跨块）、记录校验 |
| `app/store.py` | 分区数据文件、固定宽度索引、状态清单；数据→索引→清单严格 fsync 顺序与崩溃恢复 |
| `app/pipeline.py` | fetcher/decoder/writer 线程拓扑、背压协调、确认协议、毒化隔离、指标快照 |
| `app/source.py`、`app/wire.py` | 既有分块源与 HTTP 重放工具（保持兼容） |

### 线程与背压拓扑

```
源连接:  fetcher 线程 ──raw 队列──▶ decoder 线程 ──按 partition 路由──▶ 分区队列
分区:    writer 线程（落盘 + 确认）
```

内存账本对三类在途字节统一计费，所有权随数据流动转移：

* 原始压缩块（fetcher 读入即占源连接账户，decoder 消费后释放）；
* gzip **未验证 member 的解压明文**（产生即计费，形成背压；通过 gzip 尾部 CRC/ISIZE
  校验前绝不输出，截断时整段拒绝确认并释放）；
* 成行记录（在分区队列中仍占源连接账户，writer 取出才转入分区账户，落盘 fsync 后释放）。

因此下游停写 → 分区/全局配额耗尽 → decoder 阻塞 → fetcher 暂停读取，背压一路传回源端。
每个分区有独立配额，单个分区停住只冻结自己的额度，其他分区在各自配额内继续前进。

### 确认与恢复协议

提交顺序严格为：追加数据并 `fsync` → 追加索引并 `fsync` → 更新清单（原子
`os.replace`）并目录 `fsync`，**之后**才调用源 `acknowledge`。

* 清单中的 `confirmed_offset` 永远指向已持久化的连续记录；
* 分区索引保存每条记录的 `offset + sha256 摘要`，重启后重传的同 offset 记录按摘要
  判断：同内容幂等丢弃，异内容停止分区并落盘冲突指纹；
* 在“清单已 fsync、源确认未发出”之间崩溃，重启后源会重发，摘要去重保证无业务重复。

### 毒化策略

* 压缩 member 截断/损坏：只停止未验证 member 实际涉及的分区，已通过校验的前序 member
  数据正常落盘确认；探测不到受影响分区时保守停止全流；
* 坏 JSON、超长行：记录级错误，只停止对应分区并持久化 `STOPPED`，不确认该 offset；
* 内容冲突：停止分区，保留双方摘要与入站预览作为指纹。

## 物理约束

内存上限必须能容纳**至少一个完整 gzip member 的解压明文**。member 在通过 gzip 尾部
校验前无法切出行，若上限小于单个 member 的明文体积，该 member 永远无法验证。这是
gzip 流式完整性的固有约束，配置时需按源端 member 大小预留余量。

## 本地使用

使用 Python 3.12+，仅依赖标准库。隔离模式下从项目根执行（`-I` 不读 `PYTHONPATH`，
故显式注入项目路径；脚本与数据目录分离，路径均以参数传入）：

```sh
# 端到端演示，指标打印到 stdout，产物写入 runtime/
python3 -I -c "import sys; sys.path.insert(0,'.'); from app.inspect import main; main()"

# 全量测试（35 个用例，含子进程硬崩溃恢复）
python3 -I -c "import sys; sys.path.insert(0,'.'); import unittest; \
unittest.TextTestRunner().run(unittest.TestLoader().discover('tests', top_level_dir='.'))"

python3 -m compileall -q app tests scripts
```

## 验证关系（tests 如何对应需求）

| 需求关系 | 对应用例 |
| --- | --- |
| 任意字节边界 + 多 member + 中文跨块 | `GzipDecoderTests.test_multi_member_arbitrary_boundaries`、`LineAssemblerTests.test_split_inside_multibyte_char`、`HappyPathTests`（chunk 1/7/13） |
| 内存上限约束累计缓冲而非单次 read | `BufferLedgerTests.test_cumulative_not_per_read` |
| 可控慢写入下的背压与缓冲峰值 | `BackpressureTests.test_slow_write_causes_backpressure_and_bounded_peak` |
| 下游停写暂停拉取、恢复后续跑 | `BackpressureTests.test_paused_downstream_pauses_pull_then_resumes` |
| 单分区停住不阻塞其他分区 | `PartitionIsolationTests.test_stopped_partition_does_not_block_others` |
| 半个多字节字符 | `LineAssemblerTests.test_split_inside_multibyte_char`（逐字节切分） |
| 压缩 member 截断，保留已验证前缀 | `GzipDecoderTests.test_truncated_member_rejected_but_verified_prefix_kept`、`PoisonTests.test_truncated_member_keeps_verified_prefix` |
| 坏 JSON/超长行不跳过、不越位确认 | `PoisonTests.test_bad_json_stops_partition_without_skipping`、`test_too_long_line_stops_partition` |
| 同内容重传/异内容冲突指纹 | `RecoveryTests.test_retransmit_same_content_identified_different_conflicts` |
| 重启恢复、无业务重复 | `RecoveryTests.test_restart_resumes_from_persisted_offset_no_duplicates`、`test_repeated_restart_is_idempotent` |
| 提交后确认前硬崩溃（真实子进程 `os._exit`） | `RecoveryTests.test_hard_process_crash_between_commit_and_ack`（`scripts/crash_worker.py`） |
| 可观察的峰值/落盘确认位置/阻塞原因 | `ObservabilityTests.test_metrics_contain_required_fields` |

实现全程逐块流式处理，不一次性解压响应、不预读整个流到内存。运行产物放在 `runtime/`，
与原始素材分开；公开示例仅使用合成数据，网络适配时应替换为已授权来源。
