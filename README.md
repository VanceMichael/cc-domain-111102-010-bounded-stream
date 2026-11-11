# 产线遥测流的有界采集与背压恢复

这个 Python 项目保存采集侧的来源读取工具和合成资料。代码位于 app，示例输入位于 fixtures，现有读取工具的回归位于 tests。示例不代表任何真实单位的业务记录。

## 数据与接口

StreamSource 只按块读取指定压缩文件，acknowledge 记录源确认调用，不解压、不落盘。每条记录含 partition、offset、payload，offset 从 0 连续增长。members.gz 串联两个 gzip member，truncated.gz 的第二 member 尾部被截断；conflict.gz 包含同位置不同内容，badline.gz 含不完整 JSON。源恢复时可用已确认位置过滤重发记录，但不能跨越未确认记录。

## 本地使用

使用 Python 3.12 或更新版本，当前工具仅依赖标准库。从项目根目录执行：

```sh
python3 -m app.inspect
python3 -m unittest discover -s tests -v
python3 -m compileall -q app tests
```

读取命令输出当前素材的结构或单次读取结果。运行过程中产生的文件放在 runtime 中，与原始素材分开。公开示例仅使用 .invalid 域名，网络适配时应替换为已授权的来源。
