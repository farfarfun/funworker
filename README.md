# funworker

轻量级线程流水线框架：**生产者 -> 队列 -> 处理单元线程池 -> 队列 -> 消费者**，各阶段通过队列解耦。

下游只需要继承三个基类、实现一个方法，就能获得一条可优雅启停的多线程流水线：

| 角色 | 基类 | 需要实现 |
| --- | --- | --- |
| 生产者 | `BaseProducer` | `produce(self) -> Any` |
| 处理单元 | `BaseProcessor` | `process(self, item) -> Any` |
| 消费者 | `BaseConsumer` | `consume(self, item) -> None` |

`WorkerPool` 负责把 `BaseProcessor` 包装成多线程执行；`Pipeline` 负责把它们串起来，提供一个 `run()`
方法统一管理启动/停止——下游不需要接触 `producer`/`consumer` 内部的线程细节。

## 安装

```bash
pip install -e .
```

## 快速上手

```python
from funworker import BaseConsumer, BaseProcessor, BaseProducer, Pipeline, SKIP


class NumberProducer(BaseProducer):
    def __init__(self, *args, total=10, **kwargs):
        super().__init__(*args, **kwargs)
        self._total = total
        self._current = 0

    def produce(self):
        if self._current >= self._total:
            raise StopIteration  # 数据生产完毕（有限任务场景）
        value = self._current
        self._current += 1
        return value


class SquareProcessor(BaseProcessor):
    def process(self, item):
        result = item * item
        return (
            result if result % 2 == 0 else SKIP
        )  # SKIP 表示丢弃这条数据；None 是合法业务值


class PrintConsumer(BaseConsumer):
    def consume(self, item):
        print(f"result = {item}")


pipeline = Pipeline.build(
    producer_cls=NumberProducer,
    processor=SquareProcessor,  # 也可以传实例，或一个返回实例的工厂函数
    consumer_cls=PrintConsumer,
    num_workers=4,
)
pipeline.run()  # 启动并阻塞：生产者自然结束，或收到 SIGINT/SIGTERM 后，自动优雅停止整条流水线
print(pipeline.stats())
```

不用 `run()` 阻塞、而是想在流水线运行期间做别的事情（轮询指标等）时，用 `with` 语法：进入时自动
启动，退出时自动优雅停止排空，全程不需要访问 `producer.join()`。`BaseProducer`/`BaseConsumer`/
`WorkerPool`/`Pipeline` 都支持 `with`。

## 示例

`example/` 目录下有更多可以直接运行的完整示例，其他仓库可以直接参考/复制：

| 示例 | 说明 |
| --- | --- |
| [`basic_usage.py`](example/basic_usage.py) | 最小可运行示例：`run()` 与 `with` 两种用法 |
| [`multi_stage_pipeline.py`](example/multi_stage_pipeline.py) | 串联多级处理：`add_stage()` + `set_consumer()` |
| [`fan_out_fan_in.py`](example/fan_out_fan_in.py) | 一拆多 `Many`（fan-out）+ 多合一 `BaseBatchProcessor`（fan-in / 批处理） |
| [`retry_dead_letter_queue.py`](example/retry_dead_letter_queue.py) | 失败重试 + 死信队列 |
| [`progress_logging.py`](example/progress_logging.py) | 运行时进度日志：`CountingQueue` + `format_progress()`/`log_progress()` |

## 特性

- **`run()` 一步启停**：`Pipeline.run()` 内部处理启动顺序、`SIGINT`/`SIGTERM` 优雅停止、异常传播，
  下游不需要手动调用 `producer.join()`。
- **`SKIP` / `None` 语义分离**：`produce()`/`process()` 返回 `None` 是合法的业务数据；返回
  `funworker.SKIP` 才表示"丢弃这条，不进入下游队列"。
- **异常传播**：任一线程内部抛出未捕获异常，会被捕获并在 `join()`/`stop()`/`run()` 时重新抛出，
  不会静默崩溃。
- **失败重试 + 死信队列**：`WorkerPool(..., max_retries=3, dead_letter_queue=dlq)`，处理失败的数据
  会重新排队重试，重试耗尽后写入死信队列（不配置则丢弃并记录日志）。见
  [`retry_dead_letter_queue.py`](example/retry_dead_letter_queue.py)。
- **监控指标**：`producer.stats()` / `pool.stats()` / `consumer.stats()` / `pipeline.stats()` 返回
  已处理/失败/重试计数、队列长度、线程存活状态，方便接入监控。
- **多级流水线**：`pipeline.add_stage(processor2, num_workers=2)` 可以在第一级处理单元后追加更多级，
  上一级的输出队列自动作为下一级的输入队列；最后用 `pipeline.set_consumer(ConsumerCls)` 挂消费者。见
  [`multi_stage_pipeline.py`](example/multi_stage_pipeline.py)。
- **幂等停止**：`stop()`/`pool.stop()` 重复调用是安全的，第二次起直接返回。
- **按 CPU 核数自适应**：`num_workers` 不传时默认取 `os.cpu_count()`，多数场景不需要手动调线程数。
- **一拆多（fan-out）/ 多合一（fan-in）**：`process()` 返回 `funworker.Many([...])` 把一条拆成多条；
  继承 `BaseBatchProcessor` 实现 `process_batch(items)` 把多条合成一批处理，停止时缓冲区剩余数据
  也会强制产出，不丢数据。见 [`fan_out_fan_in.py`](example/fan_out_fan_in.py)。
- **批量消费**：继承 `BaseBatchConsumer` 实现 `consume_batch(items)`，攒够 `batch_size`（默认100）
  条或超过 `batch_timeout`（默认10秒）没攒满，就会触发一次批量写入；停止时缓冲区剩余数据也会强制
  消费一次，不丢数据，适合批量落库等下游场景。
- **运行时进度日志**：`format_progress()`/`log_progress()`（`BaseProducer`/`WorkerPool`/
  `BaseConsumer`/`Pipeline` 都有），统一"名字(当前/历史)"三段式，每一段的名字都可以单独设置；
  配合 `funworker.CountingQueue` 还能看到每条队列的历史累计吞吐量。见
  [`progress_logging.py`](example/progress_logging.py)。
- **`with` 语法**：`BaseWorker`（因此 `BaseProducer`/`BaseConsumer`）、`WorkerPool`、`Pipeline`
  都实现了 `__enter__`/`__exit__`，避免忘记调用停止方法导致线程泄漏。

## 设计要点

- **职责分离**：`BaseProcessor` 完全不感知线程/队列，只关心"一条数据怎么处理"，方便单测和复用。
- **每线程独立实例**：`WorkerPool` 默认给每个线程创建独立的 `BaseProcessor` 实例（传类或工厂函数即可），
  处理单元可以放心持有数据库连接、http session 等非线程安全资源。
- **优雅停止**：内部使用 `STOP` 哨兵对象 + `Queue.join()` drain，保证停止前把队列里已有数据处理完，
  不丢数据。
- **错误隔离与可见性**：单条数据处理异常默认走 `on_error` 钩子记录日志（并按配置重试/进死信队列），
  不会打断整条流水线；但线程自身崩溃（如 `on_start` 抛异常）会被显式传播给调用方，不会被吞掉。
- **可扩展**：`Pipeline` 是对 `BaseProducer` + 一至多级 `WorkerPool` + `BaseConsumer` 的编排，
  `add_stage()`/`set_consumer()` 让多级处理流水线的写法和单级一样简单。
