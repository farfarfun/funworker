"""一拆多（fan-out）+ 多合一（fan-in / 批处理）示例。

- `Many([...])`：`process()`/`on_idle()`/`on_drain()` 返回它，表示这一次要往下游
  产出多条结果（逐条 put），与直接返回一个 list 类型的业务值区分开。
- `BaseBatchProcessor`：只需实现 `process_batch(items)`，攒够 `batch_size` 条或
  超过 `batch_timeout` 秒会自动触发一次；停止时缓冲区里剩余的数据也会强制产出，不丢数据。
"""

from funworker import (
    BaseBatchProcessor,
    BaseConsumer,
    BaseProcessor,
    BaseProducer,
    Many,
    Pipeline,
)


class LineProducer(BaseProducer):
    """生产几行文本。"""

    LINES = ["hello world", "funworker is fun", "fan out fan in"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._current = 0

    def produce(self):
        if self._current >= len(self.LINES):
            raise StopIteration
        line = self.LINES[self._current]
        self._current += 1
        return line


class SplitWordsProcessor(BaseProcessor):
    """一拆多：一行文本拆成多个单词，每个单词分别进入下游队列。"""

    def process(self, line):
        return Many(line.split())


class BatchUpperProcessor(BaseBatchProcessor):
    """多合一：攒够5个单词，或最长等1秒，就一次性转大写批量输出（模拟批量写库）。"""

    def __init__(self):
        super().__init__(batch_size=5, batch_timeout=1.0)

    def process_batch(self, words):
        print(f"batch of {len(words)}: {words}")
        return Many(word.upper() for word in words)


class PrintConsumer(BaseConsumer):
    def consume(self, item):
        print(f"result = {item}")


if __name__ == "__main__":
    pipeline = Pipeline.build(
        producer_cls=LineProducer,
        processor=SplitWordsProcessor,
        consumer_cls=None,
        num_workers=1,
    )
    pipeline.add_stage(BatchUpperProcessor, num_workers=1)
    pipeline.set_consumer(PrintConsumer)

    pipeline.run()
    print("stats:", pipeline.stats())
