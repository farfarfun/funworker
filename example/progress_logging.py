"""运行时进度日志示例：生产/处理/消费 三段式，每一段都可以单独设置 name。

`Pipeline.build(..., input_name=..., output_name=..., processor_name=...)` 会用
`funworker.CountingQueue`（带名字 + 历史累计计数的队列）代替普通 `queue.Queue`；
调用 `pipeline.log_progress()` 就能打出一整行三段式进度日志。
"""

import time

from funworker import BaseConsumer, BaseProcessor, BaseProducer, Pipeline


class NumberProducer(BaseProducer):
    def __init__(self, *args, total=20, **kwargs):
        super().__init__(*args, **kwargs)
        self._total = total
        self._current = 0

    def produce(self):
        if self._current >= self._total:
            raise StopIteration
        value = self._current
        self._current += 1
        time.sleep(0.05)  # 故意放慢，方便观察进度日志变化
        return value


class SquareProcessor(BaseProcessor):
    def process(self, item):
        return item * item


class PrintConsumer(BaseConsumer):
    def consume(self, item):
        pass


if __name__ == "__main__":
    pipeline = Pipeline.build(
        producer_cls=NumberProducer,
        processor=SquareProcessor,
        consumer_cls=PrintConsumer,
        num_workers=4,
        input_name="raw",
        output_name="squared",
        processor_name="squarer",
    )
    with pipeline:
        while pipeline.stats()["producer"]["alive"]:
            pipeline.log_progress()
            time.sleep(0.5)
    pipeline.log_progress()  # 最后再打一条，确认全部处理完
