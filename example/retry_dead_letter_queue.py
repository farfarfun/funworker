"""失败重试 + 死信队列示例。

`WorkerPool(..., max_retries=N, dead_letter_queue=dlq)`：处理失败的数据会自动重新排队
重试，重试耗尽后写入死信队列（不配置 `dead_letter_queue` 则丢弃并记录日志）。
"""

from queue import Queue

from funworker import BaseConsumer, BaseProcessor, BaseProducer, Pipeline


class NumberProducer(BaseProducer):
    def __init__(self, *args, total=10, **kwargs):
        super().__init__(*args, **kwargs)
        self._total = total
        self._current = 0

    def produce(self):
        if self._current >= self._total:
            raise StopIteration
        value = self._current
        self._current += 1
        return value


class FlakyProcessor(BaseProcessor):
    """偶数处理成功，奇数每次都失败（模拟不稳定的外部依赖）。"""

    def process(self, item):
        if item % 2 != 0:
            raise RuntimeError(f"transient error on {item}")
        return item * item


class PrintConsumer(BaseConsumer):
    def consume(self, item):
        print(f"result = {item}")


if __name__ == "__main__":
    dead_letter_queue = Queue()

    pipeline = Pipeline.build(
        producer_cls=NumberProducer,
        processor=FlakyProcessor,
        consumer_cls=PrintConsumer,
        num_workers=2,
        producer_kwargs={"total": 10},
    )
    # max_retries/dead_letter_queue 是 WorkerPool 的参数，Pipeline.build() 目前只透传常用参数，
    # 这里直接在构造完成后设置到第一级处理单元上。
    pipeline.pool.max_retries = 2
    pipeline.pool.dead_letter_queue = dead_letter_queue

    pipeline.run()
    print("stats:", pipeline.stats())
    print("dead letters:", list(dead_letter_queue.queue))
