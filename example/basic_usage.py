"""最小可运行示例：下游只需要继承三个基类，实现对应的方法即可。"""

import time

from funworker import SKIP, BaseConsumer, BaseProcessor, BaseProducer, Pipeline


class NumberProducer(BaseProducer):
    """生产 0~9 十个数字，生产完毕后结束。"""

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


class SquareProcessor(BaseProcessor):
    """处理单元：对数字做平方，丢弃奇数结果。"""

    def process(self, item):
        result = item * item
        return result if result % 2 == 0 else SKIP


class PrintConsumer(BaseConsumer):
    """消费者：打印处理结果。"""

    def consume(self, item):
        print(f"result = {item}")


if __name__ == "__main__":
    # 方式一：`run()` 一步启动并阻塞，直到生产者自然结束（或收到 SIGINT/SIGTERM）后
    # 自动优雅停止整条流水线；下游不需要感知 producer/consumer 的线程细节。
    pipeline = Pipeline.build(
        producer_cls=NumberProducer,
        processor=SquareProcessor,
        consumer_cls=PrintConsumer,
        num_workers=4,
    )
    pipeline.run()
    print("stats:", pipeline.stats())

    # 方式二：需要在流水线运行期间做其它事情（如轮询 stats() 上报监控）时，配合 `with` 使用；
    # 进入时自动启动，退出时自动优雅停止（销毁），同样不需要访问 producer.join()。
    with Pipeline.build(
        producer_cls=NumberProducer,
        processor=SquareProcessor,
        consumer_cls=PrintConsumer,
        num_workers=4,
    ) as pipeline:
        while pipeline.stats()["producer"]["alive"]:
            time.sleep(0.05)
        # 退出 with 块时会自动排空处理单元/消费者队列中剩余的数据并优雅停止
