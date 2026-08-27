"""多级流水线示例：生产者 -> 处理单元A -> 处理单元B -> 消费者。

`add_stage()` 在当前末级处理单元后面追加一级处理，上一级的输出队列自动作为
新一级的输入队列；`set_consumer()` 把消费者挂到最后一级的输出队列上。
"""

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


class SquareProcessor(BaseProcessor):
    """第一级：平方。"""

    def process(self, item):
        return item * item


class AddOneProcessor(BaseProcessor):
    """第二级：加一。"""

    def process(self, item):
        return item + 1


class PrintConsumer(BaseConsumer):
    def consume(self, item):
        print(f"result = {item}")


if __name__ == "__main__":
    pipeline = Pipeline.build(
        producer_cls=NumberProducer,
        processor=SquareProcessor,
        consumer_cls=None,  # 先不挂消费者，等所有处理级追加完再挂
        num_workers=2,
        producer_kwargs={"total": 10},
    )
    pipeline.add_stage(AddOneProcessor, num_workers=2)
    pipeline.set_consumer(PrintConsumer)

    pipeline.run()
    print("stats:", pipeline.stats())
