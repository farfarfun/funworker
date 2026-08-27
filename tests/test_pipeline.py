import threading
import time
from queue import Queue

import pytest

from funworker import (
    SKIP,
    STOP,
    BaseBatchProcessor,
    BaseConsumer,
    BaseProcessor,
    BaseProducer,
    CountingQueue,
    Many,
    Pipeline,
    WorkerPool,
)


class RangeProducer(BaseProducer):
    def __init__(self, *args, total, **kwargs):
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
    def process(self, item):
        return item * item


class CollectConsumer(BaseConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.results = []
        self._lock = threading.Lock()

    def consume(self, item):
        with self._lock:
            self.results.append(item)


def _build(consumer_cls=CollectConsumer, processor=SquareProcessor, total=50, **kwargs):
    return Pipeline.build(
        producer_cls=RangeProducer,
        processor=processor,
        consumer_cls=consumer_cls,
        producer_kwargs={"total": total},
        **kwargs,
    )


def test_pipeline_end_to_end():
    pipeline = _build(num_workers=4, total=50)
    pipeline.run(install_signal_handlers=False)

    assert sorted(pipeline.consumer.results) == [i * i for i in range(50)]


def test_processor_can_skip_items():
    class DropOddProcessor(BaseProcessor):
        def process(self, item):
            return item if item % 2 == 0 else SKIP

    pipeline = _build(processor=DropOddProcessor, num_workers=2, total=10)
    pipeline.run(install_signal_handlers=False)

    assert sorted(pipeline.consumer.results) == [0, 2, 4, 6, 8]


def test_none_is_a_valid_value_not_dropped():
    class NoneProcessor(BaseProcessor):
        def process(self, item):
            return None

    pipeline = _build(processor=NoneProcessor, num_workers=2, total=5)
    pipeline.run(install_signal_handlers=False)

    assert pipeline.consumer.results == [None] * 5


def test_worker_pool_defaults_num_workers_to_cpu_count():
    import os

    pool = WorkerPool(SquareProcessor, Queue())
    assert pool.num_workers == (os.cpu_count() or 1)


def test_worker_pool_without_consumer():
    input_queue = Queue()
    for i in range(5):
        input_queue.put(i)

    pool = WorkerPool(SquareProcessor, input_queue, num_workers=2)
    pool.start()
    pool.stop(drain=True)


def test_producer_as_context_manager():
    output_queue = Queue()
    with RangeProducer(output_queue, total=5) as producer:
        producer.join()  # 生产者自己因 StopIteration 结束

    assert not producer.is_alive()
    assert sorted(output_queue.queue) == [0, 1, 2, 3, 4]


def test_consumer_as_context_manager_drains_before_stopping():
    input_queue = Queue()
    for i in range(5):
        input_queue.put(i)

    consumer = CollectConsumer(input_queue, get_timeout=0.05)
    with consumer:
        input_queue.put(STOP)
        consumer.join()  # 等待消费者把 STOP 之前的数据处理完再退出

    assert sorted(consumer.results) == [0, 1, 2, 3, 4]


def test_pool_as_context_manager():
    input_queue = Queue()
    output_queue = Queue()
    for i in range(5):
        input_queue.put(i)

    with WorkerPool(SquareProcessor, input_queue, output_queue, num_workers=2):
        input_queue.join()

    assert sorted(output_queue.queue) == [i * i for i in range(5)]


def test_pipeline_as_context_manager():
    pipeline = _build(num_workers=2, total=20)
    with pipeline:
        pipeline.producer.join()

    assert sorted(pipeline.consumer.results) == [i * i for i in range(20)]


def test_pipeline_run_is_idempotent_to_double_stop():
    pipeline = _build(num_workers=2, total=10)
    pipeline.start()
    pipeline.producer.join()
    pipeline.stop()
    pipeline.stop()  # 第二次应该直接返回，不应该抛异常或重复排空

    assert sorted(pipeline.consumer.results) == [i * i for i in range(10)]


def test_producer_crash_is_raised_on_join():
    class BrokenProducer(BaseProducer):
        def on_start(self):
            raise RuntimeError("boom")

        def produce(self):
            raise StopIteration

    producer = BrokenProducer(Queue())
    producer.start()
    with pytest.raises(RuntimeError, match="boom"):
        producer.join()


def test_pool_worker_crash_is_raised_on_stop():
    class BrokenProcessor(BaseProcessor):
        def on_start(self):
            raise RuntimeError("cannot init")

        def process(self, item):
            return item

    input_queue = Queue()
    pool = WorkerPool(BrokenProcessor, input_queue, num_workers=1)
    pool.start()
    with pytest.raises(RuntimeError, match="cannot init"):
        pool.stop(drain=False)


def test_pipeline_run_propagates_producer_crash():
    class BrokenProducer(BaseProducer):
        def on_start(self):
            raise RuntimeError("producer boom")

        def produce(self):
            raise StopIteration

    pipeline = Pipeline.build(
        producer_cls=BrokenProducer,
        processor=SquareProcessor,
        consumer_cls=CollectConsumer,
    )
    with pytest.raises(RuntimeError, match="producer boom"):
        pipeline.run(install_signal_handlers=False)


def test_retry_then_dead_letter_queue():
    attempts_by_item = {}
    lock = threading.Lock()

    class FlakyProcessor(BaseProcessor):
        def process(self, item):
            with lock:
                attempts_by_item[item] = attempts_by_item.get(item, 0) + 1
            raise RuntimeError("always fails")

    input_queue = Queue()
    dead_letter_queue = Queue()
    for i in range(3):
        input_queue.put(i)

    pool = WorkerPool(
        FlakyProcessor,
        input_queue,
        num_workers=2,
        max_retries=2,
        dead_letter_queue=dead_letter_queue,
    )
    pool.start()
    pool.stop(drain=True)

    assert sorted(dead_letter_queue.queue) == [0, 1, 2]
    assert all(
        attempts == 3 for attempts in attempts_by_item.values()
    )  # 首次 + 2次重试
    assert pool.stats()["failed"] == 3
    assert pool.stats()["retries"] == 6


def test_multi_stage_pipeline():
    class AddOneProcessor(BaseProcessor):
        def process(self, item):
            return item + 1

    pipeline = Pipeline.build(
        producer_cls=RangeProducer,
        processor=SquareProcessor,
        consumer_cls=None,
        num_workers=2,
        producer_kwargs={"total": 5},
    )
    pipeline.add_stage(AddOneProcessor, num_workers=2)
    pipeline.set_consumer(CollectConsumer)

    pipeline.run(install_signal_handlers=False)

    assert sorted(pipeline.consumer.results) == [i * i + 1 for i in range(5)]


def test_stats_reports_progress():
    pipeline = _build(num_workers=2, total=10)
    pipeline.run(install_signal_handlers=False)

    stats = pipeline.stats()
    assert stats["producer"]["produced"] == 10
    assert stats["stages"][0]["processed"] == 10
    assert stats["consumer"]["consumed"] == 10


def test_many_fans_out_one_item_into_multiple():
    class DuplicateProcessor(BaseProcessor):
        def process(self, item):
            return Many([item, item * 10])

    pipeline = _build(processor=DuplicateProcessor, num_workers=2, total=5)
    pipeline.run(install_signal_handlers=False)

    expected = sorted(x for i in range(5) for x in (i, i * 10))
    assert sorted(pipeline.consumer.results) == expected


def test_many_can_be_empty_to_drop():
    class DropAllProcessor(BaseProcessor):
        def process(self, item):
            return Many([])

    pipeline = _build(processor=DropAllProcessor, num_workers=2, total=5)
    pipeline.run(install_signal_handlers=False)

    assert pipeline.consumer.results == []


def test_batch_processor_flushes_on_size():
    flushed_batches = []
    lock = threading.Lock()

    class SumBatchProcessor(BaseBatchProcessor):
        def __init__(self):
            super().__init__(batch_size=5)

        def process_batch(self, items):
            with lock:
                flushed_batches.append(list(items))
            return sum(items)

    input_queue = Queue()
    output_queue = Queue()
    for i in range(10):
        input_queue.put(i)

    pool = WorkerPool(SumBatchProcessor, input_queue, output_queue, num_workers=1)
    pool.start()
    pool.stop(drain=True)

    assert sorted(output_queue.queue) == sorted([sum(range(0, 5)), sum(range(5, 10))])
    assert sum(len(b) for b in flushed_batches) == 10


def test_batch_processor_flushes_on_timeout_via_on_idle():
    class SumBatchProcessor(BaseBatchProcessor):
        def __init__(self):
            super().__init__(batch_size=1000, batch_timeout=0.1)

        def process_batch(self, items):
            return sum(items)

    input_queue = Queue()
    output_queue = Queue()
    input_queue.put(1)
    input_queue.put(2)

    pool = WorkerPool(
        SumBatchProcessor, input_queue, output_queue, num_workers=1, poll_interval=0.05
    )
    pool.start()
    time.sleep(0.3)
    pool.stop(drain=False)

    assert list(output_queue.queue) == [3]


def test_batch_processor_drains_remaining_buffer_on_stop():
    class SumBatchProcessor(BaseBatchProcessor):
        def __init__(self):
            super().__init__(batch_size=1000)  # 永远攒不满，只能靠 on_drain 兜底

        def process_batch(self, items):
            return sum(items)

    input_queue = Queue()
    output_queue = Queue()
    for i in range(7):
        input_queue.put(i)

    pool = WorkerPool(SumBatchProcessor, input_queue, output_queue, num_workers=1)
    pool.start()
    pool.stop(drain=True)

    assert list(output_queue.queue) == [sum(range(7))]


def test_counting_queue_tracks_historical_total():
    q = CountingQueue(name="orders")
    assert q.name == "orders"
    assert q.put_count == 0

    for i in range(3):
        q.put(i)
    assert q.put_count == 3
    assert q.qsize() == 3

    q.get()
    assert q.qsize() == 2
    assert q.put_count == 3  # 历史总数不因 get() 而减少


def test_counting_queue_default_name():
    assert CountingQueue().name == "queue"


def test_worker_pool_format_progress_with_counting_queue():
    input_queue = CountingQueue(name="raw")
    output_queue = CountingQueue(name="squared")
    input_queue.put(1)
    input_queue.put(2)

    pool = WorkerPool(
        SquareProcessor, input_queue, output_queue, num_workers=2, name="squarer"
    )
    line = pool.format_progress()

    assert line == "raw(2/2) -- squarer(2线程/0) -- squared(0/0)"


def test_worker_pool_format_progress_with_plain_queue_shows_dash():
    input_queue = Queue()
    pool = WorkerPool(SquareProcessor, input_queue, num_workers=1, name="squarer")
    line = pool.format_progress()

    assert line == "input(0/-) -- squarer(1线程/0)"


def test_pipeline_build_and_stage_naming_produces_labeled_progress():
    pipeline = Pipeline.build(
        producer_cls=RangeProducer,
        processor=SquareProcessor,
        consumer_cls=CollectConsumer,
        num_workers=1,
        input_name="raw",
        output_name="result",
        processor_name="squarer",
        producer_kwargs={"total": 3},
    )

    assert pipeline.producer.output_queue.name == "raw"
    assert pipeline.pool.name == "squarer"
    assert pipeline.pool.output_queue.name == "result"

    pipeline.run(install_signal_handlers=False)

    line = pipeline.format_progress()
    assert "RangeProducer(已产出3)" in line
    assert "raw(0/3)" in line
    assert "squarer(1线程/3)" in line
    assert "result(0/3)" in line
    assert "CollectConsumer(已消费3)" in line


def test_pipeline_add_stage_and_set_consumer_queue_naming():
    class AddOneProcessor(BaseProcessor):
        def process(self, item):
            return item + 1

    pipeline = Pipeline.build(
        producer_cls=RangeProducer,
        processor=SquareProcessor,
        consumer_cls=None,
        num_workers=1,
        producer_kwargs={"total": 3},
    )
    pipeline.add_stage(AddOneProcessor, num_workers=1, queue_name="mid", name="add-one")
    pipeline.set_consumer(CollectConsumer, queue_name="final")

    assert pipeline.stages[0].output_queue.name == "mid"
    assert pipeline.stages[1].name == "add-one"
    assert pipeline.stages[1].output_queue.name == "final"

    pipeline.run(install_signal_handlers=False)
    assert sorted(pipeline.consumer.results) == [i * i + 1 for i in range(3)]


def test_pipeline_log_progress_does_not_raise():
    pipeline = _build(num_workers=1, total=3)
    pipeline.run(install_signal_handlers=False)
    pipeline.log_progress()  # 只要不抛异常即可
