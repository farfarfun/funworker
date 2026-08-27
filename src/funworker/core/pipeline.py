"""Pipeline：把 Producer / 多级 WorkerPool / Consumer 串成完整流水线，统一管理生命周期。"""

import signal
import threading
from queue import Queue
from typing import Any, Dict, List, Optional, Type, Union

from farlog import get_logger

from funworker.core.base import STOP, CountingQueue, format_queue_progress
from funworker.core.consumer import BaseConsumer
from funworker.core.pool import ProcessorFactory, WorkerPool
from funworker.core.processor import BaseProcessor
from funworker.core.producer import BaseProducer

logger = get_logger("funworker")

_STOPPABLE_SIGNALS = (signal.SIGINT, signal.SIGTERM)


class Pipeline:
    """生产者 -> 队列 -> 一级或多级处理单元线程池 -> 队列 -> 消费者。

    Args:
        producer (BaseProducer): 生产者，负责向第一级 `WorkerPool.input_queue` 写入数据。
        pool (WorkerPool): 第一级处理单元线程池；如需多级处理，用 :meth:`add_stage` 继续追加。
        consumer (Optional[BaseConsumer], optional): 消费者，读取最后一级的 `output_queue`；
            为空表示处理单元的输出即为流水线终点。
    """

    def __init__(
        self,
        producer: BaseProducer,
        pool: WorkerPool,
        consumer: Optional[BaseConsumer] = None,
    ):
        self.producer = producer
        self.stages: List[WorkerPool] = [pool]
        self.consumer = consumer
        self._started = False
        self._stop_lock = threading.Lock()
        self._stopped = False

    @property
    def pool(self) -> WorkerPool:
        """兼容属性：第一级处理单元线程池，等价于 `self.stages[0]`。"""
        return self.stages[0]

    @classmethod
    def build(
        cls,
        producer_cls: Type[BaseProducer],
        processor: Union[BaseProcessor, ProcessorFactory],
        consumer_cls: Optional[Type[BaseConsumer]] = None,
        *,
        num_workers: Optional[int] = None,
        input_maxsize: int = 0,
        output_maxsize: int = 0,
        input_name: str = "input",
        output_name: str = "output",
        processor_name: str = "worker-pool",
        producer_kwargs: Optional[Dict[str, Any]] = None,
        consumer_kwargs: Optional[Dict[str, Any]] = None,
    ) -> "Pipeline":
        """便捷构造：自动创建两条队列，下游只需要提供 Producer/Processor/Consumer 的实现。

        Args:
            producer_cls (Type[BaseProducer]): 生产者类，会以 `output_queue=<自动创建的队列>` 实例化。
            processor (BaseProcessor | Callable[[], BaseProcessor]): 处理单元实例或工厂函数。
            consumer_cls (Optional[Type[BaseConsumer]], optional): 消费者类；为空表示不需要消费者，
                处理单元的输出留在队列里由调用方自行处理，或者后续用 :meth:`set_consumer` 追加。
            num_workers (int, optional): 处理单元线程数，默认取检测到的 CPU 核数。
            input_maxsize (int, optional): 生产者->处理单元 队列容量上限，默认0（不限）。
            output_maxsize (int, optional): 处理单元->消费者 队列容量上限，默认0（不限）。
            input_name (str, optional): 生产者->处理单元 队列的名字，用于 :meth:`format_progress`
                日志展示，默认"input"。
            output_name (str, optional): 处理单元->消费者 队列的名字，同上，默认"output"。
            processor_name (str, optional): 第一级处理单元线程池的名字，同上，默认"worker-pool"。
            producer_kwargs (Optional[dict], optional): 透传给 `producer_cls` 构造函数的其它参数。
            consumer_kwargs (Optional[dict], optional): 透传给 `consumer_cls` 构造函数的其它参数。
        """
        input_queue: Queue = CountingQueue(maxsize=input_maxsize, name=input_name)
        output_queue: Optional[Queue] = (
            CountingQueue(maxsize=output_maxsize, name=output_name)
            if consumer_cls is not None
            else None
        )

        producer = producer_cls(output_queue=input_queue, **(producer_kwargs or {}))
        pool = WorkerPool(
            processor,
            input_queue,
            output_queue,
            num_workers=num_workers,
            name=processor_name,
        )
        consumer = (
            consumer_cls(input_queue=output_queue, **(consumer_kwargs or {}))
            if consumer_cls is not None
            else None
        )

        return cls(producer=producer, pool=pool, consumer=consumer)

    def add_stage(
        self,
        processor: Union[BaseProcessor, ProcessorFactory],
        *,
        num_workers: Optional[int] = None,
        maxsize: int = 0,
        queue_name: str = "output",
        **pool_kwargs: Any,
    ) -> "Pipeline":
        """在当前末级处理单元后面追加一级处理：上一级的输出队列作为新一级的输入队列。

        必须在 `start()`/`run()` 之前调用（且此时末级还没有绑定 consumer）。返回 `self` 方便链式调用。

        Args:
            queue_name (str, optional): 新建的中间队列名字（仅当上一级还没有输出队列时生效），
                用于 :meth:`format_progress` 日志展示，默认"output"。新一级处理单元自身的名字
                可以通过 `pool_kwargs` 里的 `name=` 传入。
        """
        if self._started:
            raise RuntimeError("cannot add_stage after the pipeline has started")
        if self.consumer is not None:
            raise RuntimeError("cannot add_stage after a consumer has been attached")

        last_stage = self.stages[-1]
        if last_stage.output_queue is None:
            last_stage.output_queue = CountingQueue(maxsize=maxsize, name=queue_name)
        new_pool = WorkerPool(
            processor, last_stage.output_queue, num_workers=num_workers, **pool_kwargs
        )
        self.stages.append(new_pool)
        return self

    def set_consumer(
        self,
        consumer_cls: Type[BaseConsumer],
        *,
        maxsize: int = 0,
        queue_name: str = "output",
        **consumer_kwargs: Any,
    ) -> "Pipeline":
        """把消费者挂到当前末级处理单元的输出队列上。必须在 `start()`/`run()` 之前调用。

        Args:
            queue_name (str, optional): 新建的队列名字（仅当末级还没有输出队列时生效），用于
                :meth:`format_progress` 日志展示，默认"output"。
        """
        if self._started:
            raise RuntimeError("cannot set_consumer after the pipeline has started")

        last_stage = self.stages[-1]
        if last_stage.output_queue is None:
            last_stage.output_queue = CountingQueue(maxsize=maxsize, name=queue_name)
        self.consumer = consumer_cls(
            input_queue=last_stage.output_queue, **consumer_kwargs
        )
        return self

    def start(self) -> None:
        """启动整条流水线（从下游到上游依次启动，保证队列消费方先就绪）。"""
        self._started = True
        if self.consumer is not None:
            self.consumer.start()
        for stage in reversed(self.stages):
            stage.start()
        self.producer.start()

    def stop(self) -> None:
        """从上游到下游依次优雅停止：先让生产者停下，再逐级排空处理单元，最后停消费者。

        重复调用是安全的（第二次起直接返回）。
        """
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
        self.producer.request_stop()
        self.producer.join()
        for stage in self.stages:
            stage.stop(drain=True)
        if self.consumer is not None:
            self.consumer.input_queue.put(STOP)
            self.consumer.join()

    def run(self, *, install_signal_handlers: bool = True) -> None:
        """启动流水线并阻塞运行，直到以下任一情况发生后优雅停止并返回：

        - 生产者自然结束（`produce()` 抛出 `StopIteration`）——用于有限批处理场景；
        - 进程收到 `SIGINT`/`SIGTERM`（仅在主线程调用时会安装信号处理器）——用于常驻/流式场景；
        - 其他代码在另一个线程调用了 `pipeline.stop()`。

        任一阶段线程崩溃时，异常会在这里被重新抛出。

        Args:
            install_signal_handlers (bool, optional): 是否捕获 `SIGINT`/`SIGTERM` 并转为优雅停止，
                默认True；仅在当前线程是主线程时生效（子线程/测试环境中会自动跳过）。
        """
        self.start()

        old_handlers: Dict[int, Any] = {}
        can_install = (
            install_signal_handlers
            and threading.current_thread() is threading.main_thread()
        )
        if can_install:

            def _handle_signal(signum, frame):  # noqa: ANN001
                logger.info(f"received signal {signum}, stopping pipeline...")
                self.producer.request_stop()

            for sig in _STOPPABLE_SIGNALS:
                old_handlers[sig] = signal.signal(sig, _handle_signal)

        try:
            while self.producer.is_alive():
                self.producer.join(timeout=0.5)
        finally:
            for sig, handler in old_handlers.items():
                signal.signal(sig, handler)
            self.stop()

    def stats(self) -> Dict[str, Any]:
        """聚合各阶段的运行状态，便于监控队列积压、处理/失败/重试计数。"""
        data: Dict[str, Any] = {
            "producer": self.producer.stats(),
            "stages": [stage.stats() for stage in self.stages],
        }
        if self.consumer is not None:
            data["consumer"] = self.consumer.stats()
        return data

    def format_progress(self) -> str:
        """输出整条流水线一行进度文本，每条队列只出现一次（挂在写入方那一侧）：

        生产者自带它写入的第一条队列，消费者自带它读取的最后一条队列；中间每一级处理单元只
        在"下一站还是另一级处理单元"时才带上自己的输出队列（下一站是消费者时，队列已经由
        消费者带出，不再重复）。
        """
        parts = [self.producer.format_progress()]
        for i, stage in enumerate(self.stages):
            stats = stage.stats()
            segment = f"{stage.name}({stats['num_workers']}线程/{stats['processed']})"
            is_last_stage = i == len(self.stages) - 1
            if stage.output_queue is not None and not (
                is_last_stage and self.consumer is not None
            ):
                segment += f" -- {format_queue_progress(stage.output_queue, 'output')}"
            parts.append(segment)
        if self.consumer is not None:
            parts.append(self.consumer.format_progress())
        return " -- ".join(parts)

    def log_progress(self) -> None:
        """把 :meth:`format_progress` 的结果输出一条 INFO 日志，方便定期调用做整条流水线的进度追踪。"""
        logger.info(self.format_progress())

    def __enter__(self) -> "Pipeline":
        """进入 `with` 块时启动整条流水线。"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """退出 `with` 块时优雅停止整条流水线。

        如果 `with` 块内已经有异常在传播，则不会用流水线内部的异常掩盖它（只记录日志）。
        """
        try:
            self.stop()
        except BaseException:
            if exc_type is None:
                raise
            logger.exception(
                "error while stopping pipeline (suppressed, original exception takes priority)"
            )
        return False
