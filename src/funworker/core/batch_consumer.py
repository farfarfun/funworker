"""批量消费者基类：攒够数量或超过时间阈值后，一次性批量写入（落库、写文件等）。"""

import time
from abc import abstractmethod
from queue import Empty, Queue
from typing import Any, List, Optional

from farlog import get_logger

from funworker.core.base import STOP
from funworker.core.consumer import BaseConsumer

logger = get_logger("funworker")


class BaseBatchConsumer(BaseConsumer):
    """按数量/时间攒批的消费者，子类只需要实现 :meth:`consume_batch`。

    - 每来一条数据先缓冲，缓冲区达到 `batch_size` 时立即触发一次 `consume_batch`。
    - 缓冲区未满时，从第一条数据进入缓冲区起超过 `batch_timeout` 秒也会强制触发一次
      `consume_batch`——依赖 `get_timeout` 轮询，因此实际触发时间存在最多约一个
      `get_timeout` 的误差，不是精确定时器。
    - 优雅停止时（收到 STOP 信号后），缓冲区里剩余的数据会强制消费一次，不会丢数据。

    Args:
        input_queue (Queue): 数据来源队列。
        batch_size (int, optional): 攒够多少条触发一次批量消费，默认100。
        batch_timeout (float, optional): 缓冲区未满时的最长等待秒数，默认10秒。
        get_timeout (float, optional): 队列取数据的超时时间，同时也是检查
            `batch_timeout` 是否到期的轮询间隔，默认0.5秒（应明显小于 `batch_timeout`，
            否则超时触发会有较大延迟误差）。
        name (Optional[str], optional): 线程名。
    """

    def __init__(
        self,
        input_queue: Queue,
        *,
        batch_size: int = 100,
        batch_timeout: float = 10.0,
        get_timeout: float = 0.5,
        name: Optional[str] = None,
    ):
        super().__init__(input_queue, get_timeout=get_timeout, name=name)
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self._buffer: List[Any] = []
        self._buffer_started_at: Optional[float] = None

    @abstractmethod
    def consume_batch(self, items: List[Any]) -> None:
        """消费一批数据，由子类实现，无需返回值。

        Args:
            items (List[Any]): 攒够的一批数据，长度在 `1` 到 `batch_size` 之间。
        """

    def consume(self, item: Any) -> None:
        """`BaseConsumer` 要求实现的单条接口，批量消费者不走这条路径，不会被调用。"""

    def on_batch_error(self, items: List[Any], exc: Exception) -> None:
        """消费一批数据抛出异常时调用，默认记录日志并跳过整批。"""
        logger.exception(f"consume_batch error, size={len(items)}, err={exc}")

    def _loop(self) -> None:
        while True:
            try:
                item = self.input_queue.get(timeout=self.get_timeout)
            except Empty:
                self._flush_if_timeout()
                if self.stopping:
                    break
                continue

            try:
                if item is STOP:
                    break
                self._buffer.append(item)
                if self._buffer_started_at is None:
                    self._buffer_started_at = time.monotonic()
                if len(self._buffer) >= self.batch_size:
                    self._flush()
            finally:
                self.input_queue.task_done()

        self._flush()  # 停止前把缓冲区剩余数据消费掉，不丢数据

    def _flush_if_timeout(self) -> None:
        if (
            self._buffer
            and time.monotonic() - self._buffer_started_at >= self.batch_timeout
        ):
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        items, self._buffer = self._buffer, []
        self._buffer_started_at = None
        try:
            self.consume_batch(items)
        except Exception as exc:
            self._failed += len(items)
            self.on_batch_error(items, exc)
        else:
            self._consumed += len(items)
