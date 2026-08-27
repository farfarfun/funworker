"""批处理处理单元：攒够数量或超过时间阈值后，一次性处理一批数据（多合一 / fan-in）。"""

import time
from abc import abstractmethod
from typing import Any, List, Optional

from funworker.core.base import SKIP
from funworker.core.processor import BaseProcessor


class BaseBatchProcessor(BaseProcessor):
    """按数量/时间攒批的处理单元，子类只需要实现 :meth:`process_batch`。

    - 每来一条数据先缓冲，缓冲区达到 `batch_size` 时立即触发一次 `process_batch`。
    - 缓冲区未满时，从第一条数据进入缓冲区起超过 `batch_timeout` 秒也会强制触发一次
      `process_batch`——依赖 `WorkerPool` 的空闲轮询（`on_idle`），因此实际触发时间
      存在最多约一个 `WorkerPool.poll_interval` 的误差，不是精确定时器。
    - 优雅停止时（收到 STOP 信号后），缓冲区里剩余的数据会通过 `on_drain` 强制产出一次，
      不会丢数据。

    每个工作线程持有独立的处理单元实例（`WorkerPool` 默认行为），因此缓冲区不需要跨线程
    加锁；如果需要"整个线程池共享同一个批次"，请把 `WorkerPool(num_workers=1)` 配合使用。

    Args:
        batch_size (int, optional): 攒够多少条触发一次批处理，默认32。
        batch_timeout (Optional[float], optional): 缓冲区未满时的最长等待秒数，默认None
            表示不做超时触发（只按数量攒批）。
    """

    def __init__(self, *, batch_size: int = 32, batch_timeout: Optional[float] = None):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self._buffer: List[Any] = []
        self._buffer_started_at: Optional[float] = None

    @abstractmethod
    def process_batch(self, items: List[Any]) -> Any:
        """处理一批数据，由子类实现。

        Args:
            items (List[Any]): 攒够的一批数据，长度在 `1` 到 `batch_size` 之间。

        Returns:
            Any: 产出结果，语义与 `BaseProcessor.process` 一致：`funworker.SKIP` 表示
                这一批不产出；`funworker.Many([...])` 表示产出多条；其它值表示产出一条。
        """

    def process(self, item: Any) -> Any:
        if self._buffer_started_at is None:
            self._buffer_started_at = time.monotonic()
        self._buffer.append(item)
        if len(self._buffer) >= self.batch_size:
            return self._flush()
        return SKIP

    def on_idle(self) -> Any:
        if (
            self._buffer
            and self.batch_timeout is not None
            and time.monotonic() - self._buffer_started_at >= self.batch_timeout
        ):
            return self._flush()
        return SKIP

    def on_drain(self) -> Any:
        if self._buffer:
            return self._flush()
        return SKIP

    def _flush(self) -> Any:
        items, self._buffer = self._buffer, []
        self._buffer_started_at = None
        return self.process_batch(items)
