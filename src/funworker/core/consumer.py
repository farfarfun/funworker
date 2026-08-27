"""消费者基类：从队列取数据做终端处理（落库、写文件等）。"""

from abc import abstractmethod
from queue import Empty, Queue
from typing import Any, Dict, Optional

from farlog import get_logger

from funworker.core.base import STOP, BaseWorker, format_queue_progress

logger = get_logger("funworker")


class BaseConsumer(BaseWorker):
    """下游消费者，子类只需实现 :meth:`consume`。

    Args:
        input_queue (Queue): 数据来源队列。
        get_timeout (float, optional): 队列取数据的超时时间，用于定期检查停止信号，默认0.5秒。
    """

    def __init__(
        self,
        input_queue: Queue,
        *,
        get_timeout: float = 0.5,
        name: Optional[str] = None,
    ):
        super().__init__(name=name)
        self.input_queue = input_queue
        self.get_timeout = get_timeout
        self._consumed = 0
        self._failed = 0

    @abstractmethod
    def consume(self, item: Any) -> None:
        """消费一条数据，由子类实现，无需返回值。"""

    def on_error(self, item: Any, exc: Exception) -> None:
        """消费单条数据抛出异常时调用，默认记录日志并跳过该条数据。"""
        logger.exception(f"consume error, item={item!r}, err={exc}")

    def _loop(self) -> None:
        while True:
            try:
                item = self.input_queue.get(timeout=self.get_timeout)
            except Empty:
                if self.stopping:
                    break
                continue

            try:
                if item is STOP:
                    break
                self.consume(item)
            except Exception as exc:
                self._failed += 1
                self.on_error(item, exc)
            else:
                self._consumed += 1
            finally:
                self.input_queue.task_done()

    def stats(self) -> Dict[str, Any]:
        """返回运行状态：已消费/失败条数、上游队列长度、线程是否存活。"""
        return {
            "consumed": self._consumed,
            "failed": self._failed,
            "input_qsize": self.input_queue.qsize(),
            "alive": self.is_alive(),
        }

    def format_progress(self) -> str:
        """按 "X(当前/历史) -- X(已消费)" 格式生成一行日志文本，与
        `BaseProducer.format_progress()`/`WorkerPool.format_progress()` 拼在一起即可看到
        整条流水线各阶段的进度。
        """
        return f"{format_queue_progress(self.input_queue, 'input')} -- {self.name}(已消费{self._consumed})"

    def log_progress(self) -> None:
        """把 :meth:`format_progress` 的结果输出一条 INFO 日志。"""
        logger.info(self.format_progress())
