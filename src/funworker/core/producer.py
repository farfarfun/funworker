"""生产者基类：持续产出数据并写入下游队列。"""

import time
from abc import abstractmethod
from queue import Full, Queue
from typing import Any, Dict, Optional

from farlog import get_logger

from funworker.core.base import SKIP, BaseWorker, format_queue_progress

logger = get_logger("funworker")

_PUT_POLL_INTERVAL = 0.5


class BaseProducer(BaseWorker):
    """上游生产者，子类只需实现 :meth:`produce`。

    - 返回一条数据即写入 `output_queue`（`None` 也是合法的业务值，会被正常写入）。
    - 返回 `funworker.SKIP` 表示这一轮没有数据，直接进入下一轮
      （可配合 `interval` 降低空转频率）。
    - 抛出 `StopIteration` 表示数据已生产完毕，生产者会主动结束（用于有限任务场景）。

    Args:
        output_queue (Queue): 生产出的数据要写入的队列。
        interval (float, optional): 每轮之间的休眠秒数，默认0表示不休眠。
    """

    def __init__(
        self, output_queue: Queue, *, interval: float = 0, name: Optional[str] = None
    ):
        super().__init__(name=name)
        self.output_queue = output_queue
        self.interval = interval
        self._produced = 0

    @abstractmethod
    def produce(self) -> Any:
        """生产一条数据，由子类实现具体的取数逻辑。"""

    def _put(self, item: Any) -> None:
        """把一条数据写入下游队列；队列满时定期检查停止信号，避免停止请求被无限阻塞。"""
        while True:
            try:
                self.output_queue.put(item, timeout=_PUT_POLL_INTERVAL)
                self._produced += 1
                return
            except Full:
                if self.stopping:
                    logger.warning(
                        f"{self.name} dropped one item while shutting down (output queue full)"
                    )
                    return

    def _loop(self) -> None:
        while not self.stopping:
            try:
                item = self.produce()
            except StopIteration:
                logger.info(f"{self.name} finished producing")
                break
            except Exception:
                logger.exception(f"{self.name} produce error")
                continue

            if item is not SKIP:
                self._put(item)

            if self.interval:
                time.sleep(self.interval)

    def stats(self) -> Dict[str, Any]:
        """返回运行状态：已生产条数、下游队列长度、线程是否存活。"""
        return {
            "produced": self._produced,
            "output_qsize": self.output_queue.qsize(),
            "alive": self.is_alive(),
        }

    def format_progress(self) -> str:
        """按 "X(已产出) -- X(当前/历史)" 格式生成一行日志文本，与
        `WorkerPool.format_progress()`/`BaseConsumer.format_progress()` 拼在一起即可看到
        整条流水线各阶段的进度。
        """
        return f"{self.name}(已产出{self._produced}) -- {format_queue_progress(self.output_queue, 'output')}"

    def log_progress(self) -> None:
        """把 :meth:`format_progress` 的结果输出一条 INFO 日志。"""
        logger.info(self.format_progress())
