"""处理单元基类：只关心"一条数据怎么处理"，不感知线程/队列细节。"""

from abc import ABC, abstractmethod
from typing import Any

from farlog import get_logger

from funworker.core.base import SKIP

logger = get_logger("funworker")


class BaseProcessor(ABC):
    """处理单元，子类只需实现 :meth:`process`。

    `WorkerPool` 会按需为每个工作线程创建独立实例，因此处理单元内部持有的资源
    （数据库连接、http session 等）应当视为线程私有，不要在多个实例间共享
    不支持并发访问的对象。
    """

    def on_start(self) -> None:
        """所在线程启动时调用一次，用于初始化线程私有资源。"""

    def on_stop(self) -> None:
        """所在线程退出前调用一次，用于释放线程私有资源。"""

    @abstractmethod
    def process(self, item: Any) -> Any:
        """处理一条数据。

        Args:
            item (Any): 从上游队列取出的数据。

        Returns:
            Any: 处理结果，会被放入下游队列；`None` 也是合法的业务值。
                返回 `funworker.SKIP` 表示丢弃该条数据、不进入下游队列。
                返回 `funworker.Many([...])` 表示这一次要拆成多条结果分别进入
                下游队列（一拆多 / fan-out），常见于批处理或展开型转换。
        """

    def on_idle(self) -> Any:
        """`WorkerPool` 轮询上游队列超时（一段时间没有新数据）时调用一次。

        默认不产出（返回 `funworker.SKIP`）。主要用于需要"攒够时间也要出一批"的
        场景（如批处理），返回值语义与 :meth:`process` 完全一致。

        注意：只有在没有新数据到达时才会被检查到，触发时机存在最多一个
        `WorkerPool.poll_interval` 的误差，不是精确定时器。
        """
        return SKIP

    def on_drain(self) -> Any:
        """`WorkerPool` 收到停止信号、即将结束该线程前调用一次（在 `on_stop` 之前）。

        默认不产出（返回 `funworker.SKIP`）。主要用于把尚未攒够触发条件的残留状态
        （如批处理里还没攒够的缓冲区）强制产出一次，避免优雅停止时丢数据。
        返回值语义与 :meth:`process` 完全一致。
        """
        return SKIP

    def on_error(self, item: Any, exc: Exception, *, will_retry: bool = False) -> None:
        """处理单条数据抛出异常时调用，默认记录日志（并在不再重试时丢弃该条数据）。

        Args:
            item (Any): 处理失败的数据。
            exc (Exception): 捕获到的异常。
            will_retry (bool, optional): `WorkerPool` 配置了 `max_retries` 且还有重试次数时为
                `True`，此时数据会被重新放回队列，不会丢失。
        """
        action = "retrying" if will_retry else "dropping"
        logger.exception(f"process error ({action}), item={item!r}, err={exc}")
