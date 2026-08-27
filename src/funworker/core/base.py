"""常驻线程worker的公共基类与队列哨兵。"""

import threading
from abc import ABC, abstractmethod
from queue import Queue
from typing import Optional

from farlog import get_logger

logger = get_logger("funworker")

STOP = object()
"""放入队列中的哨兵对象，worker取到该对象即结束当前循环。"""

SKIP = object()
"""`produce()`/`process()` 的返回值哨兵，表示"这条不用往下游发"。

与 `None` 区分开，是为了让 `None` 可以作为正常的业务数据值使用。
"""


class Many:
    """`process()`/`on_idle()`/`on_drain()` 的返回值包装：表示这一次要产出多条结果，
    逐条放入下游队列（一拆多 / fan-out）。

    与直接返回 list 区分开，是为了不把"业务值本身就是一个 list"误判为要拆分成多条；
    只有显式包装成 `Many(...)` 才会被逐条发送。
    """

    __slots__ = ("items",)

    def __init__(self, items):
        self.items = list(items)

    def __repr__(self) -> str:
        return f"Many({self.items!r})"


class CountingQueue(Queue):
    """带名字 + 累计计数的队列，完全兼容 `queue.Queue`，可以在任何需要普通队列的地方直接替换使用。

    `qsize()` 反映"当前"积压；`put_count` 额外累计"历史"总共放入过多少条，配合
    `WorkerPool.format_progress()` 这类日志方法输出"入X(当前/历史)"格式的进度信息。
    只在需要按名字/累计数追踪某条队列时才需要用它替换普通 `Queue`，其它场景用普通
    `Queue` 完全不受影响。

    Args:
        maxsize (int, optional): 同 `queue.Queue`，默认0（不限）。
        name (Optional[str], optional): 队列名字，用于日志展示，默认"queue"。
    """

    def __init__(self, maxsize: int = 0, *, name: Optional[str] = None):
        super().__init__(maxsize=maxsize)
        self.name = name or "queue"
        self._put_count = 0
        self._put_count_lock = threading.Lock()

    def put(self, item, block: bool = True, timeout: Optional[float] = None) -> None:
        super().put(item, block=block, timeout=timeout)
        if item is STOP:
            return  # STOP 只是停止信号，不是业务数据，不计入历史总数
        with self._put_count_lock:
            self._put_count += 1

    @property
    def put_count(self) -> int:
        """历史累计放入总数（不因 `get()`/`task_done()` 而减少）。"""
        with self._put_count_lock:
            return self._put_count


def format_queue_progress(q: Optional[Queue], fallback: str) -> str:
    """生成 "名字(当前/历史)" 这一段进度文本，供 Producer/WorkerPool/Consumer 的
    `format_progress()` 复用。

    `q` 为 `CountingQueue` 时用它自带的 `name`/`put_count`；普通 `queue.Queue` 没有
    这两个属性，分别用 `fallback` 和占位符 "-" 兜底。
    """
    if q is None:
        return f"{fallback}(0/-)"
    name = getattr(q, "name", None) or fallback
    total = getattr(q, "put_count", None)
    return f"{name}({q.qsize()}/{total if total is not None else '-'})"


class BaseWorker(threading.Thread, ABC):
    """所有常驻线程角色（生产者、消费者）的公共基类。

    子类只需要实现 :meth:`_loop`；`on_start`/`on_stop` 是可选的生命周期钩子，
    用于初始化/释放线程内资源（如数据库连接、http session）。

    线程内抛出的未捕获异常会被记录到 `self._error`，并在 :meth:`join` 时重新抛出，
    避免线程静默崩溃却没有任何人知道。
    """

    def __init__(self, *, name: Optional[str] = None):
        super().__init__(name=name or self.__class__.__name__, daemon=True)
        self._stop_event = threading.Event()
        self._error: Optional[BaseException] = None

    def on_start(self) -> None:
        """线程启动时调用一次。"""

    def on_stop(self) -> None:
        """线程退出前调用一次（即使 `on_start`/`_loop` 抛出异常也会执行）。"""

    def request_stop(self) -> None:
        """异步请求线程结束，不阻塞等待。"""
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def run(self) -> None:
        try:
            self.on_start()
            self._loop()
        except BaseException as exc:  # noqa: BLE001 - 保存下来交给 join() 时抛出
            self._error = exc
            logger.exception(f"{self.name} crashed")
        finally:
            try:
                self.on_stop()
            except BaseException as exc:  # noqa: BLE001
                if self._error is None:
                    self._error = exc
                logger.exception(f"{self.name} on_stop error")

    @abstractmethod
    def _loop(self) -> None:
        """线程主循环，需要自行响应 `self.stopping`。"""

    def raise_if_failed(self) -> None:
        """如果线程运行期间出现未捕获异常，重新抛出给调用方。"""
        if self._error is not None:
            raise self._error

    def join(self, timeout: Optional[float] = None) -> None:
        """等待线程结束；线程已结束且运行期间出错时，会重新抛出该异常。"""
        super().join(timeout=timeout)
        if not self.is_alive():
            self.raise_if_failed()

    def __enter__(self) -> "BaseWorker":
        """进入 `with` 块时启动线程。"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """退出 `with` 块时请求停止并等待线程结束。

        如果 `with` 块内已经有异常在传播，则不会用线程内部的异常掩盖它（只记录日志）。
        """
        self.request_stop()
        try:
            self.join()
        except BaseException:
            if exc_type is None:
                raise
            logger.exception(
                f"{self.name} error while stopping (suppressed, original exception takes priority)"
            )
        return False
