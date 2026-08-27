"""WorkerPool：把一个处理单元包装成多线程执行的下游处理层。"""

import os
import threading
from queue import Empty, Queue
from threading import Thread
from typing import Any, Callable, Dict, List, Optional, Union

from farlog import get_logger

from funworker.core.base import SKIP, STOP, Many, format_queue_progress
from funworker.core.processor import BaseProcessor

logger = get_logger("funworker")

ProcessorFactory = Callable[[], BaseProcessor]


def default_num_workers() -> int:
    """默认线程池大小：取检测到的 CPU 核数（检测失败时退化为1）。"""
    return os.cpu_count() or 1


class _Envelope:
    """内部使用：包装一条正在重试中的数据，记录已经重试了多少次。"""

    __slots__ = ("item", "attempts")

    def __init__(self, item: Any, attempts: int):
        self.item = item
        self.attempts = attempts


class WorkerPool:
    """多线程处理单元包装器：从 `input_queue` 取数据交给处理单元处理，结果写入 `output_queue`。

    Args:
        processor (BaseProcessor | Callable[[], BaseProcessor]): 处理单元实例（多个线程共享，
            要求线程安全），或者用于给每个线程创建独立实例的工厂函数/类（推荐，处理单元可以
            放心持有连接等非线程安全资源）。
        input_queue (Queue): 上游队列。
        output_queue (Optional[Queue], optional): 下游队列，为空表示处理单元是流水线终点。
        num_workers (int, optional): 线程数，默认取检测到的 CPU 核数。
        max_retries (int, optional): 处理失败后的最大重试次数，默认0表示不重试。
        dead_letter_queue (Optional[Queue], optional): 重试耗尽后写入的死信队列，默认None表示
            直接丢弃（仅记录日志）。
        poll_interval (float, optional): 取不到新数据时的轮询超时，默认0.5秒；每次超时都会调用
            一次 `processor.on_idle()`（用于批处理超时触发等场景），同时用于定期检查停止信号。
        name (str, optional): 线程名前缀。
    """

    def __init__(
        self,
        processor: Union[BaseProcessor, ProcessorFactory],
        input_queue: Queue,
        output_queue: Optional[Queue] = None,
        *,
        num_workers: Optional[int] = None,
        max_retries: int = 0,
        dead_letter_queue: Optional[Queue] = None,
        poll_interval: float = 0.5,
        name: str = "worker-pool",
    ):
        is_instance = isinstance(processor, BaseProcessor)
        self._make_processor: ProcessorFactory = (
            (lambda: processor) if is_instance else processor
        )
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.num_workers = (
            num_workers if num_workers is not None else default_num_workers()
        )
        self.max_retries = max_retries
        self.dead_letter_queue = dead_letter_queue
        self.poll_interval = poll_interval
        self.name = name
        self._threads: List[Thread] = []
        self._processed = 0
        self._failed = 0
        self._retries = 0
        self._counter_lock = threading.Lock()
        self._errors: List[BaseException] = []
        self._errors_lock = threading.Lock()
        self._stop_lock = threading.Lock()
        self._stopped = False

    def start(self) -> None:
        """启动所有工作线程。"""
        for i in range(self.num_workers):
            t = Thread(target=self._run, name=f"{self.name}-{i}", daemon=True)
            self._threads.append(t)
            t.start()

    def _record_error(self, exc: BaseException) -> None:
        logger.exception(f"{self.name} worker crashed")
        with self._errors_lock:
            self._errors.append(exc)

    def _emit(self, result: Any) -> None:
        """把 `process()`/`on_idle()`/`on_drain()` 的返回值按约定语义写入下游队列。"""
        if result is SKIP or self.output_queue is None:
            return
        if isinstance(result, Many):
            for one in result.items:
                self.output_queue.put(one)
        else:
            self.output_queue.put(result)

    def _run(self) -> None:
        try:
            processor = self._make_processor()
            processor.on_start()
        except BaseException as exc:  # noqa: BLE001
            self._record_error(exc)
            return

        try:
            while True:
                try:
                    raw = self.input_queue.get(timeout=self.poll_interval)
                except Empty:
                    try:
                        idle_result = processor.on_idle()
                    except Exception:  # noqa: BLE001 - 空闲触发失败不应该打断整条流水线
                        logger.exception(f"{self.name} on_idle error")
                    else:
                        self._emit(idle_result)
                    continue

                try:
                    if raw is STOP:
                        break
                    item, attempts = (
                        (raw.item, raw.attempts)
                        if isinstance(raw, _Envelope)
                        else (raw, 0)
                    )
                    try:
                        result = processor.process(item)
                    except Exception as exc:
                        will_retry = attempts < self.max_retries
                        with self._counter_lock:
                            if will_retry:
                                self._retries += 1
                            else:
                                self._failed += 1
                        processor.on_error(item, exc, will_retry=will_retry)
                        if will_retry:
                            self.input_queue.put(_Envelope(item, attempts + 1))
                        elif self.dead_letter_queue is not None:
                            self.dead_letter_queue.put(item)
                    else:
                        with self._counter_lock:
                            self._processed += 1
                        self._emit(result)
                finally:
                    self.input_queue.task_done()
        except BaseException as exc:  # noqa: BLE001
            self._record_error(exc)
        finally:
            try:
                drain_result = processor.on_drain()
            except Exception:  # noqa: BLE001 - drain 失败不应该掩盖 on_stop / 之前的错误
                logger.exception(f"{self.name} on_drain error")
            else:
                try:
                    self._emit(drain_result)
                except Exception:  # noqa: BLE001
                    logger.exception(f"{self.name} failed to emit drained result")
            try:
                processor.on_stop()
            except BaseException as exc:  # noqa: BLE001
                self._record_error(exc)

    def raise_if_failed(self) -> None:
        """如果任一工作线程运行期间出现未捕获异常，重新抛出第一个。"""
        with self._errors_lock:
            errors = list(self._errors)
        if errors:
            raise errors[0]

    def stop(self, drain: bool = True) -> None:
        """停止所有工作线程；重复调用是安全的（第二次起直接返回）。

        Args:
            drain (bool, optional): 是否等待 `input_queue` 中已有数据（含重试中的数据）
                全部处理完再停止，默认True。
        """
        with self._stop_lock:
            if self._stopped:
                return
            self._stopped = True
        if drain:
            self.input_queue.join()
        for _ in self._threads:
            self.input_queue.put(STOP)
        for t in self._threads:
            t.join()
        self.raise_if_failed()

    def join(self) -> None:
        for t in self._threads:
            t.join()
        self.raise_if_failed()

    def stats(self) -> Dict[str, Any]:
        """返回运行状态：处理/失败/重试计数、队列长度、存活线程数。"""
        with self._counter_lock:
            processed, failed, retries = self._processed, self._failed, self._retries
        return {
            "num_workers": self.num_workers,
            "alive_workers": sum(1 for t in self._threads if t.is_alive()),
            "input_qsize": self.input_queue.qsize(),
            "output_qsize": self.output_queue.qsize()
            if self.output_queue is not None
            else None,
            "processed": processed,
            "failed": failed,
            "retries": retries,
        }

    def format_progress(self) -> str:
        """按 "X(当前/历史) -- X(线程数/已处理) -- X(当前/历史)" 格式生成一行日志文本。

        输入/输出队列如果是 `funworker.CountingQueue`，"历史"处会显示累计放入总数；
        普通 `queue.Queue` 没有这个计数，会显示占位符 "-"。每一段的名字都取自对应队列/
        处理单元的 `name`，没设置则用 "input"/"output" 兜底。
        """
        with self._counter_lock:
            processed = self._processed
        parts = [
            format_queue_progress(self.input_queue, "input"),
            f"{self.name}({self.num_workers}线程/{processed})",
        ]
        if self.output_queue is not None:
            parts.append(format_queue_progress(self.output_queue, "output"))
        return " -- ".join(parts)

    def log_progress(self) -> None:
        """把 :meth:`format_progress` 的结果输出一条 INFO 日志，方便定期调用做进度追踪。"""
        logger.info(self.format_progress())

    def __enter__(self) -> "WorkerPool":
        """进入 `with` 块时启动所有工作线程。"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """退出 `with` 块时排空队列并停止所有工作线程。

        如果 `with` 块内已经有异常在传播，则不会用工作线程内部的异常掩盖它（只记录日志）。
        """
        try:
            self.stop(drain=True)
        except BaseException:
            if exc_type is None:
                raise
            logger.exception(
                f"{self.name} error while stopping (suppressed, original exception takes priority)"
            )
        return False
