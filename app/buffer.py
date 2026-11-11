"""有界内存账本。

所有在途字节（原始压缩块、解压缓冲、跨行尾巴、待写记录）都必须先向账本
``acquire`` 配额，释放时 ``release``。因此内存上限约束的是**所有阶段的累计
缓冲**，而不是某一次 read 的大小。

账本同时维护：
* 全局字节上限；
* 每个账户（源连接或分区）的独立配额，单个分区停住不会耗尽全局额度；
* 缓冲峰值与每次阻塞的原因，供可观察性输出。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque


@dataclass
class BlockEvent:
    """一次未能立即获得配额的阻塞记录。"""

    reason: str          # global_limit / partition_limit
    account: str
    waited_seconds: float
    at: float
    limit: int
    in_use: int
    request: int


class BufferLimitExceeded(RuntimeError):
    """单次申请本身就超过配额（通常意味着单条记录超长）。"""


class BufferLedger:
    def __init__(
        self,
        global_limit: int,
        account_limit: int | None = None,
        max_events: int = 10_000,
    ) -> None:
        if global_limit <= 0:
            raise ValueError("全局内存上限必须为正")
        self._global_limit = global_limit
        self._account_limit = account_limit
        self._cond = threading.Condition()
        self._global_in_use = 0
        self._accounts: dict[str, int] = {}
        self._global_peak = 0
        self._account_peak: dict[str, int] = {}
        self._events: Deque[BlockEvent] = deque(maxlen=max_events)
        self._limit_hits: dict[str, int] = {}

    def acquire(self, account: str, size: int, timeout: float | None = None) -> None:
        """申请 ``size`` 字节，空间不足时阻塞，背压由此向上游传播。"""
        if size < 0:
            raise ValueError("申请字节数不能为负")
        if size > self._global_limit:
            raise BufferLimitExceeded(
                f"单次申请 {size} 字节超过全局上限 {self._global_limit}"
            )
        if self._account_limit is not None and size > self._account_limit:
            raise BufferLimitExceeded(
                f"单次申请 {size} 字节超过分区上限 {self._account_limit}"
            )

        deadline = None if timeout is None else time.monotonic() + timeout
        wait_started: float | None = None
        latched_reason: str | None = None

        with self._cond:
            while True:
                account_in_use = self._accounts.get(account, 0)
                reason: str | None = None
                if (
                    self._account_limit is not None
                    and account_in_use + size > self._account_limit
                ):
                    reason = "partition_limit"
                elif self._global_in_use + size > self._global_limit:
                    reason = "global_limit"

                if reason is None:
                    self._global_in_use += size
                    self._accounts[account] = account_in_use + size
                    if self._global_in_use > self._global_peak:
                        self._global_peak = self._global_in_use
                    if self._accounts[account] > self._account_peak.get(account, 0):
                        self._account_peak[account] = self._accounts[account]
                    if wait_started is not None:
                        assert latched_reason is not None
                        waited = time.monotonic() - wait_started
                        self._limit_hits[latched_reason] = (
                            self._limit_hits.get(latched_reason, 0) + 1
                        )
                        self._events.append(
                            BlockEvent(
                                reason=latched_reason,
                                account=account,
                                waited_seconds=waited,
                                at=time.time(),
                                limit=self._global_limit
                                if latched_reason == "global_limit"
                                else (self._account_limit or 0),
                                in_use=self._global_in_use,
                                request=size,
                            )
                        )
                    return

                if wait_started is None:
                    wait_started = time.monotonic()
                    latched_reason = reason
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(f"账户 {account} 等待配额超时")
                    self._cond.wait(min(remaining, 0.1))
                else:
                    # 带超时的等待：即使 notify 与 wait 存在竞争窗口，也会
                    # 周期性重查配额条件，避免唤醒丢失导致的永久死锁
                    self._cond.wait(0.1)

    def release(self, account: str, size: int) -> None:
        if size <= 0:
            return
        with self._cond:
            current = self._accounts.get(account, 0)
            if size > current:
                raise AssertionError(
                    f"账户 {account} 释放 {size} 字节，但仅持有 {current}"
                )
            self._accounts[account] = current - size
            self._global_in_use -= size
            if self._accounts[account] == 0:
                del self._accounts[account]
            self._cond.notify_all()

    def snapshot(self) -> dict:
        with self._cond:
            return {
                "global_limit": self._global_limit,
                "account_limit": self._account_limit,
                "global_in_use": self._global_in_use,
                "global_peak": self._global_peak,
                "accounts": dict(self._accounts),
                "account_peak": dict(self._account_peak),
                "limit_hits": dict(self._limit_hits),
                "recent_blocks": [
                    {
                        "reason": e.reason,
                        "account": e.account,
                        "waited_seconds": round(e.waited_seconds, 6),
                        "at": e.at,
                        "request": e.request,
                        "in_use": e.in_use,
                    }
                    for e in self._events
                ],
            }
