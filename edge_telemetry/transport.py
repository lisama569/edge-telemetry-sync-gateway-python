"""云边之间的可注入传输层（本地直连 / 可断网，无外部服务）。"""

from __future__ import annotations

import abc

from .errors import ErrorCode, IngestionError
from .gateway import CloudGateway
from .models import IngestResult, TelemetryBatch


class Transport(abc.ABC):
    @abc.abstractmethod
    def send(
        self, batch: TelemetryBatch, credential_id: str, secret: str
    ) -> IngestResult: ...


class LocalTransport(Transport):
    """直接在进程内调用 :class:`CloudGateway`。

    ``online=False`` 模拟断网 / 弱网：任何发送都得到 ``TRANSPORT_ERROR``，
    不触碰云端数据；``fail_once`` 可模拟一次瞬时失败后恢复。
    """

    def __init__(self, gateway: CloudGateway):
        self.gateway = gateway
        self.online = True
        self.fail_once = 0

    def set_online(self, online: bool) -> None:
        self.online = online

    def add_failures(self, n: int = 1) -> None:
        self.fail_once += n

    def send(
        self, batch: TelemetryBatch, credential_id: str, secret: str
    ) -> IngestResult:
        if not self.online:
            raise IngestionError(
                ErrorCode.TRANSPORT_ERROR,
                "链路离线，边缘侧应保留缓冲待重放",
                details={"batch_id": batch.batch_id, "seq": batch.seq},
            )
        if self.fail_once > 0:
            self.fail_once -= 1
            raise IngestionError(
                ErrorCode.TRANSPORT_ERROR,
                "模拟瞬时弱网失败",
                details={"batch_id": batch.batch_id, "seq": batch.seq},
            )
        return self.gateway.ingest(batch, credential_id, secret)
