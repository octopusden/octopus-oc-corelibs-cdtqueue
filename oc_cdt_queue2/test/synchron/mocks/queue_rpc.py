from oc_cdt_queue2.queue_rpc import QueueRPC as QCQ
from .queue_loopback import LoopbackConnection

class QueueRPC(QCQ):
    def __init__(self, *args, **kvargs):
        super(QueueRPC, self).__init__(*args, **kvargs)
        self._Connection = LoopbackConnection
