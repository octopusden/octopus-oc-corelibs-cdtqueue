from oc_cdt_queue2.queue_client import QueueClient as QCQ
from .queue_loopback import LoopbackConnection

class QueueClient(QCQ):
    def __init__(self, *args, **kvargs):
        super(QueueClient, self).__init__(*args, **kvargs)
        self._Connection = LoopbackConnection
