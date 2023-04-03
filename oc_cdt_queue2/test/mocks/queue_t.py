try:
    import queue
except ImportError:
    import Queue as queue


class QueueGetError(ValueError):
    def __init__(self, msg):
        super(QueueGetError, self).__init__(msg)


class JoinableQueue(object):
    # this mock is so hard because in python2.7 queue.Queue is classobj, not new-style class
    # so super() method and e.t.c does not work
    def __init__(self):
        self.__queue = queue.Queue()
        self.__joined = False
        self.__got = False

    def get(self):
        if self.__got:
            raise QueueGetError("Please call task_done() before getting next message")

        if self.__queue.empty():
            raise QueueGetError("Attempt to get from empty queue")

        self.__got = True
        return self.__queue.get()

    def close(self):
        self.__joined = True

    def join_thread(self):
        self.close()

    def put(self, msg):
        return self.__queue.put(msg)

    def empty(self):
        return self.__queue.empty()

    def qsize(self):
        return self.__queue.qsize()

    @property
    def joined(self):
        return self.__joined

    def task_done(self):
        if not self.__got:
            raise QueueGetError("Please get message first, then call task_done()")

        self.__got = False
