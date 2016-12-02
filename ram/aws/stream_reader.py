import re
import time
from threading import Thread
from Queue import Queue, Empty


class NonBlockingStreamReader:

    def __init__(self, stream):
        '''
        stream: the stream to read from.  Usually a process' stdout or stderr.
        '''
        self._s = stream
        self._q = Queue()

        def _populateQueue(stream, queue):
            '''
            Collect lines from 'stream' and put them in 'queque'.
            '''
            while True:
                line = stream.readline()
                if line:
                    queue.put(line)
                else:
                    raise UnexpectedEndOfStream

        self._t = Thread(target=_populateQueue, args=(self._s, self._q))
        self._t.daemon = True
        self._t.start()  # Start collecting lines from the stream

    def readline(self, timeout=None):
        try:
            return self._q.get(block=timeout is not None,
                               timeout=timeout)
        except Empty:
            return None

    def readuntil(self, timeout=1, re_match=None, re_error=None):
        '''
        Read from queue until re_match or re_error is found
        '''
        emptyline = 0
        time.sleep(timeout)
        self.stream_txt = self._stream_started()

        while emptyline < 10:
            nextline = self.readline(.2)
            if nextline is None:
                emptyline += 1
                continue
            self.stream_txt += nextline
            if re_match is not None:
                match_s = re.search(re_match, nextline)
                if match_s:
                    return nextline

            if re_error is not None:
                match_e = re.search(re_error, nextline)
                if match_e:
                    print 'Error string found {}'.format(nextline)
                    raise
        return

    def _stream_started(self):
        while True:
            nextline = self.readline(.2)
            if nextline is None:
                time.sleep(1)
            else:
                return nextline


class UnexpectedEndOfStream(Exception):
    pass
