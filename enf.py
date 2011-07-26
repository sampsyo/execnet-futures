import concurrent.futures._base as futbase
import execnet
import inspect
import textwrap

def _worker(channel):
    """Pure function for running tasks on a host."""
    import traceback
    while not channel.isclosed():
        ident, source, call_name, args, kwargs = channel.receive()

        co = compile(source+'\n', '', 'exec')
        loc = {}
        exec co in loc
        try:
            res = loc[call_name](*args, **kwargs)
        except BaseException:
            res = traceback.format_exc()
            failed = True
        else:
            failed = False

        try:
            channel.send((failed, ident, res))
        except BaseException:
            channel.send((True, ident, 'unserializable result'))

class RemoteException(Exception):
    def __init__(self, text):
        self.text = text.strip()

    def __str__(self):
        return self.text

class GatewayExecutor(futbase.Executor):
    def __init__(self, group):
        self._group = group
        self._pending_tasks = {}
        self._running_tasks = {}
        self._busy_gateways = {}
        self._idle_gateways = set()
        self._channels = {}

        for gateway in self._group:
            chan = gateway.remote_exec(_worker)
            chan.setcallback(self._message)

            self._channels[gateway] = chan
            self._idle_gateways.add(gateway)

    def _message(self, msg):
        # Future finished.
        failed, ident, res = msg
        fut, _, _, _ = self._running_tasks[ident]

        if failed:
            fut.set_exception(RemoteException(res))
        else:
            fut.set_result(res)

        # Gateway no longer busy.
        gw = self._busy_gateways.pop(ident)
        self._idle_gateways.add(gw)

        self._advance()

    def _advance(self):
        """Run a new task if possible (a pending task and idle gateway
        are both available).
        """
        if self._idle_gateways and self._pending_tasks:
            ident = self._pending_tasks.iterkeys().next()
            fut, fn, args, kwargs = self._pending_tasks.pop(ident)


            if not fut.set_running_or_notify_cancel():
                return
            self._running_tasks[ident] = (fut, fn, args, kwargs)

            gw = self._idle_gateways.pop()
            self._busy_gateways[ident] = gw

            call_name = fn.__name__
            # FIXME need checks here like _source_of_function
            source = inspect.getsource(fn.func_code)
            source = textwrap.dedent(source)
            self._channels[gw].send((ident, source, call_name, args, kwargs))

    def submit(self, fn, *args, **kwargs):
        fut = futbase.Future()

        idents = self._pending_tasks.keys() + self._running_tasks.keys()
        if idents:
            ident = max(idents) + 1
        else:
            ident = 0
        self._pending_tasks[ident] = (fut, fn, args, kwargs)

        self._advance()
        
        return fut

    def shutdown(self, wait=True):
        for chan in self._channels.itervalues():
            chan.close()
        self._group.terminate()
        # FIXME wait=False?

# Smoke test.
if __name__ == '__main__':
    group = execnet.Group(['popen'] * 2)
    def square(n):
        return n * n
    with GatewayExecutor(group) as executor:
        futures = [executor.submit(square, n) for n in range(5)]
        for future in futures:
            print future.result()

    print

    group = execnet.Group(['popen'] * 3)
    def pid():
        import os
        return os.getpid()
    with GatewayExecutor(group) as executor:
        futures = [executor.submit(pid) for i in range(10)]
        for future in futures:
            print future.result()
