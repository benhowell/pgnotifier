import threading
import asyncio
import psycopg as pg
import pyrsistent as pyr
import ast

from concurrent.futures import ThreadPoolExecutor


def as_async(fn, *args):
    loop = asyncio.new_event_loop()
    return loop.run_in_executor(ThreadPoolExecutor(max_workers=1), fn, *args)


class Notifier():
    """
    A simple little utility to capture and process Postgresql NOTIFY streams.
    """
    def __init__(self, db_conf):
        super().__init__()
        self.conf = db_conf     # database credentials
        self.subs = pyr.m()     # channel subscriptions
        self.loop = None        # listener loop

    def get_channels(self):
        """Returns a set of all channels."""
        return pyr.thaw(self.subs.keys())

    def add_channels(self, channels, autorun=True):
        """
        Adds one or more channels to the set of channels to monitor. Is a no-op
        if channel already exists. Optionally restarts listener.


        Args:
        * `channels` list of channels to add, as `str` (single channel), `list` or `set`
        * `autorun` restart listener with new channels added, as `bool`. Default is `True`.

        [!NOTE] Added channels *can only* be monitored by disposing and
        recreating the database connection and listener loop (as the notifier
        blocks). This mechanism happens automatically when `autorun=True`.
        Otherwise, if `autorun=False`, added channels *will not* be monitored
        until a call to `stop()` and `run()` or `restart()` is made.




        Adds one or more channels to monitor.
        If a channel already exists, this addition of that channel is ignored.
        Optionally restarts listener. If not given, defaults to True.
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        with threading.Lock():
            self.subs = self.subs.update(
                {x:pyr.m() for x in channels if x not in self.subs.keys()})
            if autorun:
                self.restart()

    def add_channel(self, channel, autorun=True):
        """Alias for add_channels, as a non-pluralised naming convenience."""
        self.add_channels(channel, autorun)

    def remove_channels(self, channels, autorun=True):
        """
        Removes one or more channels from being monitored.
        If a channel doesn't exist, the removal of that channel is ignored.
        Optionally restarts listener. If not given, defaults to True.
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        with threading.Lock():
            self.subs = pyr.pmap(
                {x:y for (x,y) in self.subs.items() if x not in channels})
            if autorun:
                self.restart()

    def remove_channel(self, channel, autorun=True):
        """Alias for remove_channels, as a non-pluralised naming convenience."""
        self.remove_channels(channel, autorun=True)

    def get_subscriptions(self):
        """Returns a dict of all channel -> subscription mappings."""
        return pyr.thaw(self.subs)

    def subscribe(self, id, channel, fn, autorun=True):
        """
        Adds a callback function with id for notifications on channel.
        Creates channel if channel does not exist.
        Optionally restarts listener if a channel is created and autorun=True. If autorun arg not given, defaults to `True`.
        """
        with threading.Lock():
            if not channel in self.subs:
                self.subs = self.subs.set(channel, pyr.m())
            self.subs = self.subs.transform([channel], lambda m: m.set(id, fn))

    def unsubscribe(self, id, channel, autorun=True):
        """
        Removes a callback function with id from notifications on channel.
        Also removes channel if that channel no longer contains any subscriptions.
        Optionally restarts listener if a channel is removed and autorun=True. If autorun arg not given, defaults to `True`.
        """
        with threading.Lock():
            self.subs = self.subs.transform([channel], lambda m: m.discard(id))
            if subs := self.subs.get(channel, None):
                if len(subs) == 0:
                    self.subs = self.subs.discard(channel)

    def restart(self):
        """(Re)starts notify listener and recreates database connection."""
        if self.loop and not self.loop.cancelled():
            self.stop()
        self.start()

    def stop(self):
        """Stops notify listener and closes database connection."""
        self.loop.cancel()
        self.cn.close()

    def start(self):
        """
        Establishes database connection and spins off a thread to listen to notify channels and execute subscribed callbacks.

        NOTE: notifies() blocks so connection never becomes available again once sent to thread executor. Whenever we need to spin a new thread (e.g. when adding or removing a channel), we must also supply a new connection.
        """
        if not self.loop.running():
            self.cn = pg.connect(**self.conf, autocommit=True)
            for c in self.subs.keys():
                self.cn.cursor().execute(str("listen " + c))
            self.loop = as_async(self._notify, self.cn.notifies())

    def _notify(self, generator):
        """
        The generator arg yields each message as a string. If expected message format is JSON, casts string to a dict/map via ast.literal_eval

        NOTE: Messages must be shorter than 8000 bytes. For almost all notifications, it's recommended to send the key of record, a view or table name, a function reference, etc.

        Subscribers to channel will have their callbacks executed with args:
        - id:  the id of the subscriber
        - channel: the notification channel
        - payload: the notification received
        - pid: the notifying sessions server process PID
        """
        for n in generator:
            payload = None
            try:
                payload = ast.literal_eval(n.payload)
            except (ValueError, TypeError,
                    SyntaxError, MemoryError, RecursionError) as e:
                print(e)
                raise
            if subs := self.subs.get(n.channel, None):
                for id, fn in subs.items():
                    fn(id, n.channel, n.payload, n.pid)
