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
    # pgnotifier
    A simple little utility to capture and process Postgresql NOTIFY streams

    #### Features
    * Monitor multiple channels at once
    * Register multiple callbacks to any number of channels
    * Add and remove channels at will
    * Add and remove callbacks at will
    * Abstracts away asynchronous context for synchronous use
    * Automatic type conversion of all valid python types via `ast.literal_eval`
    * Persistent, immutable internals
    * Tight footprint
    """
    def __init__(self, db_conf):
        super().__init__()
        self.conf = db_conf     # database credentials
        self.subs = pyr.m()     # channel subscriptions
        self.loop = None        # listener loop

    def get_channels(self):
        """
        Returns the set of registered channels, as `set`.
        """
        return pyr.thaw(self.subs.keys())

    def add_channels(self, channels, autorun=True):
        """
        Adds one or more channels to the set of channels to monitor. Is a no-op if channel already exists. Optionally restarts listener.

        Args:
        * `channels` list of channels to add, as `str` (single channel), `list` or `set`.
        * `autorun` restart listener with new channels added, as `bool`. Default is `True`.

        > [!NOTE]
        > Added channels *can only* be monitored by disposing and recreating the database connection and listener loop (as the notifier blocks). This mechanism happens automatically when `autorun=True`. Otherwise, if `autorun=False`, added channels *will not* be monitored until a call to `stop()` and `run()` or `restart()` is made.
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        with threading.Lock():
            self.subs = self.subs.update(
                {x:pyr.m() for x in channels if x not in self.subs.keys()})
            if autorun:
                self.restart()

    def add_channel(self, channel, autorun=True):
        """
        Alias for `add_channels(...)`, as a non-pluralised naming convenience.
        """
        self.add_channels(channel, autorun)

    def remove_channels(self, channels, autorun=True):
        """
        Removes one or more channels from the set of channels to monitor.
        Is a no-op if channel doesn't exist. Optionally restarts listener thread.

        Args:
        * `channels` list of channels to remove, as `str` (single channel), `list` or `set`.
        * `autorun` restart listener thread with channels removed, as `bool`. Defaults to `True`.

        > [!NOTE]
        > Removed channels *will only* cease being monitored by disposing of,
        and recreating the database connection and listener thread (as the
        notifier blocks). This mechanism happens automatically when `autorun=True`.
        Otherwise, if `autorun=False`, removed channels *will* continue to be
        monitored until a call to `stop()` and `run()` or `restart()` is made.
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        with threading.Lock():
            self.subs = pyr.pmap(
                {x:y for (x,y) in self.subs.items() if x not in channels})
            if autorun:
                self.restart()

    def remove_channel(self, channel, autorun=True):
        """
        Alias for `remove_channels(...)`, as a non-pluralised naming convenience.
        """
        self.remove_channels(channel, autorun=True)

    def get_subscriptions(self):
        """
        Returns channel -> subscription mappings, as `dict`.
        """
        return pyr.thaw(self.subs)

    def subscribe(self, id, channel, fn, autorun=True):
        """
        Adds a callback function with id for notifications on channel. Creates channel if channel does not exist. Optionally restarts listener thread.

        Args:
        * `id` subscriber id, as `hashable` (i.e. any immutable type such as strings, numbers, and tuples containing immutable types).
        * `channel` notification channel to subscribe to, as `str`.
        * `fn` callback function, as `callable` (i.e. function or method).
        * `autorun` restart listener thread if new channel added, as `bool`. Defaults to `True`.

        When a notification is received on a channel, callbacks subscribed to that channel will be executed.

        Args:
        * `id` the subscriber `id` as `hashable`.
        * `channel` the notification channel, as `str`.
        * `payload` the notification received, as native type as cast by `ast.literal_eval`.
        * `pid` the notifying sessions server process PID, as `int`.

        """
        with threading.Lock():
            if not channel in self.subs:
                self.subs = self.subs.set(channel, pyr.m())
            self.subs = self.subs.transform([channel], lambda m: m.set(id, fn))

    def unsubscribe(self, id, channel, autorun=True):
        """
        Removes a callback function with id from notifications on channel. Also removes channel if that channel no longer contains any subscriptions. Optionally restarts listener thread.

        Args:
        * `id`  the subscriber id, as `hashable`.
        * `channel` notification channel to unsubscribe from, as `str`.
        * `autorun` restart listener thread if channel removed, as `bool`. Defaults to `True`.

        """
        with threading.Lock():
            self.subs = self.subs.transform([channel], lambda m: m.discard(id))
            if subs := self.subs.get(channel, None):
                if len(subs) == 0:
                    self.subs = self.subs.discard(channel)

    def restart(self):
        """
        (Re)starts listener thread and recreates database connection. *This function is generally not needed in userland.*

        > [!NOTE]
        > Only necessary under the following conditions:
        > * Channels have been added or removed with arg `autorun=False`.
        > * Subscribers have been added or removed with arg `autorun=False`, and in the process, have themselves created or removed channels.
        > * Notifier was previously stopped by a call to `stop()`.
        > * No channels and no subscribers have been added to Notifier and no call to `run()` or `restart()` has been made.
        """
        if self.loop and not self.loop.done():
            self.stop()
        self.start()

    def stop(self):
        """
        Stops the listener thread (if running) and closes the database connection. Is a no-op if thread is not running.
        """
        self.loop.cancel()
        self.cn.close()

    def start(self):
        """
        Starts the listener thread (if not already running). Is a no-op if thread already running. *This function is generally not needed in userland.*

        Establishes database connection and spins off a thread to monitor notify channels and execute subscribed callbacks.

        > [!NOTE]
        > Only necessary under the following conditions:
        > * Channels have been added or removed with arg `autorun=False`.
        > * Subscribers have been added or removed with arg `autorun=False`, and in the process, have themselves created or removed channels.
        > * Notifier was previously stopped by a call to `stop()`.
        > * No channels and no subscribers have been added to Notifier and no call to `run()` or `restart()` has been made.
        """
        if self.loop and not self.loop.done():
            return
        self.cn = pg.connect(**self.conf, autocommit=True)
        for c in self.subs.keys():
            self.cn.cursor().execute(str("listen " + c))
        self.loop = as_async(self._notify, self.cn.notifies())

    def _notify(self, generator):
        """
        The generator arg yields each message as a string. Payload is cast to it's native data type via ast.literal_eval

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
