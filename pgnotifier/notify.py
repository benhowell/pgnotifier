import threading
import asyncio
import psycopg as pg
import pyrsistent as pyr
import ast

from concurrent.futures import ThreadPoolExecutor

def as_async(fn, *args):
    loop = asyncio.new_event_loop()
    return loop.run_in_executor(ThreadPoolExecutor(max_workers=1), fn, *args)

def assoc_in(m,pv,v):
    return m.transform(pv[:-1], lambda x: x.set(pv[-1], v))

def dissoc_in(m,pv):
    return m.transform(pv[:-1], lambda x: x.discard(pv[-1]))

def filterkv(m,f,*a):
    return {k:v for (k,v) in m.items() if f(k,v,*a)}


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
        self.channels = pyr.m() # channels
        self.subs = pyr.m()     # subscribers
        self.c2s = pyr.m()      # ch->sub index + chan specific sub meta data
        self.s2c = pyr.m()      # sub->ch index
        self.loop = None        # listener loop

    def get_channels(self):
        """
        Returns the map of registered channels, as `dict`.
        """
        return pyr.thaw(self.channels)

    def add_channels(self, channels, autorun=True):
        """
        Adds one or more channels to the set of channels to monitor.
        Is a no-op if channel already exists. Optionally restarts listener thread.

        Args:
        * `channels` list of channels to add, as `str` (single channel), `list` or `set`.
        * `autorun` restart listener thread with new channels added, as `bool`. Default is `True`.

        > [!NOTE]
        > Added channels *can only* be monitored by disposing and recreating the
        database connection and listener thread (as the notifier blocks).
        This mechanism happens automatically when `autorun=True`. Otherwise, if
        `autorun=False`, added channels *will not* be monitored until a call to
        `stop()` and `run()` or `restart()` is made.
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        with threading.Lock():
            for c in channels:
                if c not in self.channels.keys():
                    self.channels = self.channels.set(
                        c, pyr.pmap({'mute': False, 'listen': False}))
                if not self.c2s.get(c, None):
                    self.c2s = self.c2s.set(c, pyr.m())
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

        > [!WARNING]
        > All subscribers for the channel being removed will also be removed.

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
            for c in channels:
                if c in self.channels.keys():
                    self.channels = self.channels.discard(c)

                    # dereg chan fron sub idx
                    if _subs := self.c2s.get(c, None):
                        for k,v in _subs.items():
                            self.s2c = self.s2c.transform(
                                [k], lambda x: x.remove(c))
                    # remove chan from chan idx
                    self.c2s = self.c2s.discard(c)
            if autorun:
                self.restart()

    def remove_channel(self, channel, autorun=True):
        """
        Alias for `remove_channels(...)`, as a non-pluralised naming convenience.
        """
        self.remove_channels(channel, autorun)

    def get_subscribers(self):
        """
        Returns channel -> subscriber mappings, as `dict`.
        """
        return pyr.thaw(self.subs)

    def subscribe(self, id, channel, fn, autorun=True):
        """
        Adds a callback function with id for notifications on channel.
        Creates channel if channel does not exist.
        Optionally restarts listener thread.

        Args:
        * `id` subscriber id, as `hashable` (i.e. any immutable type such as
        strings, numbers, and tuples containing immutable types).
        * `channel` notification channel to subscribe to, as `str`.
        * `fn` callback function, as `callable` (i.e. function or method).
        * `autorun` restart listener thread if new channel added, as `bool`.
        Defaults to `True`.

        When a notification is received on a channel, callbacks subscribed to
        that channel will be executed.

        Args:
        * `id` the subscriber `id` as `hashable`.
        * `channel` the notification channel, as `str`.
        * `payload` the notification received, as native type as cast by `ast.literal_eval`.
        * `pid` the notifying sessions server process PID, as `int`.

        """
        with threading.Lock():

            # add channel if non-existent
            restart = False
            if not channel in self.channels:
                restart = True
                self.add_channel(channel, False)

            # set channel to listen (if already listening: no-op)
            self.channels = assoc_in(self.channels, [channel, 'listen'], True)

        # add sub
        self.subs = self.subs.set(id, fn)

        # create sub to chan idx if doesnt exist
        if not self.s2c.get(id, None):
            self.s2c = self.s2c.set(id, pyr.v())

        # create chan to sub idx if doesnt exist
        if not self.c2s.get(channel, None):
            self.c2s = self.c2s.set(channel, pyr.m())

        # reg sub with chan
        if not self.c2s.get(channel, None).get(id, None):
            self.c2s = assoc_in(self.c2s, [channel,id], pyr.pmap({'mute': False}))

        # reg chan with sub
        _x = self.s2c.get(id, None)
        if not channel in _x:
            self.s2c = self.s2c.set(id, _x.append(channel))

        if autorun and restart:
            self.restart()



    def unsubscribe(self, id, channel, autorun=True):
        """
        Removes a callback function with id from notifications on channel.
        Also removes channel if that channel no longer contains any subscribers.
        Optionally restarts listener thread.

        Args:
        * `id`  the subscriber id, as `hashable`.
        * `channel` notification channel to unsubscribe from, as `str`.
        * `autorun` restart listener thread if channel removed, as `bool`. Defaults to `True`.

        """

        # remove sub
        self.subs = self.subs.discard(id)

        # remove sub from sub idx
        self.s2c = self.s2c.discard(id)

        # dereg sub from chan idx
        if id in self.c2s.get(channel, None):
            self.c2s = dissoc_in(self.c2s, [channel,id])

        with threading.Lock():
            if len(self.c2s.get(channel, None)) == 0:
                self.channels = assoc_in(self.channels, [channel, 'listen'], False)
                if autorun:
                    self.restart()





    def mute_channels(self, channels=pyr.v()):
        self._mute_chans(channels, True)

    def mute_channel(self, channel):
        self._mute_chans(channel, True)

    def unmute_channels(self, channels=pyr.v()):
        self._mute_chans(channels, False)

    def unmute_channel(self, channel):
        self._mute_chans(channel, False)

    def get_muted_channels(self):
        return filterkv(self.channels, lambda _,v: v['mute'])

    def _mute_sub(self, id, channels, val):
        if isinstance(channels, str):
            channels = pyr.v(channels)
        if len(channels) == 0: # default: all channels
            channels = filterkv(self.c2s, lambda _,v: id in v).keys()
        with threading.Lock():
            for c in channels:
                self.c2s = assoc_in(self.c2s, [c, id, 'mute'], val)
            self.restart()

    def mute_subscriber(self, id, channels=pyr.v()):
        self._mute_sub(id, channels, True)

    def unmute_subscriber(self, id, channels=pyr.v()):
        self._mute_sub(id, channels, False)

    def _subs_by_mute_state(self, b, ch=None):
        _flt = lambda k,v: v['mute']==b
        if ch:
            return filterkv(self.c2s.get(ch, None), _flt)
        return filterkv(self.c2s, lambda k,v: filterkv(v, _flt))

    def get_muted_subscribers(self, ch=None):
        return self._subs_by_mute_state(True, ch)

    def get_non_muted_subscribers(self, ch=None):
        return self._subs_by_mute_state(False, ch)

    def restart(self):
        """
        (Re)starts listener thread and recreates database connection.
        *This function is generally not needed in userland.*

        > [!NOTE]
        > Only necessary under the following conditions:
        > * Channels have been added or removed with arg `autorun=False`.
        > * Subscribers have been added or removed with arg `autorun=False`, and
            in the process, have themselves created or removed channels.
        > * Notifier was previously stopped by a call to `stop()`.
        > * No channels and no subscribers have been added to Notifier and no
            call to `run()` or `restart()` has been made.
        """
        if self.loop and not self.loop.done():
            self.stop()
        self.start()

    def stop(self):
        """
        Stops the listener thread (if running) and closes the database connection.
        Is a no-op if thread is not running.
        """
        self.loop.cancel()
        self.cn.close()

    def start(self):
        """
        Starts the listener thread (if not already running).
        Is a no-op if thread already running.
        *This function is generally not needed in userland.*

        Establishes database connection and spins off a thread to monitor notify
        channels and execute subscribed callbacks.

        > [!NOTE]
        > Only necessary under the following conditions:
        > * Channels have been added or removed with arg `autorun=False`.
        > * Subscribers have been added or removed with arg `autorun=False`, and
            in the process, have themselves created or removed channels.
        > * Notifier was previously stopped by a call to `stop()`.
        > * No channels and no subscribers have been added to Notifier and no
            call to `run()` or `restart()` has been made.
        """
        if self.loop and not self.loop.done():
            return
        self.cn = pg.connect(**self.conf, autocommit=True)
        for k in self._valid_channels().keys():
            self.cn.cursor().execute(str("listen " + k))
        self.loop = as_async(self._notify, self.cn.notifies())

    def _notify(self, generator):
        """
        The generator arg yields each message as a string. Payload is cast to
        it's native data type via ast.literal_eval

        NOTE: Messages must be shorter than 8000 bytes. For almost all
        notifications, it's recommended to send the key of record, a view or
        table name, a function reference, etc.

        Subscribers to channel will have their callbacks executed with args:
        - id:  the id of the subscriber
        - channel: the notification channel
        - payload: the notification received
        - pid: the notifying sessions server process PID
        """
        for n in generator:
            ast_payload = None
            try:
                ast_payload = ast.literal_eval(n.payload)
            except (ValueError, TypeError,
                    SyntaxError, MemoryError, RecursionError) as e:
                print(e)
                raise

            print(self._subs_by_mute_state(False, n.channel))
            for k,v in self._subs_by_mute_state(False, n.channel).items():
                self.subs[k](k, n.channel, ast_payload, n.pid)

    def _mute_chans(self, channels, val):
        if isinstance(channels, str):
            channels = pyr.v(channels)
        if len(channels) == 0: # default: all channels
            channels = self.channels
        with threading.Lock():
            for c in channels:
                self.channels = assoc_in(self.channels, [c,'mute'], val)
            self.restart()


    def _valid_channels(self):
        return filterkv(self.channels,
                        lambda _,v: v['listen'] and not v['mute'])
