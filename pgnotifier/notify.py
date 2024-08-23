import threading
import asyncio
import psycopg as pg
import pyrsistent as pyr
import ast

from concurrent.futures import ThreadPoolExecutor

# little fn helpers
def as_async(f,*a):
    return asyncio.new_event_loop().run_in_executor(
        ThreadPoolExecutor(max_workers=1), f, *a)

def assoc_in(m,pv,v):
    return m.transform(pv[:-1], lambda x: x.set(pv[-1], v))

def dissoc_in(m,pv):
    return m.transform(pv[:-1], lambda x: x.discard(pv[-1]))

def filterkv(m,f,*a):
    return pyr.pmap({k:v for (k,v) in m.items() if f(k,v,*a)})

class Notifier:
    """
    # pgnotifier
    A simple little module to capture and process Postgresql NOTIFY streams

    #### Features
    * Monitor multiple channels
    * Register one or more callbacks with multiple channels
    * Register one or more channels with a callback
    * Control callbacks on a per channel basis
    * Add and remove channels at any time
    * Mute and un-mute channels
    * Add and remove subscribers at any time
    * Mute and un-mute subscribers
    * Abstract away asynchronous context for synchronous use
    * Automatic str -> type conversion of all valid python types via `ast.literal_eval`
    * Persistent, immutable internal data structures
    * Minimalist API
    * Tight footprint
    """
    def __init__(self, dbconf):
        super().__init__()
        self.__SYSCHAN = 'PGNOTIFIER' # always listening channel
        self.__conf = dbconf          # database credentials
        self.__channels = pyr.m()     # channels
        self.__subs = pyr.m()         # subscribers
        self.__c2s = pyr.m()          # ch->sub index & channel specific sub meta data
        self.__loop = None            # listener thread
        self.__active_channels = pyr.v() # channels in listener thread
        self.__maybe_restart()

    #--------------------------------------#
    # public API
    #--------------------------------------#

    def add_channels(self, channels):
        """
        Adds one or more channels to the set of channels to monitor.
        Is a no-op if channel already exists.

        Args:
        * `channels` list of channels to add, as `str` (single channel), `list` or `set`.
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        with threading.Lock():
            for c in channels:
                if c not in self.__channels.keys():
                    self.__channels = self.__channels.set(
                        c, pyr.pmap({'mute': False}))
                if not self.__c2s.get(c, None):
                    self.__c2s = self.__c2s.set(c, pyr.m())

    def remove_channels(self, channels, autorun=True):
        """
        Removes one or more channels from the set of channels to monitor.
        Is a no-op if channel doesn't exist. Optionally restarts listener thread (if needed).

        Args:
        * `channels` list of channels to remove, as `str` (single channel), `list` or `set`.
        * `autorun` restart listener thread with channels removed, as `bool`. Defaults to `True`.

        <br>

        > [!IMPORTANT]
        > Active channels, when removed, *will only* cease being monitored after a
        listener thread restart. Thread restarts happen automatically when `autorun=True`.
        Otherwise, if `autorun=False`, removed channels *will* continue to be
        monitored until a call to `stop()` and `start()`, or `restart()`, is made.
        >
        > Inactive channels (e.g. channel is muted and/or has no subscribers and/or
        has all muted subscribers), when removed, *do not* require a restart as
        they will have already been removed from the listener thread.
        >
        > Listener thread (re)starts are only required under certain, specific
        circumstances. It's advisable to allow pgnotifier take care of listener
        thread management via the default `autorun=True`, *unless there is a
        very good reason* to manage it manually.
        See [__maybe_restart](./private_methods.md#notifier__maybe_restart-) for more detail.
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        with threading.Lock():
            for c in channels:
                if c in self.__channels.keys():
                    self.__channels = self.__channels.discard(c)
                    self.__c2s = self.__c2s.discard(c)
        if autorun:
            self.__maybe_restart()

    def channels(self):
        """
        Returns channel and subscriber data, as `dict`.
        """
        _cs = self.__channels
        for c in self.__channels.keys():
            for k,v in self.__c2s.get(c).items():
                _cs = assoc_in(_cs, [c, 'subscribers',k],
                    assoc_in(v, ['fn'], self.__subs[k]['fn']))
            _cs = assoc_in(_cs, [c, 'active'], bool(c in self.__active_channels))
        return pyr.thaw(_cs)

    def subscribe(self, id, channel, fn, autorun=True):
        """
        Adds a callback function with id for notifications on channel.
        Creates channel if channel does not exist.
        Optionally restarts listener thread (if needed).

        Args:
        * `id` subscriber id, as `hashable` (i.e. any immutable type such as
        strings, numbers, and tuples containing immutable types).
        * `channel` notification channel to subscribe to, as `str`.
        * `fn` callback function, as `callable` (i.e. function or method).
        * `autorun` restart listener thread (if needed), as `bool`. Defaults to `True`.

        > [!IMPORTANT]
        > A new channel, when added with this subscriber, or, a channel that becomes
        active due to this subscriber *can only* be monitored after a
        listener thread restart. Thread restarts happen automatically when `autorun=True`.
        Otherwise, if `autorun=False`, activated channels containing this subscriber
        *will not* be monitored until a call to `stop()` and `start()`, or `restart()`,
        is made.
        >
        > Listener thread (re)starts are only required under certain, specific
        circumstances. It's advisable to allow pgnotifier take care of listener
        thread management via the default `autorun=True`, *unless there is a
        very good reason* to manage it manually.
        See [__maybe_restart](./private_methods.md#notifier__maybe_restart-) for more detail.

        When a notification is received on a channel, callbacks subscribed to that channel
        will be executed.

        Args:
        * `id` the subscriber `id` as `hashable`.
        * `channel` the notification channel, as `str`.
        * `payload` the notification received, as native type as cast by `ast.literal_eval`.
        * `pid` the notifying sessions server process PID, as `int`.
        """
        with threading.Lock():
            if not channel in self.__channels:
                self.add_channels([channel])
            #self.__subs = self.__subs.set(id, fn)
            self.__subs = assoc_in(self.__subs, [id,'fn'], fn)
            if not self.__c2s.get(channel, None):
                self.__c2s = self.__c2s.set(channel, pyr.m())
            if not self.__c2s.get(channel, None).get(id, None):
                self.__c2s = assoc_in(self.__c2s, [channel,id], pyr.pmap({'mute': False}))
        if autorun:
            self.__maybe_restart()

    def unsubscribe(self, id, channel, autorun=True):
        """
        Removes a callback function with id from notifications on channel.
        Optionally restarts listener thread (if needed).

        Args:
        * `id` the subscriber id, as `hashable`.
        * `channel` notification channel to unsubscribe from, as `str`.
        * `autorun` restart listener thread (if needed), as `bool`. Defaults to `True`.
        """
        with threading.Lock():
            self.__subs = self.__subs.discard(id)
            if id in self.__c2s.get(channel, None):
                self.__c2s = dissoc_in(self.__c2s, [channel,id])
        if autorun:
            self.__maybe_restart()

    def subscribers(self):
        """
        Returns subscriber and channel data, as `dict`.
        """
        _ss = self.__subs
        for k,v in self.__c2s.items():
            for sk,sv in v.items():
                if not _ss.get(sk, None):
                    _ss = _ss.set(sk,pyr.pmap({'channels':pyr.m()}))
                _ss = assoc_in(_ss, [sk, 'channels',k], pyr.pmap({
                    'active': bool(k in self.__active_channels),
                    'mute': self.__channels[k]['mute'],
                    'sub_mute': sv['mute']}))
        return pyr.thaw(_ss)

    def mute_channels(self, channels=pyr.v()):
        """
        Mutes channels. Removes channels from listener thread, thereby muting all
        subscribers associated with those channels (no matter their mute status).

        Subscribers will retain their mute status associated with those channels.

        Args:
        * `channels` list of channels to mute, as `str` (single channel), `list` or `set`.
        If no channels given, *ALL* channels will be muted.
        """
        self.__mute_chans(channels, True)

    def unmute_channels(self, channels=pyr.v()):
        """
        Un-mutes channels. Adds channels to the listener thread, thereby adding all
        un-muted subscribers associated with those channels.

        Args:
        * `channels` list of channels to un-mute, as `str` (single channel), `list` or `set`.
        If no channels given, *ALL* channels will be un-muted.

        > [!NOTE]
        > Channel will remain inactive (i.e. excluded from the listener thread)
        if it contains no non-muted subscribers.
        """
        self.__mute_chans(channels, False)

    def mute_subscriber(self, id, channels=pyr.v()):
        """
        Mutes subscriber on channels. If a channel no longer contains any non-muted
        subscribers, it is said to be *inactive* and is removed from the listener thread.

        Args:
        * `id` subscriber id, as `hashable` (i.e. any immutable type such as
        strings, numbers, and tuples containing immutable types).
        * `channels` list of channels to mute the subscriber on, as `str`
        (single channel), `list` or `set`.
        If no channels given, the subscriber will be muted on *ALL* channels it is
        subscribed to.
        """
        self.__mute_sub(id, channels, True)

    def unmute_subscriber(self, id, channels=pyr.v()):
        """
        Un-mutes subscriber on channels. If subscriber is on a non-muted, *inactive*
        channel, the channel becomes *active* and is added to the listener thread.

        Args:
        * `id` subscriber id, as `hashable` (i.e. any immutable type such as
        strings, numbers, and tuples containing immutable types).
        * `channels` list of channels to un-mute the subscriber on, as `str`
        (single channel), `list` or `set`.
        If no channels given, the subscriber will be unmuted on *ALL* channels it is
        subscribed to.
        """
        self.__mute_sub(id, channels, False)

    def start(self):
        """
        Starts the listener thread (if not already running).
        Is a no-op if thread already running.
        *This function is generally not needed in userland.*

        > [!NOTE]
        > Listener thread (re)starts are only required under certain, specific circumstances.
        See [__maybe_restart](./private_methods.md#notifier__maybe_restart-) for more detail.
        """
        self.__maybe_restart()

    def stop(self):
        """
        Stops the listener thread (if running). Is a no-op if thread is not running.
        """
        self.__maybe_stop()

    def restart(self):
        """
        (Re)starts listener thread.
        *This function is generally not needed in userland.*

        > [!NOTE]
        > Listener thread (re)starts are only required under certain, specific circumstances.
        See [__maybe_restart](./private_methods.md#notifier__maybe_restart-) for more detail.
        """
        self.__maybe_restart()

    def is_running(self):
        """
        Returns True if listener thread currently running, else False, as `bool`
        """
        return self.__loop and not self.__loop.done()


    #--------------------------------------#
    # private methods
    #--------------------------------------#

    def __maybe_stop(self):
        """
        Stops the listener thread (if running) and closes the database connection.
        Is a no-op if thread is not running.
        """
        if self.is_running():
            self.__loop.cancel()
            self.cn.cursor().close()
            self.cn.close()

    def __maybe_restart(self):
        """
        Restarts listener thread if active channels have been deemed inactive or
        inactive channels have been deemed active (i.e. there's a reason to add
        and/or remove channels to/from the listener thread).

        Listener thread restart process:
        * Valid channels (those that should be active) are compared to the
        active channels (those in the listener thread).
        * If thread not running, a restart with valid channels is required.
        * If thread running and valid channels don't match active channels, a
        restart is required.
        * Listener thread stopped if running:
          * Task thread is terminated via `cancel()` on enclosing asyncio loop.
          * Database cursor and connection are closed.
        * New database connection is created.
        * Postgresql LISTEN commands are executed (one per active channel)
        * NOTIFY message callback function, and `psycopg.connection.notifies()`
        call (which returns a blocking generator) are passed to the asyncio loop,
        which in-turn is executed as a task inside a thread returning a Future.
        Whenever the future returns NOTIFY data received from Postgresql, the
        NOTIFY message callback distributes that data to all callbacks
        subscribed to the channel the data arrived on.

        > [!NOTE]
        > The listener thread is started when Notifier is first constructed. The
        listener thread is _always_ running and always listening to at least
        one channel (the SYSCHAN: PGNOTIFIER), unless `stop()` has been called
        from userland.

        SYSCHAN needs further consideration. I expect a future release to
        properly integrate the SYSCHAN with the functionality of a regular
        channel with the exception that it is always on the listener thread.
        Perhaps system commands can be executed via messages from the channel?
        Postgresql system messages? Other non-application level stuff?
        """
        vc = self.__valid_chans()
        if not self.is_running():
            self.__active_channels = pyr.v()
        if vc != self.__active_channels:
            self.__active_channels = vc
            self.__maybe_stop()
            self.cn = pg.connect(**self.__conf, autocommit=True)
            for c in vc.append(self.__SYSCHAN):
                self.cn.cursor().execute('listen "' + c + '"')
            self.__loop = as_async(self._Notifier__notify, self.cn.notifies())
        # else: nothing has changed, as you were

    def __notify(self, generator):
        """
        Receives incoming NOTIFY message data from all active channels.
        The generator arg yields each message as a string, as it arrives,
        other.

        Payload is cast to it's native data type via `ast.literal_eval` and distributed
        to callbacks subscribed to the channel the message arrived on.

        Args:
        * `generator` message generator

        > [!IMPORTANT]
        > Messages must be shorter than 8000 bytes. For almost all
        notifications, it's recommended to send the key of record, a view or
        table name, a function reference, etc.

        Subscribers to channel will have their callbacks executed with args:
        * `id`  the id of the subscriber
        * `channel` the notification channel
        * `payload` the notification received
        * `pid` the notifying sessions server process PID
        """
        for n in generator:
            ast_payload = None
            try:
                ast_payload = ast.literal_eval(n.payload)
            except (ValueError, TypeError,
                    SyntaxError, MemoryError, RecursionError) as e:
                print(e)
                raise
            if n.channel != self.__SYSCHAN:
                ks = filterkv(
                    self.__c2s.get(n.channel), lambda _,v:not v['mute'])
                for k in ks:
                    self.__subs[k]['fn'](k, n.channel, ast_payload, n.pid)

    def __valid_chans(self):
        """
        Returns a vector of channels deemed valid to be on the listener thread, as `pyr.pvector`.

        A channel is deemed to be valid if:
        * It is not muted.
        * It contains at least one non-muted subscriber.
        """
        return pyr.pvector(filterkv(self.__channels,
                lambda k,v: not v['mute'] and
                len(filterkv(self.__c2s.get(k, {}),
                            lambda _,v: not v['mute'])) > 0).keys())

    def __mute_chans(self, channels, b):
        """
        Sets the mute state `b` to channels (i.e. will be removed from the
        listener thread).

        All subscribers associated with muted channels are also removed from the
        listener thread (no matter their mute status). Subscribers will retain
        their mute status associated with those channels.

        If a channel is unmuted, the subscribers to that channel will resume
        operation according to their mute status.

        Args:
        * `channels` list of channels to set mute state on, as `str` (single
        channel), `list` or `set`. If no channels given, *ALL* channels will be muted.
        * `b` boolean value to set mute to, as `bool`
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        if len(channels) == 0: # default: all channels
            channels = self.__channels
        with threading.Lock():
            for c in channels:
                self.__channels = assoc_in(self.__channels, [c,'mute'], b)
        self.__maybe_restart()

    def __mute_sub(self, id, channels, b):
        """
        Sets the mute state `b` subscriber with `id` on channels.

        Args:
        * `id` subscriber id, as `hashable` (i.e. any immutable type such as
        strings, numbers, and tuples containing immutable types).
        * `channels` list of channels to the subscriber mute state on, as `str`
        (single channel), `list` or `set`.
        If no channels given, subscriber with `id` will have it's mute state
        set to `b` on *ALL* channels it is subscribed to.
        """
        if isinstance(channels, str):
            channels = pyr.v(channels)
        if len(channels) == 0: # default: all channels
            channels = filterkv(self.__c2s, lambda _,v: id in v).keys()
        with threading.Lock():
            for c in channels:
                self.__c2s = assoc_in(self.__c2s, [c, id, 'mute'], b)
        self.__maybe_restart()
