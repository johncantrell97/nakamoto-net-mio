//! Poll-based reactor. This is a single-threaded reactor using a `poll` loop.
use crossbeam_channel as chan;

use nakamoto_net::error::Error;
use nakamoto_net::event::Publisher;
use nakamoto_net::time::{LocalDuration, LocalTime};
use nakamoto_net::{Disconnect, Io, PeerId};
use nakamoto_net::{Link, Service};

use mio::{Events, Poll, Interest, Token, net::TcpStream};

use log::*;

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io;
use std::net;
use std::sync::Arc;
use std::time;
use std::time::SystemTime;


use crate::fallible;
use crate::socket::Socket;
use crate::time::TimeoutManager;

/// Maximum time to wait when reading from a socket.
const READ_TIMEOUT: time::Duration = time::Duration::from_secs(6);
/// Maximum time to wait when writing to a socket.
const WRITE_TIMEOUT: time::Duration = time::Duration::from_secs(3);
/// Maximum amount of time to wait for i/o.
const WAIT_TIMEOUT: LocalDuration = LocalDuration::from_mins(60);
/// Socket read buffer size.
const READ_BUFFER_SIZE: usize = 1024 * 192;
/// Token used for events from Listener socket
const LISTENER_TOKEN: Token = Token(0);
/// Token used for events from Waker
const WAKER_TOKEN: Token = Token(1);
/// Initial key used for peer sockets
const INITIAL_PEER_TOKEN_INDEX: usize = 2;


#[derive(Clone)]
pub struct Waker(Arc<mio::Waker>);


impl nakamoto_net::Waker for Waker {
    fn wake(&self) -> io::Result<()> {
        self.0.wake()
    }
}

/// A single-threaded non-blocking reactor.
pub struct Reactor<Id: PeerId = net::SocketAddr> {
    peers: HashMap<Id, Socket<TcpStream>>,
    connecting: HashSet<Id>,
    poll: Poll,
    sources: HashMap<Token,Id>,
    source_keys: HashMap<Id, Token>,
    source_key: usize,
    waker: Waker,
    timeouts: TimeoutManager<()>,
    shutdown: chan::Receiver<()>,
    listening: chan::Sender<net::SocketAddr>,
}

/// The `R` parameter represents the underlying stream type, eg. `net::TcpStream`.
impl<Id: PeerId> Reactor<Id> {
    /// Register a peer with the reactor.
    fn register_peer(&mut self, addr: Id, mut stream: mio::net::TcpStream, link: Link) {
        let token = Token(self.source_key);
        self.source_key += 1;
        let socket_addr = addr.to_socket_addr();
        
        self.sources.insert(token, addr.clone());
        self.source_keys.insert(addr.clone(), token);

        if let Err(e) = self.poll.registry().register(&mut stream, token, Interest::READABLE.add(Interest::WRITABLE)) {
            error!("Failed to register interest: {}", e.to_string());
        }
        self.peers
            .insert(addr, Socket::from(stream, socket_addr, link));
    }

    /// Unregister a peer from the reactor.
    fn unregister_peer<S>(
        &mut self,
        addr: Id,
        reason: Disconnect<S::DisconnectReason>,
        service: &mut S,
    ) where
        S: Service<Id>,
    {
        self.connecting.remove(&addr);

        if let Some(token) = self.source_keys.remove(&addr) {
            self.sources.remove(&token);
        }

        if let Some(mut socket) = self.peers.remove(&addr) {
            if let Err(e) = self.poll.registry().deregister(socket.raw()) {
                error!("Failed to unregister interest: {}", e.to_string());
            }
        }

        service.disconnected(&addr, reason);
    }
}

impl<Id: PeerId> nakamoto_net::Reactor<Id> for Reactor<Id> {
    type Waker = Waker;

    /// Construct a new reactor, given a channel to send events on.
    fn new(
        shutdown: chan::Receiver<()>,
        listening: chan::Sender<net::SocketAddr>,
    ) -> Result<Self, io::Error> {
        let peers = HashMap::new();

        let poll = Poll::new()?;

        let waker = Waker(Arc::new(mio::Waker::new(poll.registry(), WAKER_TOKEN)?));
        let sources = HashMap::new();
        let source_keys = HashMap::new();
        let source_key = INITIAL_PEER_TOKEN_INDEX;
        let timeouts = TimeoutManager::new(LocalDuration::from_secs(1));
        let connecting = HashSet::new();

        Ok(Self {
            peers,
            connecting,
            sources,
            source_keys,
            source_key,
            poll,
            waker,
            timeouts,
            shutdown,
            listening,
        })
    }

    /// Run the given service with the reactor.
    fn run<S, E>(
        &mut self,
        listen_addrs: &[net::SocketAddr],
        mut service: S,
        mut publisher: E,
        commands: chan::Receiver<S::Command>,
    ) -> Result<(), Error>
    where
        S: Service<Id>,
        S::DisconnectReason: Into<Disconnect<S::DisconnectReason>>,
        E: Publisher<S::Event>,
    {
        let listener = if listen_addrs.is_empty() {
            None
        } else {
            let mut listener = mio::net::TcpListener::bind(listen_addrs[0])?;
            let local_addr = listener.local_addr()?;

            if let Err(e) = self.poll.registry().register(&mut listener, LISTENER_TOKEN, Interest::READABLE) {
                error!("Failed to register listener interest: {:?}", e.to_string());
            }
            
            self.listening.send(local_addr).ok();

            info!(target: "net", "Listening on {}", local_addr);

            Some(listener)
        };

        info!(target: "net", "Initializing service..");

        let local_time = SystemTime::now().into();
        service.initialize(local_time);

        self.process(&mut service, &mut publisher, local_time);

        // I/O readiness events populated by mio's `Poll`
        let mut events = Events::with_capacity(32);
        // Timeouts populated by `TimeoutManager::wake`.
        let mut timeouts = Vec::with_capacity(32);

        loop {
            let timeout = self
                .timeouts
                .next(SystemTime::now())
                .unwrap_or(WAIT_TIMEOUT)
                .into();

            trace!(
                "Polling {} source(s) and {} timeout(s), waking up in {:?}..",
                self.sources.len(),
                self.timeouts.len(),
                timeout
            );

            let result = self.poll.poll(&mut events, Some(timeout)); // Blocking.
            let local_time = SystemTime::now().into();

            service.tick(local_time);

            match result {
                Ok(()) => {
                    trace!("Woke up from our rest");

                    // we might need to run the io::ErrorKind::TimedOut code here
                    // because I believe we don't get errors on timeouts with mio


                    for ev in &events {
                        match ev.token() {
                            LISTENER_TOKEN => loop {
                                if let Some(ref listener) = listener {
                                    let (conn, socket_addr) = match listener.accept() {
                                        Ok((conn, socket_addr)) => (conn, socket_addr),
                                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                            break;
                                        }
                                        Err(e) => {
                                            error!(target: "net", "Accept error: {}", e.to_string());
                                            break;
                                        }
                                    };
                                    let addr = Id::from(socket_addr);
                                    trace!("{}: Accepting peer connection", socket_addr);

                                    let local_addr = conn.local_addr()?;
                                    let link = Link::Inbound;

                                    self.register_peer(addr.clone(), conn, link);

                                    service.connected(addr, &local_addr, link);
                                }
                            },
                            WAKER_TOKEN => {
                                trace!("Woken up by waker ({} command(s))", commands.len());

                                // Exit reactor loop if a shutdown was received.
                                if let Ok(()) = self.shutdown.try_recv() {
                                    return Ok(());
                                }

                                for cmd in commands.try_iter() {
                                    service.command_received(cmd);
                                }
                            }
                            token => {
                                if let Some(addr) = self.sources.get(&token).map(|addr| addr.clone()) {
                                    if ev.is_error() || ev.is_read_closed() || ev.is_write_closed() {
                                        // Let the subsequent read fail.
                                        trace!("{}: Socket error triggered: {:?}", addr.to_socket_addr(), ev);
                                    }
                                    
                                    if ev.is_writable() {
                                        self.handle_writable(addr.clone(), &mut service)?;
                                    }
                                    if ev.is_readable() {
                                        self.handle_readable(addr.clone(), &mut service);
                                    }
                                }
                            }
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::TimedOut => {
                    // Nb. The way this is currently used basically ignores which keys have
                    // timed out. So as long as *something* timed out, we wake the service.
                    self.timeouts.wake(local_time, &mut timeouts);

                    if !timeouts.is_empty() {
                        timeouts.clear();
                        service.timer_expired();
                    }
                }
                Err(err) => return Err(err.into()),
            }
            self.process(&mut service, &mut publisher, local_time);
        }
    }

    /// Return a new waker.
    ///
    /// Used to wake up the main event loop.
    fn waker(&self) -> Self::Waker {
        self.waker.clone()
    }
}

impl<Id: PeerId> Reactor<Id> {
    /// Process service state machine outputs.
    fn process<S, E>(&mut self, service: &mut S, publisher: &mut E, local_time: LocalTime)
    where
        S: Service<Id>,
        E: Publisher<S::Event>,
        S::DisconnectReason: Into<Disconnect<S::DisconnectReason>>,
    {
        // Note that there may be messages destined for a peer that has since been
        // disconnected.
        while let Some(out) = service.next() {
            match out {
                Io::Write(addr, bytes) => {
                    if let Some(socket) = self.peers.get_mut(&addr) {
                        if let Some(token) = self.source_keys.get(&addr) {
                            socket.push(&bytes);
                            if let Err(e) = self.poll.registry().reregister(socket.raw(), *token, Interest::READABLE.add(Interest::WRITABLE)) {
                                error!("failed to reregister writable interest: {:?}",  e.to_string());
                            }
                        }
                    }
                }
                Io::Connect(addr) => {
                    let socket_addr = addr.to_socket_addr();
                    trace!("Connecting to {}...", socket_addr);

                    match self::dial(&socket_addr) {
                        Ok(stream) => {
                            trace!("{:#?}", stream);

                            self.register_peer(addr.clone(), stream, Link::Outbound);
                            self.connecting.insert(addr.clone());

                            service.attempted(&addr);
                        }
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                            // Ignore. We are already establishing a connection through
                            // this socket.
                        }
                        Err(err) => {
                            error!(target: "net", "{}: Dial error: {}", socket_addr, err.to_string());

                            service.disconnected(&addr, Disconnect::DialError(Arc::new(err)));
                        }
                    }
                }
                Io::Disconnect(addr, reason) => {
                    if let Some(peer) = self.peers.get(&addr) {
                        trace!("{}: Disconnecting: {}", addr.to_socket_addr(), reason);

                        // Shutdown the connection, ignoring any potential errors.
                        // If the socket was already disconnected, this will yield
                        // an error that is safe to ignore (`ENOTCONN`). The other
                        // possible errors relate to an invalid file descriptor.
                        peer.disconnect().ok();

                        self.unregister_peer(addr, reason.into(), service);
                    }
                }
                Io::SetTimer(timeout) => {
                    self.timeouts.register((), local_time + timeout);
                }
                Io::Event(event) => {
                    trace!("Event: {:?}", event);

                    publisher.publish(event);
                }
            }
        }
    }

    fn handle_readable<S>(&mut self, addr: Id, service: &mut S)
    where
        S: Service<Id>,
    {
        // Nb. If the socket was readable and writable at the same time, and it was disconnected
        // during an attempt to write, it will no longer be registered and hence available
        // for reads.
        if let Some(socket) = self.peers.get_mut(&addr) {
            let mut buffer = [0; READ_BUFFER_SIZE];

            let socket_addr = addr.to_socket_addr();
            trace!("{}: Socket is readable", socket_addr);

            // Nb. Since `poll`, which this reactor is based on, is *level-triggered*,
            // we will be notified again if there is still data to be read on the socket.
            // Hence, there is no use in putting this socket read in a loop, as the second
            // invocation would likely block.
            match socket.read(&mut buffer) {
                Ok(count) => {
                    if count > 0 {
                        trace!("{}: Read {} bytes", socket_addr, count);

                        service.message_received(&addr, Cow::Borrowed(&buffer[..count]));
                    } else {
                        trace!("{}: Read 0 bytes", socket_addr);
                        // If we get zero bytes read as a return value, it means the peer has
                        // performed an orderly shutdown.
                        socket.disconnect().ok();
                        self.unregister_peer(
                            addr,
                            Disconnect::ConnectionError(Arc::new(io::Error::from(
                                io::ErrorKind::ConnectionReset,
                            ))),
                            service,
                        );
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    // This shouldn't normally happen, since this function is only called
                    // when there's data on the socket. We leave it here in case external
                    // conditions change.
                }
                Err(err) => {
                    trace!("{}: Read error: {}", socket_addr, err.to_string());

                    socket.disconnect().ok();
                    self.unregister_peer(addr, Disconnect::ConnectionError(Arc::new(err)), service);
                }
            }
        }
    }

    fn handle_writable<S: Service<Id>>(
        &mut self,
        addr: Id,
        service: &mut S,
    ) -> io::Result<()> {
        let socket_addr = addr.to_socket_addr();
        trace!("{}: Socket is writable", socket_addr);

        let token = self.source_keys.get(&addr).unwrap();
        let socket = self.peers.get_mut(&addr).unwrap();

        // "A file descriptor for a socket that is connecting asynchronously shall indicate
        // that it is ready for writing, once a connection has been established."
        //
        // Since we perform a non-blocking connect, we're only really connected once the socket
        // is writable.
        if self.connecting.remove(&addr) {
            let local_addr = socket.local_address()?;
            service.connected(addr.clone(), &local_addr, socket.link);
        }

        match socket.flush() {
            // In this case, we've written all the data, we
            // are no longer interested in writing to this
            // socket.
            Ok(()) => {
                if let Err(e) = self.poll.registry().reregister(socket.raw(), *token, Interest::READABLE) {
                    error!("failed to reregister read interest: {:?}", e.to_string());
                }
            }
            // In this case, the write couldn't complete. Set
            // our interest to `WRITE` to be notified when the
            // socket is ready to write again.
            Err(err)
                if [io::ErrorKind::WouldBlock, io::ErrorKind::WriteZero].contains(&err.kind()) =>
            {
                if let Err(e) = self.poll.registry().reregister(socket.raw(), *token, Interest::READABLE.add(Interest::WRITABLE)) {
                    error!("failed to reregister read+write interest: {:?}", e.to_string());
                }
            }
            Err(err) => {
                error!(target: "net", "{}: Write error: {}", socket_addr, err.to_string());

                socket.disconnect().ok();
                self.unregister_peer(addr, Disconnect::ConnectionError(Arc::new(err)), service);
            }
        }
        Ok(())
    }
}

/// Connect to a peer given a remote address.
fn dial(addr: &net::SocketAddr) -> Result<mio::net::TcpStream, io::Error> {
    use socket2::{Domain, Socket, Type};
    fallible! { io::Error::from(io::ErrorKind::Other) };

    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::STREAM, None)?;

    sock.set_read_timeout(Some(READ_TIMEOUT))?;
    sock.set_write_timeout(Some(WRITE_TIMEOUT))?;
    sock.set_nonblocking(true)?;

    match sock.connect(&(*addr).into()) {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(e) if e.raw_os_error() == Some(libc::EALREADY) => {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists))
        }
        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
        Err(e) => return Err(e),
    }
    Ok(mio::net::TcpStream::from_std(sock.into()))
}
