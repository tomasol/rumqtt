use std::net::{TcpStream, SocketAddr, Shutdown};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
use std::thread;
use std::io::{Write, ErrorKind};
use std::collections::VecDeque;

use threadpool::ThreadPool;
use error::{Result, Error};
use clientoptions::MqttOptions;
use stream::{NetworkStream, SslContext};
use callbacks::MqttCallback;

use mqtt3::{self, Connect, Connack, ConnectReturnCode, Protocol, Message, PacketIdentifier, QoS, Packet};
// static mut N: i32 = 0;

enum HandlePacket {
    Connack,
    Publish(Box<Message>),
    PubAck(Option<Message>),
    SubAck,
    PingResp,
    Disconnect,
    Invalid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttState {
    Handshake,
    Connected,
    Disconnected,
}

#[derive(Debug)]
pub enum NetworkRequest {
    Publish(Box<Message>),
    Shutdown,
    Disconnect,
}

pub struct Connection {
    pub addr: SocketAddr,
    pub opts: MqttOptions,
    pub stream: NetworkStream,
    pub nw_request_rx: Receiver<NetworkRequest>,
    pub state: MqttState,
    pub initial_connect: bool,
    pub await_pingresp: bool,
    pub last_flush: Instant,

    pub last_pkid: PacketIdentifier,

    // Callbacks
    pub callback: Option<MqttCallback>,

    // Queues. Note: 'record' is qos2 term for 'publish'
    /// For QoS 1. Stores outgoing publishes
    pub outgoing_pub: VecDeque<(Box<Message>)>,

    // TODO: subscriptions remember
    pub no_of_reconnections: u32,

    pub pool: ThreadPool,
}


impl Connection {
    pub fn connect(addr: SocketAddr,
                   opts: MqttOptions,
                   nw_request_rx: Receiver<NetworkRequest>,
                   callback: Option<MqttCallback>)
                   -> Result<Self> {

        let connection = Connection {
            addr: addr,
            opts: opts,
            stream: NetworkStream::None,
            nw_request_rx: nw_request_rx,
            state: MqttState::Disconnected,
            initial_connect: true,
            await_pingresp: false,
            last_flush: Instant::now(),
            last_pkid: PacketIdentifier(0),

            outgoing_pub: VecDeque::new(),

            callback: callback,
            no_of_reconnections: 0,

            // Threadpool
            pool: ThreadPool::new(1),
        };

        Ok(connection)
    }

    pub fn run(&mut self) -> Result<()> {
        'reconnect: loop {
            match self.try_reconnect() {
                Ok(_) => {
                    self.stream.set_read_timeout(Some(Duration::new(1, 0)))?;
                    self.stream.set_write_timeout(Some(Duration::new(10, 0)))?;
                }
                Err(e) => {
                    error!("Couldn't connect. Error = {:?}", e);
                    if self.initial_connect {
                        return Err(e);
                    } else {
                        continue;
                    }
                }
            }

            if let Err(e) = self.read_incoming() {
                match e {
                    Error::PingTimeout | Error::Reconnect => continue 'reconnect,
                    Error::MqttConnectionRefused(_) => {
                        if self.initial_connect {
                            return Err(e);
                        } else {
                            continue 'reconnect;
                        }
                    }
                    _ => break,
                }
            }
        }

        Ok(())
    }

    /// Creates a Tcp Connection, Sends Mqtt connect packet and sets state to
    /// Handshake mode if Tcp write and Mqtt connect succeeds
    fn try_reconnect(&mut self) -> Result<()> {
        if !self.initial_connect {
            error!("  Will try Reconnect in {:?} seconds", self.opts.reconnect);
            thread::sleep(Duration::new(self.opts.reconnect as u64, 0));
        }

        let stream = TcpStream::connect(&self.addr)?;
        let stream = match self.opts.ca {
            Some(ref ca) => {
                if let Some((ref crt, ref key)) = self.opts.client_cert {
                    let ssl_ctx: SslContext = SslContext::new(ca, Some((crt, key)), self.opts.verify_ca)?;
                    NetworkStream::Tls(ssl_ctx.connect(stream)?)
                } else {
                    let ssl_ctx: SslContext = SslContext::new(ca, None::<(String, String)>, self.opts.verify_ca)?;
                    NetworkStream::Tls(ssl_ctx.connect(stream)?)
                }
            }
            None => NetworkStream::Tcp(stream),
        };

        self.stream = stream;
        let connect = self.generate_connect_packet();
        let connect = Packet::Connect(connect);
        self.write_packet(connect)?;
        self.state = MqttState::Handshake;
        Ok(())
    }

    pub fn read_incoming(&mut self) -> Result<()> {
        'receive: loop {
            let packet = self.stream.read_packet();

            if let Ok(packet) = packet {
                if let Err(Error::MqttConnectionRefused(e)) = self.handle_packet(packet) {
                    return Err(Error::MqttConnectionRefused(e));
                }
            } else if let Err(Error::Mqtt3(mqtt3::Error::Io(e))) = packet {

                match e.kind() {
                    ErrorKind::TimedOut | ErrorKind::WouldBlock => {

                        // TODO: Test if PINGRESPs are properly recieved before
                        // next ping incase of high frequency incoming messages
                        if let Err(e) = self.ping() {
                            error!("PING error {:?}", e);
                            self.unbind();
                            return Err(Error::PingTimeout);
                        }

                        let _ = self.write();
                        continue 'receive;
                    }
                    _ => {
                        // Socket error are readily available here as soon as
                        // broker closes its socket end. (But not inbetween n/w disconnection
                        // and socket close at broker [i.e ping req timeout])

                        // UPDATE: Lot of publishes are being written by the time this notified
                        // the eventloop thread. Setting disconnect_block = true during write failure
                        error!("At line = {:?}. Error in receiving packet. Error = {:?}", line!(), e);
                        self.unbind();
                        return Err(Error::Reconnect);
                    }
                }
            } else {
                error!("At line = {:?}. Error in receiving packet. Error = {:?}", line!(), packet);
                self.unbind();
                return Err(Error::Reconnect);
            }
        }
    }

    fn write(&mut self) -> Result<()> {
        // @ Only read from `Network Request` channel when connected. Or else Empty
        // return.
        // @ Helps in case where Tcp connection happened but in MqttState::Handshake
        // state.
        if self.state == MqttState::Connected {
            for _ in 0..50 {
                match self.nw_request_rx.try_recv()? {
                    NetworkRequest::Shutdown => self.stream.shutdown(Shutdown::Both)?,
                    NetworkRequest::Disconnect => self.disconnect()?,
                    NetworkRequest::Publish(m) => self.publish(m)?,
                };
            }
        }
        Ok(())
    }

    fn ping(&mut self) -> Result<()> {
        // debug!("client state --> {:?}, await_ping --> {}", self.state,
        // self.await_ping);

        match self.state {
            MqttState::Connected => {
                if let Some(keep_alive) = self.opts.keep_alive {
                    let elapsed = self.last_flush.elapsed();

                    if elapsed >= Duration::from_millis(((keep_alive * 1000) as f64 * 0.9) as u64) {
                        if elapsed >= Duration::new((keep_alive + 1) as u64, 0) {
                            return Err(Error::PingTimeout);
                        }

                        // @ Prevents half open connections. Tcp writes will buffer up
                        // with out throwing any error (till a timeout) when internet
                        // is down. Eventhough broker closes the socket, EOF will be
                        // known only after reconnection.
                        // We just unbind the socket if there in no pingresp before next ping
                        // (What about case when pings aren't sent because of constant publishes
                        // ?. A. Tcp write buffer gets filled up and write will be blocked for 10
                        // secs and then error out because of timeout.)
                        if self.await_pingresp {
                            return Err(Error::AwaitPingResp);
                        }

                        let ping = Packet::Pingreq;
                        self.await_pingresp = true;
                        self.write_packet(ping)?;
                    }
                }
            }

            MqttState::Disconnected | MqttState::Handshake => error!("I won't ping. Client is in disconnected/handshake state"),
        }
        Ok(())
    }

    fn handle_packet(&mut self, packet: Packet) -> Result<HandlePacket> {
        match self.state {
            MqttState::Handshake => {
                if let Packet::Connack(connack) = packet {
                    self.handle_connack(connack)
                } else {
                    error!("Invalid Packet in Handshake State --> {:?}", packet);
                    Err(Error::ConnectionAbort)
                }
            }
            MqttState::Connected => {
                match packet {
                    Packet::Suback(..) => Ok(HandlePacket::SubAck),
                    Packet::Pingresp => {
                        self.await_pingresp = false;
                        Ok(HandlePacket::PingResp)
                    }
                    Packet::Disconnect => Ok(HandlePacket::Disconnect),
                    Packet::Puback(puback) => self.handle_puback(puback),
                    Packet::Publish(publ) => self.handle_message(Message::from_pub(publ)?),
                    _ => {
                        error!("Invalid Packet in Connected State --> {:?}", packet);
                        Ok(HandlePacket::Invalid)
                    }
                }
            }
            MqttState::Disconnected => {
                error!("Invalid Packet in Disconnected State --> {:?}", packet);
                Err(Error::ConnectionAbort)
            }
        }
    }

    ///  Checks Mqtt connack packet's status code and sets Mqtt state
    /// to `Connected` if successful
    fn handle_connack(&mut self, connack: Connack) -> Result<HandlePacket> {
        let code = connack.code;

        if code != ConnectReturnCode::Accepted {
            error!("Failed to connect. Error = {:?}", code);
            return Err(Error::MqttConnectionRefused(code));
        }

        if self.initial_connect {
            self.initial_connect = false;
        }

        self.state = MqttState::Connected;
        Ok(HandlePacket::Connack)
    }

    fn handle_message(&mut self, message: Box<Message>) -> Result<HandlePacket> {
        debug!("       Publish {:?} {:?} < {:?} bytes", message.qos, message.topic, message.payload.len());

        match message.qos {
            QoS::AtMostOnce => Ok(HandlePacket::Publish(message)),
            QoS::AtLeastOnce => {
                let pkid = message.pid.unwrap();
                self.puback(pkid)?;
                Ok(HandlePacket::Publish(message))
            }
            QoS::ExactlyOnce => Ok(HandlePacket::Publish(message)),
        }
    }

    fn handle_puback(&mut self, pkid: PacketIdentifier) -> Result<HandlePacket> {
        debug!("*** PubAck --> Pkid({:?})\n--- Publish Queue =\n{:#?}\n\n", pkid, self.outgoing_pub);
        let m = match self.outgoing_pub
            .iter()
            .position(|x| x.pid == Some(pkid)) {
            Some(i) => {
                if let Some(m) = self.outgoing_pub.remove(i) {
                    Some(*m)
                } else {
                    None
                }
            }
            None => {
                error!("Oopssss..unsolicited ack --> {:?}\n", pkid);
                None
            }
        };
        debug!("Pub Q Len After Ack @@@ {:?}", self.outgoing_pub.len());
        Ok(HandlePacket::PubAck(m))
    }

    fn publish(&mut self, message: Box<Message>) -> Result<()> {
        let pkid = self.next_pkid();
        let message = message.transform(Some(pkid), None);
        let payload_len = message.payload.len();
        let mut size_exceeded = false;

        match message.qos {
            QoS::AtMostOnce => (),
            QoS::AtLeastOnce => {
                if payload_len > self.opts.storepack_sz {
                    size_exceeded = true;
                    warn!("Dropping packet: Size limit exceeded");
                } else {
                    self.outgoing_pub.push_back(message.clone());
                }

                if self.outgoing_pub.len() > self.opts.pub_q_len as usize * 50 {
                    warn!(":( :( Outgoing Publish Queue Length growing bad --> {:?}", self.outgoing_pub.len());
                }
            }
            QoS::ExactlyOnce => (),
        }

        let packet = Packet::Publish(message.to_pub(None, false));
        match message.qos {
            QoS::AtMostOnce if !size_exceeded => self.write_packet(packet)?,
            QoS::AtLeastOnce | QoS::ExactlyOnce if !size_exceeded => {
                if self.state == MqttState::Connected {
                    self.write_packet(packet)?;
                } else {
                    warn!("State = {:?}. Skip network write", self.state);
                }
            }
            _ => {}
        }

        // error!("Queue --> {:?}\n\n", self.outgoing_pub);
        // debug!("       Publish {:?} {:?} > {} bytes", message.qos,
        // topic.clone().to_string(), message.payload.len());
        Ok(())
    }

    fn puback(&mut self, pkid: PacketIdentifier) -> Result<()> {
        let puback_packet = Packet::Puback(pkid);
        self.write_packet(puback_packet)?;
        Ok(())
    }

    // Spec says that client (for QoS > 0, persistant session [clean session = 0])
    // should retransmit all the unacked publishes and pubrels after reconnection.

    // NOTE: Sending duplicate pubrels isn't a problem (I guess ?). Broker will
    // just resend pubcomps
    fn force_retransmit(&mut self) {
        // Cloning because iterating and removing isn't possible.
        // Iterating over indexes and and removing elements messes
        // up the remove sequence
        let mut outgoing_pub = self.outgoing_pub.clone();
        debug!("*** Force Retransmission. Publish Queue =\n{:#?}\n\n", outgoing_pub);
        self.outgoing_pub.clear();

        while let Some(message) = outgoing_pub.pop_front() {
            let _ = self.publish(message);
        }
    }

    pub fn disconnect(&mut self) -> Result<()> {
        let disconnect = Packet::Disconnect;
        self.write_packet(disconnect)?;
        Ok(())
    }

    fn unbind(&mut self) {
        let _ = self.stream.shutdown(Shutdown::Both);
        self.await_pingresp = false;
        self.state = MqttState::Disconnected;

        // remove all the state
        if self.opts.clean_session {
            self.outgoing_pub.clear();
            // self.outgoing_rec.clear();
            // self.outgoing_rel.clear();
            // self.outgoing_comp.clear();
        }

        error!("  Disconnected {:?}", self.opts.client_id);
    }

    // http://stackoverflow.
    // com/questions/11115364/mqtt-messageid-practical-implementation
    #[inline]
    fn next_pkid(&mut self) -> PacketIdentifier {
        let PacketIdentifier(mut pkid) = self.last_pkid;
        if pkid == 65535 {
            pkid = 0;
        }
        self.last_pkid = PacketIdentifier(pkid + 1);
        self.last_pkid
    }

    // NOTE: write_all() will block indefinitely by default if
    // underlying Tcp Buffer is full (during disconnections). This
    // is evident when test cases are publishing lot of data when
    // ethernet cable is unplugged (mantests/half_open_publishes_and_reconnections
    // but not during mantests/ping_reqs_in_time_and_reconnections due to low
    // frequency writes. 10 seconds migth be good default for write timeout ?)

    #[inline]
    fn write_packet(&mut self, packet: Packet) -> Result<()> {
        if let Err(e) = self.stream.write_packet(&packet) {
            warn!("{:?}", e);
            return Err(e.into());
        }
        self.flush()?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.stream.flush()?;
        self.last_flush = Instant::now();
        Ok(())
    }

    fn generate_connect_packet(&self) -> Box<Connect> {
        let keep_alive = if let Some(dur) = self.opts.keep_alive {
            dur
        } else {
            0
        };

        Box::new(Connect {
            protocol: Protocol::MQTT(4),
            keep_alive: keep_alive,
            client_id: self.opts.client_id.clone().unwrap(),
            clean_session: self.opts.clean_session,
            last_will: None,
            username: self.opts.username.clone(),
            password: self.opts.password.clone(),
        })
    }
}
