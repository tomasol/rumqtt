use std::net::{TcpStream, SocketAddr, Shutdown};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
use std::thread;
use std::io::{Write, ErrorKind};
use std::collections::VecDeque;

use threadpool::ThreadPool;
use mqtt3::{Message, PacketIdentifier, QoS, Packet, MqttWrite, MqttRead};
use error::{Result, Error};
use clientoptions::MqttOptions;
use stream::{NetworkStream, SslContext};
use callbacks::MqttCallback;
// static mut N: i32 = 0;

enum HandlePacket {
    Publish(Box<Message>),
    PubAck(Option<Message>),
    PubRec(Option<Message>),
    PubComp,
    SubAck,
    UnSubAck,
    PingResp,
    Disconnect,
    None,
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
    // TODO: Box<Message> can be replaced with `Publish`
    pub outgoing_pub: VecDeque<(Box<Message>)>,
    /// For QoS 2. Store for incoming publishes to record.
    pub incoming_rec: VecDeque<Box<Message>>, //
    /// For QoS 2. Store for outgoing publishes.
    pub outgoing_rec: VecDeque<(Box<Message>)>,
    /// For Qos2. Store for outgoing `pubrel` packets.
    pub outgoing_rel: VecDeque<(PacketIdentifier)>,
    /// For Qos2. Store for outgoing `pubcomp` packets.
    pub outgoing_comp: VecDeque<(PacketIdentifier)>,

    // TODO: subscriptions remember
    pub no_of_reconnections: u32,

    pub pool: ThreadPool,
}


impl Connection {
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

    fn publish(&mut self, mut message: Box<Message>) -> Result<()> {
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
            QoS::ExactlyOnce => {
                if payload_len > self.opts.storepack_sz {
                    size_exceeded = true;
                    warn!("Dropping packet: Size limit exceeded");
                } else {
                    self.outgoing_rec.push_back(message.clone());
                }

                if self.outgoing_rec.len() > self.opts.pub_q_len as usize * 50 {
                    warn!(":( :( Outgoing Record Queue Length growing bad --> {:?}", self.outgoing_rec.len());
                }
            }
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

    pub fn disconnect(&mut self) -> Result<()> {
        let disconnect = Packet::Disconnect;
        self.write_packet(disconnect)?;
        Ok(())
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
}
