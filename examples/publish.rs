extern crate rumqtt;
extern crate loggerv;

use std::thread;
use std::time::Duration;

use rumqtt::{MqttOptions, ReconnectOptions, MqttClient, QoS, SecurityOptions};

fn main() {
    loggerv::init_with_verbosity(1).unwrap();
    let security_opts = SecurityOptions::GcloudIotCore(("unused".to_owned(), "rsa_private.der".to_owned(), 3600));
    let mqtt_opts = MqttOptions::new("rumqtt-test", "mqtt.googleapis.com:8883")
                                .set_reconnect_opts(ReconnectOptions::Always(10))
                                .set_security_opts(security_opts);

    let (mut client, receiver) = MqttClient::start(mqtt_opts);

    for i in 0..100000 {
        if let Err(e) = client.publish("hello/world", QoS::AtLeastOnce, vec![1, 2, 3]) {
            println!("{:?}", e);
        }
        // thread::sleep(Duration::new(1, 0));
    }

    thread::sleep(Duration::new(60, 0));
}