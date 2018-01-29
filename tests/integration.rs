
#[cfg(test)]
mod tests {
    extern crate rumqtt;
    extern crate loggerv;

    use std::thread;
    use std::time::Duration;
    use std::sync::{Arc, Mutex};

    use self::rumqtt::{MqttOptions, ReconnectOptions, MqttClient, QoS, SecurityOptions, ConnectionMethod};

    #[test]
    fn basic_publish_notifications() {
        loggerv::init_with_verbosity(1).unwrap();
        let mqtt_opts = MqttOptions::new("rumqtt-core", "127.0.0.1:1883").unwrap()
                                    .set_reconnect_opts(ReconnectOptions::Always(10));

        let (mut client, receiver) = MqttClient::start(mqtt_opts);
        let counter = Arc::new(Mutex::new(0));

        client.subscribe(vec![("hello/world", QoS::AtLeastOnce)]).unwrap();

        let counter_clone = Arc::clone(&counter);
        thread::spawn(move || {
            for _ in receiver {
                *counter_clone.lock().unwrap() += 1;
            }
        });

        for _ in 0..3 {
            if let Err(e) = client.publish("hello/world", QoS::AtLeastOnce, vec![1, 2, 3]) {
                println!("{:?}", e);
            }
            thread::sleep(Duration::new(1, 0));
        }

        thread::sleep(Duration::new(10, 0));
        // 1 for suback
        // 3 for puback
        // 3 for actual published messages
        assert_eq!(*counter.lock().unwrap(), 7);
    }

    #[test]
    #[should_panic]
    fn no_client_id_set() {
        loggerv::init_with_verbosity(1).unwrap();
        let mqtt_opts = MqttOptions::new("", "127.0.0.1:8883").unwrap()
                                    .set_reconnect_opts(ReconnectOptions::Always(10));
    }

    #[test]
    #[should_panic]
    fn client_id_starts_with_whitespace() {
        loggerv::init_with_verbosity(1).unwrap();
        let mqtt_opts = MqttOptions::new(" mqtt", "127.0.0.1:8883").unwrap()
                                    .set_reconnect_opts(ReconnectOptions::Always(10));
    }

    #[test]
    fn client_deliberate_disconnects() {
        loggerv::init_with_verbosity(1).unwrap();
        let mqtt_opts = MqttOptions::new("clean_session_check", "localhost:8883").unwrap()
                            .set_reconnect_opts(ReconnectOptions::Never)
                            .set_clean_session(false)
                            .set_security_opts(SecurityOptions::None)
                            .set_connection_method(ConnectionMethod::Tls("/home/creativcoder/certs/ca.cert.pem".into(), None));

        let (mut client, receiver) = MqttClient::start(mqtt_opts);
        if let Err(e) = client.publish("/devices/RAVI-MAC/events/imu", QoS::AtLeastOnce, vec![1, 2, 3]) {
            println!("Publish error = {:?}", e);
        }
        thread::sleep_ms(4000);
        // TODO how do we assert here ?
        client.disconnect();
    }

    // #[test]
    // fn clean_session_set_to_false_same_client_on_new_connection_should_retrieve_older_messages() {
    //     loggerv::init_with_verbosity(1).unwrap();

    //     {
    //         let mqtt_opts = MqttOptions::new("clean_session_check", "localhost:8883").unwrap()
    //                                     .set_reconnect_opts(ReconnectOptions::Never)
    //                                     .set_clean_session(false)
    //                                     .set_security_opts(SecurityOptions::None)
    //                                     .set_connection_method(ConnectionMethod::Tls("/home/creativcoder/certs/ca.cert.pem".into(), None));

    //         let (mut client, receiver) = MqttClient::start(mqtt_opts);
    //         for _ in 0..100 {
    //             if let Err(e) = client.publish("/devices/RAVI-MAC/events/imu", QoS::AtLeastOnce, vec![1, 2, 3]) {
    //                 println!("Publish error = {:?}", e);
    //             }
    //         }

    //         thread::sleep_ms(4000);
    //         client.disconnect();
    //     }

        // TODO init same new connection
    // }
}