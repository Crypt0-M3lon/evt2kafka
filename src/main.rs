use core::ffi::c_void;
use std::time::Duration;
use std::{thread, time, ptr::null_mut};
use windows::Win32::System::EventLog::*;
use windows::Win32::Foundation::{GetLastError,ERROR_EVT_CHANNEL_NOT_FOUND, ERROR_EVT_INVALID_QUERY, ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS };
use quickxml_to_serde::{xml_string_to_json, Config};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
//use tokio::runtime::Runtime;

const EVT_RENDER_FLAG_EVENT_XML: u32 = 1;

fn main() {
    let ten_millis = time::Duration::from_millis(10);

    let channel = "Windows PowerShell";
    let query = "*";
    let callback: EVT_SUBSCRIBE_CALLBACK = Some(subscription_callback);
    let context = std::ptr::null_mut();
    unsafe {
        let h_subscription = EvtSubscribe(0 , None , channel, query, 0, context, callback, 1);

        println!("{0}", h_subscription);
        if h_subscription == 0 {
            let status = GetLastError();

            if status == ERROR_EVT_CHANNEL_NOT_FOUND {
                println!("Channel {0} was not found.", channel);
            } else if status == ERROR_EVT_INVALID_QUERY {
                println!("Query {0} is not valid.", query);
            }
            else {
                println!("EvtSubscribe failed");
            }
        }

        loop {
            thread::sleep(ten_millis);
        }
    }
}


unsafe extern "system" fn subscription_callback(action: EVT_SUBSCRIBE_NOTIFY_ACTION, _usercontext: *const c_void, h_event: isize) -> u32 {
    println!("New event !");
    let producer = init_kafka("127.0.0.1:29092");
    if action == EvtSubscribeActionError {
        println!("oups error");
        return 1;
    } else if action == EvtSubscribeActionDeliver {
        let xml = render_event(h_event, EVT_RENDER_FLAG_EVENT_XML);
        let json = render_json(xml);
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(send_to_kafka(json, "test", producer));
    } else {
        println!("Unknown error");
        return 1;
    }
    
    return 0;
}

fn render_event(h_event: isize, flag: u32) -> String {
    
    let mut buffersize = 0;
    let mut bufferused: u32 = 0;

    unsafe {
        EvtRender(0, h_event, flag, buffersize, null_mut(), &mut bufferused as *mut u32, null_mut());


        let status = GetLastError();
        if status != ERROR_SUCCESS && status != ERROR_INSUFFICIENT_BUFFER {
            println!("Unknown error during rendering");
        } 
    }

    buffersize = bufferused;
    let mut buf: Vec<u16> = vec![0; buffersize as usize];
    
    if !unsafe {
        EvtRender(
            0,
            h_event,
            flag,
            buffersize,
            buf.as_mut_ptr() as *mut c_void,
            &mut bufferused,
            std::ptr::null_mut(),
        )
        .as_bool()
    } {
        println!("Unable to render event");
    }
    
    let xml = String::from_utf16_lossy(&buf[..]).trim_matches(char::from(0)).to_string();
    return xml;
    
}


fn render_json(xml: String) -> String {
    let json = xml_string_to_json(xml, &Config::new_with_defaults());
    let json_text = json.unwrap();
    return json_text.to_string();
} 

async fn send_to_kafka(json: String, topic: &str, producer: FutureProducer) {
    let _delivery_status = producer
    .send(
        FutureRecord::to(topic)
            .payload(&format!("Message {}", json))
            .key("evtx")
            ,
        Duration::from_secs(0),
    )
    .await;

    // This will be executed when the result is received.
    println!("Delivery status for message received");
    
}

fn init_kafka(brokers: &str) -> FutureProducer {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    return producer;
}