use core::ffi::c_void;
use std::{thread, time, ptr::null_mut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::path::Path;
use std::fs::File;
use std::io::prelude::*;
use clap::{Command, Arg};
use windows::Win32::System::EventLog::*;
use windows::Win32::Foundation::{GetLastError,ERROR_EVT_CHANNEL_NOT_FOUND, ERROR_EVT_INVALID_QUERY, ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS };
use quickxml_to_serde::{xml_string_to_json, Config};
use rdkafka::{ClientContext, Message};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseRecord, BaseProducer, ProducerContext, DeliveryResult, Producer};
use crossbeam::channel::{unbounded, Sender, Receiver};
use ctrlc;
use threadpool::ThreadPool;
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;
use std::time::{Duration, Instant};

mod etw;
use etw::test;

const EVT_RENDER_FLAG_EVENT_XML: u32 = 1;
const EVT_RENDER_FLAG_BOOKMARK: u32 = 2;
const EVT_SUBSCRIBE_FUTURE_EVENTS: u32 = 1;
const EVT_SUBSCRIBE_AFTER_BOORKMARK: u32 = 3;

/// Kafka producer context that logs producer errors
pub struct CaptureErrorContext {
    tx: Sender<String>,
}

impl ClientContext for CaptureErrorContext {}

impl ProducerContext for CaptureErrorContext {
    type DeliveryOpaque = ();

    /// This method is called after attempting to send a message to Kafka.
    /// It's called asynchronously for every message, so we want to handle errors explicitly here.
    fn delivery(&self, result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        if let Err((error, message)) = result {
            let message_str: &str = match message.payload_view::<str>() {
                Some(x) => x.unwrap_or("{}"),
                None => ""
            };

            // println!(
            //     "failed to produce message to Kafka (delivery callback): {}",
            //     error
            // );
            self.tx.send(message_str.to_string()).unwrap();
        }
    }
}

struct Communication {
    tx: Sender<String>,
    bookmark: isize,
}

fn validate_workers_args(args: &str) -> Result<(), String> {
    let test = args.parse::<u8>();
    if test.is_ok() {
        Ok(())
    }
    else {
        Err("value must be an int".to_string())
    }
}

fn produce_message(rx: Receiver<String>, tx: Sender<String>, brokers: &str, topic: &str, is_running_thread: &AtomicBool) {
    let producer = init_kafka(brokers, tx);

    let pool = ThreadPool::with_name("sender".into(), 10);

    let producer1 = producer.clone();
    thread::spawn(move || 
        loop {
            let milli_time = time::Duration::from_millis(100);
            thread::sleep(milli_time);
            producer1.poll(milli_time);
        }
    );

    while is_running_thread.load(Ordering::Relaxed) {
        let received = rx.recv_timeout(Duration::from_millis(1));
        match received {
            Ok(xml) => {
                
                let topic2 = topic.to_string();
                let producer2 = producer.clone();
                pool.execute(move || {
                    let json = render_json(xml);
                    send_to_kafka(json.clone(), topic2, producer2)
                }
                );
            },
            Err(_) => ()
        }
        
    }

    println!("[Worker thread] Interruption signal received, waiting for all messages to be sent");  
    while pool.active_count() != 0 || producer.in_flight_count() > 0 {
       ()  
    };
    println!("[Worker thread] Terminated");
}

unsafe fn create_subscription(channel: &str, query: &str, producer_ptr: *const c_void, callback: EVT_SUBSCRIBE_CALLBACK) -> Result<isize, String> {
    let h_subscription = EvtSubscribe(0 , None , channel, query, 0, producer_ptr, callback, EVT_SUBSCRIBE_FUTURE_EVENTS);

        if h_subscription == 0 {
            let status = GetLastError();

            if status == ERROR_EVT_CHANNEL_NOT_FOUND {
                return Err(format!("Channel \"{0}\" not found.", channel));
            } else if status == ERROR_EVT_INVALID_QUERY {
                return Err(format!("Query \"{0}\" is not valid.", query));
            }
            else {
                return Err(format!("EvtSubscribe failed"));
            }
        }
    Ok(h_subscription)
}

unsafe fn create_subscription_with_bookmark(channel: &str, query: &str, producer_ptr: *const c_void, callback: EVT_SUBSCRIBE_CALLBACK, boorkmark: isize) -> Result<isize, String> {
    let h_subscription = EvtSubscribe(0 , None , channel, query, boorkmark, producer_ptr, callback, EVT_SUBSCRIBE_AFTER_BOORKMARK);

        if h_subscription == 0 {
            let status = GetLastError();

            if status == ERROR_EVT_CHANNEL_NOT_FOUND {
                return Err(format!("Channel \"{0}\" not found.", channel));
            } else if status == ERROR_EVT_INVALID_QUERY {
                return Err(format!("Query \"{0}\" is not valid.", query));
            }
            else {
                return Err(format!("EvtSubscribe failed"));
            }
        }
    Ok(h_subscription)
}

unsafe fn close_subscription(subscription: isize) -> Result<(), ()> {
    let result = EvtClose(subscription);
    if result == false { Err(())}
    else {Ok(())}
}

extern "system" fn subscription_callback(action: EVT_SUBSCRIBE_NOTIFY_ACTION, usercontext: *const c_void, h_event: isize) -> u32 {
    let tx = &mut(unsafe { &mut *(usercontext as *mut Communication)}).tx;
    let bookmark =  &mut(unsafe { &mut *(usercontext as *mut Communication)}).bookmark;

    if action == EvtSubscribeActionError {
        println!("Error: {:?}", action);
        return 1;
    } else if action == EvtSubscribeActionDeliver {
        unsafe { EvtUpdateBookmark(*bookmark, h_event) };
        let xml = render_event(h_event, EVT_RENDER_FLAG_EVENT_XML);
        //let json = render_json(xml);
        tx.send(xml).unwrap();
    } else {
        println!("Unknown error: {:?}", action);
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

fn init_kafka(brokers: &str, tx: Sender<String>) -> BaseProducer<CaptureErrorContext> {
    let producer: BaseProducer<CaptureErrorContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.username", "admin")
        .set("sasl.password", "admin-secret")
        .set("sasl.mechanism", "PLAIN")
        .set("batch.size", "1000000")
        .set("linger.ms", "10")
        .set("compression.type", "lz4")
        .set("acks", "1")
        .create_with_context(CaptureErrorContext {tx: tx})
        .expect("Producer creation error");

    return producer;
}

fn send_to_kafka(json: String, topic: String, producer: BaseProducer<CaptureErrorContext>) -> () {
    let message = &format!("{}", json);
    let delivery_status = producer
    .send(
        BaseRecord::to(&topic)
            .payload(message)
            .key("evtx"),
    );

    match delivery_status {
        Ok(_) => (),
        Err(_) => println!("oups"),
    }
}

fn get_bookmark(bookmark_file: Option<&str>) -> isize {
    match bookmark_file {
        Some(file_path) => {
            println!("[Main thread] Using bookmark file {}", file_path);
            let file = File::open(file_path);
            let mut contents: String = String::new();
            match file  {
                Ok(mut file) => {
                    match file.read_to_string(&mut contents) {
                        Ok(_) => return unsafe { EvtCreateBookmark(contents) },
                        Err(e) => println!("Error while reading bookmark file: {}", e)
                    }
                }
                Err(e) => println!("Error while opening bookmark file: {}", e)
            }
        },
        _ => ()
    }

    return unsafe { EvtCreateBookmark(None) }
}

fn main() {
    let matches = Command::new("evt2kafka")
    .author("Crypt0-M3lon")
    .arg(Arg::new("brokers")
        .short('b')
        .long("brokers")
        .required(true)
        .takes_value(true)
        .multiple_occurrences(true)
        .help("Brokers addresses"))
    .arg(Arg::new("topic")
        .short('t')
        .long("topic")
        .required(true)
        .takes_value(true)
        .help("Destination topic"))
    .arg(Arg::new("channel")
        .short('c')
        .long("channel")
        .required(true)
        .takes_value(true)
        .help("Channel to subscribe to"))
    .arg(Arg::new("query")
        .short('q')
        .long("query")
        .required(true)
        .takes_value(true)
        .help("XPath filter"))
    .arg(Arg::new("workers")
        .short('w')
        .long("workers")
        .takes_value(true)
        .default_value("1")
        .validator(validate_workers_args)
        .help("Number of worker threads"))
    .arg(Arg::new("bookmark")
        .short('k')
        .long("bookmark")
        .takes_value(true)
        .help("Bookmark file to use"))
    .arg(Arg::new("keep-queue")
        .long("keep-queue")
        .takes_value(false)
        .help("Read/Write queue to disk instead of dropping events"))
    .get_matches();

    //test();

    let is_running = Arc::new(AtomicBool::new(true));
    let is_thread_running = Arc::new(AtomicBool::new(true));

    let channel = matches.value_of("channel").expect("required channel");
    let query = matches.value_of("query").expect("required query");
    let topic = matches.value_of("topic").expect("required topic").to_string();
    let workers = matches.value_of("workers").and_then(|s| s.parse::<u8>().ok()).expect("required workers number");
    let bookmark_file = matches.value_of("bookmark");
    let bookmark = get_bookmark(bookmark_file);

    let brokers : Vec<_> = matches.values_of("brokers").unwrap().collect();
    let brokers_list = brokers.join(",");

    
    let (tx, rx) = unbounded();
    let tx_callback = tx.clone();

    let mut communication = Communication { tx: tx_callback , bookmark: bookmark};
    let communication_ptr: *mut c_void = &mut communication as *mut _ as *mut c_void;
    let callback: EVT_SUBSCRIBE_CALLBACK = Some(subscription_callback);

    // load queue.flate
    if matches.is_present("keep-queue") {
        if Path::new("queue.flate").exists() {
            println!("Loading queue file");
            let file = File::open("queue.flate").unwrap();
            let mut deflater = GzDecoder::new(file);
            let mut buf = Vec::new(); // = String::new();
            deflater.read_to_end(&mut buf).expect("cannot deflate");

            let s = String::from_utf8(buf).expect("convert from utf8");
            for line in s.split("\n") {
                tx.send(line.to_string()).expect("cannot send");
            }
        }
    }

    // create the subscription
    let subscription: isize;
    unsafe {
        if bookmark_file.is_some() {
            match create_subscription_with_bookmark(channel, query, communication_ptr, callback, bookmark) {
                Err(e) => {
                    eprintln!("{}", e); 
                    std::process::exit(1);
                },
                Ok(sub) => {
                    println!("[Main thread] Subscription setup complete");
                    subscription = sub;
                },
            }
        }
        else {
            match create_subscription(channel, query, communication_ptr, callback) {
                Err(e) => {
                    eprintln!("{}", e); 
                    std::process::exit(1);
                },
                Ok(sub) => {
                    println!("[Main thread] Subscription setup complete");
                    subscription = sub;
                },
            }
        }
    }

    // if ctrlc is received, update bool to send signal to stop
    let is_running_ctrl = Arc::clone(&is_running);
    ctrlc::set_handler(move || {
        println!("[Interruption thread] Interruption received");
        is_running_ctrl.swap(false, Ordering::Relaxed);
    })
    .expect("Error setting Ctrl-C handler");

    let pool = ThreadPool::with_name("worker".into(), workers.into());

    // spawn N workers according to args
    for _ in 0..workers {
        let tx_thread = tx.clone();
        let rx_thread = rx.clone();
        let brokers_list_thread = brokers_list.clone();
        let topic_thread = topic.clone();
        let is_running_thread = Arc::clone(&is_thread_running);
        pool.execute(move || produce_message(rx_thread, tx_thread, &brokers_list_thread, &topic_thread, &is_running_thread));
    }

    let one_sec = time::Duration::from_millis(1000);

    while is_running.load(Ordering::Relaxed) {
        thread::sleep(one_sec);
    }

    // block until a signal is received on control channel
    unsafe {close_subscription(subscription).unwrap()};
    println!("[Main thread] Subscription closed");

    is_thread_running.swap(false, Ordering::Relaxed);

    println!("[Main thread] Waiting for all worker threads to finish"); 
    while pool.active_count() != 0 {
        ()
    };

    if !rx.is_empty() {
        println!("[Main thread] {} messages remaining in the queue", rx.len());
        
        if matches.is_present("keep-queue") {
            let v: Vec<_> = rx.try_iter().collect();
            
            let queue_file = "queue.flate";    
            let file = File::create(queue_file).unwrap();
            let mut encoder = GzEncoder::new(file, Compression::default());
            for line in v.iter() {
                write!(encoder, "{}\n", line).expect("Error while write queue line");
            }
            encoder.finish().unwrap();
            println!("[Main thread] Queue saved to file {}", queue_file);
        } else { println!("[Main thread] Queue dropped") }
    }
    
    if bookmark_file.is_some() {
        let bookmark_xml = render_event(bookmark, EVT_RENDER_FLAG_BOOKMARK);
        let path = bookmark_file.unwrap();
        println!("[Main thread] Saving bookmark to file {}", path);
        let mut file = File::create(path).unwrap();
        file.write_all(bookmark_xml.as_bytes()).expect("Unable to save bookmark");
    };
}