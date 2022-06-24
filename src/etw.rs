use windows::core::{GUID, PSTR};
use windows::Win32::Foundation::WIN32_ERROR;
use core::ptr::null_mut;
use windows::Win32::System::Diagnostics::Etw::{StartTraceA, StopTraceA, EnableTraceEx2, OpenTraceA, ProcessTrace};
use windows::Win32::System::Diagnostics::Etw::{
    EVENT_TRACE_PROPERTIES, EVENT_TRACE_LOGFILEA, EVENT_RECORD, PEVENT_RECORD_CALLBACK,
    PROCESS_TRACE_MODE_REAL_TIME,  PROCESS_TRACE_MODE_EVENT_RECORD, EVENT_TRACE_USE_PAGED_MEMORY,
    EVENT_TRACE_REAL_TIME_MODE, WNODE_FLAG_TRACED_GUID, TRACE_LEVEL_VERBOSE
};
use std::{thread, time::Duration};

/// Complete Trace Properties struct
///
/// The [EventTraceProperties] struct contains the information about a tracing session, this struct
/// also needs two buffers right after it to hold the log file name and the session name. This struct
/// provides the full definition of the properties plus the the allocation for both names
///
/// See: [EVENT_TRACE_PROPERTIES](https://docs.microsoft.com/en-us/windows/win32/api/evntrace/ns-evntrace-event_trace_properties)

extern "system" fn callback_func(pevent: *mut EVENT_RECORD) {
    let event = unsafe {*pevent};

    println!("{:?}", event.EventHeader.EventDescriptor );
    println!("{}", event.UserDataLength);
    unsafe {
        let buffer: Vec<u8> = std::slice::from_raw_parts(event.UserData as *mut _, event.UserDataLength.into()).to_vec();
        println!("{:?}", buffer);
        //buffer.dr
    }

}


pub fn test () {
    let guid_str = "1C95126E-7EEA-49A9-A3FE-A378B03DDB4D";
    let dns_guid = GUID::from(guid_str);
    let mut session_name = "etw2Kafka".to_owned();
    let callback: PEVENT_RECORD_CALLBACK = Some(callback_func);

    let mut h_trace = 0;

    

    let mut trace = EVENT_TRACE_LOGFILEA::default();
    trace.LoggerName = PSTR(session_name.as_mut_ptr());
    trace.Anonymous2.EventRecordCallback = callback;
    trace.Anonymous1.ProcessTraceMode = PROCESS_TRACE_MODE_REAL_TIME | PROCESS_TRACE_MODE_EVENT_RECORD;

    let mut trace_prop = EVENT_TRACE_PROPERTIES::default();
    trace_prop.Wnode.BufferSize = std::mem::size_of::<EVENT_TRACE_PROPERTIES>() as u32 + session_name.len() as u32 + 2;
    trace_prop.Wnode.ClientContext = 1; // system time
    trace_prop.Wnode.Flags = WNODE_FLAG_TRACED_GUID;
    trace_prop.LogFileMode = EVENT_TRACE_REAL_TIME_MODE | EVENT_TRACE_USE_PAGED_MEMORY;
    trace_prop.LogFileNameOffset = 0;
    trace_prop.LoggerNameOffset = std::mem::size_of::<EVENT_TRACE_PROPERTIES>() as u32;
    

    let mut ret = unsafe { StartTraceA(&mut h_trace, session_name.clone(),  &mut trace_prop) };
    println!("StartTraceA = {}", ret);
    println!("Trace handle = {}", h_trace);
    println!("{:?}", dns_guid);
    ret = unsafe { EnableTraceEx2(
        h_trace, 
        &dns_guid, 
        1,
        TRACE_LEVEL_VERBOSE.try_into().unwrap(), 
        0xffffffffffffffff, 
        0,
        0, 
        null_mut()
    ) };

    println!("EnableTraceEx2 = {}", ret);

    let trace_h = unsafe { OpenTraceA(&mut trace) };
    println!("OpenTraceA handle = {}", trace_h);

    thread::spawn(move || unsafe { 
        let mut ret = ProcessTrace(&[trace_h], std::ptr::null_mut(), std::ptr::null_mut());
        println!("ProcessTrace = {}", ret);
        ret = StopTraceA(h_trace, session_name.clone(),  &mut trace_prop);
        println!("StopTraceA = {}", ret);
    });

    println!("Going to sleep");
    thread::sleep(Duration::from_secs(60));
    //ret = unsafe { StopTraceA(h_trace, session_name.clone(),  &mut trace_prop) };
    //println!("StopTraceA = {}", ret);
}