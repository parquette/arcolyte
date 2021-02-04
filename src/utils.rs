//! Extra utility functions.

use std::sync::Once;
use fern;
use log::LogLevelFilter;
use chrono::Local;

use crate::errors::*;


/// Initialize the global logger and log to `rest_client.log`.
///
/// Note that this is an idempotent function, so you can call it as many
/// times as you want and logging will only be initialized the first time.
#[no_mangle]
pub extern "C" fn initialize_logging() {
    static INITIALIZE: Once = Once::new();
    INITIALIZE.call_once(|| {
        fern::Dispatch::new()
            .format(|out, message, record| {
                let loc = record.location();

                out.finish(format_args!(
                    "{} {:7} ({}#{}): {}{}",
                    Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                    record.level(),
                    loc.module_path(),
                    loc.line(),
                    message,
                    if cfg!(windows) { "\r" } else { "" }
                ))
            })
            .level(LogLevelFilter::Info)
            // .level(log::LevelFilter::Debug)
            .chain(std::io::stdout())
            // .chain(fern::log_file("rest_client.log").unwrap())
            .apply()
            .unwrap();
    });
}

/// Log an error and each successive error which caused it.
pub fn backtrace(e: &Error) {
    error!("Error: {}", e);

    for cause in e.iter().skip(1) {
        warn!("\tCaused By: {}", cause);
    }
}
