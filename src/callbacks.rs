// from: https://github.com/nickwilcox/recipe-swift-rust-callbacks

#![allow(dead_code)]

use std::thread;
use std::time::Duration;
use std::ffi::c_void;

#[repr(C)]
pub struct CallbackT<T> {
    userdata: *mut c_void,
    callback: extern "C" fn(*mut c_void, T),
}

type CallbackBool = CallbackT<bool>;
unsafe impl Send for CallbackBool {}

impl CallbackBool {
    fn succeeded(self) {
        (self.callback)(self.userdata, true);
        std::mem::forget(self)
    }
    
    fn failed(self) {
        (self.callback)(self.userdata, false);
        std::mem::forget(self)
    }
}

type CallbackInt64 = CallbackT<i64>;
unsafe impl Send for CallbackInt64 {}

impl CallbackInt64 {
    fn succeeded(self, value: i64) {
        (self.callback)(self.userdata, value);
        std::mem::forget(self)
    }
}

// impl Drop for CallbackBool {
//     fn drop(&mut self) {
//         panic!("CallbackBool must have explicit succeeded or failed call")
//     }
// }

#[no_mangle]
pub extern "C" fn callback_bool_after(millis: u64, callback: CallbackBool) {
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(millis));
        callback.succeeded()
    });
}

#[no_mangle]
pub extern "C" fn callback_int64_after(millis: u64, value: i64, callback: CallbackInt64) {
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(millis));
        callback.succeeded(value)
    });
}
