use std::{ffi::c_str, num::NonZeroU32, str::FromStr};

pub fn if_nametoindex(name: &str) -> Option<NonZeroU32> {
    let string = c_str::CString::from_str(name).expect("to convert into c string");
    let index = unsafe { nix::libc::if_nametoindex(string.as_ptr()) };
    NonZeroU32::new(index)
}
