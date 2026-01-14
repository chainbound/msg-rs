#![doc(issue_tracker_base_url = "https://github.com/chainbound/msg-rs/issues/")]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

//! In-process network emulation for Linux, powered by `rtnetlink`.

macro_rules! cfg_linux_modules {
    ($($mod:ident),* $(,)?) => {
        $(
            #[cfg(target_os = "linux")]
            pub mod $mod;
        )*
    };
}
cfg_linux_modules!(dynch, ip, namespace, network, sysctl, tc, wrappers);
