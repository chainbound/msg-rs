# `msg-sim`

todo: update README.md

#### How to run tests

Given that the tests require root privileges to modify the networking stack,
you can run them with the following command:

```bash
sudo HOME=$HOME $(which cargo) test # add your arguments here
```

We need to provide the `$HOME` environment variable to `sudo` to ensure that
it can find the Rust toolchain, and then we also need to provide the path of `cargo`.
