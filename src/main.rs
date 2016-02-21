#[macro_use]
extern crate nom;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate time;
extern crate getopts;
extern crate rand;

mod bench;
mod cmd;
mod protocol;

pub fn main() {
    env_logger::init().unwrap();

    cmd::main()
}
