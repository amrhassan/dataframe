#[macro_use]
extern crate clap;

mod avrofile;
mod cli;
mod commands;
mod df;

fn main() {
    cli::run()
}
