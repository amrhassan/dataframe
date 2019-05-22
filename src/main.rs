#[macro_use]
extern crate clap;

mod avrofile;
mod cli;
mod commands;
mod df;
mod parquetfile;

fn main() {
    cli::run()
}
