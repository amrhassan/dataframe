#[macro_use]
extern crate clap;

mod cli;
mod commands;
mod df;
mod parquet;

fn main() {
    cli::run()
}
