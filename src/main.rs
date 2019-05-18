#[macro_use]
extern crate clap;

mod parquet;
mod df;
mod cli;
mod commands;

fn main() {
    cli::run()
}
