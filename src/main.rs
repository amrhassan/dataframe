#[macro_use]
extern crate clap;

mod avrofile;
mod cli;
mod commands;
mod ds;

fn main() {
    cli::run()
}
