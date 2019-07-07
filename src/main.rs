#[macro_use]
extern crate clap;

#[macro_use]
extern crate derive_more;


mod avrofile;
mod parquetfile;
mod cli;
mod commands;
mod ds;

fn main() {
    cli::run()
}
