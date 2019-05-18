use std::path::Path;
use crate::df::DataFrameError;
use clap::App;
use clap::ArgMatches;
use crate::commands;

pub fn run() {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();
    if let Some(m) = matches.subcommand_matches("size") {
        run_size(m)
    } else {
        panic!("Unexpected command!")
    }
}

fn fail(err: DataFrameError) {
    let message = match err {
        DataFrameError::UnsupportedFormat => "Unsupported file format".to_owned(),
        DataFrameError::IOError(s) => format!("IO Error: {}", s),
        DataFrameError::CorruptedFile(s) => format!("Corrupted file: {}", s)
    };
    eprintln!("{}", message);
    std::process::exit(1);
}

fn run_size(matches: &ArgMatches) {
    let path = Path::new(matches.value_of("FILE").unwrap());
    commands::size(&path).unwrap_or_else(fail)
}
