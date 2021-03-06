use crate::commands;
use crate::ds::*;
use clap::App;
use clap::ArgMatches;
use std::path::Path;

pub fn run() {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from_yaml(yaml).get_matches();
    if let Some(m) = matches.subcommand_matches("rows") {
        run_with_path(m, commands::size)
    } else if let Some(m) = matches.subcommand_matches("format") {
        run_with_path(m, commands::format)
    } else {
        panic!("Unexpected command!")
    }
}

fn fail(err: DatasetError) {
    let message = match err {
        DatasetError::UnsupportedFormat => "Unsupported file format".to_owned(),
        DatasetError::IOError(s) => format!("IO Error: {}", s),
        DatasetError::CorruptedFile(s) => format!("Corrupted file: {}", s),
        DatasetError::AvroError(s) => format!("Avro error: {:?}", s),
        DatasetError::ParquetError(s) => format!("Parquet error: {:?}", s),
    };
    eprintln!("{}", message);
    std::process::exit(1);
}

fn run_with_path(matches: &ArgMatches, command: impl Fn(&Path) -> Result<()>) {
    let path = Path::new(matches.value_of("PATH").unwrap());
    command(&path).unwrap_or_else(fail)
}
