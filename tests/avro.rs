use assert_cmd::prelude::*;
use std::process::Command;

#[test]
fn avro_rows() {
    Command::cargo_bin("dataset")
        .unwrap()
        .args(&["rows", "sample-datasets/participants.avro"])
        .assert()
        .success()
        .stdout("2\n");
}

#[test]
fn avro_format() {
    Command::cargo_bin("dataset")
        .unwrap()
        .args(&["format", "sample-datasets/participants.avro"])
        .assert()
        .success()
        .stdout("Avro\n");
}
