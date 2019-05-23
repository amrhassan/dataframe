use assert_cmd::prelude::*;
use std::process::Command;

#[test]
fn parquet_rows() {
    Command::cargo_bin("dataset")
        .unwrap()
        .args(&["rows", "sample-datasets/nyctaxi.snappy.parquet"])
        .assert()
        .success()
        .stdout("36234\n");
}

#[test]
fn parquet_format() {
    Command::cargo_bin("dataset")
        .unwrap()
        .args(&["format", "sample-datasets/nyctaxi.snappy.parquet"])
        .assert()
        .success()
        .stdout("Parquet\n");
}
