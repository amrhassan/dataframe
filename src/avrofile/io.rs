use super::error::*;
use avro_rs::{from_avro_datum, types::Value, Schema};
use std::io::{ErrorKind, Read};

/// Decode a value out of the byte stream if not EOF, otherwise return None
pub fn decode<R: Read>(schema: &Schema, reader: &mut R) -> Option<Result<Value>> {
    let v = from_avro_datum(schema, reader, Some(schema));
    match v {
        Ok(value) => Some(Ok(value)),
        Err(err) => match err.downcast_ref::<std::io::Error>() {
            Some(io_error) if io_error.kind() == ErrorKind::UnexpectedEof => None,
            _ => Some(Err(AvroFileError::corrupted(err))),
        },
    }
}

pub fn decode_long<R: Read>(reader: &mut R) -> Option<Result<i64>> {
    decode(&Schema::Long, reader).map(|result| match result {
        Ok(Value::Long(long_value)) => Ok(long_value),
        Ok(v) => Err(AvroFileError::corrupted(format!(
            "Unexpected value when decoding a long: {:?}",
            v
        ))),
        Err(err) => Err(err),
    })
}
