use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use csv::StringRecord;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use serde_json::{Map, Value};

fn record_to_json(headers: &StringRecord, record: &StringRecord) -> PyResult<Value> {
    if headers.len() != record.len() {
        return Err(PyValueError::new_err(format!(
            "CSV row has {} fields but header has {} fields",
            record.len(),
            headers.len()
        )));
    }

    let object = headers
        .iter()
        .zip(record.iter())
        .map(|(header, value)| (header.to_owned(), Value::String(value.to_owned())))
        .collect::<Map<String, Value>>();

    Ok(Value::Object(object))
}

#[pyfunction]
pub fn csv_to_json_file(csv_path: &str, json_path: &str) -> PyResult<String> {
    let csv_path = Path::new(csv_path);
    let json_path = Path::new(json_path);

    let mut reader = csv::Reader::from_path(csv_path).map_err(|error| {
        PyIOError::new_err(format!(
            "Failed to open CSV file {}: {error}",
            csv_path.display()
        ))
    })?;

    let headers = reader.headers().map_err(|error| {
        PyValueError::new_err(format!(
            "Failed to read CSV headers from {}: {error}",
            csv_path.display()
        ))
    })?;
    let headers = headers.clone();

    let mut rows = Vec::new();
    for result in reader.records() {
        let record = result.map_err(|error| {
            PyValueError::new_err(format!(
                "Failed to read a CSV record from {}: {error}",
                csv_path.display()
            ))
        })?;
        rows.push(record_to_json(&headers, &record)?);
    }

    let file = File::create(json_path).map_err(|error| {
        PyIOError::new_err(format!(
            "Failed to create JSON file {}: {error}",
            json_path.display()
        ))
    })?;
    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, &rows).map_err(|error| {
        PyIOError::new_err(format!(
            "Failed to write JSON file {}: {error}",
            json_path.display()
        ))
    })?;

    Ok(format!(
        "Converted {} to {}",
        csv_path.display(),
        json_path.display()
    ))
}
