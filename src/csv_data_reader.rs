use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use csv::StringRecord;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use serde::ser::{SerializeMap, Serializer};
use serde::Serialize;

fn validate_record(headers: &StringRecord, record: &StringRecord) -> PyResult<()> {
    if headers.len() != record.len() {
        return Err(PyValueError::new_err(format!(
            "CSV row has {} fields but header has {} fields",
            record.len(),
            headers.len()
        )));
    }

    Ok(())
}

struct RowAsJsonObject<'a> {
    headers: &'a StringRecord,
    record: &'a StringRecord,
}

impl Serialize for RowAsJsonObject<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.headers.len()))?;
        for (header, value) in self.headers.iter().zip(self.record.iter()) {
            map.serialize_entry(header, value)?;
        }
        map.end()
    }
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

    let file = File::create(json_path).map_err(|error| {
        PyIOError::new_err(format!(
            "Failed to create JSON file {}: {error}",
            json_path.display()
        ))
    })?;
    let mut writer = BufWriter::new(file);
    writer.write_all(b"[\n").map_err(|error| {
        PyIOError::new_err(format!(
            "Failed to start JSON file {}: {error}",
            json_path.display()
        ))
    })?;

    let mut is_first = true;
    for result in reader.records() {
        let record = result.map_err(|error| {
            PyValueError::new_err(format!(
                "Failed to read a CSV record from {}: {error}",
                csv_path.display()
            ))
        })?;
        validate_record(&headers, &record)?;

        if !is_first {
            writer.write_all(b",\n").map_err(|error| {
                PyIOError::new_err(format!(
                    "Failed to append separator to JSON file {}: {error}",
                    json_path.display()
                ))
            })?;
        }
        is_first = false;

        serde_json::to_writer_pretty(
            &mut writer,
            &RowAsJsonObject {
                headers: &headers,
                record: &record,
            },
        )
        .map_err(|error| {
            PyIOError::new_err(format!(
                "Failed to write JSON row to {}: {error}",
                json_path.display()
            ))
        })?;
    }
    writer.write_all(b"\n]\n").map_err(|error| {
        PyIOError::new_err(format!(
            "Failed to finish JSON file {}: {error}",
            json_path.display()
        ))
    })?;

    Ok(format!(
        "Converted {} to {}",
        csv_path.display(),
        json_path.display()
    ))
}
