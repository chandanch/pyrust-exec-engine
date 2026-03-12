mod csv_data_reader;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::csv_data_reader::csv_to_json_file;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pyrust_exec_engine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(csv_to_json_file, m)?)?;
    Ok(())
}
