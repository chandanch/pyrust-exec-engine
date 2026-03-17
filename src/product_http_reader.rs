use pyo3::exceptions::{PyConnectionError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;

#[pyfunction]
pub fn fetch_products(url: &str) -> PyResult<String> {
    if url.trim().is_empty() {
        return Err(PyValueError::new_err("URL cannot be empty"));
    }

    let client = Client::new();
    let response = client
        .get(url)
        .header(ACCEPT, "application/json")
        .send()
        .map_err(|error| PyConnectionError::new_err(format!("Request failed for {url}: {error}")))?;

    let status = response.status();
    if !status.is_success() {
        return Err(PyRuntimeError::new_err(format!(
            "Request to {url} returned HTTP {status}"
        )));
    }

    response
        .text()
        .map_err(|error| PyRuntimeError::new_err(format!("Failed to read response body from {url}: {error}")))
}
