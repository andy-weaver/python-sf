pub mod core;

use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::wrap_pyfunction;
use crate::core::*;
use pyo3::types::PyModule;

#[pyclass]
#[allow(dead_code)]
struct PgnTag {
    name: String, 
    value: String,
}

impl PgnTag {
    fn to_dict(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("name", self.name.clone())?;
            dict.set_item("value", self.value.clone())?;
            Ok(dict.into())
        })
    }
}

#[pyfunction]
fn extract_tags(pgn_string: &str) -> Vec<PgnTag> {
    let tag_vec = extract_tags_rs(pgn_string);
    let mut tags = Vec::with_capacity(tag_vec.len());
    for tag in tag_vec {
        tags.push(PgnTag { name: tag.name, value: tag.value });
    }
    tags
}

#[pyfunction]
fn extract_moves(pgn_string: &str) -> PgnTag {
    let moves = extract_moves_rs(pgn_string);
    PgnTag { name: moves.name, value: moves.value }
}

#[pyfunction]
fn tag_to_dict(tag: &PgnTag) -> PyResult<PyObject> {
    tag.to_dict()
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn _pgn_parser(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PgnTag>()?;
    m.add_function(wrap_pyfunction!(tag_to_dict, m)?)?;
    m.add_function(wrap_pyfunction!(extract_tags, m)?)?;
    m.add_function(wrap_pyfunction!(extract_moves, m)?)?;
    Ok(())
}
