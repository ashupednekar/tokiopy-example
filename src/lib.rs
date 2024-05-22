use ::tokio::runtime::Runtime;
use futures::future::join_all;
use pyo3::prelude::*;
use tokio::task;

async fn prepend_hello(s: String, cb: Option<Py<PyAny>>) -> String {
    match cb {
        Some(ref callback) => Python::with_gil(|py| {
            let r = callback.call1(py, (s.clone(),));
            println!("r: {:?}", r);
        }),
        None => {
            println!("no python callback passed, skipping");
        }
    }
    format!("hello {}", s)
}

pub async fn greet(users: Vec<String>, cb: Option<Py<PyAny>>) -> Vec<String> {
    let tasks: Vec<_> = users
        .clone()
        .into_iter()
        .map(|s| {
            // Spawn a new asynchronous task for each item
            for _ in 1..50 {
                let callback = cb.clone();
                let s = s.clone();
                task::spawn(async move { prepend_hello(s, callback).await });
            }
            let callback = cb.clone();
            task::spawn(async move { prepend_hello("aaa".to_string(), callback).await })
        })
        .collect();

    // Wait for all tasks to complete and collect their results
    let results = join_all(tasks).await;
    println!("results: {:?}", results);
    users
}

#[pyfunction]
pub fn greet_user(py: Python, names: Vec<String>, cb: Option<Py<PyAny>>) -> PyResult<()> {
    //let rt = Builder::new_multi_thread().enable_all().build().unwrap();
    let rt = Runtime::new().unwrap();
    py.allow_threads(move || {
        rt.block_on(async {
            let _ = greet(names, cb).await;
        });
    });
    Ok(())
}
/// A Python module implemented in Rust.
#[pymodule]
fn tokiopy(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(greet_user, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let users = vec![
            "ashu".to_string(),
            "prerna".to_string(),
            "dewani".to_string(),
        ];
        let result = greet(users.clone(), None).await;
        assert_eq!(result, users);
    }
}
