use super::ThreadPool;
use crate::Result;
use std::thread;

pub struct RayonThreadPool {}

impl ThreadPool for RayonThreadPool {
    fn new(_threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
