use crate::Result;
use crossbeam_channel::{Receiver, Sender, unbounded};
use std::thread;

enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

struct Worker {
    receiver: Receiver<ThreadPoolMessage>,
}

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub struct NaiveThreadPool {}

pub struct RayonThreadPool {}

pub struct SharedQueueThreadPool {
    sender: Sender<ThreadPoolMessage>,
}

impl Worker {
    fn new(receiver: Receiver<ThreadPoolMessage>) -> Worker {
        Worker { receiver }
    }
    fn receiver_request_loop(&self) {
        for job in self.receiver.iter() {
            match job {
                ThreadPoolMessage::RunJob(job_exec) => job_exec(),
                ThreadPoolMessage::Shutdown => return,
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if !thread::panicking() {
            return;
        }
        let replacement_worker = Worker::new(self.receiver.clone());
        thread::spawn(move || replacement_worker.receiver_request_loop());
    }
}

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(NaiveThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}

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

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = unbounded();

        for _ in 0..threads {
            let receiver_clone = receiver.clone();
            let worker = Worker::new(receiver_clone);
            thread::spawn(move || worker.receiver_request_loop());
        }

        Ok(SharedQueueThreadPool { sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self.sender.send(ThreadPoolMessage::RunJob(Box::new(job))) {
            Ok(()) => return,
            Err(_) => return,
        };
    }
}
