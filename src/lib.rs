use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicBool, Arc},
    thread::{self, JoinHandle},
};

/// A lock meant for very low contention environments, such as a workstealing queue.
pub struct Mutex<A: ?Sized> {
    lock: AtomicBool,
    item: UnsafeCell<A>,
}

unsafe impl<A: ?Sized> Send for Mutex<A> {}
unsafe impl<A: ?Sized> Sync for Mutex<A> {}

/// A smart pointer for accessing the value underneath a mutex.
pub struct MutexGuard<'a, A: ?Sized + 'a> {
    mutex: &'a Mutex<A>,
}

impl<'a, A: ?Sized> Drop for MutexGuard<'a, A> {
    fn drop(&mut self) {
        // upon drop, set the boolean to false
        self.mutex
            .lock
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

impl<'a, A: ?Sized> Deref for MutexGuard<'a, A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.mutex.item.get() }
    }
}

impl<'a, A: ?Sized> DerefMut for MutexGuard<'a, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.mutex.item.get() }
    }
}

impl<A> Mutex<A> {
    pub fn new(a: A) -> Mutex<A> {
        Mutex {
            lock: AtomicBool::new(false),
            item: UnsafeCell::new(a),
        }
    }

    pub fn take(&'_ self) -> MutexGuard<'_, A> {
        // spin trying to change the boolean from false to true
        while let Err(_) = self.lock.compare_exchange_weak(
            false,
            true,
            std::sync::atomic::Ordering::Acquire,
            std::sync::atomic::Ordering::Relaxed,
        ) {
            std::hint::spin_loop();
        }
        MutexGuard { mutex: self }
    }
}

#[test]
fn test_mutex() {
    let m = Mutex::new(1i32);

    {
        let mut g = m.take();
        *g += 1;
    }

    {
        let mut h = m.take();
        *h += 1;
    }
    {
        assert_eq!(3, *m.take());
    }
}

pub struct WorkstealingQueue {
    work_queues: Arc<Vec<Mutex<WorkQueue>>>,
    death_signal: Arc<AtomicBool>,
    join_handles: Vec<Option<JoinHandle<()>>>,
}

impl Drop for WorkstealingQueue {
    fn drop(&mut self) {
        self.death_signal
            .store(true, std::sync::atomic::Ordering::SeqCst);
        for join_handle in self.join_handles.iter_mut().map(Option::take) {
            if let Some(join_handle) = join_handle {
                let _ = join_handle.join();
            }
        }
    }
}

impl WorkstealingQueue {
    pub fn add_to_queue(&mut self, queue: usize, work: Box<dyn FnOnce() + Send>) {
        if let Some(mutex) = self.work_queues.get(queue) {
            let mut work_queue = mutex.take();
            work_queue.work_items.push(work);
        }
    }

    pub fn add(&mut self, work: Box<dyn FnOnce() + Send>) {
        self.add_to_queue(rand::random::<usize>() % self.work_queues.len(), work);
    }

    pub fn new(n: usize) -> WorkstealingQueue {
        let work_queues = Arc::new(
            (0..n)
                .map(|_| Mutex::new(WorkQueue::new()))
                .collect::<Vec<_>>(),
        );
        let death_signal = Arc::new(AtomicBool::new(false));
        let mut join_handles = Vec::new();
        for i in 0..n {
            let death_signal_clone = death_signal.clone();
            let work_queues_clone = work_queues.clone();
            join_handles.push(Some(thread::spawn(move || {
                loop {
                    let opt_item = { work_queues_clone[i].take().work_items.pop() };
                    match opt_item {
                        Some(item) => {
                            item();
                            continue;
                        }
                        None => {}
                    }
                    // Steal from a random work queue
                    let j = rand::random::<usize>() % n;
                    let opt_item = { work_queues_clone[j].take().work_items.pop() };
                    match opt_item {
                        Some(item) => {
                            item();
                            continue;
                        }
                        None => {}
                    }
                    if death_signal_clone.load(std::sync::atomic::Ordering::SeqCst) {
                        break;
                    }
                }
            })));
        }
        WorkstealingQueue {
            work_queues,
            death_signal,
            join_handles,
        }
    }
}

pub struct WorkQueue {
    work_items: Vec<Box<dyn FnOnce() + Send>>,
}

impl WorkQueue {
    fn new() -> Self {
        WorkQueue {
            work_items: Vec::new(),
        }
    }
}

#[test]
fn test_workstealing_queue() {
    use std::sync::atomic::AtomicU64;
    let mut workstealing_queue = WorkstealingQueue::new(8);
    let counter = Arc::new(AtomicU64::new(0));
    const N: u64 = 100000;
    for _ in 0..N {
        let counter_clone = counter.clone();
        workstealing_queue.add(Box::new(move || {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }));
    }
    loop {
        if counter.load(std::sync::atomic::Ordering::SeqCst) == N {
            break;
        }
    }
}
