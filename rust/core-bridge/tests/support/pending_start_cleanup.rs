use super::*;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::channel;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Future that never completes and records when Tokio drops it.  Keeping the
/// marker in the future itself means the test observes task destruction even
/// when cancellation wins before Tokio first polls the task.
struct PendingProbe(Arc<AtomicBool>);

impl Future for PendingProbe {
    type Output = ();

    /// Keep the task pending until the cleanup path aborts and joins it.
    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingProbe {
    /// Publish the exact point at which the task-owned future is released.
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Proves GC-style non-blocking disposal transfers aborted start handles to
/// the cleanup thread and waits there before dropping Core.  If the handles
/// were merely dropped by [`Runtime::close`], the Tokio task would be detached
/// and this marker could remain false after the runtime graph was released.
#[test]
fn nonblocking_close_joins_aborted_start_tasks_before_core_drop() {
    let options = RuntimeOptions::builder()
        .build()
        .expect("default Temporal Core runtime options");
    let core = CoreRuntime::new(options, TokioRuntimeBuilder::default())
        .expect("default Temporal Core runtime");
    let mut runtime = Runtime::new(core).expect("runtime cleanup thread starts");

    let dropped = Arc::new(AtomicBool::new(false));
    let task = runtime
        .core
        .as_ref()
        .expect("runtime owns Core before close")
        .tokio_handle()
        .spawn(PendingProbe(Arc::clone(&dropped)));
    let (_sender, receiver) = channel();
    runtime.pending_starts.insert(
        "ticket".to_owned(),
        PendingStart {
            request: Arc::new(client_protocol::StartWorkflowRequest {
                request_id: "request".to_owned(),
                namespace: "default".to_owned(),
                workflow_id: "workflow".to_owned(),
                workflow_type: "workflow".to_owned(),
                task_queue: "queue".to_owned(),
                input: Vec::new(),
            }),
            receiver,
            task,
        },
    );

    assert_eq!(runtime.close(false), STATUS_OK);

    let deadline = Instant::now() + Duration::from_secs(5);
    while !dropped.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "cleanup thread did not join the aborted start task"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
