//! Time-controlled fuzz test for rill message queue.
//!
//! Generates random sequences of queue operations, applies them to both the
//! real backend and an in-memory oracle (`VecDeque`), then asserts they agree.
//! This catches subtle interaction bugs in FIFO ordering, sequence counters,
//! queue isolation, and create/delete semantics.
//!
//! # Environment variables
//!
//! - `RILL_FUZZ_SECS`: runtime in seconds (default: 5)
//! - `RILL_FUZZ_SEED`: RNG seed for reproducibility (default: random)
//!
//! # Scope
//!
//! Tests: create_queue, delete_queue, push_message, pop_message, queue_length,
//! list_queues — all verified against an oracle.

use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use rill::backend::Backend;
use rkv::{Config, DB};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const QUEUE_NAMES: &[&str] = &["alpha", "beta", "gamma", "delta"];
const VERIFY_INTERVAL: u64 = 200;

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

/// Ground-truth model: queue name -> FIFO deque of messages.
struct Oracle {
    queues: HashMap<String, VecDeque<String>>,
}

impl Oracle {
    fn new() -> Self {
        Self {
            queues: HashMap::new(),
        }
    }

    fn create_queue(&mut self, name: &str) {
        self.queues.entry(name.to_owned()).or_default();
    }

    fn delete_queue(&mut self, name: &str) {
        self.queues.remove(name);
    }

    fn push(&mut self, name: &str, msg: &str) {
        self.queues
            .entry(name.to_owned())
            .or_default()
            .push_back(msg.to_owned());
    }

    fn pop(&mut self, name: &str) -> Option<String> {
        // Accessing a non-existent queue recreates it (db.namespace auto-creates)
        self.queues.entry(name.to_owned()).or_default().pop_front()
    }

    fn length(&mut self, name: &str) -> usize {
        // Accessing a non-existent queue recreates it
        self.queues.entry(name.to_owned()).or_default().len()
    }

    fn list_queues(&self) -> Vec<String> {
        let mut names: Vec<_> = self.queues.keys().cloned().collect();
        names.sort();
        names
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn embed_backend() -> Backend {
    let db = DB::open(Config::in_memory()).unwrap();
    Backend::Embed(
        Box::new(db),
        std::sync::Arc::new(rill::msgid::MsgIdGen::new()),
    )
}

fn gen_queue_name(rng: &mut fastrand::Rng) -> &'static str {
    QUEUE_NAMES[rng.usize(0..QUEUE_NAMES.len())]
}

fn gen_message(rng: &mut fastrand::Rng, seq: u64) -> String {
    format!("msg-{}-{}", seq, rng.u32(0..10000))
}

fn fuzz_duration() -> u64 {
    std::env::var("RILL_FUZZ_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5)
}

fn fuzz_seed() -> u64 {
    std::env::var("RILL_FUZZ_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| fastrand::u64(..))
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

fn verify_all(backend: &Backend, oracle: &mut Oracle, rt: &tokio::runtime::Runtime) {
    // Verify list_queues
    let mut backend_queues = rt.block_on(backend.list_queues()).unwrap();
    backend_queues.sort();
    let oracle_queues = oracle.list_queues();
    assert_eq!(
        backend_queues, oracle_queues,
        "list_queues mismatch: backend={backend_queues:?} oracle={oracle_queues:?}"
    );

    // Verify length of each queue
    for name in &oracle_queues {
        let backend_len = rt.block_on(backend.queue_length(name)).unwrap();
        let oracle_len = oracle.length(name);
        assert_eq!(
            backend_len, oracle_len,
            "queue_length mismatch for '{name}': backend={backend_len} oracle={oracle_len}"
        );
    }
}

// ---------------------------------------------------------------------------
// Main fuzz test
// ---------------------------------------------------------------------------

#[test]
fn fuzz_random_queue_ops() {
    let seed = fuzz_seed();
    let duration = fuzz_duration();
    let mut rng = fastrand::Rng::with_seed(seed);

    eprintln!("rill fuzz: seed={seed}, duration={duration}s");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = embed_backend();
    let mut oracle = Oracle::new();

    let deadline = Instant::now() + std::time::Duration::from_secs(duration);
    let mut ops: u64 = 0;
    let mut msg_seq: u64 = 0;

    while Instant::now() < deadline {
        let op = rng.u32(0..100);
        let name = gen_queue_name(&mut rng);

        match op {
            // create_queue (10%)
            0..10 => {
                rt.block_on(backend.create_queue(name)).unwrap();
                oracle.create_queue(name);
            }
            // delete_queue (5%)
            10..15 => {
                rt.block_on(backend.delete_queue(name)).unwrap();
                oracle.delete_queue(name);
            }
            // push_message (35%)
            15..50 => {
                // Ensure queue exists before push
                rt.block_on(backend.create_queue(name)).unwrap();
                oracle.create_queue(name);

                let msg = gen_message(&mut rng, msg_seq);
                msg_seq += 1;
                rt.block_on(backend.push_message(name, &msg, None)).unwrap();
                oracle.push(name, &msg);
            }
            // pop_message (25%)
            50..75 => {
                let backend_result = rt.block_on(backend.pop_message(name)).unwrap();
                let oracle_result = oracle.pop(name);
                assert_eq!(
                    backend_result, oracle_result,
                    "pop mismatch for '{name}': backend={backend_result:?} oracle={oracle_result:?} (op={ops}, seed={seed})"
                );
            }
            // queue_length (10%)
            75..85 => {
                let backend_len = rt.block_on(backend.queue_length(name)).unwrap();
                let oracle_len = oracle.length(name);
                assert_eq!(
                    backend_len, oracle_len,
                    "length mismatch for '{name}': backend={backend_len} oracle={oracle_len} (op={ops}, seed={seed})"
                );
            }
            // list_queues (15%)
            _ => {
                let mut backend_queues = rt.block_on(backend.list_queues()).unwrap();
                backend_queues.sort();
                let oracle_queues = oracle.list_queues();
                assert_eq!(
                    backend_queues, oracle_queues,
                    "list_queues mismatch (op={ops}, seed={seed})"
                );
            }
        }

        ops += 1;

        // Periodic full verification
        if ops % VERIFY_INTERVAL == 0 {
            verify_all(&backend, &mut oracle, &rt);
        }
    }

    // Final verification
    verify_all(&backend, &mut oracle, &rt);

    eprintln!(
        "rill fuzz: completed {ops} operations, seed={seed}, {} queues active",
        oracle.list_queues().len()
    );
}

// ---------------------------------------------------------------------------
// Queue name validation fuzz
// ---------------------------------------------------------------------------

#[test]
fn fuzz_queue_name_validation() {
    let seed = fuzz_seed();
    let duration = fuzz_duration();
    let mut rng = fastrand::Rng::with_seed(seed);

    eprintln!("rill name fuzz: seed={seed}, duration={duration}s");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = embed_backend();

    let deadline = Instant::now() + std::time::Duration::from_secs(duration);
    let mut ops: u64 = 0;
    let mut valid_count: u64 = 0;
    let mut invalid_count: u64 = 0;

    // Characters to pick from — mix of valid and invalid
    let chars: Vec<char> =
        "abcdefghijklmnopqrstuvwxyz0123456789-_./\\@ !#$%^&*()+=[]{}|;:'\",<>?`~\t\n"
            .chars()
            .collect();

    while Instant::now() < deadline {
        // Generate a random queue name of random length (0..200)
        let len = rng.usize(0..200);
        let name: String = (0..len).map(|_| chars[rng.usize(0..chars.len())]).collect();

        // Determine if this name should be valid per our rules
        let expected_valid = !name.is_empty()
            && name.len() <= 128
            && name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '-' || c == '_');

        // Try to use it as a queue name
        let result = rt.block_on(backend.create_queue(&name));

        if expected_valid {
            // Valid names should always succeed (backend doesn't validate,
            // but it should work with rKV namespaces)
            assert!(
                result.is_ok(),
                "expected valid name to work: {:?} (op={ops}, seed={seed})",
                name
            );
            valid_count += 1;

            // Push and pop should also work
            rt.block_on(backend.push_message(&name, "test", None))
                .unwrap();
            let popped = rt.block_on(backend.pop_message(&name)).unwrap();
            assert_eq!(
                popped,
                Some("test".to_string()),
                "push/pop failed for valid name: {:?} (op={ops}, seed={seed})",
                name
            );

            // Clean up
            let _ = rt.block_on(backend.delete_queue(&name));
        } else {
            invalid_count += 1;
        }

        ops += 1;
    }

    eprintln!(
        "rill name fuzz: {ops} names tested, {valid_count} valid, {invalid_count} invalid, seed={seed}"
    );
}

// ---------------------------------------------------------------------------
// FIFO ordering stress test
// ---------------------------------------------------------------------------

#[test]
fn fuzz_fifo_ordering_stress() {
    let seed = fuzz_seed();
    let duration = fuzz_duration();
    let mut rng = fastrand::Rng::with_seed(seed);

    eprintln!("rill FIFO stress: seed={seed}, duration={duration}s");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let backend = embed_backend();

    // Use a single queue to stress the FIFO ordering invariant
    rt.block_on(backend.create_queue("stress")).unwrap();

    let deadline = Instant::now() + std::time::Duration::from_secs(duration);
    let mut expected: VecDeque<String> = VecDeque::new();
    let mut ops: u64 = 0;
    let mut push_seq: u64 = 0;

    while Instant::now() < deadline {
        let op = rng.u32(0..100);

        match op {
            // Push (60%)
            0..60 => {
                let msg = format!("s{push_seq}");
                push_seq += 1;
                rt.block_on(backend.push_message("stress", &msg, None))
                    .unwrap();
                expected.push_back(msg);
            }
            // Pop (30%)
            60..90 => {
                let got = rt.block_on(backend.pop_message("stress")).unwrap();
                let want = expected.pop_front();
                assert_eq!(
                    got, want,
                    "FIFO violation: got={got:?} want={want:?} (op={ops}, seed={seed})"
                );
            }
            // Verify length (10%)
            _ => {
                let got = rt.block_on(backend.queue_length("stress")).unwrap();
                assert_eq!(
                    got,
                    expected.len(),
                    "length mismatch: got={got} want={} (op={ops}, seed={seed})",
                    expected.len()
                );
            }
        }

        ops += 1;
    }

    // Drain remaining messages and verify order
    while let Some(want) = expected.pop_front() {
        let got = rt.block_on(backend.pop_message("stress")).unwrap();
        assert_eq!(
            got,
            Some(want.clone()),
            "FIFO drain mismatch: got={got:?} want={want:?} (seed={seed})"
        );
    }

    // Queue should be empty
    let final_pop = rt.block_on(backend.pop_message("stress")).unwrap();
    assert_eq!(final_pop, None, "queue not empty after drain (seed={seed})");

    eprintln!("rill FIFO stress: {ops} operations, {push_seq} messages pushed, seed={seed}");
}
