use std::{
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Instant,
};

use anyhow::Result;
use foundationdb::{tuple::Subspace, Database};

use rand::Rng;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "fdbbench")]
struct Opt {
  /// Number of concurrent tasks.
  #[structopt(long, default_value = "300")]
  concurrency: usize,

  /// Iterations per task.
  #[structopt(long, default_value = "500")]
  iterations: usize,

  /// Key pool size.
  #[structopt(long, default_value = "30000")]
  key_pool: usize,

  /// Reads per iteration.
  #[structopt(long, default_value = "9")]
  reads: usize,

  /// Writes per iteration.
  #[structopt(long, default_value = "1")]
  writes: usize,

  /// Min value size.
  #[structopt(long, default_value = "32")]
  min_value_size: usize,

  /// Max value size.
  #[structopt(long, default_value = "512")]
  max_value_size: usize,

  /// Test subspace.
  #[structopt(long)]
  subspace: String,
}

#[derive(Default, Debug)]
struct Stats {
  num_retries: AtomicUsize,
  num_txns: AtomicUsize,
  num_reads: AtomicUsize,
  num_writes: AtomicUsize,
}

fn main() {
  let network = unsafe { foundationdb::boot() };
  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()
    .unwrap()
    .block_on(async_main())
    .unwrap();
  drop(network);
}

async fn async_main() -> Result<()> {
  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "debug");
  }

  pretty_env_logger::init_timed();
  let opt = Opt::from_args();

  let db = Arc::new(Database::default()?);
  let subspace = Subspace::from_bytes(opt.subspace.as_bytes());

  log::info!("Preparing subspace.");
  {
    let mut futures = vec![];
    for i in 0..opt.key_pool {
      let key = subspace.pack(&(i as u64));
      let len = rand::thread_rng().gen_range(opt.min_value_size..=opt.max_value_size);
      let mut buf = vec![0u8; len];
      rand::thread_rng().fill(buf.as_mut_slice());
      let db = db.clone();
      let fut = tokio::spawn(async move {
        loop {
          let txn = db.create_trx().unwrap();
          txn.set(&key, &buf);
          if let Err(e) = txn.commit().await {
            if e.is_retryable() {
              continue;
            }
            panic!("commit error");
          }
          break;
        }
      });
      futures.push(fut);
    }
    futures::future::join_all(futures).await;
  }
  log::info!("Starting benchmark.");
  let start_time = Instant::now();

  let stats = Arc::new(Stats::default());

  let tasks = (0..opt.concurrency).map(|_| {
    let db = db.clone();
    let iterations = opt.iterations;
    let reads = opt.reads;
    let writes = opt.writes;
    let min_value_size = opt.min_value_size;
    let max_value_size = opt.max_value_size;
    let subspace = subspace.clone();
    let key_pool = opt.key_pool;
    let stats = stats.clone();
    tokio::spawn(async move {
      for _ in 0..iterations {
        loop {
          let txn = db.create_trx().unwrap();
          for _ in 0..reads {
            let x = rand::thread_rng().gen_range(0..key_pool);
            let key = subspace.pack(&(x as u64));
            let _ = txn
              .get(&key, false)
              .await
              .unwrap()
              .expect("missing value for key");
          }
          stats.num_reads.fetch_add(reads, Ordering::Relaxed);

          for _ in 0..writes {
            let x = rand::thread_rng().gen_range(0..key_pool);
            let key = subspace.pack(&(x as u64));
            let len = rand::thread_rng().gen_range(min_value_size..=max_value_size);
            let mut buf = vec![0u8; len];
            rand::thread_rng().fill(buf.as_mut_slice());
            txn.set(&key, &buf);
          }

          if let Err(e) = txn.commit().await {
            if e.is_retryable() {
              stats.num_retries.fetch_add(1, Ordering::Relaxed);
              continue;
            }
            panic!("commit error");
          }
          stats.num_txns.fetch_add(1, Ordering::Relaxed);
          stats.num_writes.fetch_add(writes, Ordering::Relaxed);
          break;
        }
      }
    })
  });
  futures::future::join_all(tasks).await;
  let end_time = Instant::now();
  println!("{:?}", stats);
  println!("{:?}", end_time.duration_since(start_time));
  let secs = end_time.duration_since(start_time).as_secs_f64();
  println!(
    "Read/sec: {:.2}",
    stats.num_reads.load(Ordering::Relaxed) as f64 / secs
  );
  println!(
    "Write/sec: {:.2}",
    stats.num_writes.load(Ordering::Relaxed) as f64 / secs
  );
  println!(
    "Conflict rate: {:.2}%",
    stats.num_retries.load(Ordering::Relaxed) as f64
      / stats.num_txns.load(Ordering::Relaxed) as f64
      * 100.0
  );

  Ok(())
}
