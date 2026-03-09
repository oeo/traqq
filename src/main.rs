use std::env;
use std::process;
use std::sync::Arc;

use traqq::prelude::*;

const DEFAULT_ADDR: &str = "127.0.0.1:9876";
const DEFAULT_PREFIX: &str = "traqq";

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        process::exit(1);
    }

    match args[1].as_str() {
        "serve" => cmd_serve(&args[2..]),
        "record" => cmd_record(&args[2..]),
        "query" => cmd_query(&args[2..]),
        "help" | "--help" | "-h" => print_usage(),
        other => {
            eprintln!("unknown command: {}", other);
            print_usage();
            process::exit(1);
        }
    }
}

fn print_usage() {
    eprintln!("traqq - high-performance event metrics\n");
    eprintln!("usage:");
    eprintln!(
        "  traqq serve [--addr 127.0.0.1:9876] [--storage memory|redis] [--redis-url redis://...]"
    );
    eprintln!(
        "  traqq record --addr 127.0.0.1:9876 --event '{{\"event\":\"purchase\",\"amount\":99}}'"
    );
    eprintln!("  traqq query  --addr 127.0.0.1:9876 --days 10");
}

fn cmd_serve(args: &[String]) {
    let mut addr = DEFAULT_ADDR.to_string();
    let mut storage_type = "memory".to_string();
    let mut redis_url = "redis://127.0.0.1:6379".to_string();
    let mut prefix = DEFAULT_PREFIX.to_string();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" | "-a" => {
                i += 1;
                addr = args.get(i).cloned().unwrap_or(addr);
            }
            "--storage" | "-s" => {
                i += 1;
                storage_type = args.get(i).cloned().unwrap_or(storage_type);
            }
            "--redis-url" => {
                i += 1;
                redis_url = args.get(i).cloned().unwrap_or(redis_url);
            }
            "--prefix" | "-p" => {
                i += 1;
                prefix = args.get(i).cloned().unwrap_or(prefix);
            }
            _ => {}
        }
        i += 1;
    }

    // default config - users can customize via config file in the future
    let config = TraqqConfig::default();

    let storage: Box<dyn Storage> = match storage_type.as_str() {
        "memory" => Box::new(MemoryStorage::new()),
        #[cfg(feature = "redis-storage")]
        "redis" => match traqq::storage::redis::RedisStorage::new(&redis_url) {
            Ok(s) => Box::new(s),
            Err(e) => {
                eprintln!("failed to connect to redis: {}", e);
                process::exit(1);
            }
        },
        other => {
            eprintln!("unknown storage type: {}", other);
            #[cfg(not(feature = "redis-storage"))]
            if other == "redis" {
                eprintln!("redis support not compiled. rebuild with: cargo build --features redis-storage");
            }
            process::exit(1);
        }
    };

    let traqq = match Traqq::new(config, storage, &prefix) {
        Ok(t) => Arc::new(t),
        Err(e) => {
            eprintln!("failed to initialize: {}", e);
            process::exit(1);
        }
    };

    if let Err(e) = traqq::server::run(traqq, &addr) {
        eprintln!("server error: {}", e);
        process::exit(1);
    }
}

fn cmd_record(args: &[String]) {
    let mut addr = DEFAULT_ADDR.to_string();
    let mut event_json = String::new();

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" | "-a" => {
                i += 1;
                addr = args.get(i).cloned().unwrap_or(addr);
            }
            "--event" | "-e" => {
                i += 1;
                event_json = args.get(i).cloned().unwrap_or(event_json);
            }
            _ => {}
        }
        i += 1;
    }

    if event_json.is_empty() {
        eprintln!("--event is required");
        process::exit(1);
    }

    let event: serde_json::Value = match serde_json::from_str(&event_json) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("invalid JSON: {}", e);
            process::exit(1);
        }
    };

    let mut client = match traqq::client::Client::connect(&addr) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to connect to {}: {}", addr, e);
            process::exit(1);
        }
    };

    match client.record(event) {
        Ok(r) if r.success => println!("ok"),
        Ok(r) => eprintln!("error: {}", r.error.unwrap_or_default()),
        Err(e) => eprintln!("error: {}", e),
    }
}

fn cmd_query(args: &[String]) {
    let mut addr = DEFAULT_ADDR.to_string();
    let mut days: i32 = 7;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" | "-a" => {
                i += 1;
                addr = args.get(i).cloned().unwrap_or(addr);
            }
            "--days" | "-d" => {
                i += 1;
                days = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(days);
            }
            _ => {}
        }
        i += 1;
    }

    let mut client = match traqq::client::Client::connect(&addr) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to connect to {}: {}", addr, e);
            process::exit(1);
        }
    };

    match client.query_days(days) {
        Ok(r) if r.success => {
            let json = serde_json::to_string_pretty(&r.data).unwrap_or_default();
            println!("{}", json);
        }
        Ok(r) => eprintln!("error: {}", r.error.unwrap_or_default()),
        Err(e) => eprintln!("error: {}", e),
    }
}
