use std::collections::HashMap;
use std::net::TcpStream;

use time::precise_time_ns;
use rand;
use rand::Rng;

use protocol;
use protocol::Response;

struct Range(usize, usize);

struct Spec {
    count: usize,
    keys: Range,
    values: Range,
}

impl Spec {
    pub fn new(count: usize, keys: Range, values: Range) -> Spec {
        Spec{count: count, keys: keys, values: values}
    }
}

fn bigstr(lower_size: usize, upper_size: usize) -> Vec<u8> {
    let characters = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let how_many = rand::random::<usize>()%(upper_size-lower_size)+lower_size;
    let mut ret = Vec::with_capacity(how_many);
    for _ in 0..how_many {
        let which = rand::random::<usize>() % characters.len();
        ret.push(characters[which]);
    }
    ret
}

fn timeit<F>(name: &str,
             times: u64,
             mut func: F)
        -> u64
        where F: FnMut() -> () {

    trace!("Starting {}", name);

    let mut total_time_ns: u64 = 0;
    let start_time = precise_time_ns();
    for _ in 0..times {
        func();
    }
    let took_ns = precise_time_ns() - start_time;
    total_time_ns += took_ns;
    let in_ms = |n| n/1_000_000;
    println!("{:?} executed {} times in {}ms ({}ms each)",
             name, times,
             in_ms(total_time_ns), in_ms(total_time_ns/times));
    in_ms(total_time_ns)
}

pub fn bench(host: &str, port: u16) {
    info!("Connecting to {}:{}", host, port);
    let mut conn = protocol::connect(host, port).unwrap();

    let times = 1;

    info!("Building sample data");

    type Samples = Vec<(Vec<u8>, Vec<u8>)>;
    let mut samples: Samples = Vec::new();

    // a description of how our data sizes and distributions will look for the
    // setters
    let specs = vec![
        Spec::new(10000, Range(1, 10),    Range(100, 1_000)),
        Spec::new(1000,  Range(10, 100),  Range(1_000, 10_000)),
        Spec::new(100,   Range(100, 200), Range(10_000, 100_000)),
        Spec::new(10,    Range(200, 250), Range(100_000, 1_000_000)),
    ];

    for Spec{count,
             keys: Range(key_lower, key_upper),
             values: Range(value_lower, value_upper)} in specs {
        for _ in 0..count {
            let key = bigstr(key_lower, key_upper);
            let value = bigstr(value_lower, value_upper);
            samples.push((key, value))
        }
    }

    rand::thread_rng().shuffle(&mut samples);

    let keys: Vec<Vec<u8>> = samples.iter().map(|&(ref k, _)| (*k).to_vec()).collect();

    protocol::flush_all(&mut conn).unwrap();

    timeit("sets_empty_withexpires", times, || {
        for &(ref key, ref value) in &samples {
            let res = protocol::set(&mut conn, &key, &value, 1, 1).unwrap();
            assert!(res == Response::Stored);
        }
    });

    timeit("sets_populated_withexpires", times, || {
        for &(ref key, ref value) in &samples {
            let res = protocol::set(&mut conn, &key, &value, 1, 1).unwrap();
            assert!(res == Response::Stored);
        }
    });

    protocol::flush_all(&mut conn).unwrap();

    timeit("get_missing", times, || {
        for ref key in &keys {
            let keys = vec![key.to_vec()];
            let res = protocol::get(&mut conn, &keys).unwrap();
            assert!(res == Response::Gets{responses: vec![]});
        }
    });

    timeit("sets_empty", times, || {
        for &(ref key, ref value) in &samples {
            let res = protocol::set(&mut conn, &key, &value, 0, 0).unwrap();
            assert!(res == Response::Stored);
        }
    });

    timeit("sets_populated", times, || {
        for &(ref key, ref value) in &samples {
            let res = protocol::set(&mut conn, &key, &value, 0, 0).unwrap();
            assert!(res == Response::Stored);
        }
    });

    timeit("get_populated", times, || {
        let mut found = 0;
        for ref key in &keys {
            let keys = vec![key.to_vec()];
            let res = protocol::get(&mut conn, &keys).unwrap();
            match res {
                Response::Gets{responses} => {
                    found += responses.len();
                },
                other => panic!("bad Get response {:?}", other)
            }
        }
        trace!("found {} of {} keys", found, keys.len())
    });

    timeit("get_multi_populated", times, || {
        let mut found = 0;
        for key_slice in keys.chunks(20) {
            let key_slice = key_slice.to_vec();
            let res = protocol::get(&mut conn, &key_slice).unwrap();
            match res {
                Response::Gets{responses} => {
                    found += responses.len();
                },
                other => panic!("bad Get response {:?}", other)
            }
        }
        trace!("found {} of {} keys", found, keys.len())
    });

    protocol::flush_all(&mut conn).unwrap();


}
