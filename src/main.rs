#![feature(test)]
#![feature(array_chunks)]
#![feature(slice_as_chunks)]
#![warn(unused_must_use)]
#![warn(dead_code)]
extern crate derive_more;
#[allow(unused_imports)]
#[allow(unused_variables)]
// extern crate test;
extern crate stopwatch;
use derive_more::Display;
extern crate arrayvec;
extern crate derive_new;
use arrayvec::{ArrayString, ArrayVec};
use derive_new::new;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::io::Write;
use stopwatch::Stopwatch;
use wide::*;

#[allow(unused_imports)]
#[allow(unused_variables)]
use rayon::{prelude::*, ThreadPool};
use std::sync::Arc;
use std::thread;

use black_scholes_pricer::bs_single::*;
use bytemuck::cast;

#[derive(new, Copy, Clone, Debug, Display, Serialize, Deserialize)]
#[display(
    fmt = "(spot: {}, strike: {}, ir: {}, mat: {}, vol: {})",
    spot,
    strike,
    ir,
    maturity,
    volatility
)]
pub struct CalcParams {
    spot: f32,
    strike: f32,
    ir: f32,
    maturity: f32,
    volatility: f32,
}

#[derive(
    new, Display, Clone, Copy, Debug, Ord, Eq, PartialOrd, PartialEq, Hash, Serialize, Deserialize,
)]
pub struct ScenarioId(u16);

#[derive(Clone, Debug, new)]
pub struct Scenario<const LENGTH: usize> {
    scenario_id: ScenarioId,
    calc_params: [CalcParams; LENGTH],
}

// impl<const LENGTH: usize> Scenario<LENGTH> {
//     pub fn new(scenario_id: ScenarioId, calc_params: [CalcParams; LENGTH]) -> Self {
//         Self { scenario_id, calc_params }
//     }
// }

#[derive(
    Clone, Copy, Debug, Display, Ord, Eq, PartialOrd, PartialEq, Hash, Serialize, Deserialize,
)]
pub struct BucketIdL<const BUCKET_KEY_LENGTH: usize>(ArrayString<BUCKET_KEY_LENGTH>);

impl<const BUCKET_KEY_LENGTH: usize> BucketIdL<BUCKET_KEY_LENGTH> {
    pub fn new(bucket: &str) -> Self {
        Self(ArrayString::<BUCKET_KEY_LENGTH>::from(bucket).expect(&format!("Incorrect BucketId length: {}", bucket.len())))
    }
}

#[derive(
    new, Clone, Copy, Debug, Display, Ord, Eq, PartialOrd, PartialEq, Hash, Serialize, Deserialize,
)]
pub struct InstrumentId(u16);

#[derive(
    new, Clone, Copy, Debug, Display, Ord, Eq, PartialOrd, PartialEq, Hash, Serialize, Deserialize,
)]
pub struct Day(u16);

#[derive(new, Copy, Clone, Debug, Display, Serialize, Deserialize)]
pub enum Bump {
    Benchmark,
    AddBumpIdx(f32),
    MulBumpIdx(f32),
}

impl Bump {
    pub fn to_u64(self: &Self) -> u64 {
        match self {
            Bump::Benchmark => 0u64,
            Bump::AddBumpIdx(v) => (1u64 << 32 + u32::from_ne_bytes(v.to_ne_bytes())),
            Bump::MulBumpIdx(v) => (2u64 << 32 + u32::from_ne_bytes(v.to_ne_bytes())),
        }
    }
}

impl Hash for Bump {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.to_u64().hash(state)
    }
}

impl PartialEq for Bump {
    fn eq(&self, other: &Self) -> bool {
        self.to_u64().eq(&other.to_u64())
    }
}

impl Eq for Bump {}

impl PartialOrd for Bump {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.to_u64().partial_cmp(&other.to_u64())
    }
}

impl Ord for Bump {
    fn cmp(&self, other: &Self) -> Ordering {
        self.to_u64().cmp(&other.to_u64())
    }
}

#[derive(new, Hash, Copy, Clone, Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct DataPointKeyL<const BUCKET_KEY_LENGTH: usize> {
    bucket: BucketIdL<BUCKET_KEY_LENGTH>,
    instrument: InstrumentId,
    day: Day,
    scenario: ScenarioId,
    point: Bump,
}

#[derive(new, Clone, Debug)]
pub struct ScenarioPlan<const LENGTH: usize> {
    instruments: Vec<InstrumentId>,
    days: Vec<Day>,
    scenario: Scenario<LENGTH>,
}

impl<const LENGTH: usize> ScenarioPlan<LENGTH> {
    fn span_across(
        scenario_id: ScenarioId,
        calc_params: [CalcParams; LENGTH],
        instrument_count: u16,
        days_count: u16,
    ) -> Self {
        Self {
            instruments: (0..instrument_count)
                .map(InstrumentId::new)
                .collect::<Vec<_>>(),
            days: (0..days_count).map(Day::new).collect::<Vec<_>>(),
            scenario: Scenario::new(scenario_id, calc_params),
        }
    }
}

#[derive(new, Clone, Debug)]
pub struct ScenarioSurfaceL<const LENGTH: usize, const BUCKET_KEY_LENGTH: usize> {
    plan: ScenarioPlan<LENGTH>,
    surface: BTreeMap<DataPointKeyL<BUCKET_KEY_LENGTH>, Vec<f32>>,
}

impl<const LENGTH: usize, const BUCKET_KEY_LENGTH: usize>
    ScenarioSurfaceL<LENGTH, BUCKET_KEY_LENGTH>
{
    fn build_surface(plan: ScenarioPlan<LENGTH>) -> Self {
        let days = &plan.days;
        let instruments = &plan.instruments;
        let sid = plan.scenario.scenario_id;
        let surface = plan
            .scenario
            .calc_params
            .iter()
            .flat_map(move |p| {
                let pd = days;
                let sid = sid;
                instruments
                    .iter()
                    .flat_map(move |i| {
                        let i1 = i.clone();
                        pd.iter().map(move |d| {
                            let i2 = i1;
                            (i2, d.clone())
                        })
                    })
                    .map(move |(i, d)| {
                        (
                            DataPointKeyL::<BUCKET_KEY_LENGTH>::new(
                                BucketIdL::<BUCKET_KEY_LENGTH>::new("AAAA"),
                                i,
                                d,
                                sid.clone(),
                                Bump::Benchmark,
                            ),
                            vec![bs_price(
                                OptionDir::CALL,
                                p.spot,
                                p.strike,
                                p.volatility,
                                p.ir,
                                0.01,
                                p.maturity,
                            )],
                        )
                    })
            })
            .collect::<BTreeMap<DataPointKeyL<BUCKET_KEY_LENGTH>, _>>();
        Self {
            plan,
            surface,
        }
    }
}

const BUCKET_KEY_LENGTH: usize = 4;

type BucketId = BucketIdL<BUCKET_KEY_LENGTH>;
type DataPointKey = DataPointKeyL<BUCKET_KEY_LENGTH>;
type ScenarioSurface<const LENGTH: usize> = ScenarioSurfaceL<LENGTH, BUCKET_KEY_LENGTH>;

pub fn generate_linear_vector(initial_value: f32, bump: f32, count: u16) -> Vec<f32> {
    let mut acc = initial_value;
    (0..count)
        .map(|_| {
            acc += bump;
            acc
        })
        .collect::<Vec<_>>()
}

type FStep = dyn Fn(u32) -> f32;

pub fn generate_calc_params<const LENGTH: usize>(
    // count: usize,
    spot: &FStep,
    strike: &FStep,
    ir: &FStep,
    maturity: &FStep,
    volatility: &FStep,
) -> [CalcParams; LENGTH] {
    (0u32..LENGTH as u32)
        .map(|i| CalcParams::new(spot(i), strike(i), ir(i), maturity(i), volatility(i)))
        .collect::<ArrayVec<_, LENGTH>>()
        .into_inner()
        .expect("Wrong CalcParams array length requested")
}

///////////////////////

pub fn count_64(input: Vec<f64>) -> f64 {
    input.iter().fold(0., |acc, e| (acc + e / 2.))
}

pub fn count(input: Vec<f32>) -> f32 {
    // let mut count = 0 as T;
    // for i in 0..total {
    //     count = count + (i as T) / 2.0;
    //     // count = count + (i as T).exp() / 2.0;
    // }

    // let a = input.iter().map(|e| *e).reduce(|acc, e| (acc + e / 2.0));
    // let a = input.iter().map(|e| *e).fold((0 as T),|acc, e| (acc.wrapping_add(e / (2 as T))));
    input
        .iter()
        .map(|e| *e)
        .reduce(|acc, e| (acc + e / 2.))
        .unwrap()

    // input.par_iter().fold(|| (0 as T), |acc, e| acc + e / (2 as T)).sum::<T>();

    // println!("Hello, world! {}", count);
}

pub fn count_vectorized(input: &[f32]) -> f32 {
    // input.array_chunks::<8>().map(|e| f32x8::from(*e)).reduce(|acc, e| (acc + e /*/ f32x8::splat(2.0)*/) ).unwrap_or_default()
    let chunks = input.as_chunks::<8>().0;
    chunks
        .iter()
        .map(|e| f32x8::from(*e))
        .reduce(|acc, e| (acc + e/*/ f32x8::splat(2.0)*/))
        .unwrap_or_default()
        .reduce_add()
}

pub fn count_vectorized_par(input: &[f32]) -> f32x8 {
    let chunks = input.as_chunks::<8>().0;
    chunks.
        par_iter().
        // array_chunks::<8>().
        map(|e| f32x8::from(*e)).
        reduce(|| f32x8::splat(0.0), |acc, e| (acc + e))
}

pub fn count_vectorized_par_groups(input: &[f32]) -> f32 {
    input
        .splitn(input.len() / 8, |_| false)
        .collect::<Vec<_>>()
        .par_iter()
        .map_with((), |_, x| count_vectorized(x))
        .collect::<Vec<_>>()
        .into_iter()
        .reduce(|acc, e| (acc + e))
        .unwrap_or_default()
}

mod thread_pool {
    use crossbeam::channel::{unbounded, Receiver, Sender};
    #[allow(unused_imports)]
    #[allow(unused_variables)]
    use std::{
        sync::{Arc, Mutex},
        thread::{self, JoinHandle},
        usize,
    };

    pub struct ThreadPool {
        threads: Vec<JoinHandle<()>>,
        tx: Sender<Vec<f32>>,
        result_rx: Receiver<f32>,
    }

    impl ThreadPool {
        pub fn new(size: usize, f: &'static (dyn Fn(Vec<f32>) -> f32 + Sync)) -> Self {
            let mut threads = Vec::new();
            let (tx, rx) = unbounded();
            let (res_tx, res_rx) = unbounded();
            for _i in 0..size {
                let rx_ = rx.clone();
                // let res_tx_ = res_tx.clone();
                threads.push(thread::spawn(move || {
                    while let Ok(v) = rx_.recv() {
                        let res = f(v);
                        println!("{}", res);
                        // res_tx_.send(res).unwrap();
                    }
                }));
            }
            drop(rx);
            drop(res_tx);
            Self {
                threads: threads,
                tx: tx,
                result_rx: res_rx,
            }
        }

        pub fn tx(&self) -> Sender<Vec<f32>> {
            self.tx.clone()
        }

        pub fn join(self) {
            drop(self.tx);
            for t in self.threads.into_iter() {
                t.join().unwrap();
            }
        }
    }

    // impl Drop for ThreadPool {
    //     fn drop(&mut self) {
    //         for t in &self.threads {
    //             t.join();
    //         }
    //     }
    // }
}

pub fn count_vectorized_par_threads(input: Vec<Vec<f32>>) -> f32 {
    let thread_pool = thread_pool::ThreadPool::new(
        input.len(),
        &(|v| {
            let r = count_vectorized(&v);
            // println!("{}", r);
            r
        }),
    );

    for v in input {
        thread_pool.tx().send(v).unwrap();
    }

    thread_pool.join();

    0.
}

pub fn count_vectorized_par_threads1(input: Vec<f32>) -> f32 {
    // let (tx, rx) = mpsc::channel();

    // let chunk_size = input.len() / 8;
    println!("Boo1");

    // let mut threads = Vec::<JoinHandle<()>>::new();
    //
    // let handle = thread::spawn(move || {
    //     println!("Here's a vector: {:?}", v);
    // });
    //
    // handle.join().unwrap();

    let mut threads = Vec::<JoinHandle<()>>::new();
    let vals1 = Arc::new(input);

    for i in 0..1 {
        let vals = Arc::clone(&vals1);
        threads.push(thread::spawn(move || {
            let _count = count_vectorized(&vals[vals.len() / 8 * i..vals.len() / 8 * (i + 1)]);
            // println!("Here's a count: {:?}", count);
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    // let mut threads = Vec::<JoinHandle<()>>::new();
    //
    // for chunk in input.array_chunks::<8>() {
    //     // let slice = input.slice(i * chunk_size, (i + 1) * chunk_size);
    //     let vals = chunk.clone();
    //     // let ttx = tx.clone();
    //     threads.push(thread::spawn(move || {
    //         let count = count_vectorized(&vals);
    //         // ttx.send(count).unwrap();
    //     }));
    //     // count_vectorized(x))
    // }
    //
    // for t in threads {
    //     t.join();//.unwrap();
    // }

    // let r = rx.iter().collect::<Vec<_>>();
    //     // .reduce(|acc, e| (acc + e))
    //     // .unwrap_or_default();

    println!("Boo");

    // r
    0.

    // input
    //     .splitn(input.len() / 8 / 8, |_| false)
    //     .collect::<Vec<_>>()
    //     .into_par_iter()
    //     .map_with((), |_, x| count_vectorized(x))
    //     .collect::<Vec<_>>()
    //     .into_iter()
    //     .reduce(|acc, e| (acc + e))
    //     .unwrap_or_default()
}

#[macro_use]
extern crate time_test;

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use num_traits::{AsPrimitive, Num};
    use std::mem::size_of;

    use super::*;

    fn generate_vec_f32(size: usize) -> Vec<f32> {
        (1..size).map(|i| i as f32).collect()
    }

    fn generate_vec_f64(size: usize) -> Vec<f64> {
        (1..size).map(|i| i as f64).collect()
    }

    const TOTAL: usize = 1_000_000_000;

    #[test]
    fn generate() {
        let f = |i| (i as f32) * 0.1;
        let params = generate_calc_params::<16>(&f, &f, &f, &f, &f);

        let sp = ScenarioPlan::span_across(ScenarioId(1), params, 5, 5);

        let surface = ScenarioSurface::build_surface(sp);

        println!("{:?}", surface);
    }

    #[test]
    fn sizes() {
        // CalcParams
        // BucketId

        // InstrumentId
        // Day

        //  DataPointKey
        // ScenarioPlan

        let sizeof_usize = size_of::<usize>();

        println!("usize: {}", sizeof_usize);
        println!(
            "CalcParams: {} - {}",
            size_of::<CalcParams>(),
            size_of::<CalcParams>() % sizeof_usize == 0
        );
        println!(
            "BucketId: {} - {}",
            size_of::<BucketId>(),
            (size_of::<BucketId>() % sizeof_usize) == 0
        );
        println!(
            "InstrumentId: {} - {}",
            size_of::<InstrumentId>(),
            size_of::<InstrumentId>() % sizeof_usize == 0
        );
        println!(
            "Day: {} - {}",
            size_of::<Day>(),
            size_of::<Day>() % sizeof_usize == 0
        );

        println!(
            "DataPointKey: {} - {}",
            size_of::<DataPointKey>(),
            size_of::<DataPointKey>() % sizeof_usize == 0
        );
        println!(
            "Bump: {} - {}",
            size_of::<Bump>(),
            size_of::<Bump>() % sizeof_usize == 0
        );
        println!(
            "ScenarioId: {} - {}",
            size_of::<ScenarioId>(),
            size_of::<ScenarioId>() % sizeof_usize == 0
        );
        println!(
            "Scenario<0>: {} - {}",
            size_of::<Scenario<0>>(),
            size_of::<Scenario<0>>() % sizeof_usize == 0
        );
        println!(
            "Scenario<1>: {} - {}",
            size_of::<Scenario<1>>(),
            size_of::<Scenario<1>>() % sizeof_usize == 0
        );
        println!(
            "Scenario<2>: {} - {}",
            size_of::<Scenario<2>>(),
            size_of::<Scenario<2>>() % sizeof_usize == 0
        );
        println!(
            "ScenarioPlan<0>: {} - {}",
            size_of::<ScenarioPlan<0>>(),
            size_of::<ScenarioPlan<0>>() % sizeof_usize == 0
        );
        println!(
            "ScenarioPlan<1>: {} - {}",
            size_of::<ScenarioPlan<1>>(),
            size_of::<ScenarioPlan<1>>() % sizeof_usize == 0
        );
        println!(
            "ScenarioPlan<2>: {} - {}",
            size_of::<ScenarioPlan<2>>(),
            size_of::<ScenarioPlan<2>>() % sizeof_usize == 0
        );
        println!(
            "ScenarioSurface<0>: {} - {}",
            size_of::<ScenarioSurface<0>>(),
            size_of::<ScenarioSurface<0>>() % sizeof_usize == 0
        );
        println!(
            "ScenarioSurface<1>: {} - {}",
            size_of::<ScenarioSurface<1>>(),
            size_of::<ScenarioSurface<1>>() % sizeof_usize == 0
        );
        println!(
            "ScenarioSurface<2>: {} - {}",
            size_of::<ScenarioSurface<2>>(),
            size_of::<ScenarioSurface<2>>() % sizeof_usize == 0
        );
    }

    #[test]
    fn test_count_64() {
        time_test!();
        let input = generate_vec_f64(TOTAL);
        let r;
        {
            time_test!("run");
            r = count_64(input);
        }
        println!("{}", r);
    }

    #[test]
    fn test_count_32() {
        time_test!();
        let input = generate_vec_f32(TOTAL);
        let r;
        {
            time_test!("run");
            r = count(input);
        }
        println!("{}", r);
    }

    #[test]
    fn test_count_vectorized() {
        time_test!();
        let input = generate_vec_f32(TOTAL);
        let r;
        {
            time_test!("run");
            r = count_vectorized(&input);
        }
        println!("{}", r);
    }

    #[test]
    fn test_count_vectorized_par() {
        time_test!();
        let input = generate_vec_f32(TOTAL);
        let r;
        {
            time_test!("run");
            r = count_vectorized_par(&input);
        }
        println!("{}", r);
    }

    #[test]
    fn test_count_vectorized_par_groups() {
        time_test!();
        let input = generate_vec_f32(TOTAL);
        let r;
        {
            time_test!("run");
            r = count_vectorized_par_groups(&input);
        }
        println!("{}", r);
    }

    #[test]
    fn test_count_vectorized_par_threads() {
        time_test!();
        let input = (0..8).map(|_| generate_vec_f32(TOTAL / 8)).collect();
        let r;
        {
            time_test!("run");
            r = count_vectorized_par_threads(input);
        }
        println!("{}", r);
    }

    #[test]
    fn test_count_vectorized_par_threadpool() {
        time_test!();
        let input: Vec<_> = (0..8).map(|_| generate_vec_f32(TOTAL / 8)).collect();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build()
            .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();

        {
            time_test!("run");
            for v in input.into_iter() {
                let tx = tx.clone();
                pool.spawn(move || {
                    let mut r = 0.0;
                    for _i in 0..20 {
                        r += count_vectorized(&v);
                    }
                    tx.send(r).unwrap();
                });
            }
            drop(tx); // need to close all senders, otherwise...
            let ret: Vec<f32> = rx.into_iter().collect(); // ... this would block
            println!("{:?}", ret);
        }
    }

    #[test]
    fn test_count_vectorized_par_threadpool2() {
        time_test!();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build()
            .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();

        {
            time_test!("run");
            for _i in 0..8 {
                let tx = tx.clone();
                pool.spawn(move || {
                    let v = generate_vec_f32(TOTAL / 4);
                    let r = count_vectorized(&v);
                    tx.send(r).unwrap();
                });
            }
            drop(tx); // need to close all senders, otherwise...
            let ret: Vec<f32> = rx.into_iter().collect(); // ... this would block
            println!("{:?}", ret);
        }
    }

    #[test]
    fn it_works() {
        time_test!();
        // assert_eq!(4, add_two(2));
    }
}

fn main() {
    let mut meter = self_meter::Meter::new(Duration::new(1, 0)).unwrap();
    meter.track_current_thread("main");
    meter
        .scan()
        .map_err(|e| writeln!(&mut stderr(), "Scan error: {}", e))
        .ok();

    let input: Vec<_> = (1..1_000_000_000u64).map(|i| i as f32).collect();
    let sw = Stopwatch::start_new();
    // let res = count_vectorized_par_groups(&input);
    let res = count_vectorized_par_threads1(input);
    println!("Thing took {}ms", sw.elapsed_ms());
    println!("Thing took {:?}", sw.elapsed());
    println!("Result {:?}", res);

    meter
        .scan()
        .map_err(|e| writeln!(&mut stderr(), "Scan error: {}", e))
        .ok();
    println!("Report: {:#?}", meter.report());
    // println!("Threads: {:#?}", meter.thread_report().map(|x| x.collect::<BTreeMap<_,_>>()));
}

extern crate timely;

use black_scholes_pricer::OptionDir;
#[allow(unused_imports)]
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::stderr;
use std::thread::JoinHandle;
use std::time::Duration;
use timely::dataflow::operators::{Input, Inspect};

fn main1() {
    let sw = Stopwatch::start_new();

    // construct and execute a timely dataflow
    let _r = timely::execute_from_args(std::env::args(), |worker| {
        // add an input and base computation off of it
        let mut input = worker.dataflow(|scope| {
            let (input, stream) = scope.new_input();
            stream.inspect(|x| println!("hello {:?}", x));
            input
        });

        // introduce input, advance computation
        for round in 0..10 {
            input.send(round);
            input.advance_to(round + 1);
            worker.step();
        }
    })
    .unwrap();
    // println!("Result {:?}", r.);
    println!("Thing took {}ms", sw.elapsed_ms());
    println!("Thing took {:?}", sw.elapsed());
    println!("Hello, world!");
}
