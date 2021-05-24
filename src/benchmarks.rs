use super::*;
use test::Bencher;

// #[test]
fn it_works() {
    // assert_eq!(4, add_two(2));
}

#[bench]
fn bench_add_two(b: &mut Bencher) {
    let mut sum = 0u64;
    b.iter(|| {
        let mut count = 0;
        for _i in 0..1_000_000_000 {
            count += 1;
        }
        sum += count;
        println!("Hello, world! {}", count);
    });
    println!("Total! {}", sum);
}

#[bench]
fn bench_array_fold(b: &mut Bencher) {
    let input: Vec<_> = (1..1_000_000_000).map(|i| i as T).collect();
    b.iter(|| {
        let res = count(&input);
        println!("Hello, world! {}", res);
    });
}

#[bench]
fn bench_array_fold_vectorized(b: &mut Bencher) {
    let input: Vec<_> = (1..1_000_000_000).map(|i| i as T).collect();
    b.iter(|| {
        let res = count_vectorized(&input);
        println!("Hello, world! {}", res);
    });
}