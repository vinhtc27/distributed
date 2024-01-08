use crdts::{CmRDT, CvRDT, PNCounter};

fn main() {
    let actor_a = "A".to_owned();
    let mut a: PNCounter<String> = PNCounter::new();
    a.apply(a.inc(actor_a.clone()));
    a.apply(a.inc_many(actor_a.clone(), 10));
    a.apply(a.dec(actor_a.clone()));
    a.apply(a.inc(actor_a.clone()));
    a.apply(a.dec_many(actor_a.clone(), 2));

    println!("A value: {:?}", a.read());

    let json = serde_json::to_string(&a).expect("Serialization error");
    println!("A value as JSON: {}", json);
    let mut b: PNCounter<String> = match serde_json::from_str(&json) {
        Ok(deserialized) => deserialized,
        Err(e) => {
            eprintln!("Deserialization error: {}", e);
            return;
        }
    };

    println!("B value: {:?}", b.read());
    let actor_b = "B".to_owned();
    b.apply(b.inc(actor_b.clone()));
    b.apply(b.inc_many(actor_b.clone(), 12));
    b.apply(b.dec(actor_b.clone()));
    b.apply(b.inc(actor_b.clone()));

    let start = std::time::Instant::now();
    let _ = a.merge(b);
    println!("Merge time: {:?}", start.elapsed());

    println!("A value after merge: {:?}", a.read());
}
