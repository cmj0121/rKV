use std::path::Path;

fn generate_and_write(n: usize, seed: u64, path: &Path) {
    let mut buf = Vec::with_capacity(21 + n * 8);
    buf.extend_from_slice(b"rKVB");
    buf.push(1);
    buf.extend_from_slice(&(n as u64).to_be_bytes());
    buf.extend_from_slice(&seed.to_be_bytes());
    for i in 0..n {
        buf.extend_from_slice(&(i as i64).to_be_bytes());
    }
    let compressed = zstd::encode_all(&buf[..], 3).unwrap();
    std::fs::write(path, &compressed).unwrap();
    eprintln!(
        "  {}: {} keys, raw {} bytes, compressed {} bytes",
        path.display(),
        n,
        buf.len(),
        compressed.len()
    );
}

fn main() {
    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("bench/data");
    std::fs::create_dir_all(&data_dir).unwrap();

    let seed: u64 = 42;
    let sizes = [
        (1_000, "1k"),
        (8_000, "8k"),
        (16_000, "16k"),
        (1_000_000, "1m"),
    ];

    eprintln!("Generating benchmark datasets...");
    for (n, label) in &sizes {
        let path = data_dir.join(format!("{label}.zst"));
        generate_and_write(*n, seed, &path);
    }
    eprintln!("Done.");
}
