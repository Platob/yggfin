use serde::Deserialize;
use xxhash_rust::xxh3::xxh3_64_with_seed;

#[derive(Deserialize)]
struct Corpus {
    protocol: String,
    algorithm: String,
    seed: u64,
    raw_vectors: Vec<RawVector>,
    vectors: Vec<Vector>,
}
#[derive(Deserialize)]
struct RawVector {
    name: String,
    raw_hex: String,
    digest_hex: String,
    signed_i64: i64,
}

#[derive(Deserialize)]
struct Vector {
    name: String,
    parts: Vec<Part>,
    frame_hex: String,
    digest_hex: String,
    signed_i64: i64,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Part {
    Null,
    Utf8 { value: String },
    Bytes { hex: String },
    Bool { value: bool },
    I64 { value: String },
    F64 { bits: String },
    Uuid { value: String },
}

fn payload(part: &Part) -> Option<Vec<u8>> {
    match part {
        Part::Null => None,
        Part::Utf8 { value } => Some(value.as_bytes().to_vec()),
        Part::Bytes { hex } => Some(from_hex(hex)),
        Part::Bool { value } => Some(vec![u8::from(*value)]),
        Part::I64 { value } => Some(value.parse::<i64>().unwrap().to_le_bytes().to_vec()),
        Part::F64 { bits } => {
            let bits = u64::from_str_radix(bits, 16).unwrap();
            let value = f64::from_bits(bits);
            let canonical = if value.is_nan() {
                0x7ff8_0000_0000_0000
            } else {
                bits
            };
            Some(canonical.to_le_bytes().to_vec())
        }
        Part::Uuid { value } => Some(from_hex(&value.replace('-', ""))),
    }
}

fn frame(parts: &[Part]) -> Vec<u8> {
    assert!(!parts.is_empty(), "an identity needs at least one part");
    let mut framed = Vec::new();
    for part in parts {
        match payload(part) {
            None => framed.extend_from_slice(&(-1_i64).to_le_bytes()),
            Some(raw) => {
                let length = i64::try_from(raw.len()).expect("part exceeds signed int64");
                framed.extend_from_slice(&length.to_le_bytes());
                framed.extend_from_slice(&raw);
            }
        }
    }
    framed
}

fn from_hex(value: &str) -> Vec<u8> {
    assert!(value.len() % 2 == 0, "hex must contain whole bytes");
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn into_hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn main() {
    let corpus: Corpus =
        serde_json::from_str(include_str!("../../../../docs/assets/identity-v1.json")).unwrap();
    assert_eq!(corpus.protocol, "rekep-identity-v1");
    assert_eq!(corpus.algorithm, "XXH3-64");
    assert_eq!(corpus.seed, 0);

    for vector in &corpus.raw_vectors {
        let digest = xxh3_64_with_seed(&from_hex(&vector.raw_hex), corpus.seed);
        assert_eq!(
            format!("{digest:016x}"),
            vector.digest_hex,
            "{} raw bits",
            vector.name
        );
        assert_eq!(
            digest as i64, vector.signed_i64,
            "{} raw signed",
            vector.name
        );
    }
    for vector in &corpus.vectors {
        let framed = frame(&vector.parts);
        let digest = xxh3_64_with_seed(&framed, corpus.seed);
        assert_eq!(into_hex(&framed), vector.frame_hex, "{} frame", vector.name);
        assert_eq!(
            format!("{digest:016x}"),
            vector.digest_hex,
            "{} bits",
            vector.name
        );
        assert_eq!(digest as i64, vector.signed_i64, "{} signed", vector.name);
    }
    println!(
        "{}: {} raw + {} framed vectors match",
        corpus.protocol,
        corpus.raw_vectors.len(),
        corpus.vectors.len()
    );
}
