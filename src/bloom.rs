use std::io::{self, Read, Write};

const MAGIC: &[u8; 4] = b"BLM1";

#[allow(dead_code)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    k: u32,
}

impl BloomFilter {
    pub fn new(num_bits: usize, k: u32) -> Self {
        assert!(
            num_bits > 0 && num_bits.is_multiple_of(8),
            "num_bits must be a positive multiple of 8"
        );
        Self {
            bits: vec![0; num_bits / 8],
            num_bits,
            k,
        }
    }

    pub fn with_capacity(estimated_items: usize, target_fpp: f64) -> Self {
        let num_bits = optimal_bits(estimated_items, target_fpp);
        let k = optimal_k(num_bits, estimated_items);
        Self::new(num_bits, k)
    }

    pub fn insert(&mut self, data: &[u8]) {
        let (h1, h2) = hash_pair(data);
        for i in 0..self.k {
            let idx = double_hash(h1, h2, i as u64, self.num_bits);
            self.bits[idx / 8] |= 1 << (idx % 8);
        }
    }

    pub fn contains(&self, data: &[u8]) -> bool {
        let (h1, h2) = hash_pair(data);
        for i in 0..self.k {
            let idx = double_hash(h1, h2, i as u64, self.num_bits);
            if self.bits[idx / 8] & (1 << (idx % 8)) == 0 {
                return false;
            }
        }
        true
    }

    #[cfg(test)]
    pub fn insert_str_trigrams(&mut self, s: &str) {
        for tri in trigrams(s) {
            self.insert(&tri);
        }
    }

    pub fn might_contain_substr(&self, needle: &str) -> bool {
        let tris = trigrams(needle);
        if tris.is_empty() {
            return true;
        }
        for tri in tris {
            if !self.contains(&tri) {
                return false;
            }
        }
        true
    }

    #[allow(dead_code)]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Bytes this filter holds in memory for as long as its part is open.
    pub fn resident_bytes(&self) -> usize {
        self.bits.len()
    }

    #[allow(dead_code)]
    pub fn k(&self) -> u32 {
        self.k
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.bits.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < 12 {
            return Err("bloom buffer too short".to_string());
        }
        if &buf[0..4] != MAGIC {
            return Err("bloom magic mismatch".to_string());
        }
        let num_bits = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let k = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        if num_bits == 0 || !num_bits.is_multiple_of(8) {
            return Err(format!("invalid bloom num_bits {}", num_bits));
        }
        let bits_len = num_bits / 8;
        if buf.len() != 12 + bits_len {
            return Err(format!(
                "bloom length mismatch: expected {} got {}",
                12 + bits_len,
                buf.len()
            ));
        }
        Ok(Self {
            bits: buf[12..].to_vec(),
            num_bits,
            k,
        })
    }

    #[allow(dead_code)]
    pub fn write_to(&self, path: &std::path::Path) -> io::Result<()> {
        let mut f = std::fs::File::create(path)?;
        f.write_all(&self.encode())?;
        f.sync_all()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn read_from(path: &std::path::Path) -> Result<Self, String> {
        let mut f = std::fs::File::open(path).map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf).map_err(|e| e.to_string())?;
        Self::decode(&buf)
    }
}

pub fn trigrams(s: &str) -> Vec<[u8; 3]> {
    let lower = s.to_lowercase();
    let bytes = lower.as_bytes();
    let mut out = Vec::new();
    if bytes.len() < 3 {
        return out;
    }
    for i in 0..bytes.len() - 2 {
        out.push([bytes[i], bytes[i + 1], bytes[i + 2]]);
    }
    out
}

fn double_hash(h1: u64, h2: u64, i: u64, m: usize) -> usize {
    let combined = h1.wrapping_add(h2.wrapping_mul(i));
    (combined as usize) % m
}

fn hash_pair(data: &[u8]) -> (u64, u64) {
    (
        fnv1a_64(data, 0xcbf29ce484222325),
        fnv1a_64(data, 0x517cc1b727220a95),
    )
}

fn fnv1a_64(data: &[u8], seed: u64) -> u64 {
    let mut h = seed;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn optimal_bits(n: usize, p: f64) -> usize {
    if n == 0 {
        return 8192;
    }
    let m = (-(n as f64) * p.ln() / (std::f64::consts::LN_2.powi(2))).ceil() as usize;
    let m = m.max(1024);
    (m + 7) & !7
}

fn optimal_k(m: usize, n: usize) -> u32 {
    if n == 0 {
        return 4;
    }
    let k = ((m as f64 / n as f64) * std::f64::consts::LN_2).round() as u32;
    k.clamp(1, 16)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_contains() {
        let mut b = BloomFilter::new(8192, 4);
        b.insert(b"hello world");
        assert!(b.contains(b"hello world"));
        assert!(!b.contains(b"goodbye world"));
    }

    #[test]
    fn trigram_substring_pruning() {
        let mut b = BloomFilter::with_capacity(100, 0.01);
        b.insert_str_trigrams("the quick brown fox");
        assert!(b.might_contain_substr("quick brown"));
        assert!(b.might_contain_substr("brown fox"));
        assert!(!b.might_contain_substr("zzzzzz"));
    }

    #[test]
    fn short_needle_skips_pruning() {
        let mut b = BloomFilter::new(8192, 4);
        b.insert_str_trigrams("abcdef");
        assert!(b.might_contain_substr("ab"));
    }

    #[test]
    fn encode_roundtrip() {
        let mut b = BloomFilter::with_capacity(200, 0.01);
        b.insert_str_trigrams("loggytracy bloom filter test");
        let bytes = b.encode();
        let b2 = BloomFilter::decode(&bytes).expect("decode");
        assert_eq!(b.num_bits(), b2.num_bits());
        assert_eq!(b.k(), b2.k());
        assert!(b2.might_contain_substr("bloom filter"));
    }

    #[test]
    fn trigrams_basic() {
        let t = trigrams("abcd");
        assert_eq!(t, vec![*b"abc", *b"bcd"]);
        assert!(trigrams("ab").is_empty());
    }

    #[test]
    fn trigrams_lowercase() {
        let t = trigrams("ABC");
        assert_eq!(t, vec![*b"abc"]);
    }
}
