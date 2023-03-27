use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufRead, BufReader, BufWriter};
use std::path::PathBuf;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::size_of;
use clap::Parser;
use flate2::read::GzDecoder;
use serde_json;
use unicode_segmentation::UnicodeSegmentation;
use rand::Rng;
use ahash::RandomState;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::sync::atomic::{AtomicU32, Ordering};
use trim_in_place::TrimInPlace;


#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    bloom_filter_file: PathBuf,

    /// The size of the bloom filter in bytes. If the filter already exists, this parameter is
    /// ignored.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    bloom_filter_size: usize,

    /// The number of expected ngrams. This is used to calculate the optimal number of hashers.
    /// If the filter already exists, this parameter is ignored.
    #[arg(long, default_value_t = 1000000000)]
    expected_ngram_count: usize,

    /// The smallest ngram size to consider. Paragraphs that have less than this number of tokens
    /// are not deduplicated and always kept. These ngrams are never added to the bloom filter.
    /// Note that this value only matters if the paragraph has fewer tokens than the max ngram size.
    #[arg(long, default_value_t = 3)]
    min_ngram_size: usize,

    /// The largest ngram size to consider. Paragraphs are deduplicated based on the number of
    /// ngrams of this size that are already present in the bloom filter.
    #[arg(long, default_value_t = 20)]
    max_ngram_size: usize,

    /// If this fraction of ngrams of the max ngram size are already present in the bloom filter,
    /// the paragraph is considered a duplicate and is discarded.
    /// Set this to 0 to never produce any output. This is useful when you want to prime the filter
    /// with some content that should be considered duplicates, without deduplicating that content
    /// itself.
    #[arg(long, default_value_t = 0.80)]
    filtering_threshold: f64,

    /// Whether or not to update the bloom filter. If this is false, the filter is not updated, but
    /// the input is still deduplicated based on the filter.
    #[arg(long, default_value_t = true)]
    update_bloom_filter: bool,

    /// The number of threads to use for processing.
    /// If this is 0, the number of threads is automatically determined.
    #[arg(long, default_value_t = 0)]
    threads: usize,

    #[arg(index = 1)]
    input: Vec<PathBuf>,
}

fn tokenize(s: &str) -> impl Iterator<Item = &str> {
    s.split_word_bounds().filter(|w| {
        for c in w.chars() {
            if !c.is_whitespace() {
                return true;
            }
        }
        false
    })
}

struct BloomFilter {
    bits: Vec<AtomicU32>,
    hash_builder_seeds: Vec<[u64; 4]>, // RandomState does not store its seeds, so we have to store them ourselves.
    hash_builders: Vec<RandomState>
}

impl BloomFilter {
    const MAGIC: u32 = 0x81F0F117;
    const VERSION: u32 = 1;

    fn optimal_number_of_hashers(size_in_bytes: usize, expected_elements: usize) -> usize {
        let expected_elements = expected_elements as f64;
        let size_in_bits = (size_in_bytes * 8) as f64;
        let k = (size_in_bits / expected_elements) * (2.0f64.ln());
        k.ceil() as usize
    }

    fn new(size_in_bytes: usize, num_hashers: usize) -> Self {
        let mut rng = rand::thread_rng();
        let mut hash_builder_seeds = Vec::with_capacity(num_hashers);
        let mut hash_builders = Vec::with_capacity(num_hashers);
        for _ in 0..num_hashers {
            let seeds = rng.gen::<[u64; 4]>();
            hash_builders.push(RandomState::with_seeds(
                seeds[0],
                seeds[1],
                seeds[2],
                seeds[3]));
            hash_builder_seeds.push(seeds);
        }

        let mut bits = Vec::new();
        let number_of_u32 = size_in_bytes / size_of::<AtomicU32>();
        bits.reserve_exact(number_of_u32);
        for _ in 0..number_of_u32 {
            bits.push(AtomicU32::new(0));
        }

        Self { bits, hash_builder_seeds, hash_builders }
    }

    fn from_file(path: &PathBuf) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;
        let mut stream = BufReader::new(&mut file);

        let magic: u32 = stream.read_u32::<LittleEndian>()?;
        if magic != Self::MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid magic"));
        }

        let version: u32 = stream.read_u32::<LittleEndian>()?;
        if version != Self::VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid version"));
        }

        let num_hashers: u32 = stream.read_u32::<LittleEndian>()?;
        let mut hash_builder_seeds = Vec::with_capacity(num_hashers as usize);
        let mut hash_builders = Vec::with_capacity(num_hashers as usize);
        for _ in 0..num_hashers {
            let seeds = [
                stream.read_u64::<LittleEndian>()?,
                stream.read_u64::<LittleEndian>()?,
                stream.read_u64::<LittleEndian>()?,
                stream.read_u64::<LittleEndian>()?,
            ];
            hash_builders.push(RandomState::with_seeds(
                seeds[0],
                seeds[1],
                seeds[2],
                seeds[3]));
            hash_builder_seeds.push(seeds);
        }

        let numer_of_elements = stream.read_u64::<LittleEndian>()?;
        let mut bits = Vec::new();
        bits.reserve_exact(numer_of_elements as usize);
        for _ in 0..numer_of_elements {
            bits.push(AtomicU32::new(stream.read_u32::<LittleEndian>()?));
        }

        Ok(Self { bits, hash_builder_seeds, hash_builders })
    }

    fn write_to_file(&self, path: &PathBuf) -> io::Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        let mut stream = BufWriter::new(&file);

        stream.write_u32::<LittleEndian>(Self::MAGIC)?;
        stream.write_u32::<LittleEndian>(Self::VERSION)?;
        stream.write_u32::<LittleEndian>(self.hash_builder_seeds.len() as u32)?;
        for hash_builder_seed in &self.hash_builder_seeds {
            for seed in hash_builder_seed {
                stream.write_u64::<LittleEndian>(*seed)?;
            }
        }

        stream.write_u64::<LittleEndian>(self.bits.len() as u64)?;
        for bit in &self.bits {
            stream.write_u32::<LittleEndian>(bit.load(Ordering::Relaxed))?;
        }

        Ok(())
    }

    fn hashes(&self, s: &VecDeque<&str>) -> Vec<u64> {
        self.hash_builders.iter().map(|hash_builder| {
            let mut hasher = hash_builder.build_hasher();
            s.hash(&mut hasher);
            hasher.finish()
        }).collect()
    }

    fn insert_hashes(&mut self, hashes: &Vec<u64>) {
        for hash in hashes {
            let hash = *hash as usize;
            let index = hash / 32 % self.bits.len();
            let bit = hash % 32;
            self.bits[index].fetch_or(1 << bit, Ordering::Relaxed);
        }
    }

    fn insert(&mut self, s: &VecDeque<&str>) {
        let hashes = self.hashes(s);
        self.insert_hashes(&hashes);
    }

    fn contains_hashes(&self, hashes: &Vec<u64>) -> bool {
        for hash in hashes {
            let hash = *hash as usize;
            let index = hash / 32 % self.bits.len();
            let bit = hash % 32;
            if self.bits[index].load(Ordering::Relaxed) & (1 << bit) == 0 {
                return false;
            }
        }

        return true;
    }

    fn contains(&self, s: &VecDeque<&str>) -> bool {
        let hashes = self.hashes(s);
        self.contains_hashes(&hashes)
    }
}

fn main() {
    let args = Args::parse();

    let mut bloom_filter = if args.bloom_filter_file.exists() {
        BloomFilter::from_file(&args.bloom_filter_file).unwrap()
    } else {
        let num_hashers = BloomFilter::optimal_number_of_hashers(
            args.bloom_filter_size,
            args.expected_ngram_count);
        BloomFilter::new(args.bloom_filter_size, num_hashers)
    };

    for input in args.input {
        let file = File::open(input).unwrap();
        let reader = BufReader::with_capacity(
            1024 * 1024,
            GzDecoder::new(file));

        for line in reader.lines() {
            let line = line.unwrap();
            let mut data: serde_json::Value = serde_json::from_str(&line).unwrap();
            let text = data["text"].as_str().unwrap();
            let paragraphs = text.split("\n");
            let mut output_paragraphs = String::new();

            for paragraph in paragraphs {
                // calculate hashes for the paragraph
                let mut hashes: Vec<Vec<u64>> = Vec::new();
                let mut ngram: VecDeque<&str> = VecDeque::with_capacity(args.max_ngram_size);
                for token in tokenize(paragraph) {
                    if ngram.len() >= args.max_ngram_size {
                        hashes.push(bloom_filter.hashes(&ngram));
                        ngram.pop_front();
                    }
                    ngram.push_back(token);
                }
                if !ngram.is_empty() && ngram.len() < args.max_ngram_size {
                    if args.update_bloom_filter {
                        hashes.push(bloom_filter.hashes(&ngram));
                    }
                }

                if args.filtering_threshold <= 0.0 {
                    // If we're just priming the filter, just do it right here without checking whether
                    // the ngrams are in the filter.
                    if args.update_bloom_filter {
                        for ngram in hashes {
                            bloom_filter.insert_hashes(&ngram);
                        }
                    }
                } else {
                    // calculate how many ngrams are in the bloom filter
                    let contained_ngrams = hashes.iter().filter(|ngram| {
                        bloom_filter.contains_hashes(ngram)
                    }).count();
                    let number_of_ngrams = hashes.len();

                    // produce output
                    if contained_ngrams as f64 / number_of_ngrams as f64 <= args.filtering_threshold {
                        output_paragraphs.push_str(paragraph);
                        output_paragraphs.push('\n');
                        if args.update_bloom_filter {
                            for ngram in hashes {
                                bloom_filter.insert_hashes(&ngram);
                            }
                        }
                    }
                }
            }

            output_paragraphs.trim_in_place();
            if !output_paragraphs.is_empty() {
                data["text"] = serde_json::Value::String(output_paragraphs);
                println!("{}", serde_json::to_string(&data).unwrap());
            }
        }
    }

    if args.update_bloom_filter {
        bloom_filter.write_to_file(&args.bloom_filter_file).unwrap();
    }
}
