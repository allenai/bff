use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fs::File;
use std::path::PathBuf;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::size_of;
use std::sync::Arc;
use std::env;
use std::process;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use serde_json;
use aws_sdk_s3::{Client as S3Client, config::Region};
use unicode_segmentation::UnicodeSegmentation;
use rand::Rng;
use ahash::RandomState;
use byteorder::{LittleEndian, NativeEndian, ReadBytesExt, WriteBytesExt};
use std::sync::atomic::{AtomicU32, Ordering};
use flate2::Compression;
use serde_json::Value;
use threadpool::ThreadPool;
use rayon::prelude::*;

fn tokenize(s: &str) -> impl Iterator<Item=&str> {
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
    hash_builder_seeds: Vec<[u64; 4]>,
    // RandomState does not store its seeds, so we have to store them ourselves.
    hash_builders: Vec<RandomState>,
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

    fn prob_of_false_positive(size_in_bytes: usize, expected_elements: usize, num_hashers: usize) -> f64 {
        let k = num_hashers as f64;
        let m = (size_in_bytes * 8) as f64;
        let n = expected_elements as f64;
        (1.0 - (1.0 - (1.0 / m)).powf(k * n)).powf(k)
    }

    fn suggest_size_in_bytes(expected_elements: usize, desired_false_positive_rate: f64) -> usize {
        let mut size_in_bytes = 1024 * 1024;
        while size_in_bytes < usize::MAX / 2 && Self::prob_of_false_positive(
            size_in_bytes,
            expected_elements,
            Self::optimal_number_of_hashers(size_in_bytes, expected_elements),
        ) > desired_false_positive_rate {
            size_in_bytes *= 2;
        }
        size_in_bytes
    }

    fn my_prob_of_false_positive(&self, expected_elements: usize) -> f64 {
        Self::prob_of_false_positive(
            self.size_in_bytes(),
            expected_elements,
            self.hash_builders.len())
    }

    fn size_in_bytes(&self) -> usize {
        self.bits.len() * size_of::<AtomicU32>()
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

        let number_of_elements = stream.read_u64::<LittleEndian>()?;
        let mut bits = Vec::new();
        bits.reserve_exact(number_of_elements as usize);
        for _ in 0..number_of_elements {
            bits.push(AtomicU32::new(stream.read_u32::<NativeEndian>()?));
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
        unsafe {
            let bytes: &[u8] = std::slice::from_raw_parts(
                self.bits.as_ptr() as *const u8,
                self.bits.len() * size_of::<AtomicU32>());
            stream.write_all(bytes)?;
        };

        Ok(())
    }

    fn hashes(&self, s: &VecDeque<&str>) -> Vec<u64> {
        self.hash_builders.iter().map(|hash_builder| {
            let mut hasher = hash_builder.build_hasher();
            s.hash(&mut hasher);
            hasher.finish()
        }).collect()
    }

    fn insert_hashes(&self, hashes: &Vec<u64>) {
        for hash in hashes {
            let hash = *hash as usize;
            let index = hash / 32 % self.bits.len();
            let bit = hash % 32;
            self.bits[index].fetch_or(1 << bit, Ordering::Relaxed);
        }
    }

    fn insert(&self, s: &VecDeque<&str>) {
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

fn process_shard(
    shard: Shard,
    bloom_filter: Arc<BloomFilter>,
    update_bloom_filter: bool,
    annotate_only: bool
) -> Result<(), io::Error> {
    let output_file = OpenOptions::new().
        read(false).
        write(true).
        create(true).
        truncate(true).
        open(shard.output)?;

    let mut writer = BufWriter::with_capacity(
        1024 * 1024,
        GzEncoder::new(output_file, Compression::default()));

    for input_path in shard.inputs {
        let input_file = OpenOptions::new().
            read(true).
            write(false).
            create(false).
            open(input_path)?;
        let reader = BufReader::with_capacity(
            1024 * 1024,
            GzDecoder::new(input_file));

        let mut line_number = 0;
        let mut lines_written = 0;
        for line in reader.lines() {
            line_number += 1;
            let line = line.unwrap();
            let mut data: Value = serde_json::from_str(&line).unwrap();
            let url = data["metadata"]["url"].as_str().unwrap();
            let mut url_ngram = VecDeque::with_capacity(1);
            url_ngram.push_back(url);
            let mut should_write = true;

            if bloom_filter.contains(&url_ngram) {
                if annotate_only {
                    data["duplicate"] = Value::Bool(true);
                } else {
                    should_write = false;
                }
            } else {
                if update_bloom_filter {
                    bloom_filter.insert(&url_ngram);
                }
            }

            if should_write {
                lines_written += 1;
                serde_json::to_writer(&mut writer, &data)?;
                writer.write_all(b"\n")?;
            }
        }
        log::info!("Dropped {} of {} documents", line_number - lines_written, line_number);
    }


    Ok(())
}

#[derive(Serialize, Deserialize)]
struct BloomFilterConfig {
    file: String,
    size_in_bytes: usize,
    read_only: bool,
    estimated_doc_count: usize,
    desired_false_positive_rate: f64,
}

#[derive(Serialize, Deserialize)]
struct StreamOutputConfig {
    path: String,
    max_size_in_bytes: usize,
}

#[derive(Serialize, Deserialize)]
struct StreamConfig {
    name: String,
    documents: Vec<String>,
    output: StreamOutputConfig,
}

#[derive(Serialize, Deserialize)]
struct Config {
    streams: Vec<StreamConfig>,
    bloom_filter: BloomFilterConfig,
    processes: usize,
}

#[derive(Clone)]
struct Shard {
    inputs: Vec<String>,
    output: String,
}

fn main() {
    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info")
    }
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        log::error!("Usage: {} <config file>", args[0]);
        process::exit(1);
    }
    let config: Config = serde_json::from_reader(File::open(&args[1]).unwrap()).unwrap();
    let bloom_filter_file = PathBuf::from(&config.bloom_filter.file);

    let bloom_filter =
        if bloom_filter_file.exists() {
            log::info!("Loading bloom filter from {:?}...", config.bloom_filter.file);
            BloomFilter::from_file(&bloom_filter_file).unwrap()
        } else {
            log::info!("Creating new bloom filter...");
            let mut bloom_filter_size: usize = config.bloom_filter.size_in_bytes;
            if bloom_filter_size == 0 {
                bloom_filter_size = BloomFilter::suggest_size_in_bytes(config.bloom_filter.estimated_doc_count, config.bloom_filter.desired_false_positive_rate);
                log::info!("Creating bloom filter with size {} bytes to achieve false positive rate {}", bloom_filter_size, config.bloom_filter.desired_false_positive_rate);
            }
            let num_hashers = BloomFilter::optimal_number_of_hashers(
                bloom_filter_size,
                config.bloom_filter.estimated_doc_count);
            let p = BloomFilter::prob_of_false_positive(bloom_filter_size, config.bloom_filter.estimated_doc_count, num_hashers);
            log::info!("Bloom filter will have {} hashers and a false positive probability of {}.", num_hashers, p);
            BloomFilter::new(bloom_filter_size, num_hashers)
        };

    let bloom_filter = Arc::new(bloom_filter);


    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build().unwrap();
    let aws_config = rt.block_on(aws_config::from_env().region(Region::new("us-east-1")).load());
    let s3_client = S3Client::new(&aws_config);

    let mut shards: Vec<Shard> = Vec::new();
    for stream_config in config.streams {
        log::info!("Computing shards for stream {}...", stream_config.name);
        let mut stream_inputs: Vec<String> = Vec::new();
        for pattern in stream_config.documents {
            let index = pattern.chars().position(|c| c == '*').unwrap();
            let prefix = pattern[..index].to_string();
            let suffix = pattern[index + 2..].to_string();
            let mut has_more = true;
            let mut token: Option<String> = None;
            while has_more {
                let resp =
                    if token.is_some() {
                        rt.block_on(s3_client.list_objects_v2()
                            .bucket("ai2-llm")
                            .prefix(&prefix)
                            .delimiter("/")
                            .continuation_token(token.unwrap())
                            .send()).unwrap()
                    } else {
                        rt.block_on(s3_client.list_objects_v2()
                            .bucket("ai2-llm")
                            .prefix(&prefix)
                            .delimiter("/")
                            .send()).unwrap()
                    };
                for sub_folder in resp.common_prefixes().unwrap_or_default() {
                    let full_path = sub_folder.prefix().unwrap().to_owned() + &suffix;
                    stream_inputs.push(full_path);
                }
                token = resp.next_continuation_token().map(String::from);
                has_more = token.is_some();
            }
        }
        stream_inputs.sort();
        let inputs_with_sizes = stream_inputs.par_iter().map(|input| {
            let resp = rt.block_on(s3_client.head_object()
                .bucket("ai2-llm")
                .key(input)
                .send()).unwrap();
            (input.to_string(), resp.content_length as usize)
        }).collect::<Vec<(String, usize)>>();
        let mut shard_size = inputs_with_sizes[0].1;
        let mut shard_inputs: Vec<String> = Vec::new();
        shard_inputs.push(inputs_with_sizes[0].0.clone());
        for (input, size) in inputs_with_sizes[1..].iter() {
            shard_size += size;
            if shard_size > stream_config.output.max_size_in_bytes {
                let output = format!("{}/{}-{:04}.json.gz", stream_config.output.path, stream_config.name, shards.len());
                let shard = Shard {
                    inputs: shard_inputs.clone(),
                    output: output.clone(),
                };
                shards.push(shard);
                shard_size = 0;
                shard_inputs = Vec::new();
            }
            shard_inputs.push(input.clone());
        }
        if shard_inputs.len() > 0 {
            let output = format!("{}/{}-{:04}.json.gz", stream_config.output.path, stream_config.name, shards.len());
            let shard = Shard {
                inputs: shard_inputs.clone(),
                output: output.clone(),
            };
            shards.push(shard);
        }
        log::info!("Splitting {} files for {} into {} shards", stream_inputs.len(), stream_config.name, shards.len());
    }

    let threadpool = ThreadPool::new(config.processes);
    for shard in shards {
        let bloom_filter = bloom_filter.clone();

        threadpool.execute(move || {
            log::info!("Processing {:?}...", shard.output);
            process_shard(
                shard.clone(),
                bloom_filter,
                !config.bloom_filter.read_only,
                false,
            ).unwrap();
        });
    }
    threadpool.join();

    if !config.bloom_filter.read_only {
        log::info!("Writing bloom filter to {:?}...", config.bloom_filter.file);
        bloom_filter.write_to_file(&bloom_filter_file).unwrap();
        log::info!("Bloom filter written.");
    }
    log::info!("Done!");
}
