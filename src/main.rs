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
use clap::Parser;
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
use std::thread::{available_parallelism};
use flate2::Compression;
use serde_json::Value;
use threadpool::ThreadPool;

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

fn process_file(
    input_path: &PathBuf,
    output_path: &PathBuf,
    bloom_filter: Arc<BloomFilter>,
    max_ngram_size: usize,
    min_ngram_size: usize,
    update_bloom_filter: bool,
    filtering_threshold: f64,
    annotate_only: bool,
    dedupe_by: &String,
) -> Result<(), io::Error> {
    let input_file = OpenOptions::new().
        read(true).
        write(false).
        create(false).
        open(input_path)?;
    let reader = BufReader::with_capacity(
        1024 * 1024,
        GzDecoder::new(input_file));

    let output_file = OpenOptions::new().
        read(false).
        write(true).
        create(true).
        truncate(true).
        open(output_path)?;
    let mut writer = BufWriter::with_capacity(
        1024 * 1024,
        GzEncoder::new(output_file, Compression::default()));

    let mut line_number = 0;
    let mut lines_written = 0;
    for line in reader.lines() {
        line_number += 1;
        let line = line.unwrap();
        let mut data: Value = serde_json::from_str(&line).unwrap();
        if dedupe_by == "paragraphs" {
            let text = data["text"].as_str().unwrap();
            let mut newlines = Vec::new();
            newlines.push(0);
            for i in text.match_indices("\n") {
                newlines.push(i.0);
            }
            newlines.push(text.len());
            let mut windows_to_remove = Vec::new();

            for paragraph_window in newlines.windows(2) {
                let paragraph = &text[paragraph_window[0]..paragraph_window[1]];

                // calculate hashes for the paragraph
                let mut hashes: Vec<Vec<u64>> = Vec::new();
                let mut ngram: VecDeque<&str> = VecDeque::with_capacity(max_ngram_size);
                for token in tokenize(paragraph) {
                    ngram.push_back(token);
                    if ngram.len() >= max_ngram_size {
                        hashes.push(bloom_filter.hashes(&ngram));
                        ngram.pop_front();
                    }
                }
                // If the paragraph was too short, put in a shorter ngram, so we can dedupe short
                // paragraphs exactly.
                if hashes.is_empty() && ngram.len() >= min_ngram_size {
                    hashes.push(bloom_filter.hashes(&ngram));
                }

                if filtering_threshold <= 0.0 {
                    // If we're just priming the filter, just do it right here without checking whether
                    // the ngrams are in the filter.
                    if update_bloom_filter {
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
                    let too_many_duplicate_ngrams =
                        contained_ngrams as f64 / number_of_ngrams as f64 > filtering_threshold;
                    if too_many_duplicate_ngrams {
                        windows_to_remove.push(paragraph_window);
                    } else if update_bloom_filter {
                        for ngram in hashes {
                            bloom_filter.insert_hashes(&ngram);
                        }
                    }
                }
            }

            if annotate_only {
                data["duplicate_spans"] = serde_json::to_value(windows_to_remove).unwrap();
            } else {
                let mut output_paragraphs = String::new();
                let mut last_end = 0;
                for paragraph_window in windows_to_remove {
                    output_paragraphs.push_str(&text[last_end..paragraph_window[0]]);
                    last_end = paragraph_window[1];
                }
                output_paragraphs.push_str(&text[last_end..]);
                data["text"] = Value::String(output_paragraphs);
            }

            serde_json::to_writer(&mut writer, &data)?;
            writer.write_all(b"\n")?;
        } else if dedupe_by == "url" {
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
        } else {
            panic!("Unknown dedupe_by: {}", dedupe_by);
        }
    }

    log::info!("Dropped {} of {} documents in {}", line_number - lines_written, line_number, input_path.display());

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
struct StreamConfig {
    name: String,
    documents: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Config {
    streams: Vec<StreamConfig>,
    bloom_filter: BloomFilterConfig,
    processes: usize,
}

struct Shard {
    name: String,
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
        let mut inputs: Vec<String> = Vec::new();
        for pattern in stream_config.documents {
            let index = pattern.chars().position(|c| c == '*').unwrap();
            let prefix = pattern[..index].to_string();
            let suffix = pattern[index + 2..].to_string();
            let mut has_more = true;
            let mut token: Option<String> = None;
            while has_more {
                let mut resp =
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
                    let full_prefix = prefix.to_owned() + sub_folder.prefix().unwrap();
                    let full_path = full_prefix + &suffix;
                    inputs.push(full_path);
                }
                has_more = resp.next_continuation_token().is_some();
                token = resp.next_continuation_token().map(String::from);
                has_more = token.is_some();
            }
            // let sub_folders = block_on(s3_client.list_objects_v2(ListObjectsV2Request {
            //     bucket: "ai2-llm".to_string(),
            //     prefix: Some(prefix),
            //     ..Default::default()
            // })).unwrap().contents.unwrap();
        }
        //shards.push(Shard { name: stream_config.name, inputs, output: stream_config.output });
    }

    // let threadpool = ThreadPool::new(threads);
    // for input in args.inputs {
    //     let mut output = args.output_directory.clone();
    //     output.push(&input.file_name().unwrap());
    //     let bloom_filter = bloom_filter.clone();
    //     let dedupe_by = args.dedupe_by.clone();
    //
    //     threadpool.execute(move || {
    //         log::info!("Processing {:?}...", input);
    //         process_file(
    //             &input,
    //             &output,
    //             bloom_filter,
    //             args.max_ngram_size,
    //             args.min_ngram_size,
    //             args.update_bloom_filter,
    //             args.filtering_threshold,
    //             args.annotate_only,
    //             &dedupe_by,
    //         ).unwrap();
    //     });
    // }
    // threadpool.join();

    if !config.bloom_filter.read_only {
        log::info!("Writing bloom filter to {:?}...", config.bloom_filter.file);
        bloom_filter.write_to_file(&bloom_filter_file).unwrap();
        log::info!("Bloom filter written.");
    }
    log::info!("Done!");
}
