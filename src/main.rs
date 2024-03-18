use ahash::RandomState;
use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, NativeEndian, ReadBytesExt, WriteBytesExt};
use clap::Parser;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use glob::glob;
use human_bytes::human_bytes;
use indicatif::{ProgressBar,ProgressStyle};
use rand::Rng;
use serde_json::Value;
use std::clone::Clone;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant};
use std::thread::available_parallelism;
use sysinfo::{
    System,
};
use threadpool::ThreadPool;
use unicode_segmentation::UnicodeSegmentation;




#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    bloom_filter_file: PathBuf,

    /// The size of the bloom filter in bytes. If the filter already exists, this parameter is
    /// ignored.
    /// If ==0 this _requires_ that fp_rate is > 0
    #[arg(long, default_value_t=0)]
    bloom_filter_size: usize,

    /// The desired per-ngram false positive rate. If bloom_filter_size is not specified, this MUST 
    /// be specified, and the filter size will be computed using this FP rate and optimal number of 
    /// hashers. Maxes out at 90% of system RAM
    #[arg(long, default_value_t=0.01)]
    fp_rate: f64,


    /// The number of expected ngrams. This is used to calculate the optimal number of hashers.
    /// If the filter already exists, this parameter is ignored.
    #[arg(long)]
    expected_ngram_count: usize,

    /// The smallest ngram size to consider. Paragraphs that have fewer than this number of tokens
    /// are not deduplicated and always kept. These ngrams are never added to the bloom filter.
    /// Note that this value only matters if the paragraph has fewer tokens than the max ngram size.
    #[arg(long, default_value_t = 5)]
    min_ngram_size: usize,

    /// The largest ngram size to consider. Paragraphs are deduplicated based on the number of
    /// ngrams of this size that are already present in the bloom filter.
    #[arg(long, default_value_t = 13)]
    max_ngram_size: usize,

    /// If this fraction of ngrams of the max ngram size are already present in the bloom filter,
    /// the paragraph is considered a duplicate and is discarded.
    /// Set this to 0 to never produce any output. This is useful when you want to prime the filter
    /// with some content that should be considered duplicates, without deduplicating that content
    /// itself.
    #[arg(long, default_value_t = 0.80)]
    filtering_threshold: f64,

    /// Whether or not to update the bloom filter. If this is true, the filter is not updated, but
    /// the input is still deduplicated based on the filter. Default is false.
    #[arg(long, default_value_t = false)]
    no_update_bloom_filter: bool,

    /// Whether or not to save the bloom filter at the end. Defaults to false (i.e., saves the bloom filter)
    /// If this is True, the bloom filter will NOT be saved, regardless of what no_update_bloom_filter suggests
    #[arg(long, default_value_t = false)]
    no_save_bloom_filter: bool,


    /// If this is true, we keep the input intact, but we add an annotation to each document that
    /// explains which spans from the text would have been deleted.
    #[arg(long, default_value_t = false)]
    annotate_only: bool,

    /// If this is true, we only write out document id and source, and annotate which spans would
    /// have been deleted. This produces an attribute file per the llm-data specification.
    #[arg(long, default_value_t = false)]
    annotate_attribute_only: bool,

    /// If you want ngrams to span across paragraph breaks, set this to true.
    /// This also means that bff will only remove a complete document at a time. When this happens
    /// the resulting document will be empty. This also means that deduplication within a document
    /// no longer works. All in all, it might be best to only use this when you're also using
    /// --annotate-only.
    #[arg(long, default_value_t = false)]
    whole_document: bool,

    /// If you want to always match whole paragraphs instead of ngrams, set this to true.
    /// Paragraphs smaller than min_ngram_size will still be excluded.
    #[arg(long, default_value_t = false)]
    whole_paragraphs: bool,

    /// If you don't want to include the progress bar, set this to true.
    /// Will print out filenames as they get processed if this is true
    #[arg(long, default_value_t = false)]
    no_progress: bool,

    /// The number of threads to use for processing.
    /// If this is 0, the number of threads is automatically determined.
    #[arg(long, short = 't', default_value_t = 0)]
    threads: usize,

    /// Input files. These are expected to be gzip compressed newline-delimited JSON files with a
    /// "text" field.
    #[arg(index = 1)]
    inputs: Vec<PathBuf>,

    /// Output directory. The output files will have the same name as the input files, but be placed
    /// in this directory.
    #[arg(long, short = 'o')]
    output_directory: PathBuf,
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
    hash_builders: Vec<RandomState>,
}

impl BloomFilter {
    const MAGIC: u32 = 0x81F0_F117;
    const VERSION: u32 = 1;

    fn optimal_number_of_hashers(size_in_bytes: usize, expected_elements: usize) -> usize {
        let expected_elements = expected_elements as f64;
        let size_in_bits = (size_in_bytes * 8) as f64;
        let k = (size_in_bits / expected_elements) * (2.0f64.ln());
        k.ceil() as usize
    }

    fn prob_of_false_positive(
        size_in_bytes: usize,
        expected_elements: usize,
        num_hashers: usize,
    ) -> f64 {
        let k = num_hashers as f64;
        let m = (size_in_bytes * 8) as f64;
        let n = expected_elements as f64;
        (1.0 - (1.0 - (1.0 / m)).powf(k * n)).powf(k)
    }

    fn suggest_size_in_bytes(expected_elements: usize) -> usize {
        let mut size_in_bytes = 1024 * 1024;
        while size_in_bytes < usize::MAX / 2
            && Self::prob_of_false_positive(
                size_in_bytes,
                expected_elements,
                Self::optimal_number_of_hashers(size_in_bytes, expected_elements),
            ) > 0.01
        {
            size_in_bytes *= 2;
        }
        size_in_bytes
    }

    fn my_prob_of_false_positive(&self, expected_elements: usize) -> f64 {
        Self::prob_of_false_positive(
            self.size_in_bytes(),
            expected_elements,
            self.hash_builders.len(),
        )
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
                seeds[0], seeds[1], seeds[2], seeds[3],
            ));
            hash_builder_seeds.push(seeds);
        }

        let number_of_u32 = size_in_bytes / size_of::<AtomicU32>();
        let mut bits = {
            (0..number_of_u32).map(|_| AtomicU32::default()).collect()
        };


        Self {
            bits,
            hash_builder_seeds,
            hash_builders,
        }
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
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid version",
            ));
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
                seeds[0], seeds[1], seeds[2], seeds[3],
            ));
            hash_builder_seeds.push(seeds);
        }

        let number_of_elements = stream.read_u64::<LittleEndian>()?;
        let mut bits = Vec::new();
        bits.reserve_exact(number_of_elements as usize);
        for _ in 0..number_of_elements {
            bits.push(AtomicU32::new(stream.read_u32::<NativeEndian>()?));
        }

        Ok(Self {
            bits,
            hash_builder_seeds,
            hash_builders,
        })
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
                self.bits.as_ptr().cast::<u8>(),
                self.bits.len() * size_of::<AtomicU32>(),
            );
            stream.write_all(bytes)?;
        };

        Ok(())
    }

    fn hashes(&self, s: &VecDeque<&str>) -> Vec<u64> {
        self.hash_builders
            .iter()
            .map(|hash_builder| {
                let mut hasher = hash_builder.build_hasher();
                s.hash(&mut hasher);
                hasher.finish()
            })
            .collect()
    }

    fn insert_hashes(&self, hashes: &Vec<u64>) {
        for hash in hashes {
            let hash = *hash as usize;
            let index = hash / 32 % self.bits.len();
            let bit = hash % 32;
            self.bits[index].fetch_or(1 << bit, Ordering::Relaxed);
        }
    }

    #[allow(dead_code)] // use in unit test
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

        true
    }

    #[allow(dead_code)] // use in unit test
    fn contains(&self, s: &VecDeque<&str>) -> bool {
        let hashes = self.hashes(s);
        self.contains_hashes(&hashes)
    }
}



fn compute_bloom_size(fp_rate: f64, expected_ngram_count: usize) -> usize {
    /* Uses binary search to find optimal size of bloom filter using optimal number of hashers
       and provided ngram counts
    */
    // compute 90% of system ram 
    let mut sys = System::new_all();
    sys.refresh_all();


    let mut lo = 1 as usize;
    let mut hi = ((sys.total_memory() as f64) * 0.9) as usize;

    // Save some time by checking endpoint first
    if BloomFilter::prob_of_false_positive(hi, expected_ngram_count, 
                                           BloomFilter::optimal_number_of_hashers(hi, expected_ngram_count)) > fp_rate {
        println!(
            "WARNING: To achieve desired false-positive rate, you'd need >90% of system RAM. Defaulting to 90% \
            system RAM.");
        return hi;
    }

    // Then do binary search to find optimal size
    while lo < hi-1 {
        let mid = lo + (hi - lo) / 2;
        let num_hashers = BloomFilter::optimal_number_of_hashers(mid, expected_ngram_count);
        let computed_fp = BloomFilter::prob_of_false_positive(mid, expected_ngram_count, num_hashers) ;
        if computed_fp > fp_rate {
            // FP rate too high, need to go bigger
            lo =  mid + 1;
        } else {
            // FP rate too low, can make bloom filter smaller
            hi = mid -1;
        }
    }
    hi
}


fn expand_dirs(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = vec![];
    for path in paths {
        if path.is_dir() {
            let path_str = path
                .to_str()
                .ok_or_else(|| anyhow!("invalid path '{}'", path.to_string_lossy()))?;
            for entry in glob(&format!("{}/**/*.json*.gz", path_str))? {
                files.push(entry?.to_path_buf());
            }
        } else {
            files.push(path.clone());
        }
    }

    Ok(files)
}


#[allow(clippy::too_many_arguments)] // TODO : abstract parameters into a struct
fn process_file(
    input_file: &PathBuf,
    output_file: &PathBuf,
    bloom_filter: &Arc<BloomFilter>,
    max_ngram_size: usize,
    min_ngram_size: usize,
    update_bloom_filter: bool,
    filtering_threshold: f64,
    annotate_only: bool,
    annotate_attribute_only: bool,
    whole_document: bool,
    whole_paragraphs: bool,
    pbar_option: &Option<Arc<Mutex<ProgressBar>>>,
) -> Result<(), io::Error> {
    let input_file = OpenOptions::new()
        .read(true)
        .write(false)
        .create(false)
        .open(input_file)?;
    let reader = BufReader::with_capacity(1024 * 1024, MultiGzDecoder::new(input_file));

    let output_file = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_file)?;
    let mut writer = BufWriter::with_capacity(
        1024 * 1024,
        GzEncoder::new(output_file, Compression::default()),
    );

    for line in reader.lines() {
        let line = line.unwrap();
        let mut data: Value = serde_json::from_str(&line).unwrap();
        let text = data["text"].as_str().unwrap();

        let newlines = if whole_document {
            vec![0, text.len()]
        } else {
            let mut newlines = Vec::new();
            newlines.push(0);
            for i in text.match_indices('\n') {
                newlines.push(i.0);
            }
            newlines.push(text.len());
            newlines
        };
        let mut windows_to_remove = Vec::new();
        let mut total_contained_ngrams = 0;

        for paragraph_window in newlines.windows(2) {
            let paragraph = &text[paragraph_window[0]..paragraph_window[1]];

            // calculate hashes for the paragraph
            let mut hashes: Vec<Vec<u64>> = Vec::new();
            let mut ngram: VecDeque<&str> = VecDeque::with_capacity(max_ngram_size);
            for token in tokenize(paragraph) {
                ngram.push_back(token);
                // If not hashing whole paragraphs, add ngrams to the bloom filter as they reach max size
                if !whole_paragraphs && ngram.len() >= max_ngram_size {
                    hashes.push(bloom_filter.hashes(&ngram));
                    ngram.pop_front();
                }
            }
            // If the paragraph was too short, put in a shorter ngram, so we can dedupe short
            // paragraphs exactly.
            if hashes.is_empty() && ngram.len() >= min_ngram_size {
                hashes.push(bloom_filter.hashes(&ngram));
            }

            let contained_ngrams = hashes
                .iter()
                .filter(|ngram| bloom_filter.contains_hashes(ngram))
                .count();
            total_contained_ngrams += contained_ngrams;

            // calculate how many ngrams are in the bloom filter
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

        // if annotate_attribute_only or annotate_only, add the annotation to the json
        if annotate_attribute_only || annotate_only {
            data["bff_duplicate_spans"] = serde_json::to_value(windows_to_remove).unwrap();
            data["bff_contained_ngram_count"] =
                serde_json::to_value(total_contained_ngrams).unwrap();
        } else {
            let mut output_paragraphs = String::new();
            let mut last_end = 0;
            for paragraph_window in windows_to_remove {
                output_paragraphs.push_str(&text[last_end..paragraph_window[0]]);
                last_end = paragraph_window[1];
            }
            output_paragraphs.push_str(&text[last_end..]);
            data["text"] = Value::String(output_paragraphs);
            data["bff_contained_ngram_count_before_dedupe"] =
                serde_json::to_value(total_contained_ngrams).unwrap();
        }

        if annotate_attribute_only {
            // Allowed fields
            let allowed_fields = [
                "bff_duplicate_spans",
                "bff_contained_ngram_count",
                "id",
                "source",
            ];

            // Iterate through the keys of the JSON object and remove any field that is not in the allowed_fields list
            if let Value::Object(ref mut map) = data {
                map.retain(|key, _| allowed_fields.contains(&key.as_str()));
                }
            
        }

        serde_json::to_writer(&mut writer, &data)?;
        writer.write_all(b"\n")?;
    }
    match pbar_option {
        Some(pbar) => pbar.lock().unwrap().inc(1),
        None => (),
    }
    Ok(())
}

fn main() {
    let args = Args::parse();
    let inputs = expand_dirs(&args.inputs).unwrap();
    println!("Parsed {:?} input files...", inputs.len());
    let threads = if args.threads == 0 {
        available_parallelism().unwrap().get()
    } else {
        args.threads
    };
    let now = Instant::now();
    let mut bloom_filter_size = args.bloom_filter_size;
    let bloom_filter = if args.bloom_filter_file.exists() {
        println!("Loading bloom filter from {:?}...", args.bloom_filter_file);
        BloomFilter::from_file(&args.bloom_filter_file).unwrap()
    } else {
        println!("Creating new bloom filter...");
        if args.bloom_filter_size == 0 {
            bloom_filter_size = compute_bloom_size(args.fp_rate, args.expected_ngram_count);
        }
        let num_hashers = BloomFilter::optimal_number_of_hashers(
            bloom_filter_size,
            args.expected_ngram_count,
        );
        BloomFilter::new(bloom_filter_size, num_hashers)
    };
    let bloom_filter = Arc::new(bloom_filter);
    println!(
        "\t...Bloom filter loaded. ({} hashers) ({} seconds)",
        bloom_filter.hash_builders.len(), now.elapsed().as_secs()
    );

    let p = bloom_filter.my_prob_of_false_positive(args.expected_ngram_count);
    if p >= 0.5 {
        println!(
            "WARNING: Probability of a false positive after {} elements is {}.",
            args.expected_ngram_count, p
        );
    } else {
        println!(
            "Probability of a false positive after {} elements: {}",
            args.expected_ngram_count, p
        );
    }

    let suggested_size = BloomFilter::suggest_size_in_bytes(args.expected_ngram_count);
    println!("Suggested size is {} | Actual size is {} ", 
             human_bytes(suggested_size as f64), human_bytes(bloom_filter.size_in_bytes() as f64));
    if suggested_size * 2 < bloom_filter.size_in_bytes() {
        println!(
            "WARNING: Your bloom filter is more than twice as large as suggested for {} elements. \
            This is good for accuracy, but it is much slower, and likely not worth the trade-off.",
            args.expected_ngram_count
        );
    }
    // Build Progress bar (do some hacky arc/mutex wrapping)
    let num_files = inputs.len() as u64;
    let pbar = ProgressBar::new(num_files)
        .with_style(
            ProgressStyle::with_template(
                "Files {human_pos}/{human_len} [{elapsed_precise}/{duration_precise}] [{wide_bar:.cyan/blue}]",
            ).unwrap()
        );

    let pbar = Arc::new(Mutex::new(pbar));


    if !args.no_progress {
        pbar.lock().unwrap().inc(0); // initializes pbar
    }



    let now = Instant::now();
    let threadpool = ThreadPool::new(threads);
    for input in inputs {
        let mut output = args.output_directory.clone();
        output.push(input.file_name().unwrap());
        let bloom_filter = bloom_filter.clone();


        let pbar_option: Option<Arc<Mutex<ProgressBar>>> = if args.no_progress {
            None
        } else {
            Some(pbar.clone())
        };

        threadpool.execute(move || {
            if args.no_progress {
                println!("Procaessing {input:?}...");
            }
            process_file(
                &input,
                &output,
                &bloom_filter,
                args.max_ngram_size,
                args.min_ngram_size,
                !args.no_update_bloom_filter,
                args.filtering_threshold,
                args.annotate_only,
                args.annotate_attribute_only,
                args.whole_document,
                args.whole_paragraphs,
                &pbar_option,
            )
            .unwrap();
        });
    }
    threadpool.join();
    println!("Completed deduplication in {} seconds", now.elapsed().as_secs());
    if (!args.no_update_bloom_filter) && (!args.no_save_bloom_filter) {
        println!("Writing bloom filter to {:?}...", args.bloom_filter_file);
        bloom_filter.write_to_file(&args.bloom_filter_file).unwrap();
        println!("Bloom filter written.");
    }
}
