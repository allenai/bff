mod config;
mod bloom_filter;
mod shard;

use std::collections::VecDeque;
use flate2::Compression;
use std::fs::OpenOptions;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::env;
use std::process;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde_json;
use serde_json::Value;
use threadpool::ThreadPool;
use aws_sdk_s3::{Client as S3Client, config::Region};
use tokio::fs::{File as TokioFile};

use config::Config;
use bloom_filter::BloomFilter;
use shard::Shard;

async fn download_from_s3(
    s3_client: &S3Client,
    bucket: &str,
    key: &str,
    path: &Path,
) -> Result<(), io::Error> {
    let result = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    std::fs::create_dir_all(path.parent().unwrap())?;
    let mut file = TokioFile::create(path).await?;
    let mut body = result.body.into_async_read();
    tokio::io::copy(&mut body, &mut file).await?;

    Ok(())
}

fn process_shard(
    shard: Shard,
    bloom_filter: Arc<BloomFilter>,
    update_bloom_filter: bool,
    annotate_only: bool,
) -> Result<(), io::Error> {
    std::fs::create_dir_all(Path::new(&shard.output).parent())?;

    let inputs_cache_dir = Path::new("test-data");
    std::fs::create_dir_all(inputs_cache_dir)?;

    let tmp_output = shard.output.clone() + ".tmp";
    let output_file = OpenOptions::new().
        read(false).
        write(true).
        create(true).
        truncate(true).
        open(tmp_output.clone())?;

    let mut writer = BufWriter::with_capacity(
        1024 * 1024,
        GzEncoder::new(output_file, Compression::default()));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build().unwrap();
    let aws_config = rt.block_on(aws_config::from_env().region(Region::new("us-east-1")).load());
    let s3_client = S3Client::new(&aws_config);

    for input_path in shard.inputs {
        log::info!("Merging {} into {}", input_path, shard.output);
        let tmp_input = inputs_cache_dir.join(Path::new(&input_path));
        log::info!("Downloading {} to {}", input_path, tmp_input.display());
        rt.block_on(download_from_s3(
            &s3_client,
            "ai2-llm",
            &input_path,
            &tmp_input,
        ))?;
        let input_file = OpenOptions::new().
            read(true).
            write(false).
            create(false).
            open(tmp_input.clone())?;
        let reader = BufReader::with_capacity(
            1024 * 1024,
            GzDecoder::new(input_file));

        let mut line_number = 0;
        let mut lines_written = 0;
        for line in reader.lines() {
            line_number += 1;
            let line = line?;
            let mut data: Value = serde_json::from_str(&line)?;
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
        std::fs::remove_file(tmp_input)?;
        log::info!("Dropped {} of {} documents", line_number - lines_written, line_number);
    }

    std::fs::rename(&tmp_output, &shard.output.clone())?;

    Ok(())
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
    let config: Config = Config::read_from_file(&args[1]).unwrap();

    let bloom_filter = BloomFilter::initialize(&config.bloom_filter).unwrap();
    let bloom_filter = Arc::new(bloom_filter);

    let shards = Shard::split_streams(&config.streams).unwrap();

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
        let bloom_filter_file = PathBuf::from(&config.bloom_filter.file);
        log::info!("Writing bloom filter to {:?}...", config.bloom_filter.file);
        bloom_filter.write_to_file(&bloom_filter_file).unwrap();
        log::info!("Bloom filter written.");
    }
    log::info!("Done!");
}
