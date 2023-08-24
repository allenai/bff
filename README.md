BFF
===

The big friendly filter 😁

Getting started
---------------

1. Install Rust on your machine.
    1. `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
    2. Add `~/.cargo/bin` to your `PATH` environment variable.
2. Run `cargo build --release`. It places the binary at `target/release/bff`.
3. Run `./target/release/bff --help` to see the available options.

Examples
--------

### Deduplicating a file against itself

This is how you deduplicate a file against itself:
```bash
target/release/bff \
  --bloom-filter-file filter.bff \
  --bloom-filter-size 274877906944 \
  --expected-ngram-count 1000000000 \
  --output-directory deduped/ \
  input.json.gz
```

This creates the filter at `filter.bff`, with a size of 256 GB.
This size should be a little smaller than the amount of main memory you have.
It calculates the optimal setup for the filter based on the expected number of ngrams.
Getting that number right is very important.
If in doubt, guess high.
It's safer to guess a higher number than a lower number.
The filter will be created in memory, and only written to disk at the end of the job.

### Deduplicating multiple files

To get a lot of speed out of `bff`, you have to process multiple files at once:
```bash
target/release/bff \
  --bloom-filter-file filter.bff \
  --bloom-filter-size 274877906944 \
  --expected-ngram-count 1000000000 \
  --output-directory deduped/ \
  *.json.gz
```

Each input file will run in its own thread, and the filter will be shared between them.
In the end, as before the filter will be written to disk.

### Pre-load the filter

You can stick ngrams into the filter ahead of time, for example if you want to decontaminate your dataset:
```bash
target/release/bff \
  --bloom-filter-file decontaminating_filter.bff \
  --bloom-filter-size 274877906944 \
  --expected-ngram-count 1000000000 \
  --output-directory deduped/ \
  --filtering-threshold 1.0 \
  my_test_set.json.gz
```

This will copy the output unchanged to the `deduped/` directory, but it will also produce a filter that you can use afterwards.
It is important that you still take a good guess at the ngram count you expect to see when you do the actual
deduplication.
The parameters of the bloom filter are baked in when you first create the file, so you have to guess right the
first time.

### Only decontaminate

If you only want to decontaminate, but not deduplicate against itself, you can do that by using the filter
you just created in the previous step:
```bash
target/release/bff \
  --bloom-filter-file decontaminating_filter.bff \
  --bloom-filter-size 274877906944 \
  --expected-ngram-count 1000000000 \
  --output-directory deduped/ \
  --update-bloom-filter false \
  *.json.gz
```

If you are using the filter this way, you can use the number of ngrams in the decontamination set for the
`--expected-ngram-count` parameter.
Since this is usually much smaller, it might make the filter run faster.
