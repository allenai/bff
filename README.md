BFF
===

The big friendly filter 😁

Getting started
---------------

1. Install Rust on your machine. This is a total nightmare and yet rust people are super proud of it.
   2. `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
   3. Add `~/.cargo/bin` to your `PATH` environment variable.
4. In the `bff/` directory, run `rustup override set nightly`.
2. Run `cargo build --release`. It places the binary at `target/release/bff`.
3. Run `./target/release/bff --help` to see the available options.

Examples
--------

This is how you deduplicate a file against itself:
```bash
target/release/bff \
  --bloom-filter-file filter.bff \
  --bloom-filter-size 274877906944 \
  --expected-ngram-count 1000000000 \
  input.json.gz | gzip > output.json.gz
```

This creates the filter at `filter.bff`, with a size of 256 GB.
This size should be a little bit smaller than the amount of main memory you have, or the filter will run very slowly.
It calculates the optimal setup for the filter based on the expected number of ngrams.
It does not have to be exact, but it should be a good estimate.

This is how you deduplicate many files at the same time:
```bash
parallel --eta --delay 1s 'target/release/bff \
  --bloom-filter-file filter.bff \
  --bloom-filter-size 274877906944 \
  --expected-ngram-count 1000000000 \
  {} | gzip > deduped/{/}' ::: *.json.gz
```