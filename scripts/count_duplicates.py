"""
A script for counting the annotations made by BFF at a source level
"""
import argparse
import multiprocessing as mp
import json
from tqdm import tqdm
import smart_open
from collections import defaultdict

def read_jsonl_smart_open_file(in_file: str):
    smart_open
    with smart_open.open(in_file, "rt", encoding="UTF8") as f:
        for line in f:
            yield json.loads(line)

def process_shard(shard, args):
    # read shard
    data = read_jsonl_smart_open_file(shard)

    any_ngram_per_source = defaultdict(int)
    docs_with_removal_per_source = defaultdict(int)
    tokens_removed_per_source = defaultdict(int)
    total_docs_per_source = defaultdict(int)
    
    for doc in data:
        source = doc['source'] if 'source' in doc else 'no_source'
        total_docs_per_source[source] += 1
        if doc['bff_contained_ngram_count'] > 0:
            any_ngram_per_source[source] += 1
        if len(doc['bff_duplicate_spans']) > 0:
            docs_with_removal_per_source[source] += 1
            tokens_removed_per_source[source] += sum(pair[1] - pair[0] for pair in doc['bff_duplicate_spans'])

    return any_ngram_per_source, docs_with_removal_per_source, tokens_removed_per_source, total_docs_per_source


def main(args):
    # Create a pool of processes
    with mp.Pool(processes=args.num_processes) as pool:
        # Process each shard
        results = [pool.apply_async(process_shard, args=(shard, args)) for shard in args.shards]
        counts = [result.get() for result in tqdm(results, desc="Processing shards")]

        # Wait for all the processes to finish
        pool.close()
        pool.join()

    any_ngram_per_source = defaultdict(int)
    docs_with_removal_per_source = defaultdict(int)
    tokens_removed_per_source = defaultdict(int)
    total_docs_per_source = defaultdict(int)
    for shard_counts in counts:
        for source, count in shard_counts[0].items():
            any_ngram_per_source[source] += count
        for source, count in shard_counts[1].items():
            docs_with_removal_per_source[source] += count
        for source, count in shard_counts[2].items():
            tokens_removed_per_source[source] += count
        for source, count in shard_counts[3].items():
            total_docs_per_source[source] += count

    output = {
        'any_ngram_per_source': any_ngram_per_source,
        'docs_with_removal_per_source': docs_with_removal_per_source,
        'tokens_removed_per_source': tokens_removed_per_source,
        'total_docs_per_source': total_docs_per_source
    }

    if args.outfile is not None:
        with smart_open.open(args.outfile, 'w') as f:
            json.dump(output, f)

    print(output)



if __name__ == "__main__":
    parse = argparse.ArgumentParser("")

    parse.add_argument("--shards", nargs='+', type=str)
    parse.add_argument("--num_processes", type=int, default=mp.cpu_count())
    parse.add_argument("--outfile", type=str)


    args = parse.parse_args()

    if args.num_processes > len(args.shards):
        args.num_processes = len(args.shards)

    main(args)
