"""
a script to filter data from shards based on aligned attribute files
"""
import argparse
import multiprocessing as mp
import os
import json
from tqdm import tqdm
import smart_open
from collections import defaultdict

def read_jsonl_smart_open_file(in_file: str):
    smart_open
    with smart_open.open(in_file, "rt", encoding="UTF8") as f:
        for line in f:
            yield json.loads(line)

def process_shard(shard, attribute_shard, args):
    # read shard
    data = read_jsonl_smart_open_file(shard)
    attributes = read_jsonl_smart_open_file(attribute_shard)

    duplicates_removed = 0
    duplicates_per_source = defaultdict(int)
    
    with smart_open. open(os.path.join(args.outpath, os.path.basename(shard)), 'wt') as fout:
        for doc, doc_attributes in zip(data, attributes):
            assert doc['id'] == doc_attributes['id'], "Shards must be in the same order"
            if args.exact_match and not doc_attributes['contaminated']:
                fout.write(json.dumps(doc) + '\n')
            if args.bff_ngram_any and doc_attributes['bff_contained_ngram_count'] > 0:
                raise NotImplementedError("Not implemented yet")
            if args.bff_docs_affected and len(doc_attributes['bff_duplicate_spans']) == 0:
                print(doc)
                print(doc_attributes['bff_duplicate_spans'])
                fout.write(json.dumps(doc) + '\n')
                exit()

    return duplicates_removed, duplicates_per_source


def main(args):
    # Create a pool of processes
    with mp.Pool(processes=args.num_processes) as pool:
        # Process each shard
        results = [pool.apply_async(process_shard, args=(shard, attribute_shard, args)) for shard, attribute_shard in zip(args.shards, args.attribute_shards)]
        removed_counts = [result.get() for result in tqdm(results, desc="Processing shards")]

        # Wait for all the processes to finish
        pool.close()
        pool.join()

    total_removed_by_source = defaultdict(int)
    total_removed = 0
    for shard, (removed_count, removed_by_source) in zip(args.shards, removed_counts):
        print(f"{removed_count} documents matched in {shard}")
        total_removed += removed_count
        for source, count in removed_by_source.items():
            total_removed_by_source[source] += count
    for source, count in total_removed_by_source.items():
        print(f"{count} documents were matched from {source}")
    print(f"A total of {total_removed} documents were matched")

if __name__ == "__main__":
    parse = argparse.ArgumentParser("")

    parse.add_argument("--shards", nargs='+', type=str)
    parse.add_argument("--attribute_shards", nargs='+', type=str)
    parse.add_argument("--exact_match", action='store_true')
    parse.add_argument("--bff_ngram_any", action='store_true')
    parse.add_argument("--bff_docs_affected", action='store_true')
    parse.add_argument("--num_processes", type=int, default=mp.cpu_count())
    parse.add_argument("--outpath", type=str)


    args = parse.parse_args()

    # fix the shard ordering
    base2path_shard = {}
    base2path_attribute_shard = {}
    for shard, attribute_shard in zip(args.shards, args.attribute_shards):
        base2path_shard[os.path.basename(shard)] = shard
        base2path_attribute_shard[os.path.basename(attribute_shard)] = attribute_shard
    shard_order = list(sorted(base2path_shard.keys()))
    args.shards = [base2path_shard[shard] for shard in shard_order]
    args.attribute_shards = [base2path_attribute_shard[shard] for shard in shard_order]

    if args.bff_ngram_any or args.bff_docs_affected:
        assert args.bff_ngram_any != args.bff_docs_affected, "Can only specify one of --bff_ngram_any or --bff_docs_affected"

    if args.num_processes > len(args.shards):
        args.num_processes = len(args.shards)

    main(args)
