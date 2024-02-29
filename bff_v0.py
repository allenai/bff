""" Quick'n'dirty mapping of bff for python before I can make it pure-rust
How this works: (using ray)
- map to DOWNLOAD all files and store on some local location
- use this local mapping to feed to bff.rs
- map to UPLOAD all files to some output location
*Yes I know this is clunky and requires 2x dataset size in local storage

ASSUMES .jsonl.gz files, with 'text' field and FLAT file structure on s3
i.e.
    s3://bucket/input_dir/
            file_00.jsonl.gz
            file_01.jsonl.gz
            .
            .
            .
            file_N.jsonl.gz 
            # AND NO SUBDIRECTORIES!

TODOS:
- Add default tokenizer/counter to get good filter/ngram sizes automatically
- Make this pure rust
    + Only need |num_threads| * |file_size| *2 + |filter_size| memory
    + Sadly I code rust like a preschooler, so I'm working on this =(
- Add a progress bar vs printing filenames w/in rust code
"""


import argparse
import ray
import boto3
from pathlib import Path
import os
from tqdm.auto import tqdm
import glob
import subprocess


RUST_CMD = os.path.join(os.path.dirname(__file__), 'target/release/bff')

# =================================================
# =           Ray/S3 Utils                        =
# =================================================

def to_iterator(obj_ids, batch_size=100):
    while obj_ids:
        done, obj_ids = ray.wait(obj_ids, num_returns=min(batch_size, len(obj_ids)))
        for d in done:
            yield ray.get(d)


def block_until_complete(ret):
    """Use this when we want to efficiently wait until an iterable
    of ray object ids finishes
    """

    for _ in tqdm(to_iterator(ret), total=len(ret)):
        pass
    ret = ray.get(ret)


def copy_to_s3(local_obj, remote_dir, disable_tqdm=True):
    if remote_dir == None:
        return
    remote_obj = os.path.join(remote_dir, os.path.basename(local_obj)).replace("s3://", "")
    bucket, key = remote_obj.split("/", 1)
    s3 = boto3.client("s3")
    local_obj_size = os.path.getsize(local_obj)
    with tqdm(total=local_obj_size, unit="B", unit_scale=True, desc=local_obj, disable=disable_tqdm) as pbar:
        s3.upload_file(Bucket=bucket, Filename=local_obj, Key=key, Callback=lambda b: pbar.update(b))

@ray.remote
def RAY_copy_to_s3(local_obj, remote_dir, disable_tqdm=True):
    copy_to_s3(local_obj, remote_dir, disable_tqdm=disable_tqdm)


def copy_from_s3(remote_obj, local_dir, disable_tqdm=True):
    bucket, key = remote_obj.replace("s3://", "").split("/", 1)
    s3 = boto3.client("s3")
    remote_obj_size = s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    target = os.path.join(local_dir, os.path.basename(key))
    with tqdm(total=remote_obj_size, unit="B", unit_scale=True, desc=remote_obj, disable=disable_tqdm) as pbar:
        s3.download_file(Bucket=bucket, Key=key, Filename=target, Callback=lambda b: pbar.update(b))

@ray.remote
def RAY_copy_from_s3(remote_obj, local_dir, disable_tqdm=True):
    copy_from_s3(remote_obj, local_dir, disable_tqdm=disable_tqdm)


def list_s3_keys(prefix, suffix='.jsonl.gz'):
    """ Lists full s3 uri's of all keys that match both the prefix and suffix.
        if Suffix==None => no filtering on suffx
    """
    s3 = boto3.resource("s3")
    bucket_name, path_within_bucket = prefix.replace("s3://", "").split("/", 1)
    bucket = s3.Bucket(bucket_name)

    s3_uris = []
    for x in bucket.objects.filter(Prefix=path_within_bucket):
        if suffix == None or x.key.endswith(suffix):
            s3_uris.append(os.path.join('s3://%s' % bucket_name, x.key))
    return s3_uris



# ================================================
# =           Main block                         =
# ================================================


def run_bff(tmp_dir, input_dir, output_dir, filter_size, expected_ngram_count):
    # Subprocess DIY python<->rust bindings. Might be better to use maturin, but this is a bit simpler
    cmd = '%s --bloom-filter-file %s --bloom-filter-size %s --expected-ngram-count %s --output-directory %s %s' % (
        RUST_CMD,
        os.path.join(tmp_dir, 'filter.bff'),
        filter_size,
        expected_ngram_count,
        os.path.join(output_dir, ''),
        os.path.join(input_dir, '*')
        )
    return_code = subprocess.run(cmd, shell=True).returncode
    assert return_code == 0


def main(s3_input_dir, s3_output_dir, filter_size, 
         expected_ngram_count, tmp_storage_dir, suffix='.jsonl.gz'):
    # Step 0: initialize ray and args and local storage directories
    ray.init(ignore_reinit_error=True) 

    input_dir = os.path.join(tmp_storage_dir, 'input')
    output_dir = os.path.join(tmp_storage_dir, 'output')
    for p in input_dir, output_dir:
        Path(p).mkdir(parents=True, exist_ok=True)

    # step 1: collect and download s3 files to disk
    print("Collecting and downloading s3 files...")
    s3_uris = list_s3_keys(s3_input_dir, suffix=suffix)[:10]
    download_refs = [RAY_copy_from_s3.remote(uri, input_dir) for uri in s3_uris]
    block_until_complete(download_refs)

    # Step 2: Run BFF
    print("Running BFF on %s local files..." % len(s3_uris))
    run_bff(tmp_storage_dir, input_dir, output_dir, filter_size, expected_ngram_count)


    # Step 3: upload output files to S3
    print("Uploading filtered files...")
    output_files = glob.glob(os.path.join(output_dir, '*%s' % suffix if suffix != None else '*'))
    upload_refs = [RAY_copy_to_s3.remote(f, s3_output_dir) for f in output_files]
    block_until_complete(upload_refs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # S3 adapter args
    parser.add_argument('--s3-input-dir', type=str)
    parser.add_argument('--s3-output-dir', type=str)

    # Args passed to rust function
    parser.add_argument('--filter-size', type=int)
    parser.add_argument('--expected-ngram-count', type=int)
    parser.add_argument('--tmp-storage-dir', type=str, default='/tmp/bff')

    args = parser.parse_args()
    main(s3_input_dir=args.s3_input_dir,
         s3_output_dir=args.s3_output_dir,
         filter_size=args.filter_size,
         expected_ngram_count=args.expected_ngram_count,
         tmp_storage_dir=args.tmp_storage_dir)
