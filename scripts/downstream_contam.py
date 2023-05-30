"""
A tool to that finds training documents that contain a significant portion of a whole evaluation dataset.
Specifically if n% of lines in all the instances in the evaluation set occur in a single document it is marked.
"""

import argparse
import multiprocessing as mp
import os
import smart_open
from tqdm import tqdm
import json

def normalize(text):
    text = text.strip()
    # normalize whitespace
    text = " ".join(text.split())
    # normalize case
    text = text.lower()
    return text

def generator_from_files(files):
    for file in files:
        with smart_open.open(file, 'r', encoding="utf8") as f:
            for row in f:
                yield json.loads(row)

def get_lines_set(data):
    lines = set()
    for doc in data:
        text = doc["text"]
        for line in text.split("\n"):
            lines.add(normalize(line))
    return lines

def worker_initializer(e_lines, a):
    global eval_lines
    eval_lines = e_lines

    global args
    args = a

def process_training_file(filename):
    contaminatated_ids = []
    data = generator_from_files([filename])

    with smart_open.open(os.path.join(args.out_dir, os.path.basename(filename)), 'w') as out:
        for doc in data:
            doc_lines = get_lines_set([doc])
            if len(doc_lines.intersection(eval_lines)) / len(eval_lines) > (0.0 if args.any_contam else args.contam_threshold):
                doc['contaminated_ratio'] = len(doc_lines.intersection(eval_lines)) / len(eval_lines)
                doc['contaminated_lines'] = list(doc_lines.intersection(eval_lines))
                out.write(json.dumps(doc) + "\n")
                contaminatated_ids.append(doc["id"])
    return contaminatated_ids



def main(args):
   
    # build a set of all lines in the eval set
    eval_lines = get_lines_set(generator_from_files(args.eval_files))

    # check set against all training docs in parallel
    with mp.Pool(processes=args.num_processes, initializer=worker_initializer, initargs=(eval_lines, args)) as pool:
        contaminated_ids = list(tqdm(pool.imap(process_training_file, args.train_files), total=len(args.train_files), desc="finding contamination"))

    contaminated_ids = [item for sublist in contaminated_ids for item in sublist]
    print(f"found {len(contaminated_ids)} contaminated documents")
    print("contaminated ids:")
    if len(contaminated_ids) < 10:
        for id in contaminated_ids:
            print(id)



if __name__ == "__main__":
    parser = argparse.ArgumentParser("")
    parser.add_argument("--eval_files", type=str, nargs="+", help="evaluation data")
    parser.add_argument("--train_files", type=str, nargs="+", help="training data")
    parser.add_argument("--out_dir", type=str, help="directory to write output shards")
    parser.add_argument("--contam_threshold", type=float, default=0.8, help="fraction of lines in eval set that must be in a single training document to be marked as contaminated")
    parser.add_argument("--any_contam", action="store_true", help="if set, any contamination will be marked, not just those that are above the threshold")
    parser.add_argument("--num_processes", type=int, default=mp.cpu_count(), help="number of processes to use")
    
    args = parser.parse_args()
    main(args)