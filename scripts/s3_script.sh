#!/bin/bash 

#Mostly GPT-written bash script to do the following:
#1. Download all jsonl.gz files from s3-src to stash-dir
#2. Compute arguments to feed to bff. Defaults:
#   a. filter-size: (Main memory) * 0.90 = filter size
#   b. expected-ngram-count: use wimbd to compute
#3. Run BFF 
#4. Upload back to S3
#


expected_ngrams="-1"
# Parse the named arguments
while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
        -i|--s3-input)
            s3_input_dir="$2"
            shift
            shift
            ;;
        -t|--temp-dir)
            temp_dir="$2"
            shift
            shift
            ;;
        -o|--s3-output)
            s3_output_dir="$2"
            shift
            shift
            ;;
        -fp|--fp-rate)
            fp_rate="$2"
            shift
            shift
            ;;
        --tok|--tokens)
            exepected_ngrams="$2"
            shift
            shift 
            ;;

        *)
            echo "Unknown option: $key"
            exit 1
            ;;
    esac
done

input_files="$temp_dir/input"
output_files="$temp_dir/output"
mkdir -p $input_files
mkdir -p $output_files

# ========================================
# =           Download from S3           =
# ========================================
aws s3 cp $s3_input_dir $input_files --recursive



# =======================================================
# =           Compute stats to give to script           =
# =======================================================

# Use wimbd to get total tokens
if [ "$expected_ngrams" == "-1"]; then
    wimbd_stats=$(wimbd stats $input_files/*.jsonl.gz)
    total_tokens=$(echo "$wimbd_stats" | grep "^total tokens" | sed 's/^total tokens: //' | tr -d ',')
    total_documents=$(echo "$wimbd_stats" | grep "^total documents" | sed 's/^total documents: //' | tr -d ',')
    expected_ngrams=$(( total_tokens-total_documents ))
fi

echo "BFF ARGS"
echo "FP RATE $fp_rate"
echo "NGRAMS $expected_ngrams"


# ======================================================
# =           Actually run bff                         =
# ======================================================
rm -f filter.bff # Always rebuilds the filter froms scratch 
target/release/bff --bloom-filter-file filter.bff --bloom-filter-size $bloom_filter_size --expected-ngram-count $expected_ngrams --output-directory $output_files $input_files

# ==================================================
# =           And then upload back to S3           =
# ==================================================
aws s3 cp $output_files $s3_output_dir --recursive







