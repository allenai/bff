# a script to count the number of newlines in many files of .jsonl.gz data

# for each file count the newlines then sum them
parallel --eta --bar "zcat {} |jq -r .text | wc -l" ::: $@ | awk '{s+=$1} END {print s}'