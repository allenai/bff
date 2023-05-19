# sum the bff_contained_ngram_count_before_dedupe field over all the json lines in the shards
zcat $@  | jq -r '.bff_contained_ngram_count_before_dedupe' | awk '{sum += $1} END {print sum}'