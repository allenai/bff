infile=$1
outfile=$2
zcat $infile | grep -E '"bff_contained_ngram_count_before_dedupe":0' | gzip > $outfile