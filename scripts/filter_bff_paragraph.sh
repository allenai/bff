infile=$1
outfile=$2
zcat $infile | grep -E '"bff_duplicate_spans":\[\]' | gzip > $outfile