downstream_dataset=$1
output_dir=$2
training_shards=${@:3}

parallel --eta --bar "zcat {} | grep -f $downstream_dataset -on | cut -d':' -f1 | uniq -c > $output_dir/{/.}.counts" ::: $training_shards