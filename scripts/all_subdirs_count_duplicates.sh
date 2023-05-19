in_dir=$1
out_dir=$2


# Get script directory
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# count the dirs in the subdirs of the in_dir
num_subsubdirs=`find $in_dir -mindepth 2 -maxdepth 2 -type d | wc -l`

# for each subsub in the in_dir, run count_duplicates.py on all shards
completed_subsubdirs=0
for subdir in `ls $in_dir`
do
    for subsubdir in `ls $in_dir/$subdir`
    do
        echo "Running count_duplicates.py on $in_dir/$subdir/$subsubdir"
        echo "Completed $completed_subsubdirs of $num_subsubdirs"
        python $SCRIPT_DIR/count_duplicates.py --shards $in_dir/$subdir/$subsubdir/* --outfile $out_dir/$subdir/$subsubdir/counts.json
        completed_subsubdirs=$((completed_subsubdirs+1))
    done
done
