#!/bin/bash
declare -A datasets
num_columns=4

num_rows=1
datasets[1,1]="/mnt/hddhelp/botao/dataset/Seismicdataset.bin" #Name of the dataset
datasets[1,2]="5242880"				      #Size of the dataset
datasets[1,3]="/mnt/hddhelp/themis-lefteris/Seismic/queries_ctrl100_seismic_len256_znorm.bin"  #Name of the queries file
datasets[1,4]="100"					      #Size of the queries size


for ((i=1;i<=num_rows;i++)) 
do
	filename="results/results_[${datasets[$i,2]}].txt"

	echo "Dataset [${datasets[$i,1]}] with size [${datasets[$i,2]}]"
	echo "Dataset [${datasets[$i,2]}]" >> $filename


	

	for num_threads in 24
	do
		echo "Threads [$num_threads]"
		echo "Threads [$num_threads]" >> $filename

		for read_block_length in 20000 
		do

			echo "Read Block Length [$read_block_length]"
			echo "Read Block Length [$read_block_length]" >> $filename

			for ts_group_length in 64 
			do
				echo "ts-group-length [$ts_group_length]"
				echo "ts-group-length [$ts_group_length]" >> $filename

				for version in 999777013  			
				do
					echo "Running version [$version]"
					echo "Running version [$version]" >> $filename

					for iteration in 1 2 #3 4 5 6 7 8 9 10
					do
		        		LD_PRELOAD=`jemalloc-config --libdir`/libjemalloc.so.`jemalloc-config --revision` ./bin/ads --dataset ${datasets[$i,1]} --leaf-size 2000 --initial-lbl-size 2000 --min-leaf-size 2000 --dataset-size ${datasets[$i,2]} --flush-limit 1000000 --cpu-type $num_threads --function-type $version --in-memory --ts-group-length $ts_group_length --queries ${datasets[$i,3]} --queries-size ${datasets[$i,4]} --read-block $read_block_length --backoff-power-summarization 0 --backoff-power-tree-construction 0 --backoff-power-tree-pruning 0 
				   	 done
				 done	
			done
		done
	done
done


iterations=1
