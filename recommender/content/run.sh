#!/bin/bash


# root folder
path="categories"
output_path = "output"
mkdir -p ${output_path};


#get files_paths
declare -a paths=(${path}/*)

# run description_based.py 4 every file
for ((i = 0; i < ${#paths[@]}; i++))
do
	#get filename
	IFS='/' read -ra path_split_array <<< "${paths[$i]}"
	filename=${path_split_array[-1]}
	
	python description_based.py "${paths[$i]}"  "${output_path}/${filename}" 40
done
