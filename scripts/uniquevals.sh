#!/usr/bin/env bash

# This script will print out the unique values in a column of a csv file
# Usage: uniquevals.sh <csv file> <column name>

# Check for the correct number of arguments
if [ $# -ne 2 ]; then
    echo "Usage: uniquevals.sh <csv file> <column name>"
    exit 1
fi

# Check that the file exists
if [ ! -f $1 ]; then
    echo "File $1 does not exist"
    exit 1
fi

# read the file
csvdata=$(in2csv $1)

# Check that the column name is in the file
colnames=$(echo $csvdata | awk "NR == 1")
if [[ ! $colnames == *"$2"* ]]; then
    echo "Column $2 not found in $1"
    exit 1
fi

# Print the unique values
in2csv $1 | csvcut -c "$2" | awk "NR > 1" | sort -u