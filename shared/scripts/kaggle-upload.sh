#!/usr/bin/env bash

# move files into new assets dir
mkdir assets
cd assets
ln -s ../input/* .
ln -s ../hand/* .
cd ..

# check if dataset exists
if kaggle d list --mine | grep "$(cat hand/dataset-metadata.json | jq -r ".title")" ; then
    # if yes then version
    kaggle d version -p assets --dir-mode skip -m "$(date)" ; 
else 
    # if no then create
    kaggle d create -p assets --dir-mode skip
fi

# remove temp dir
rm -rf assets