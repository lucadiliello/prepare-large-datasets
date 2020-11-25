#!/bin/bash

python create_paragraph_dataset.py -i $1 -o paragraphs_dataset.tsv
python create_weaved_pairs_dataset.py -i paragraphs_dataset.tsv -o $2.tsv
rm paragraphs_dataset.tsv