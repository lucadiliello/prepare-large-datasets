# Weaved-dataset 

The final weaved-dataset is composed by a ```.tsv``` of 7 columns that are:

- ```id```: the univoque id of the example
- ```id_seq_a```: the if of the document from which the first sequence was extracted
- ```id_seq_b```: the if of the document from which the second sequence was extracted
- ```title_seq_a```: the title of the document of the first sequence
- ```title_seq_b```: the title of the document of the second sequence
- ```seq_a```: the first sequence
- ```seq_b```: the second sequence

## Usage:
```
bash create_weaved_dataset.sh <extracted wikipedia dump main folder> <outputname> <max seq len of each sequence (#words)>
```