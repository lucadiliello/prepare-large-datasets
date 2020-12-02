# prepare-large-datasets
Process wikipedia or other large corpuses and create a dataset for NLP applications. Data preparation is available, at the moment, only or wikipedia and 

## Data preparation

Data will be generated containing a paragraph per line and empty lines separating different documents.

### Wikipedia
Preprocessing of wikipedia dumps is described [here](wikipedia/)

### OpenWebText Corpus
Preprocessing of owt archives is described [here](openwebtext/)


## Pre-process the data

Preprocess data and save in a file with the same name and `_preprocessed` suffix. Be sure to install the `blingfire` tokenizer:

```bash
pip install blingfire
python preprocess.py data/enwiki-latest-pages-articles.txt
```


## Create the dataset

### Monolingual

You can pass the name of a tokenizer from the `huggingface` library to create lines that are filled with `<target_len>` tokens you require. For example, with `--target_len 128` it will fill up to 128 tokens per line based on the specified tokenizer. This will occupay all the available CPUs on your machine and may take some time if tokenization is enabled (by passing `--fill_for_tokenizer <pre-trained-tok-name>`)

```bash
pip install transformes
python create_dataset.py -i data/enwiki-latest-pages-articles_preprocessed.txt -o data/enwiki-latest-pages-articles_preprocessed_dense_bert_128.tsv --fill_for_tokenizer bert-base-cased -f --target_len 128 --separate_documents
```

### Multilingual

You can create a multilingual dataset by passing the lang_file and multiple input files.

```bash
python create_dataset.py -i data/*_preprocessed.txt -o data/multilingual_dataset.tsv --fill_for_tokenizer bert-base-multilingual-cased -f --target_len 128 --lang_file lang_dict.json
```

Each line will contain an additional id of the language. `ids` are store in `lang_dict.json`.


## Test
Test that created dataset has an average length similar to the one defined through `--target_len`
```bash
python test_dataset.py -i <create_dataset.tsv> --tokenizer <tokenizer-name> --limit 10000 --column 1
```
If `--tokenizer` is omitted, words average will be counted. `--limit` limits the number of rows to read. The first thousands are usually more than enough for a good statistic. `--column` specifies the column in the `tsv` file in which sentences are stored, default `1`.


# Credits

Most of the `sh` scripts has been taken from [Steven van de Graaf](https://towardsdatascience.com/pre-processing-a-wikipedia-dump-for-nlp-model-training-a-write-up-3b9176fdf67) article
