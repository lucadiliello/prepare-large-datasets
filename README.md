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
python preprocess.py -i data/enwiki-latest-pages-articles.txt
```

Generate pre-processed data files are of the form:
```txt
paragraph-0 # doc 0
paragraph-1 # doc 0

paragraph-0 # doc 1
paragraph-1 # doc 1
paragraph-2 # doc 1

...
```

that is, documents are separated by empty lines and paragraphs within documents stay on different lines.


## Create the dataset

### Monolingual

You can pass the name of a tokenizer from the `huggingface` library to create lines that are filled with `<target_len>` tokens you require. For example, with `--target_len 128` it will fill up to 128 tokens per line based on the specified tokenizer. This will occupay all the available CPUs on your machine and may take some time if tokenization is enabled (by passing `--fill_for_tokenizer <pre-trained-tok-name>`)

```bash
pip install transformes
python create_dataset.py -i data/enwiki-latest-pages-articles_preprocessed.txt -o data/enwiki-latest-pages-articles_preprocessed_dense_bert_128.tsv --fill_for_tokenizer bert-base-cased --target_len 128 --separate_documents
```

Other available arguments for `create_dataset.py` are:
- `-l` or `--limit`: Limit number of rows in output file
- `-m` or `--min_word_per_sentence`: Minimun number of words in a sentence to be considered (works only when tokenization is disabled')
- `--fill_for_tokenizer`: Path of some pre-trained tokenizer to be used for splitting and rows filling
- `--separate_documents`: Do not fill rows with sentence coming from different documents. We suggest to use it
- `--processes`: Number or parallel processes to use for tokenization
- `--target_len`: Target length (in tokens) you would like to have on each row
- `--batch_tokenization`: How many sentence should be tokenizer in one tokenizer call
- `--no_split_long_paragraphs`: Do not split long paragraphs on multiple lines

### Multilingual

<font style="color: red">To be updated</font>

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
If `--tokenizer` is omitted, words average will be counted. `--limit` limits the number of rows to read (this can be usefull with very long datasets to avout re-tokenizing all the data). The first thousands are usually more than enough for a good statistic. `--column` specifies the column in the `tsv` file in which sentences are stored, default `1` (`0` is the index).


# Credits

Most of the `sh` scripts has been taken from [Steven van de Graaf](https://towardsdatascience.com/pre-processing-a-wikipedia-dump-for-nlp-model-training-a-write-up-3b9176fdf67) article
