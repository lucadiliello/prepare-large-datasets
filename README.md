# prepare-large-datasets
Process wikipedia or other large corpuses and create a dataset for NLP applications. Data preparation is available, at the moment, only or wikipedia and openwebtext.

## Data preparation

Data will be generated containing a paragraph per line and empty lines separating different documents. Data cleaning is performed in the next section.

### Wikipedia
Downloading and preparation of wikipedia dumps is described [here](wikipedia/)

### OpenWebText Corpus
Preparation of owt archives is described [here](openwebtext/)


## Pre-process the data

This script preprocesses and cleans data and saves them in a file with the same name plus the `_preprocessed` suffix. Be sure to install the `blingfire` tokenizer:

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

Example:
```txt
Game Boy Advance. The (GBA) is a 32-bit handheld game console developed, manufactured and marketed by Nintendo as the successor to the Game Boy Color. It was released in Japan on March 21, 2001, in North America on June 11, 2001, in Australia and Europe on June 22, 2001, and in mainland China on June 8, 2004 as iQue Game Boy Advance. The GBA is part of the sixth generation of video game consoles. The original model does not have an illuminated screen; Nintendo addressed that with the release of a redesigned model with a frontlit screen, the Game Boy Advance SP, in 2003. A newer revision of the redesign was released in 2005, with a backlit screen. The final redesign, the Game Boy Micro, was released in 2005.
As of June 30, 2010, 81.51 million units of the Game Boy Advance series have been sold worldwide. Its successor, the Nintendo DS, was released in November 2004 and is backward compatible with Game Boy Advance software.

Google Search. Google Search, or simply Google, is a web search engine developed by Google LLC. It is the most used search engine on the World Wide Web across all platforms, with 92.62% market share as of June 2019, handling more than 5.4 billion searches each day.
The order of search results returned by Google is based, in part, on a priority rank system called "PageRank". Google Search also provides many different options for customized search, using symbols to include, exclude, specify or require certain search behavior, and offers specialized interactive experiences, such as flight status and package tracking, weather forecasts, currency, unit, and time conversions, word definitions, and more.

...
```


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

You can create a multilingual dataset by passing the lang_file and multiple input files to `multilingual_dataset.py`. Each file must have been created with `create_dataset.py`, `multilingual_dataset.py` will only do the collage.

Example with many (multilingual) wikipedia:
```bash
python multilingual_dataset.py -i data/wikipedia/*-dataset.tsv -o data/wikipedia/multilingual-dataset.tsv --lang_file wikipedia/lang_maps/lang_dict.json
```

Each line will contain an additional id of the language. `ids` are retrieved from `lang_dict.json`.

Additional parameters:
- `-l` or `--limit`: Limit of sentences to be taken from each file
- `-f` or `--force-overwrite`: Force overwrite of output file if it does already exist


## Test
Test that created dataset has an average length similar to the one defined through `--target_len`
```bash
python test_dataset.py -i <create_dataset.tsv> --tokenizer <tokenizer-name> --limit 10000 --column 1
```
If `--tokenizer` is omitted, words average will be counted. `--limit` limits the number of rows to read (this can be usefull with very long datasets to avout re-tokenizing all the data). The first thousands are usually more than enough for a good statistic. `--column` specifies the column in the `tsv` file in which sentences are stored, default `1` (`0` is the index).


# Credits

Most of the `sh` scripts has been taken from [Steven van de Graaf](https://towardsdatascience.com/pre-processing-a-wikipedia-dump-for-nlp-model-training-a-write-up-3b9176fdf67) article
