# Wikipedia pre-processing


## Install dependencies

Install our custom wikiextractor version
```bash
pip install git+https://github.com/lucadiliello/wikiextractor.git --upgrade
```

## Download the data

Download a specific wikipedia dump (to `data/` folder):
```
./download_dump.sh it
```

Download dumps for all languages used by multilingual BERT:
```bash
for i in $(cat required_languages.tsv| cut -f2); do ./download_dump.sh $i; done
```

## Extract and clean the data

This will create another file with the txt extension containing 1 file per line:
```bash
python -m wikiextractor.main data/enwiki-latest-pages-articles.xml.bz2 -o - > enwiki-latest-pages-articles.txt
```
