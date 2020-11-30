#!/bin/bash
set -e

WIKI_DUMP_FILE_IN=$1
WIKI_DUMP_FILE_OUT=${WIKI_DUMP_FILE_IN%%.*}.txt

# extract and clean the chosen Wikipedia dump
echo "Extracting and cleaning $WIKI_DUMP_FILE_IN to $WIKI_DUMP_FILE_OUT..."
python -m wikiextractor.main $WIKI_DUMP_FILE_IN -o - \
| grep -v "^<doc id=" \
| grep -v "</doc>\$" \
> $WIKI_DUMP_FILE_OUT
echo "Succesfully extracted and cleaned $WIKI_DUMP_FILE_IN to $WIKI_DUMP_FILE_OUT"