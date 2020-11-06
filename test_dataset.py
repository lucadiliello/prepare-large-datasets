import os
from argparse import ArgumentParser
import csv
import json
import numpy
import transformers
from tqdm import tqdm
from transformers.tokenization_auto import AutoTokenizer


# Test dataset average len in tokens or words

if __name__ == "__main__":
    parser = ArgumentParser("Measure average line length in tokens or words")

    # Global level parameters
    parser.add_argument('-i', '--input_file', type=str, required=True, nargs='+',
                        help="List of input wikipedia processed dumps with one sentence per line")
    parser.add_argument('--tokenizer', type=str, default=None, required=False,
                        help="Path of some pre-trained tokenizer")
    parser.add_argument('--limit', default=20000, required=False, help="Limit number of rows to be faster")

    # get NameSpace of paramters
    args = parser.parse_args()

    assert os.path.isfile(args.input_file), f"File {args.input_file} does not exist"
    assert not args.lang_file or os.path.isfile(args.lang_file), f"file {args.lang_file} does not exist"

    if args.tokenizer is not None:
        tokenizer = AutoTokenizer.from_pretrained(args.tokenizer)
    else:
        tokenizer = None

    lengths = []

    with open(args.input_file) as in_file:
        reader = csv.reader(in_file, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for line in tqdm(reader, desc="Reading file", total=args.limit):
            
            if tokenizer is None:
                lengths.append(len(line.strip().split(" ")))
            else:
                lengths.append(len(tokenizer.encode(line.strip())))

    print(f"Average {'tokens' if tokenizer is not None else 'words'} per line: {np.mean(lengths)}")







