from itertools import count
import os
from argparse import ArgumentParser
import csv
import numpy as np
from tqdm import tqdm
from transformers.tokenization_auto import AutoTokenizer


# Test dataset average len in tokens or words

if __name__ == "__main__":
    parser = ArgumentParser("Measure average line length in tokens or words")

    # Global level parameters
    parser.add_argument('-i', '--input_file', type=str, required=True,
                        help="List of input wikipedia processed dumps with one sentence per line")
    parser.add_argument('--tokenizer', type=str, default=None, required=False,
                        help="Path of some pre-trained tokenizer")
    parser.add_argument('--limit', default=20000, required=False, type=int, help="Limit number of rows to be faster")
    parser.add_argument('--column', default=1, required=False, type=int, help="Column id of the sentences")

    # get NameSpace of paramters
    args = parser.parse_args()

    assert os.path.isfile(args.input_file), f"File {args.input_file} does not exist"

    if args.tokenizer is not None:
        tokenizer = AutoTokenizer.from_pretrained(args.tokenizer)
    else:
        tokenizer = None

    lengths = []
    counter = 0

    with open(args.input_file) as in_file:
        reader = csv.reader(in_file, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for line in tqdm(reader, desc="Reading file", total=args.limit):

            line = line[args.column]
            if counter >= args.limit:
                break

            counter += 1

            if tokenizer is None:
                lengths.append(len(line.strip().split(" ")))
            else:
                lengths.append(len(tokenizer.encode(line.strip())))

    print(f"Average {'tokens' if tokenizer is not None else 'words'} per line: {np.mean(lengths)}")







