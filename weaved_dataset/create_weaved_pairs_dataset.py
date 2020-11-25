import sys
import os
from argparse import ArgumentParser
import csv
from tqdm import tqdm

if __name__ == "__main__":
    parser = ArgumentParser("Parse multiple wikipedia processed dumps into a single dataset")

    # Global level parameters
    parser.add_argument('-i', '--input_file', type=str)
    parser.add_argument('-o', '--output_file', type=str)
    args = parser.parse_args()

    i = 0
    with open(args.input_file, 'r') as fin, open(args.output_file, 'w') as fout:
        #writer = csv.writer(fout, delimiter="\t", quoting=csv.QUOTE_NONE)

        pair = []
        i = 0
        for line in tqdm(fin):
                       
            if len(line.strip()) > 0:

                id, title, line = line.split('\t')

                if len(pair) == 0:
                    pair.append((id, title, line))
                elif len(pair) == 1:
                    pair.append((id, title, line))

                    row = [str(i), pair[0][0], pair[1][0], pair[0][1], pair[1][1], pair[0][2], pair[1][2]]
                    fout.write('\t'.join(row) + "\n") 
                    i+=1
                    pair = []

                else:
                    raise Exception

    print('Done.')
