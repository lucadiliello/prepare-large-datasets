import sys
import os
from argparse import ArgumentParser
import csv
from tqdm import tqdm

csv.field_size_limit(csv.field_size_limit() * 3)

def get_seq(args, ret, sequences, pos, current_len):

    for i in range(pos, len(sequences)):
                        
        words = sequences[i].strip().split(' ')

        if len(words) > args.max_seq_len or len(words) < args.min_seq_len:
            pos = i+1 if i+1 < len(sequences) else -1
            break

        current_len += len(words)
        if current_len > args.max_seq_len:
            pos = i
            break
        ret.append(words)
        pos = -1

    return ret, pos, current_len



if __name__ == "__main__":
    parser = ArgumentParser("Parse multiple wikipedia processed dumps into a single dataset")

    # Global level parameters
    parser.add_argument('-i', '--input_file', type=str)
    parser.add_argument('-o', '--output_file', type=str)
    parser.add_argument('-maxsl', '--max_seq_len', type=int, default=128)
    parser.add_argument('-minsl', '--min_seq_len', type=int, default=2)
    args = parser.parse_args()

    i = 0
    with open(args.input_file, 'r') as fin, open(args.output_file, 'w') as fout:
        writer = csv.writer(fout, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        reader = csv.reader(fin, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        seq_a=None
        seq_b=None
        seq_A = []
        seq_B = []
        len_A = 0
        len_B = 0
        i_a = 0
        i_b = 0

        idx = 0
        for line in tqdm(reader):

            assert len(line) == 3
            id, title, line = line

            if len(line) > 0:
                if seq_a is None:
                    seq_a = (id, title, line.split('.'))
                elif seq_b is None:
                    seq_b = (id, title, line.split('.'))

            if seq_a and seq_b:

                id_A, title_A, phrases_A = seq_a
                id_B, title_B, phrases_B = seq_b

                while seq_a is not None and seq_b is not None:                
                
                    seq_A, i_a, len_A = get_seq(args, seq_A, phrases_A, i_a, len_A)
                    seq_B, i_b, len_B = get_seq(args, seq_B, phrases_B, i_b, len_B)

                    """
                    for i in range(i_a, len(phrases_A)):
                        
                        words = phrases_A[i].split(' ')

                        if len(words) > args.max_seq_len or len(words) < args.min_seq_len:
                            i_a = i+1 if i+1 < len(phrases_A) else -1
                            break

                        len_A += len(words)
                        if len_A > args.max_seq_len:
                            i_a = i
                            break
                        seq_A.append(words)
                        i_a = -1

                    for i in range(i_b, len(phrases_B)):

                        if len(phrases_B[i]) > args.max_seq_len:
                            i_b = i+1 if i+1 < len(phrases_B) else -1
                            break

                        len_B += len(phrases_B[i])
                        if len_B > args.max_seq_len:
                            i_b = i
                            break
                        seq_B.append(phrases_B[i])
                        i_b = -1
                    """

                    if len(seq_A) > 0 and len(seq_B) > 0:

                        A = '.'.join([" ".join(l).strip() for l in seq_A]).strip()
                        B =  '.'.join([" ".join(l).strip() for l in seq_B]).strip()

                        if len(A) <= 1:
                            print(":::", len_A, "-->",seq_A, "-->", A,"<<<")
                        if len(B) <= 1:
                            print(":::", len_B,"-->",seq_B, "-->", B,"<<<")

                        if len(A) > 1 and len(B) > 1:
                            row = [idx, int(id_A), int(id_B), title_A, title_B, A, B]
                            assert len(row) == 7
                            assert "  " not in row[5] and "  " not in row[6]

                        writer.writerow(row)
                        seq_A = []
                        seq_B = []
                        len_A = 0
                        len_B = 0
                        idx+=1

                    if i_a < 0:
                        seq_a = None
                        i_a = 0
                    
                    if i_b < 0:
                        seq_b = None
                        i_b = 0

    print('Done.')
