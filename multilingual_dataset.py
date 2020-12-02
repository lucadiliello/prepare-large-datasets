import os
from argparse import ArgumentParser
import csv
import json
import logging
import transformers
from tqdm import tqdm
from multiprocessing import cpu_count, Pool, Queue


FORMAT_LOGGING = '%(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT_LOGGING)
logging.getLogger().setLevel(logging.INFO)




def main(args):
    
    logging.info(f"Checking I/O files")
    if os.path.isfile(args.output_file):
        assert args.force_overwrite, f"Cannot overwrite {args.output_file}, add -f option if you know what you are doing."
        os.remove(args.output_file)
    assert os.path.isfile(args.input_file), f"Input file {args.input} does not exist"



    tokenizer = transformers.AutoTokenizer.from_pretrained(tok_name)

    def parse_line(line):
        line = line.strip()
        if not line.endswith("."):
            line += "."
        return line, len(tokenizer.encode(line))


    logging.info(f"Creating dataset from {len(args.input_files)} input file(s)")

    with open(args.output_file, "w") as out_file:
        writer = csv.writer(out_file, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for _file in tqdm(args.input_files, desc="Processed files", position=0):
            
            
            # remember the actual number of written lines
            written_lines = 0

            def write(l, written_lines):
                row = [written_lines, _id, l] if _id is not None else [written_lines, l]
                writer.writerow(row)
                return written_lines + 1

            # used to accumulate sequences
            accumulator = None
            accumulator_len = 0

            for line, line_len in tqdm(batch_read_with_tokenization_parallel(_file, args.fill_for_tokenizer, args.processes), desc="Processing lines", position=1):

                if args.limit and written_lines >= args.limit:
                    break

                line = line.strip()

                # without tokenizer write line by line
                if args.fill_for_tokenizer is None:
                    if len(line.split()) >= args.min_word_per_sentence:
                        written_lines = write(line, written_lines)
                
                else:
                    # length of actual line in tokens

                    # if we are under the max len
                    if accumulator is None:
                        accumulator = line
                        accumulator_len = line_len

                    # if adding the new sequence is still under the max len
                    elif (
                        (accumulator_len + line_len <= args.target_len) and
                        (not args.separate_documents or len(line) > 0) # empty lines are used to separate documents
                    ):
                        accumulator = accumulator + " " + line if accumulator else line
                        accumulator_len += line_len
    
                    # if we went over, write and init accu with actual line
                    else:
                        written_lines = write(accumulator, written_lines)
                        accumulator = line
                        accumulator_len = line_len

            # if last accumulator was not written because for cycle ended before, write it now
            if (not args.limit or written_lines >= args.limit) and accumulator:
                written_lines = write(accumulator, written_lines)

            print(f"Written {written_lines} lines per file {_file} with id {_id}")

    print("Done")



if __name__ == "__main__":

    parser = ArgumentParser("Parse multiple wikipedia processed dumps into a single dataset")

    # Global level parameters
    parser.add_argument('-i', '--input_file', type=str, required=True,
                        help="List of input wikipedia processed dumps with one sentence per line")
    parser.add_argument('-l', '--limit', type=int, required=False, default=None,
                        help='Limit of sentences to be taken from each file')
    parser.add_argument('-m', '--min_word_per_sentence', type=int, required=False, default=3,
                        help='Minimun number of words in a sentence to be considered. Works only if `fill_for_tokenizer` is None')
    parser.add_argument('-o', '--output_file', type=str, required=True,
                        help='Specify an output file')
    parser.add_argument('--lang_file', type=str, required=False, default=None,
                        help="Specify an input language file with pairs of languages and ancronyms")
    parser.add_argument('-f', '--force_overwrite', action="store_true",
                        help='Overwrite output file if it does already exist')
    parser.add_argument('--fill_for_tokenizer', type=str, default=None, required=False,
                        help="Path of some pre-trained tokenizer")
    parser.add_argument('--separate_documents', action="store_true",
                        help="Path of some pre-trained tokenizer")
    parser.add_argument('--processes', type=int, default=cpu_count(), required=False,
                        help="Number or parallel processes to use")
    parser.add_argument('--target_len', type=int, default=128, required=False)

    # get NameSpace of paramters
    args = parser.parse_args()

    main(args)
