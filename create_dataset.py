import os
from argparse import ArgumentParser
import csv
import json
import transformers
from tqdm import tqdm
from itertools import islice
import multiprocessing as mp

NCORE = mp.cpu_count()

def get_lang_from_wikipedia_filename(filename):
    return os.path.basename(filename).strip().lower().split("-")[0].replace("wiki", "")

def batch_read_with_tokenization(file_descriptor, tokenizer=None):
    if tokenizer is None:
        for line in file_descriptor:
            yield (line, 0)
    else:
        for n_lines in iter(lambda: tuple(islice(file_descriptor, 100000)), ()):
            lengths = [len(x) for x in tokenizer(n_lines)['input_ids']]
            for a in zip(n_lines, lengths):
                yield a

def batch_read_with_tokenization_parallel(lines, tok_name):

    def gen_to_queue(input_q, lines):
        # This function simply consume our generator and write it to the input queue
        for line in lines:
            input_q.put(line)
        for _ in range(NCORE):    # Once generator is consumed, send end-signal
            input_q.put(None)

    def process(input_q, output_q, tok_name):
        tokenizer = transformers.AutoTokenizer.from_pretrained(tok_name)
        while True:
            line = input_q.get()
            if line is None:
                output_q.put(None)
                break
            else:
                line = line.strip()
            if not line.endswith("."):
                line += "."
            output_q.put((line, len(tokenizer.encode(line))))

    input_q = mp.Queue(maxsize=NCORE * 2)
    output_q = mp.Queue(maxsize=NCORE * 2)

    gen_pool = mp.Pool(1, initializer=gen_to_queue, initargs=(input_q, lines))
    pool = mp.Pool(NCORE, initializer=process, initargs=(input_q, output_q, tok_name))

    finished_workers = 0
    while True:
        line = output_q.get()
        if line is None:
            finished_workers += 1
            if finished_workers == NCORE:
                break
        else:
            yield line


if __name__ == "__main__":
    parser = ArgumentParser("Parse multiple wikipedia processed dumps into a single dataset")

    # Global level parameters
    parser.add_argument('-i', '--input_files', type=str, required=True, nargs='+',
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
    parser.add_argument('--target_len', type=int, default=128, required=False)

    # get NameSpace of paramters
    args = parser.parse_args()

    if os.path.isfile(args.output_file):
        assert args.force_overwrite, f"Cannot overwrite {args.output_file}, add -f option if you know what you are doing."
        os.remove(args.output_file)

    for f in args.input_files:
        assert os.path.isfile(f), f"File {f} does not exist"

    assert not args.lang_file or os.path.isfile(args.lang_file), f"file {args.lang_file} does not exist"

    lang_dict = json.load(open(args.lang_file)) if args.lang_file is not None else None
    if lang_dict:
        print(f"Assigning lang ids: {lang_dict}")

    print(f"Creating dataset from {len(args.input_files)} input file(s)")

    with open(args.output_file, "w") as out_file:
        writer = csv.writer(out_file, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for _file in tqdm(args.input_files, desc="Processed files", position=0):
            
            # whether to insert lang _id
            _id = None
            if lang_dict is not None:
                lang_name = get_lang_from_wikipedia_filename(_file)
                _id = lang_dict[lang_name]['id']

            # remember the actual number of written lines
            written_lines = 0

            def write(l, written_lines):
                row = [written_lines, _id, l] if _id is not None else [written_lines, l]
                writer.writerow(row)
                return written_lines + 1

            with open(_file) as input_file:

                accumulator = None
                accumulator_len = 0

                for line, line_len in tqdm(batch_read_with_tokenization_parallel(input_file, args.fill_for_tokenizer), desc="Processing lines", position=1):

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







