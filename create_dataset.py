import os
from argparse import ArgumentParser
import csv
import logging
import transformers
from tqdm import tqdm
from multiprocessing import cpu_count, Process, Queue


FORMAT_LOGGING = '%(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT_LOGGING)
logging.getLogger().setLevel(logging.INFO)


# process a single line
def parse_line(line: str, tokenizer: transformers.PreTrainedTokenizer):
    line = line.strip()
    if not line.endswith('.'): line += '.'
    return line, len(tokenizer.encode(line)) if tokenizer is not None else None


# process a set of lines in a separate process with a dedicated tokenizer
def worker(in_queue: Queue, out_queue: Queue, tokenizer_name: str = None, accumulate: int = 1):
    tokenizer = transformers.AutoTokenizer.from_pretrained(tokenizer_name) if tokenizer_name else None
    while True:
        line = in_queue.get()
        if line is None:
            out_queue.put(None)
            break
        out_queue.put(parse_line(line, tokenizer))


# read from input and fill input queue
def filler(filename: str, in_queue: Queue, n_cpus: int):
    with open(filename) as in_fi:
        for line in in_fi:
            in_queue.put(line)
    for _ in range(n_cpus):
        in_queue.put(None)


# read from out_queue and write to file
def writer(
    out_queue: Queue,
    filename: str,
    n_cpus: int,
    limit: int = None,
    min_word_per_sentence: int = 1,
    separate_documents: bool = False,
    target_len: int = 510
):

    # used to accumulate sequences
    accumulator = None
    accumulator_len = 0
    written_lines = 0
    terminated = 0
    pbar = tqdm(desc="Writing to output file")

    with open(filename, "w") as out_file:
        writer = csv.writer(out_file, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        while True:

            res = out_queue.get()
            if res is None:
                terminated += 1
                if terminated == n_cpus:
                    break
                else:
                    continue

            # unpack
            line, line_len = res

            # without tokenizer write line by line
            if line_len is None:
                if len(line.strip()) > 0 and len(line.split()) >= min_word_per_sentence:
                    writer.writerow([written_lines, line])
                    written_lines += 1
                    pbar.update()

            # length of actual line in tokens
            else:
                # empty lines are used to separate documents
                if (separate_documents and len(line) == 0) and accumulator is not None: 
                    writer.writerow([written_lines, accumulator])
                    written_lines += 1
                    pbar.update()
                    accumulator = line
                    accumulator_len = line_len

                # if we are under the max len
                if accumulator is None:
                    accumulator = line
                    accumulator_len = line_len

                    # TODO: if single line is > target_len, split in place and write many times
                    if accumulator_len > target_len:
                        pass

                # if adding the new sequence is still under the max len
                elif accumulator_len + line_len <= target_len:
                    accumulator = accumulator + " " + line if len(accumulator) > 0 else line
                    accumulator_len += line_len

                # if we went over, write and init accu with actual line
                else:
                    writer.writerow([written_lines, accumulator])
                    written_lines += 1
                    pbar.update()
                    accumulator = line
                    accumulator_len = line_len

            if limit is not None and written_lines >= limit:
                break

        # if last accumulator was not written because for cycle ended before, write it now
        if limit is None and accumulator is not None and len(accumulator) > 0:
            writer.writerow([written_lines, accumulator])
            written_lines += 1
            pbar.update()
        
        pbar.close()
        logging.info(f"Written {written_lines} lines successfully.")



def main(args):
    
    logging.info(f"Checking I/O files")
    if os.path.isfile(args.output_file):
        assert args.force_overwrite, f"Cannot overwrite {args.output_file}, add -f option if you are cocky"
        os.remove(args.output_file)
    assert os.path.isfile(args.input_file), f"Input file {args.input} does not exist"

    logging.info("Creating queues")
    in_queue = Queue()
    out_queue = Queue()

    logging.info("Spawning produces")
    filler_process = Process(target=filler, args=(args.input_file, in_queue, args.processes))
    
    logging.info("Spawning workers")
    workers = [Process(target=worker, args=(in_queue, out_queue, args.fill_for_tokenizer)) for _ in range(args.processes)]
    logging.info("Starting workers")
    for w in workers:
        w.start()

    logging.info("Starting producer")
    filler_process.start()
    
    logging.info("Spawning writer")
    writer_process = Process(target=writer, args=(out_queue, 
                                                  args.output_file,
                                                  args.processes), kwargs={'limit': args.limit,
                                                                           'min_word_per_sentence': args.min_word_per_sentence,
                                                                           'separate_documents': args.separate_documents,
                                                                           'target_len': args.target_len})

    logging.info("Starting writer")
    writer_process.start()

    logging.info("Waiting for processes to finish")
    writer_process.join()
    for w in workers:
        if w.is_alive():
            w.terminate()

    if filler_process.is_alive():
        filler_process.terminate()


if __name__ == "__main__":

    parser = ArgumentParser("Parse multiple wikipedia processed dumps into a single dataset")

    # Global level parameters
    parser.add_argument('-i', '--input_file', type=str, required=True,
                        help="Preprocessed dataset")
    parser.add_argument('-l', '--limit', type=int, required=False, default=None,
                        help='Limit of rows in output file')
    parser.add_argument('-m', '--min_word_per_sentence', type=int, required=False, default=1,
                        help='Minimun number of words in a sentence to be considered.')
    parser.add_argument('-o', '--output_file', type=str, required=True,
                        help='Specify an output file')
    parser.add_argument('-f', '--force_overwrite', action="store_true",
                        help='Overwrite output file if it does already exist')
    parser.add_argument('--fill_for_tokenizer', type=str, default=None, required=False,
                        help="Path of some pre-trained tokenizer")
    parser.add_argument('--separate_documents', action="store_true",
                        help="Path of some pre-trained tokenizer")
    parser.add_argument('--processes', type=int, default=cpu_count(), required=False,
                        help="Number or parallel processes to use")
    parser.add_argument('--target_len', type=int, default=128, required=False)
    parser.add_argument('--chunk_size', type=int, default=1024*128, required=False)

    # get NameSpace of paramters
    args = parser.parse_args()

    main(args)
