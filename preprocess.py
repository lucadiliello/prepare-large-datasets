import os
import logging
from pathlib import Path
from tqdm import tqdm
from blingfire import text_to_sentences
from argparse import ArgumentParser
from multiprocessing import Queue, Process, cpu_count


FORMAT_LOGGING = '%(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT_LOGGING)
logging.getLogger().setLevel(logging.INFO)

# process lines
def worker(in_queue: Queue, out_queue: Queue):
    while True:
        text = in_queue.get()
        if text is None:
            out_queue.put(None)
            break
        sentences = text_to_sentences(text)
        sentences.replace("\n", " ")
        out_queue.put(sentences)

# fill input queue by reading from source file
def filler(filename: str, in_queue: Queue, n_proc: int):
    with open(filename, 'r') as in_f:
        for line in in_f:
            in_queue.put(line)
    for _ in range(n_proc):
        in_queue.put(None)

# write from output queue to dest file
def writer(filename: str, out_queue: Queue, n_proc: int, total: int):
    finished = 0
    pbar = tqdm(total=total, desc="Preprocessing file")
    with open(filename, 'w') as out_f:
        while True:
            line = out_queue.get()
            if line is None:
                finished += 1
                if finished == n_proc:
                    break
            else:
                pbar.update()
                out_f.write(line)
                out_f.write("\n")


def main(args):
    
    logging.info(f'Pre-processing {args.input_file} to {args.output_file}...')

    with open(args.input_file, 'r') as in_f:
        total = sum(1 for _ in tqdm(in_f, desc="Overviewing input files"))

    logging.info('Preparing queues for multiprocessing')
    in_queue = Queue()
    out_queue = Queue()

    logging.info('Spawning worker processes')
    processes = [Process(target=worker, args=(in_queue, out_queue)) for _ in range(args.processes)]
    for p in processes:
        p.start()

    logging.info('Spawning fill process')
    process_fill = Process(target=filler, args=(args.input_file, in_queue, args.processes))
    process_fill.start()

    logging.info('Spawning writer process')
    process_writer = Process(target=writer, args=(args.output_file, out_queue, args.processes, total))
    process_writer.start()

    logging.info('Waiting for processes to complete')
    process_fill.join()
    for p in processes:
        p.join()
    process_writer.join()

    logging.info(f'Successfully pre-processed {args.input_file} to {args.output_file}...')


if __name__ == '__main__':

    parser = ArgumentParser("Clean a line-by-line document with the blingfire tokenizer")

    # Global level parameters
    parser.add_argument('-i', '--input_file', type=str, required=True,
                        help="List of input wikipedia processed dumps with one sentence per line")
    parser.add_argument('-o', '--output_file', type=str, required=False, default=None,
                        help='Specify an output file')
    parser.add_argument('-f', '--force_overwrite', action="store_true",
                        help='Overwrite output file if it does already exist')
    parser.add_argument('-p', '--processes', type=int, default=cpu_count(),
                        help='Number of processes to use')

    args = parser.parse_args()

    assert os.path.isfile(args.input_file), (
        f"Input file {args.input_file} does not exist"
    )

    if args.output_file is None:
        input_dump_file_in = Path(args.input_file)
        args.output_file = input_dump_file_in.parent / \
            f'{input_dump_file_in.stem}_preprocessed{input_dump_file_in.suffix}'

    assert not os.path.isfile(args.output_file) or args.force_overwrite, (
        f"Output file {args.output_file} does already exist"
    )

    main(args)