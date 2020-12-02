import logging
import os
from argparse import ArgumentParser
from multiprocessing import Pool, cpu_count
from pathlib import Path

from blingfire import text_to_sentences
from tqdm import tqdm


FORMAT_LOGGING = '%(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT_LOGGING)
logging.getLogger().setLevel(logging.INFO)

def clean_text(text):
    text = text.strip()
    if len(text) == 0:
        return "\n"
    sentences = text_to_sentences(text).replace("\n", " ").strip()
    if len(text) > 0 and len(sentences) == 0:
        return "" # this was a text line that contained not parseable text
    return sentences + "\n"

def main(args):

    logging.info(f'Pre-processing {args.input_file} to {args.output_file}...')

    with open(args.input_file, 'r') as in_f:
        total = sum(1 for _ in tqdm(in_f, desc="Overviewing input files"))

    with open(args.output_file, 'w') as out_f:
        with open(args.input_file, 'r') as in_f:
            with Pool(args.processes) as p:
                with tqdm(total=total, desc="Preprocessing file") as pbar:                        
                    for res in p.imap(clean_text, in_f, chunksize=args.chunk_size):
                        pbar.update()
                        out_f.write(res)


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
    parser.add_argument('-c', '--chunk_size', type=int, default=10000,
                        help='Number of entries per process. Increase with very large datasets.')

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
