import multiprocessing
import os
import logging
from pathlib import Path
from tqdm import tqdm
from blingfire import text_to_sentences
from argparse import ArgumentParser
from multiprocessing import Pool


FORMAT_LOGGING = '%(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT_LOGGING)
logging.getLogger().setLevel(logging.INFO)


def clean_text(text):
    sentences = text_to_sentences(text)
    sentences.replace("\n", " ")
    return sentences


def main(args):
    
    logging.info(f'Pre-processing {args.input_file} to {args.output_file}...')

    with open(args.input_file, 'r') as in_f:
        total = sum(1 for _ in tqdm(in_f, desc="Overviewing input files"))

    with open(args.output_file, 'w') as out_f:
        with open(args.input_file, 'r') as in_f:
            with Pool(args.processes) as p:
                with tqdm(total=total, desc="Preprocessing file") as pbar:                        
                    for res in p.imap(clean_text, in_f, chunksize=1000):
                        pbar.update()
                        out_f.write(res)
                        out_f.write("\n")

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
    parser.add_argument('-p', '--processes', type=int, default=multiprocessing.cpu_count(),
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