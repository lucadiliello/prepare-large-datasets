import os
from argparse import ArgumentParser
import csv
import logging
from tqdm import tqdm
import random
from pathlib import Path

FORMAT_LOGGING = '%(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT_LOGGING)
logging.getLogger().setLevel(logging.INFO)


def main(args):

    logging.info(f"Checking I/O files")
    if args.output_file is None:
        input_dump_file_in = Path(args.input_file)
        args.output_file = input_dump_file_in.parent / f'{input_dump_file_in.stem}-shuffled{input_dump_file_in.suffix}'

    assert not os.path.isfile(args.output_file) or args.force_overwrite, (
        f"Cannot overwrite {args.output_file}, add -f option if you are cocky"
    )
    assert os.path.isfile(args.input_file), f"Input file {args.input_file} does not exist"

    random.seed(args.seed)

    logging.info("Assigning round number to each line")
    all_lines = []

    with open(args.input_file) as in_file:
        reader = csv.reader(in_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL, quotechar='"')
        for _ in tqdm(reader, desc="Reading input file"):
            all_lines.append(random.randint(0, args.rounds-1))

    logging.info(f"Starting {args.rounds} rounds")

    new_id = 0
    with open(args.output_file, "w") as out_file:
        writer = csv.writer(out_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL, quotechar='"')

        for i in range(args.rounds):
            logging.info(f"Round {i+1}")

            lines_to_write = []
            with open(args.input_file) as in_file:
                reader = csv.reader(in_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL, quotechar='"')
                for row, _round in tqdm(zip(reader, all_lines), desc="Reading input file"):
                    if _round == i:
                        lines_to_write.append(row)

            random.shuffle(lines_to_write)
            for row in lines_to_write:
                if args.id_column is not None:
                    row[args.id_column] = new_id
                writer.writerow(row)
                new_id += 1

    logging.info(f"Written {new_id} lines, done!")


if __name__ == "__main__":

    parser = ArgumentParser("Parse multiple wikipedia processed dumps into a single dataset")

    # Global level parameters
    parser.add_argument('-i', '--input_file', type=str, required=True,
                        help="Preprocessed dataset")
    parser.add_argument('-o', '--output_file', type=str, required=False, default=None,
                        help='Specify an output file')
    parser.add_argument('-f', '--force_overwrite', action="store_true",
                        help='Overwrite output file if it does already exist')
    parser.add_argument('--seed', type=int, required=False, default=999,
                        help="Seed used for shuffling")
    parser.add_argument('--rounds', type=int, required=False, default=10,
                        help="Seed used for shuffling")
    parser.add_argument('--id_column', type=int, required=False, default=None,
                        help="Id column that should be changed ")

    # get NameSpace of paramters
    args = parser.parse_args()

    main(args)
