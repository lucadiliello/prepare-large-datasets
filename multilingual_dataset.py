import os
from argparse import ArgumentParser
import csv
import json
import logging
import logging
from tqdm import tqdm


FORMAT_LOGGING = '%(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT_LOGGING)
logging.getLogger().setLevel(logging.INFO)


def get_lang_from_wikipedia_filename(filename):
    """ Expected filename like `enwiki-dump-latest.tsv` ..."""
    return os.path.basename(filename).strip().lower().split("-")[0].replace("wiki", "")

def main(args):

    logging.info("Checking I/O files")
    assert not os.path.isfile(args.output_file) or args.force_overwrite, (
        f"Cannot overwrite {args.output_file}, add -f option if you know what you are doing."
    )
    for f in args.input_files:
        assert os.path.isfile(f), (
            f"File {f} does not exist"
        )
    assert not args.lang_file or os.path.isfile(args.lang_file), (
        f"file {args.lang_file} does not exist"
    )

    logging.info("Eventually loading lang file")
    lang_dict = json.load(open(args.lang_file)) if args.lang_file is not None else None
    if lang_dict:
        logging.info(f"Assigning lang ids: {lang_dict}")
        lang_ids = []
        for filename in args.input_files:
            lang_name = get_lang_from_wikipedia_filename(filename)
            assert lang_name in lang_dict, (
                f"Could not recognize language of file {filename} "
                f"Assert filename is like `enwiki-*` or simply `it-*`"
            )
            lang_ids.append(
                lang_dict[lang_name]['id']
            )
    else:
        lang_ids = [None] * len(args.input_files)

    logging.info(f"Creating dataset from {len(args.input_files)} input file(s)")

    written_lines = 0
    with open(args.output_file, "w") as out_file:
        writer = csv.writer(out_file, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        for filename, lang_id in tqdm(zip(args.input_files, lang_ids), desc="Processed files", position=0):

            # remember the actual number of written lines
            written_lines_file = 0
            with open(filename) as in_file:
                filename_reader = csv.reader(in_file, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

                for line in tqdm(filename_reader, desc="Processing lines", position=1):
                    row = [written_lines, lang_id, line[1]] if lang_id is not None else [written_lines, line[1]]
                    writer.writerow(row)
                    written_lines_file += 1
                    written_lines += 1

                    if args.limit and written_lines_file >= args.limit:
                        break

                logging.info(f"- Written {written_lines_file} lines from file {filename} with id {lang_id}")
    
    logging.info(f"- Written a total of {written_lines}, done!")


if __name__ == "__main__":
    parser = ArgumentParser("Parse multiple wikipedia processed dumps into a single dataset")

    # Global level parameters
    parser.add_argument('-i', '--input_files', type=str, required=True, nargs='+',
                        help="List of input wikipedia processed dumps with one sentence per line")
    parser.add_argument('-l', '--limit', type=int, required=False, default=None,
                        help='Limit of sentences to be taken from each file')
    parser.add_argument('-o', '--output_file', type=str, required=True,
                        help='Specify an output file')
    parser.add_argument('--lang_file', type=str, required=False, default=None,
                        help="Specify an input language file with pairs of languages and ancronyms")
    parser.add_argument('-f', '--force_overwrite', action="store_true",
                        help='Overwrite output file if it does already exist')

    # get NameSpace of paramters
    args = parser.parse_args()

    main(args)
