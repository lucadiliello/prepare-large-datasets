import io
import multiprocessing
import os
import lzma
import tarfile
import logging
from pathlib import Path
from tqdm import tqdm
from argparse import ArgumentParser
from multiprocessing import Pool


FORMAT_LOGGING = '%(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT_LOGGING)
logging.getLogger().setLevel(logging.INFO)


def clean_text(text):
    return "\n".join([x for x in text.split("\n") if len(x) > 0]).strip() + "\n\n"

def worker(input_file):
    decompressed_file = io.BytesIO(lzma.decompress(input_file))
    with tarfile.open(fileobj=decompressed_file, mode='r') as tar:
        return [clean_text(tar.extractfile(member_internal).read().decode('utf-8')) for member_internal in tar.getmembers()]

def yield_extracted_files(members, tar):
    for member in tqdm(members, desc="Reading file containers", total=len(members)):
        yield tar.extractfile(member).read()

def main(args):

    r"""
    Structure of OpenWebText corpus (openwebcorpus.tar.xz)
    openwebcorpus.tar.xz
     - urlsf_subsetXX-XXX_data.xz
        - XXXXXXX-XXXXXXXXXXXXXXXXXXXXXXX.txt
        - ...
     - ...
    """

    logging.info("Opening input compressed archive")
    with open(args.output_file, "w") as out_file:
        with tarfile.open(args.input_file, mode='r:xz') as tar:

            logging.info("Reading containers")
            members = tar.getmembers()
            logging.info("Started high level extraction")
            input_containers = yield_extracted_files(members, tar)
            logging.info("Spawning processes")

            with Pool(args.processes) as p:
                with tqdm(total=len(members), desc="Processing internal files") as pbar:
                    for res in p.imap(worker, input_containers, chunksize=20):
                        pbar.update()
                        for r in res:
                            out_file.write(r)

if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument('-i', '--input_file', type=str, required=True, help="openwebtext xz compressed tar archive (.tar.xz)")
    parser.add_argument('-o', '--output_file', type=str, required=False, default=None, help="Output txt file")
    parser.add_argument('-f', '--force', action="store_true", help="Overwrite output file if it exists")
    parser.add_argument('-p', '--processes', type=int, help="Number of parallel processes to spawn", default=multiprocessing.cpu_count())
    args = parser.parse_args()

    if args.output_file is None:
        input_dump_file_in = Path(args.input_file)
        args.output_file = input_dump_file_in.parent / f'{input_dump_file_in.name.split(".")[0]}-extracted.txt'

    logging.info("Cheking I/O files")

    assert os.path.isfile(args.input_file), (
        f"Input file {args.input_file} does not exist!"
    )
    assert args.force or not os.path.isfile(args.output_file), (
        f"Output file {args.output_file} does not exist, use `--force` to overwrite"
    )

    main(args)