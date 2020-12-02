import os, io
import lzma
import tarfile
from tqdm import tqdm
from argparse import ArgumentParser


def main(args):

    assert os.path.isfile(args.input_file), (
        f"Input file {args.input_file} does not exist!"
    )

    assert args.force or not os.path.isfile(args.output_file), (
        f"Output file {args.output_file} does not exist, use `--force` to overwrite"
    )

    r"""
    Structure of OpenWebText corpus (openwebcorpus.tar.xz)
    main_file.tar.xz
     - urlsf_subsetXX-XXX_data.xz
        - XXXXXXX-XXXXXXXXXXXXXXXXXXXXXXX.txt
        - ...
     - ...
    """

    with open(args.output_file, "w") as out_file:
        with tarfile.open(args.input_file, mode='r:xz') as tar:
            members = tar.getmembers()
            for member in tqdm(members, desc="Reading file containers", total=len(members)):
                internal_file = tar.extractfile(member).read()
                decompressed_file = io.BytesIO(lzma.decompress(internal_file))
                with tarfile.open(fileobj=decompressed_file, mode='r') as tar_2:
                    members_internal = tar_2.getmembers()
                    for member_internal in members_internal:
                        data_file = tar_2.extractfile(member_internal).read().decode('utf-8')
                        out_file.write(data_file + "\n")


if __name__ == '__main__':

    parser = ArgumentParser()
    parser.add_argument('-i', '--input_file', type=str, required=True, help="openwebtext xz compressed tar archive (.tar.xz)")
    parser.add_argument('-o', '--output_file', type=str, required=True, help="Output txt file")
    parser.add_argument('-f', '--force', action="store_true", help="Overwrite output file if it exists")
    parser.add_argument('-p', '--processes', type=int, help="Number of parallel processes to spawn", required=False, default=None)
    args = parser.parse_args()

    main(args)