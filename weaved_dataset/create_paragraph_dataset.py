import os
from argparse import ArgumentParser
import csv
from tqdm import tqdm

if __name__ == "__main__":
    parser = ArgumentParser("Parse multiple wikipedia processed dumps into a single dataset")

    # Global level parameters
    parser.add_argument('-i', '--input_folder', type=str)
    parser.add_argument('-o', '--output_file', type=str)
    args = parser.parse_args()

    i = 0
    with open(args.output_file, 'w') as fout:
        writer = csv.writer(fout, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        subdirs = os.listdir(args.input_folder)
        for subfolder in tqdm(subdirs, desc="Subfolder iterator", total=len(subdirs), position=1):

            path_subfolder = os.path.join(args.input_folder, subfolder)

            page_documents = []
            pages = os.listdir(path_subfolder)
            for page in tqdm(pages, desc="File iterator", total=len(pages), position=1):

                with open(os.path.join(path_subfolder, page), 'r') as f:
                    for line in f.readlines():
                        line = line.strip()
                        if len(line) > 0:
                            if line.startswith("<doc id="):
                                page_documents.append([])
                            elif line.startswith("</doc>"):
                                pass
                            else:
                                while "  " in line:
                                    line = " ".join(line.split('  '))

                                page_documents[-1].append(line)
            
            print('Writing {} to {}...'.format(i, i + len(page_documents)), end='\r')
            rows = [(i + rel_idx, doc[0], ' '.join(doc[1:])) for rel_idx, doc in enumerate(page_documents)]
            
            for row in rows:
                writer.writerow(row)
            i += len(page_documents)

    print('Done.')
