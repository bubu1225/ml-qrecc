#
# For licensing see accompanying LICENSE file.
# Copyright (C) 2020 Apple Inc. All Rights Reserved.
#

"""This code compares the qrecc jsonl file before split and after assemble"""

from argparse import ArgumentParser
import json
import logging
import multiprocessing
from pathlib import Path
from typing import List, Tuple
import random


def is_wikipedia_url(url: str) -> bool:
    if WIKI_LINK in url:
        return True
    return False
    """ if LINKEDIN in url:
        return False
    random_number = random.randint(1, 100)
    if random_number <= THRESHOLD:
        return True
    return False """
    

def process_files(input1: str, input2: str, output: str) -> None:
    """Filter all documents based on url."""
    url_to_contents_length = {}
    missed_f1 = 0;
    missed_f2 = 0;
    with open(input1) as f1:
            for jsonl in f1:
                doc = json.loads(jsonl)
                doc_url = doc['url'] 
                if doc_url not in url_to_contents_length:
                    url_to_contents_length[doc_url] = [len(doc['contents'])]
                else:
                    print(f'duplicated url: {doc_url}')                  

    with open(input2) as f1:
            for jsonl in f1:
                doc = json.loads(jsonl)
                doc_url = doc['uri'] 
                if doc_url not in url_to_contents_length:
                    print(f'missed url in input1: {doc_url}')
                    missed_f1 += 1
                    url_to_contents_length[doc_url] = [0]
                url_to_contents_length[doc_url].append(len(doc['contents']))
                    
    diff_url = {}
    for url, contents_length in url_to_contents_length.items():
        if len(contents_length) == 1:
            missed_f2 += 1
            print(f'missed url in input2: {url}')
            url_to_contents_length[url] = [0, contents_length[0], 0 - contents_length[0]]
        else:
            url_to_contents_length[url].append(contents_length[0] - contents_length[1])
            if contents_length[0] - contents_length[1] != 0:
                print(f'{url}: {url_to_contents_length[url]}')

    print(f'total missed in f1: {missed_f1}')
    print(f'total missed in f2: {missed_f2}')

    with open(output, 'w') as fo:
        for url, contents_length in url_to_contents_length.items():
            fo.write(json.dumps({'url': url, 'contents_length': contents_length}, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    parser = ArgumentParser(
        description='Chunk documents in .jsonl files into many passages.'
    )
    parser.add_argument(
        '--input1',
        required=True,
        help='jsonl file to compare',
    )
    parser.add_argument(
        '--input2',
        required=True,
        help='jsonl file to compare',
    )
    parser.add_argument(
        '--output',
        required=True,
        help='json file to dump result',
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    process_files(args.input1, args.input2, args.output)
    