#
# For licensing see accompanying LICENSE file.
# Copyright (C) 2020 Apple Inc. All Rights Reserved.
#

"""Get all urls and titles of documents and write to output file."""

from argparse import ArgumentParser
import json
import logging
import multiprocessing
import os
from pathlib import Path
from typing import List, Tuple
import random

MIN_PASSAGE_TOKENS = 3000
WIKI_LINK = 'wikipedia.org'
LINKEDIN = 'linkedin.com'
THRESHOLD = 20

good_url_cnt = 0
bad_url_cnt = 0


def is_wikipedia_url(url: str) -> bool:
    if WIKI_LINK in url:
        return True
    return False

    

def process_files(input_directory: str, output_file: str) -> None:
    """Filter all documents based on url."""
    good_url_cnt = 0
    input_directory_path = Path(input_directory)
    jsonl_files = list(input_directory_path.glob('**/*.jsonl'))
    
  
    for input_file in jsonl_files:
        output_path = Path(os.path.join(input_directory, output_file))
  
        with open(input_file) as f1, open(output_path, 'w') as f2:
            for jsonl in f1:
                doc = json.loads(jsonl)
                if is_wikipedia_url(doc['url']) and doc['contents']:
                    good_url_cnt +=1
                    json_body = {
                        'url': doc['url'],
                        'title': doc['title']
                    }
                    f2.write(json.dumps(json_body, ensure_ascii=False) + '\n')
    

if __name__ == '__main__':
    parser = ArgumentParser(
        description='Chunk documents in .jsonl files into many passages.'
    )
    parser.add_argument(
        '--input-directory',
        required=True,
        help='Directory containing .jsonl files to chunk',
    )
    parser.add_argument(
        '--output-file',
        required=True,
        help='Directory to store .jsonl files containing document passages',
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    process_files(args.input_directory, args.output_file)
