#
# For licensing see accompanying LICENSE file.
# Copyright (C) 2020 Apple Inc. All Rights Reserved.
#

"""For a directory of nested JSON lines files, where each line is a document, chunk each document into many passages."""

from argparse import ArgumentParser
import json
import logging
import multiprocessing
from pathlib import Path
from typing import List, Tuple
import random
import spacy

MIN_PASSAGE_TOKENS = 3000
WIKI_LINK = 'wikipedia.org'
LINKEDIN = 'linkedin.com'
THRESHOLD = 20

nlp = spacy.load("en_core_web_sm")


def chunk_doc(content: str) -> List[str]:
    """Given a document, return a list of passages of no fewer than MIN_PASSAGE_TOKENS tokens / passage until EOF."""
    passages = []
    passage_tokens = []
    # lines = content.split('\n')

    nlp_content = nlp(content)

    chunks = []    
    current_chunk = []
    current_chunk_tokens = 0

    for sentence in nlp_content.sents:
        sentence_tokens = len(sentence)
        
        if current_chunk_tokens + sentence_tokens <= MIN_PASSAGE_TOKENS:
            current_chunk.append(sentence.text)
            current_chunk_tokens += sentence_tokens
        else:
            # print(f'current_chunk size: {len(current_chunk)}')
            chunks.append(" ".join(current_chunk))
            current_chunk = list(sentence.text)
            current_chunk_tokens = sentence_tokens



    '''for line in nlp_content.sents:
        tokens = line.split()
        passage_tokens.extend(tokens)

        if len(passage_tokens) > MIN_PASSAGE_TOKENS:
            passages.append(' '.join(passage_tokens))
            passage_tokens = []'''

    chunks.append(" ".join(current_chunk))
    return chunks


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
    

def process_file(tup: Tuple[str, str, Path]) -> int:
    """Chunk all documents in a single file."""
    input_directory, output_directory, input_file = tup
    output_file = str(input_file).replace(input_directory, output_directory)
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    max_page_cnt = 0
    with open(input_file) as f1, open(output_path, 'w') as f2:
        for jsonl in f1:
            doc = json.loads(jsonl)
            if not is_wikipedia_url(doc['url']):
                continue
            # url_set.add(doc['url'])
            passages = chunk_doc(doc['contents'])

            for i, passage in enumerate(passages):
                paragraph = {'id': f"{doc['id']}_p{i}",
                             'url': doc['url'],
                             'title': doc['title'],
                             'contents': passage}

                f2.write(json.dumps(paragraph, ensure_ascii=False) + '\n')
            max_page_cnt = max(max_page_cnt, len(passages))   
    return max_page_cnt

def chunk_documents_concurrent(input_directory: str, output_directory: str, workers: int) -> set:
    """Iterate .jsonl files in input_directory and output .jsonl files in output_directory where each doc is chunked."""
    input_directory_path = Path(input_directory)

    jsonl_files = list(input_directory_path.glob('**/*.jsonl'))
 
    with multiprocessing.Pool(workers) as p:
        for i, _ in enumerate(
            p.imap_unordered(
                process_file,
                [(input_directory, output_directory, f) for f in jsonl_files],
                chunksize=16,
            )
        ):
            if (i + 1) % 100 == 0:
                logging.info(f'Processed {i + 1} / {len(jsonl_files)} files...')
                print(f'Processed {i + 1} / {len(jsonl_files)} files...')

    # return url_set


def chunk_documents(input_directory: str, output_directory: str, workers: int) -> set:
    """Iterate .jsonl files in input_directory and output .jsonl files in output_directory where each doc is chunked."""
    input_directory_path = Path(input_directory)

    jsonl_files = list(input_directory_path.glob('**/*.jsonl'))
    url_set = set()

    max_page_cnt = 0
    for f in jsonl_files:
        max_page_cnt = process_file(url_set, (input_directory, output_directory, f))
        print(f'The largest file is splited to {max_page_cnt}')        

    return url_set



def check_documents(input_directory: str):
    """Check each documents in sequence"""
    good_url_cnt = 0
    input_directory_path = Path(input_directory)
    jsonl_files = list(input_directory_path.glob('**/*.jsonl'))
    for jsonl_file in jsonl_files:
        with open(jsonl_file) as f1:
            for jsonl in f1:
                doc = json.loads(jsonl)
                if is_wikipedia_url(doc['url']) and doc['contents'] != '':
                    good_url_cnt += 1
    
    print(f'the document cnt with good urls: {good_url_cnt}')



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
        '--output-directory',
        required=True,
        help='Directory to store .jsonl files containing document passages',
    )
    parser.add_argument(
        '--workers',
        default=8,
        type=int,
        help='Number of workers for downloading in parallel',
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    check_documents(args.input_directory)

    chunk_documents_concurrent(args.input_directory, args.output_directory, args.workers)

    #all_url_set = chunk_documents(args.input_directory, args.output_directory, args.workers)
    #print(f'Total documents cnt: {len(all_url_set)}')
    #print(f'Bad url link: {bad_url_cnt}')