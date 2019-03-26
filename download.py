from __future__ import unicode_literals

import argparse
import csv
import errno
import logging
import multiprocessing
import os
import shutil
import time
import traceback
import sys

from PIL import Image

import requests
import six



def config_logger():
    logger = logging.getLogger('download')
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(process)d @ %(asctime)s (%(relativeCreated)d) '
                                  '%(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    return logger


def parse_args():
    parser = argparse.ArgumentParser(description='Download image dataset.')

    parser.add_argument('--timeout', type=float, default=2.0,
                        help='image download timeout')
    parser.add_argument('-c','--consumers', type=int, default=36,
                        help='number of download workers')
    parser.add_argument('--min-dim', type=int, default=-1,
                        help='smallest dimension for the aspect ratio preserving scale'
                             '(-1 for no scale)')
    parser.add_argument('--subdirs', default=False,
                        help='split into sub directories')
    parser.add_argument('--force', default=False, action='store_true',
                        help='force download and overwrite local files')

    parser.add_argument('input', help='open image input csv')
    parser.add_argument('output', help='save directory')

    return parser.parse_args()


def scale(content, min_dim):
    """ Aspect-ratio preserving scale such that the smallest dim is equal to `min_dim` """

    image = Image.open(content)

    # no scaling, keep images full size
    if min_dim == -1:
        return image

    # aspect-ratio preserving scale so that the smallest dimension is `min_dim`
    width, height = image.size
    scale_dimension = width if width < height else height
    scale_ratio = float(min_dim) / scale_dimension

    if scale_ratio == 1:
        return image

    return image.resize(
        (int(width * scale_ratio), int(height * scale_ratio)),
        Image.ANTIALIAS,
    )


def read_image(response, min_dim):
    """ Download response in chunks and convert to a scaled Image object """

    content = six.BytesIO()
    shutil.copyfileobj(response.raw, content)
    content.seek(0)
    
    if min_dim > 0 :
        return scale(content, min_dim)
    else:
        return content


def consumer(args, queue):
    """ Whilst the queue has images, download and save them """

    while queue.empty():
        time.sleep(20.0)  # give the queue a chance to populate

    while not queue.empty():
        id, url = queue.get(block=True, timeout=None)
        
        if args.subdirs:
            dir = args.output + id[:3]
            saving_path = args.output + id[:3] + '/' + id + '.jpg'
        else:
            dir = args.output
            saving_path = args.output + id + '.jpg'
            
        if not os.path.exists(saving_path):
            try:
                response = requests.get(url, stream=True, timeout=args.timeout)
                image = Image.open(read_image(response, args.min_dim))
                image.save(saving_path)
            except Exception:
                log.warning('error {}'.format(traceback.format_exc()))
            #else:
            #    log.debug('saving {} to {}'.format(url, out_path))
        else :
            log.debug('/data/yfcc' + id + '.jpg already exists ')

        if queue.empty():
            time.sleep(0.1)

    log.debug('!!!!!!!!!!!!! \n EXITING COSUMER \n !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

def producer(args, queue):
    """ Populate the queue with image_id, url pairs. """
    print('opening file')
    with open(args.input, newline='\n') as f:
        print('Reading File : ')
        for i, row in enumerate(f):
            if i < 40700000:
                continue

            s = row.split('\t')
            if '0' in s[-1] :
                url = s[16]
                id = s[1]
                queue.put([id, url], block=True, timeout=None)
                #print("%2.2f" % (i/100000000.0)*100.0 , '\%', end='\r')
                print("%2.5f"% ((i/100000000.0)*100.0), '\%', end='\r')

    queue.close()


log = config_logger()


if __name__ == '__main__':
    args = parse_args()
    log.debug(args)

    #queue = multiprocessing.Queue(args.queue_size)
    queue = multiprocessing.Queue(20000)
    processes = [
        multiprocessing.Process(target=producer, args=(args, queue))
    ]

    for i in range(args.consumers):
        processes.append(multiprocessing.Process(target=consumer, args=(args, queue)))
    print('There is :', len(processes), 'processes' )

    for p in processes:
        p.start()

    for p in processes:
        p.join()
