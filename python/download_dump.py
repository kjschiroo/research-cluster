"""
Checks if a completed MediaWiki XML dump is available,
and if so download it and stream it to HDFS.
Files will be stored in folder: <base-path/<wikidb>-<day>/xmlbz2

WARNING: Problems with hdfs write rights.
         Successfully tested on globally writable folders (hdfs:///tmp)

Usage:
    download_dump <wikidb> <day>
             [--name-node=<host>] [--base-path=<path>] [--user=<user>]
             [--num-threads=<num>] [--num-retries=<num>]
             [--buffer=<bytes>] [--timeout=<num>]
             [--force] [--debug]


Options:
    <wikidb>                The wiki to download (wikidb format, like enwiki)
    <day>                   The day to check (yyyyMMdd format)
    --name-node=<host>      The host of the cluster name-node
                            [default: http://nn-ia.s3s.altiscale.com:50070]
    -p --base-path=<path>   The base path where to store the files
                            [default: /wikimedia_data]
    -u --user=<user>        Hadoop user to impersonate
                            (defaults to user running the script)
    -n --num-threads=<num>  Number of parallel downloading threads
                            [default: 2]
    -r --num-retries=<num>  Number of retries in case of download failure
                            [default: 3]
    -b --buffer=<bytes>     Number of bytes for the download buffer
                            [default: 4096]
    -t --timeout=<num>      Number of seconds before timeout while downloading
                            [default: 120]
    -f --force              If set, will delete existing content if any
    -d --debug              Print debug logging
"""
import logging
import os.path
import subprocess
import sys

import docopt
import hdfs
import requests
import re

import Queue
import threading
import hashlib

logger = logging.getLogger(__name__)

BASE_DUMP_URI_PATTERN = 'http://dumps.wikimedia.org/{0}/{1}'
DUMP_STATUS_URI_PATTERN = BASE_DUMP_URI_PATTERN + '/status.html'
DUMP_SHA1_URI_PATTERN = BASE_DUMP_URI_PATTERN + '/{0}-{1}-sha1sums.txt'
DUMP_MD5_URI_PATTERN = BASE_DUMP_URI_PATTERN + '/{0}-{1}-md5sums.txt'
DUMP_BZ2_FILE_PATTERN = '{0}-{1}-pages-meta-history.*\.xml.*\.bz2'
DOWNLOAD_FILE_PATTERN = BASE_DUMP_URI_PATTERN + '/{2}'

FILE_PRESENT = 0
FILE_ABSENT = 1
FILE_CORRUPT = 2


def main():
    args = docopt.docopt(__doc__)

    logging.basicConfig(
        format='%(asctime)s %(levelname)s:%(name)s -- %(message)s'
    )
    logger.setLevel(logging.DEBUG if args['--debug'] else logging.INFO)

    wikidb = args['<wikidb>']
    day = args['<day>']

    name_node = args['--name-node']
    base_path = args['--base-path']
    user = args['--user']
    num_threads = int(args['--num-threads'])
    num_retries = int(args['--num-retries'])
    buffer_size = int(args['--buffer'])
    timeout = int(args['--timeout'])
    force = args['--force']

    run(wikidb, day, name_node, base_path, user, num_threads, num_retries,
        buffer_size, timeout, force)


def run(wikidb, day, name_node, base_path, user, num_threads, num_retries,
        buffer_size, timeout, force):

    # Force insecure client usage for correct hdfs user setup
    hdfs_client = hdfs.client.InsecureClient(name_node, user=user)
    output_path = os.path.join(base_path, '{0}-{1}'.format(wikidb, day),
                               'xmlbz2')

    ################# TESTING ####################
    checksums = get_dump_file_md5(DUMP_MD5_URI_PATTERN.format(wikidb, day))
    filenames = dump_filenames(DUMP_SHA1_URI_PATTERN.format(wikidb, day),
                               DUMP_BZ2_FILE_PATTERN.format(wikidb, day))
    statuses = file_status(hdfs_client, output_path, filenames, checksums)
    for k in statuses:
        print('{0} - {1}'.format(k, statuses[k]))
    return
    ################# DONE TESTING ###############

    if not prepare_hdfs(hdfs_client, output_path, force):
        raise RuntimeError("Problem preparing hdfs")

    if not dump_completed(DUMP_STATUS_URI_PATTERN.format(wikidb, day)):
        raise RuntimeError("Dump not ready to be downloaded from MediaWiki")

    download_dumps(wikidb, day, name_node, output_path, user, num_threads,
                   num_retries, buffer_size, timeout)


def download_dumps(wikidb, day, name_node, output_path, user, num_threads,
                   num_retries, buffer_size, timeout):
    filenames = dump_filenames(DUMP_SHA1_URI_PATTERN.format(wikidb, day),
                               DUMP_BZ2_FILE_PATTERN.format(wikidb, day))

    logger.debug("Instantiating {0} workers ".format(num_threads) +
                 "to download {0} files.".format(len(filenames)))

    q = Queue.Queue()
    errs = []

    for filename in filenames:
        file_url = DOWNLOAD_FILE_PATTERN.format(wikidb, day, filename)
        hdfs_file_path = os.path.join(output_path, filename)
        q.put((file_url, hdfs_file_path, ))

    threads = [threading.Thread(target=worker,
                                args=[q, errs, name_node, user, num_retries,
                                      buffer_size, timeout])
               for _i in range(num_threads)]

    for thread in threads:
        thread.start()
        q.put((None, None))  # one EOF marker for each thread

    q.join()

    if errs:
        raise RuntimeError("Failed to download some file(s):\n\t{0}".format(
            '\n\t'.join(errs)))


def get_dump_file_md5(url):
    req = requests.get(url)
    md5s = {}
    text = req.text.strip()
    for line in text.split('\n'):
        md5, filename = line.split()
        md5s[filename] = md5
    return md5s


def file_status(hdfs_client, output_path, file_names, file_checksums):
    statuses = {}
    present_files = hdfs_client.list(output_path)
    for f_name in file_names:
        fullpath = os.path.join(output_path, f_name)
        if f_name not in present_files:
            statuses[f_name] = FILE_ABSENT
        elif confirm_checksum(hdfs_client, fullpath, file_checksums[f_name]):
            statuses[f_name] = FILE_PRESENT
        else:
            statuses[f_name] = FILE_CORRUPT
    return statuses


def confirm_checksum(hdfs_client, file_path, known_checksum):
    print(file_path)
    found = md5sum_for_file(hdfs_client, file_path)
    print('Found {0}'.format(found))
    print('Known {0}'.format(known_checksum))
    return known_checksum == found


def md5sum_for_file(hdfs_client, file_path):
    md5 = hashlib.md5()
    with hdfs_client.read(file_path, chunk_size=4096) as reader:
        for chunk in reader:
            md5.update(chunk)
    return md5.hexdigest()


def prepare_hdfs(hdfs_client, output_path, force):
    logger.debug("Preparing hdfs for path {0}".format(output_path))
    bz2_files_pattern = os.path.join(output_path, "*.bz2")

    if hdfs_client.content(output_path, strict=False):
        # Output path already exists
        if force:
            try:
                logger.debug("Deleting and recreating directory {0}".format(
                    output_path))
                hdfs_client.delete(output_path, recursive=True)
                hdfs_client.makedirs(output_path)
                return True
            except hdfs.HdfsError as e:
                logger.error(e)
                return False
        else:
            return False
    else:
        try:
            logger.debug("Creating directory {0}".format(output_path))
            hdfs_client.makedirs(output_path)
            return True
        except hdfs.HdfsError as e:
            logger.error(e)
            return False


def dump_completed(url):
    logger.debug("Checking for dump completion at {0}".format(url))
    req = requests.get(url)
    return ((req.status_code == 200) and ('Dump complete' in req.text))


def dump_filenames(url, bz2_pattern):
    logger.debug("Getting files list to download {0}".format(url))
    req = requests.get(url)
    filenames = []
    if (req.status_code == 200):
        p = re.compile(bz2_pattern)
        for line in req.text.split('\n'):
            match = p.search(line)
            if match:
                filenames.append(match.group(0))
    return filenames


def worker(q, errs, name_node, user, num_retries, buffer_size, timeout):
    thread_name = threading.current_thread().name
    if user:
        hdfs_client = hdfs.client.InsecureClient(name_node, user=user)
    else:
        hdfs_client = hdfs.client.InsecureClient(name_node)
    logger.debug("Starting worker {0}".format(thread_name))
    while True:
        (file_url, hdfs_file_path) = q.get()
        if file_url is None:  # EOF?
            q.task_done()
            logger.debug("Received EOF, stopping worker {0}".format(
                thread_name))
            return
        if (not download_to_hdfs(hdfs_client, file_url, hdfs_file_path,
                                 buffer_size, num_retries, timeout)):
            errs.append(file_url)
            logger.warn("Unsuccessful task for worker {0}".format(
                thread_name))
        else:
            logger.debug("Successful task for worker {0}".format(thread_name))
        q.task_done()


def download_to_hdfs(hdfs_client, file_url, hdfs_file_path,
                     buffer_size, num_retries, timeout):
    session = requests.Session()
    session.mount("http://",
                  requests.adapters.HTTPAdapter(max_retries=num_retries))
    req = session.get(file_url, stream=True, timeout=timeout)
    logger.debug("Downloading from {0} ".format(file_url) +
                 "and uploading to {0} ".format(hdfs_file_path))
    try:
        hdfs_client.write(hdfs_file_path,
                          data=req.iter_content(buffer_size),
                          buffersize=buffer_size,
                          overwrite=True)
        return True
    except Exception as e:
        logger.debug("Error while downloading {0}: {1}".format(file_url,
                                                               str(e)))
        return False


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        logger.error(e)
