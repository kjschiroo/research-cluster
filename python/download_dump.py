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


class DumpDownloader(object):

    def __init__(self,
                 wikidb,
                 day,
                 name_node,
                 base_path,
                 user,
                 num_threads,
                 buffer_size,
                 timeout,
                 force):
        self.wikidb = wikidb
        self.day = day
        self.name_node = name_node
        self.base_path = base_path
        self.user = user
        self.num_threads = num_threads
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.force = force

    def run(self):
        self._verifty_dump_ready_for_download()
        self._configure_hdfs_client()
        self._configure_output_path()
        self._identify_target_file_list_and_md5s()
        self._check_status_of_existing_files()
        self._prepare_hdfs()

    def _verifty_dump_ready_for_download(self):
        url = DUMP_STATUS_URI_PATTERN.format(wikidb, day)
        logger.debug("Checking for dump completion at {0}".format(url))
        req = requests.get(url)
        if not ((req.status_code == 200) and ('Dump complete' in req.text)):
            raise RuntimeError("Dump not ready to be downloaded from MediaWiki")

    def _configure_hdfs_client(self):
        name_node = self.name_node
        user = self.user
        self.hdfs_client = hdfs.client.InsecureClient(name_node, user=user)

    def _configure_output_path(self):
        self.output_path = os.path.join(base_path,
                                        '{0}-{1}'.format(self.wikidb, self.day),
                                        'xmlbz2')

    def _identify_target_file_list_and_md5s(self):
        url = DUMP_MD5_URI_PATTERN.format(self.wikidb, self.day),
        bz2_pattern = DUMP_BZ2_FILE_PATTERN.format(self.wikidb, self.day)
        logger.debug("Getting files list to download {0}".format(url))
        req = requests.get(url)
        self.filenames = []
        self.md5s = {}
        if (req.status_code == 200):
            p = re.compile(bz2_pattern)
            for line in req.text.split('\n'):
                match = p.search(line)
                if match:
                    md5, filename = line.split()
                    self.filenames.append(filename)
                    self.md5s[filename] = md5
        else:
            raise RuntimeError("MD5 hash listing unavailable")

    def _check_status_of_existing_files(self):
        self.statuses = {}
        present_files = self.hdfs_client.list(self.output_path)
        for filename in self.filenames:
            fullpath = os.path.join(output_path, filename)
            if f_name not in present_files:
                self.statuses[filename] = FILE_ABSENT
            elif self._confirm_checksum(filename):
                self.statuses[filename] = FILE_PRESENT
            else:
                self.statuses[filename] = FILE_CORRUPT

    def _confirm_checksum(self, filename):
        found = self._md5sum_for_file(filename)
        given = self.md5s[filename]
        return given == found

    def _md5sum_for_file(self, filename):
        md5 = hashlib.md5()
        filepath = os.path.join(self.output_path, filename)
        with self.hdfs_client.read(filepath, chunk_size=4096) as reader:
            for chunk in reader:
                md5.update(chunk)
        return md5.hexdigest()

    def _prepare_hdfs(self, hdfs_path, files_info, force):
        if self.hdfs_client.content(self.output_path, strict=False):
            if self.force:
                try:
                    self.hdfs_client.delete(self.output_path, recursive=True)
                    self.hdfs_client.makedirs(self.output_path)
                    [self.statuses[f] = FILE_ABSENT for f in self.statuses]
                except hdfs.HdfsError as e:
                    logger.error(e)
                    raise RuntimeError("Problem preparing for download [force].")
            else:
                logger.debug("Checking existing files in folder before download")
                try:
                    self._remove_corrupt_and_unexpected_files()
                except hdfs.HdfsError as e:
                    logger.error(e)
                    raise RuntimeError("Problem preparing for download [merge].")
        else:
            try:
                self.hdfs_client.makedirs(self.output_path)
            except hdfs.HdfsError as e:
                logger.error(e)
                raise RuntimeError("Problem preparing for download [new].")

    def _remove_corrupt_and_unexpected_files(self):
        present_files = self.hdfs_client.list(self.output_path)
        for filename in present_files:
            file_path = os.path.join(hdfs_path, filename)
            if (filename not in self.filenames):
                hdfs_client.delete(file_path, recursive=True)
            if (self.statuses[filename] == FILE_CORRUPT):
                hdfs_client.delete(file_path, recursive=True)
                self.statuses[filename] = FILE_ABSENT

    def _download_dumps():
        logger.debug("Instantiating {0} workers ".format(self.num_threads) +
                     "to download {0} files.".format(len(self.filenames)))

        q = Queue.Queue()
        errs = []

        files_to_download = [f for f in self.statuses
                             if self.statuses[f] != FILE_PRESENT]
        for filename in files_to_download:
            file_url = DOWNLOAD_FILE_PATTERN.format(self.wikidb,
                                                    self.day,
                                                    filename)
            hdfs_file_path = os.path.join(self.output_path, filename)
            q.put((file_url, hdfs_file_path, ))

        threads = [threading.Thread(target=worker,
                                    args=[q,
                                          errs,
                                          self.name_node,
                                          self.user,
                                          self.num_retries,
                                          self.buffer_size,
                                          self.timeout])
                   for _i in range(self.num_threads)]

        for thread in threads:
            thread.start()
            q.put((None, None))  # one EOF marker for each thread

        q.join()

        if errs:
            raise RuntimeError("Failed to download some file(s):\n\t{0}".format(
                '\n\t'.join(errs)))


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

    dl = DumpDownloader(
        wikidb,
        day,
        name_node,
        base_path,
        user,
        num_threads,
        num_retries,
        buffer_size,
        timeout,
        force
    )
    dl.run()


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
