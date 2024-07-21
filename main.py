#!/usr/bin/env python3

# Scrape the Makaut AWS database for pictures
# https://ucanassessm.s3.ap-south-1.amazonaws.com/Latest/707616547717311/{id}/{id}.png
#
# UPPERBOUND = 520000
# LOWERBOUND = 406000

import os
import threading
import logging
import resource
import time
import requests

# Logging disabled, enable to debug
logging.basicConfig(level = logging.DEBUG,
                    # filename = __file__.replace('.py', '.log'),
                    # filemode = 'w',
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s',
                    datefmt = '%d/%m/%Y %I:%M:%S %p',
                )
logging.getLogger('urllib3.connectionpool').disabled = True
logging.disable(logging.CRITICAL)

class Downloader:
    """
    Downloader class that scrapes a static link with ranged id in a brute force attempt at
    gathering pictures; Use the function object as a functor
    """

    UPPERBOUND = 520000     # default id upperbound
    LOWERBOUND = 406000     # default id lowerbound
    # static link with placeholders for id
    URLFMT = "https://ucanassessm.s3.ap-south-1.amazonaws.com/Latest/707616547717311/{id}/{id}.png"
    # fake app requests to server as client-side browser
    HEADER = {'user-agent':
              'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.28 Safari/537.36'}
    CHUNKSIZE = 10000000    # byte-size for image saving

    def __init__(self, upperbound: int = None, lowerbound: int = None):
        if upperbound:
            self.UPPERBOUND = upperbound
        if lowerbound:
            self.LOWERBOUND = lowerbound

        self.mutex = threading.Lock()
        self.numDownloads = 0

    def __call__(self, downloadDir: str = 'Makaut', per_thread: int = 50) -> int:
        " Entry point; calls the function object to start download "

        # create directory specified if not already present; potential permission exception
        if not os.path.isdir(downloadDir):
            os.makedirs(downloadDir, exist_ok=True)
        self.downloadDir = downloadDir

        session = requests.Session()        # potential speedup requests by perpetual connection
        session.headers.update(self.HEADER)
        self.__session = session
        self.numDownloads = 0

        # deploy threads at most the maximum number of open file descriptors
        # permitted to the user at a time by the operating system
        per_thread = max(per_thread, resource.getrlimit(resource.RLIMIT_NOFILE)[0])

        threads = []
        startTime = time.time()
        for start in range(self.LOWERBOUND, self.UPPERBOUND, per_thread):
            thread = threading.Thread(target=self._thread, args=(start, start + per_thread))
            threads.append(thread)
            thread.start()

        for thread in threads: thread.join()

        self.__session.close()
        return round(time.time() - startTime, 2)

    def _thread(self, start: int, stop: int) -> None:
        """
        Target method for threading; download a given set of
        images from start to stop
        """

        for url in self.generate_links(start, stop):
            self._download(url)

    def generate_links(self, start: int, stop: int) -> None:
        " Convenience method for getting formatted links "

        for i in range(start, stop):
            yield self.URLFMT.format(id=i)

    def _download(self, url: str) -> str:
        """
        Method to check and download given link
        Return image path if save successful, None otherwise
        """

        try:
            logging.debug(f'{url = }')
            response = self.__session.get(url)
        except:                            # if network error or cant fetch
            return None
        if response.status_code == requests.codes.ok:   # if image received
            imagename = os.path.join(self.downloadDir, os.path.basename(url))
            with open(imagename, 'wb') as imageFile:
                for chunk in response.iter_content(self.CHUNKSIZE):
                    imageFile.write(chunk)

            with self.mutex:               # make downloads thread-safe
                self.numDownloads += 1
            return imagename


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('directory', nargs='?', default=os.curdir, help='Download Directory')
    parser.add_argument('--upper',      default=None, type=int, help='Upper bound of numeric url to try')
    parser.add_argument('--lower',      default=None, type=int, help='Lower bound of numeric url to try')
    parser.add_argument('--per_thread', default=20,   type=int, help='Number of images per thread')

    args = parser.parse_args()

    downloader = Downloader(args.upper, args.lower)
    time_taken = downloader(args.directory, args.per_thread)
    print(f'Downloaded {downloader.numDownloads} images in "{os.path.abspath(args.directory)}", in {time_taken} seconds.')
