#!/usr/bin/env python3

# Scrape the Makaut AWS database for pictures
# https://ucanassessm.s3.ap-south-1.amazonaws.com/Latest/707616547717311/{id}/{id}.png
#
# UPPERBOUND = 520000
# LOWERBOUND = 406000

import os
from abc import ABC, abstractmethod
import threading
import logging
import time
import requests     # use `pip install --user requests` if ModuleNotFoundError

# Logging disabled, enable to debug
logging.basicConfig(level = logging.DEBUG,
                    # filename = __file__.replace('.py', '.log'),
                    # filemode = 'w',
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s',
                    datefmt = '%d/%m/%Y %I:%M:%S %p',
                )
logging.getLogger('urllib3.connectionpool').disabled = True
logging.disable(logging.CRITICAL)


class AbstractDownloader(ABC):
    try:
        import resource # works in *nix platforms
    except ImportError:
        resource = None

    MAX_OPEN_FILE = (resource.getrlimit(resource.RLIMIT_NOFILE)[0]
                     if resource # if resource got imported successfully
                     else 1200)  # a hardcoded value that works in Windows

    # static link with placeholders for id
    URLFMT = "https://ucanassessm.s3.ap-south-1.amazonaws.com/Latest/{server_id}/{id}/{id}.png"
    # fake app requests to server as client-side browser
    HEADER = {'user-agent':
              'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.28 Safari/537.36'}
    CHUNKSIZE = 10000000    # byte-size for image saving

    def __init__(self, server_id: int):
        self.server_id = server_id
        self.mutex = threading.Lock()
        self.numDownloads = 0

    @abstractmethod
    def run(self):
        pass

    def __call__(self, downloadDir: str = os.curdir, trace: bool = True) -> int:
        " Entry point; calls run defined in subclasses to start download "

        # create directory specified if not already present; potential permission exception
        logging.debug(f'{downloadDir = }')
        if not os.path.isdir(downloadDir):
            os.makedirs(downloadDir, exist_ok=True)
        self.downloadDir = downloadDir

        session = requests.Session()        # potential speedup requests by perpetual connection
        session.headers.update(self.HEADER)
        self.trace = trace
        self.__session = session
        self.numDownloads = 0
        startTime = time.time()

        self.run()

        self.__session.close()
        return round(time.time() - startTime, 2)

    def _download(self, url: str, name: str = None, savedir: str = None) -> str:
        """
        Method to check and download given link
        Return image path if save successful, None otherwise
        """

        try:
            response = self.__session.get(url)
        except:                            # if network error or cant fetch
            return None
        if response.status_code == requests.codes.ok:   # if image received
            imagename = (name if name
                         else os.path.basename(url) )
            savedir = savedir if savedir else self.downloadDir
            imagepath = os.path.join(savedir, imagename)
            logging.debug(f'{savedir = }')
            logging.debug(f'{imagename = }')
            logging.debug(f'{imagepath = }')

            with open(imagepath, 'wb') as imageFile:
                for chunk in response.iter_content(self.CHUNKSIZE):
                    imageFile.write(chunk)

            with self.mutex:               # make downloads thread-safe
                if self.trace:
                    print(f'Downloaded {imagename}')
                self.numDownloads += 1

            return imagepath


class Downloader(AbstractDownloader):
    """
    Downloader class that scrapes a static link with ranged id in a brute force attempt at
    gathering pictures; Use the function object as a functor
    """

    UPPERBOUND = 520000     # default id upperbound
    LOWERBOUND = 406000     # default id lowerbound

    def __init__(self, server_id: int, upperbound: int = None, lowerbound: int = None):
        if upperbound:
            self.UPPERBOUND = upperbound
        if lowerbound:
            self.LOWERBOUND = lowerbound

        AbstractDownloader.__init__(self, server_id)

    def __call__(self, *args, per_thread: int = 50, **kw) -> int:
        " Entry point; calls the function object to start download "

        # deploy threads at most the maximum number of open file descriptors
        # permitted to the user at a time by the operating system
        self.per_thread = max(per_thread, self.MAX_OPEN_FILE)
        AbstractDownloader.__call__(self, *args, **kw)

    def run(self) -> None:

        threads = []
        for start in range(self.LOWERBOUND, self.UPPERBOUND, self.per_thread):
            thread = threading.Thread(target=self._thread, args=(start, start + self.per_thread))
            threads.append(thread)
            thread.start()

        for thread in threads: thread.join()

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
            yield self.URLFMT.format(server_id=self.server_id, id=i)


class Watcher(AbstractDownloader):
    " Watch for images for given ids "
    WAIT_TIME = 20

    def __init__(self, server_id: str, watch_ids: list):
        self.watch_ids = watch_ids
        AbstractDownloader.__init__(self, server_id)

    def run(self):
        loop_id = 1
        logging.debug(f'{self.watch_ids = }')
        while True:
            try:
                for watch_id in self.watch_ids:
                    thread = threading.Thread(target=self._thread, args=(watch_id, loop_id))
                    thread.daemon = True
                    thread.start()
                time.sleep(self.WAIT_TIME)
                loop_id += 1
            except KeyboardInterrupt:
                break

    def _thread(self, watch_id: int, loop_id: int) -> None:
        logging.debug(f'{self.downloadDir = }')
        savedir = os.path.join(self.downloadDir, str(watch_id))
        if not os.path.isdir(savedir):
            os.makedirs(savedir, exist_ok=True)
        imagename = f'{watch_id}_{loop_id}.png'
        self._download(self.URLFMT.format(server_id=self.server_id, id=watch_id),
                       imagename, savedir)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('server_id', help='Unique server id for the day')
    parser.add_argument('directory', nargs='?', default='Makaut', help='Download Directory')
    parser.add_argument('--watch',   nargs='+', type=int, help='Watch and keep saving given id(s)')
    parser.add_argument('-u', '--upper',  default=None,  type=int, help='Upper bound of numeric url to try')
    parser.add_argument('-l', '--lower',  default=None,  type=int, help='Lower bound of numeric url to try')
    parser.add_argument('-s', '--silent', default=False, action='store_true', help='Disable download messages')
    parser.add_argument('--per_thread',   default=20,    type=int, help='Number of images per thread')

    args = parser.parse_args()
    logging.info(f'{args = }')

    if args.watch:
        downloader = Watcher(args.server_id, args.watch)
        time_taken = downloader(args.directory, not args.silent)
    else:
        downloader = Downloader(args.server_id, args.upper, args.lower)
        time_taken = downloader(args.directory, not args.silent, per_thread=args.per_thread)

    print('\n\n' + ' Stats '.center(25, '*') + '\n')
    print('Downloaded %d images in "%s", in %d seconds.' % (
        downloader.numDownloads, os.path.abspath(args.directory), time_taken
    ))
