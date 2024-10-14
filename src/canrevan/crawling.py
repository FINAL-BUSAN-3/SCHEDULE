import argparse
import asyncio
import warnings
from asyncio import Semaphore
from concurrent.futures import Executor, ProcessPoolExecutor
from typing import Callable, Dict, Iterable, List, Optional, TypeVar
import uuid
from datetime import datetime

from aiohttp import ClientSession, ClientTimeout

import utils as utils
from trino.dbapi import connect

# Ignore warnings from `aiohttp` module.
warnings.filterwarnings("ignore", module="aiohttp")

T = TypeVar("T")


class Crawler:
    def __init__(
        self,
        concurrent_tasks: int = 500,
        num_parsing_processes: int = 1,
        request_headers: Optional[Dict[str, str]] = None,
        request_timeout: Optional[float] = None,
    ):
        self.now_time = datetime.now().strftime("%Y-%m-%d")
        self.concurrent_tasks = concurrent_tasks
        self.num_parsing_processes = num_parsing_processes
        self.request_headers = request_headers
        self.request_timeout = request_timeout

    async def _fetch_and_parse(
        self,
        sem: Semaphore,
        pool: Executor,
        sess: ClientSession,
        url: str,
        include_reporter_name: bool,
        parse_fn: Optional[Callable[[str, str, bool], T]] = None,
    ) -> Optional[str]:
        try:
            async with sess.get(url) as resp:
                content = await resp.text()

            # Run `parse_fn` in subprocess from process-pool for parallelism.
            if parse_fn is not None:
                content = await asyncio.get_event_loop().run_in_executor(
                    pool, parse_fn, content, include_reporter_name
                )
        except Exception:
            content = None

        sem.release()
        return content

    async def _fetch_and_parse_with_url(
        self,
        sem: Semaphore,
        pool: Executor,
        sess: ClientSession,
        url: str,
        include_reporter_name: bool,
        parse_fn: Optional[Callable[[str, str, bool], T]] = None,
    ) -> Optional[str]:
        try:
            async with sess.get(url) as resp:
                content = await resp.text()

            # Run `parse_fn` in subprocess from process-pool for parallelism.
            if parse_fn is not None:
                content = await asyncio.get_event_loop().run_in_executor(
                    pool, parse_fn, content, include_reporter_name
                )
        except Exception:
            content = None

        sem.release()
        return [uuid.uuid1(), url, content]

    async def _crawl_and_reduce(
        self,
        urls: Iterable[str],
        include_reporter_name: bool,
        parse_fn: Optional[Callable[[str], T]] = None,
        callback_fn: Optional[Callable[[Optional[T]], None]] = None,
    ):
        # Create a semaphore to limit the number of concurrent tasks, a process-pool
        # executor to run `parse_fn` in parallel and a http client session for
        # asynchronous HTTP requests.
        sem = Semaphore(self.concurrent_tasks)
        pool = ProcessPoolExecutor(max_workers=self.num_parsing_processes)
        sess = ClientSession(
            headers=self.request_headers,
            timeout=ClientTimeout(total=self.request_timeout),
        )

        futures = []
        for url in urls:
            await sem.acquire()

            # Create a fetching future.
            f = asyncio.ensure_future(
                self._fetch_and_parse(sem, pool, sess, url, include_reporter_name, parse_fn)
            )

            # Add done-callback function to the future.
            if callback_fn is not None:
                f.add_done_callback(lambda f: callback_fn(f.result()))

            futures.append(f)

        # Wait for the tasks to be complete and close the http client session and
        # process-pool executor
        await asyncio.wait(futures)
        await sess.close()
        pool.shutdown(wait=True)


    async def _crawl_and_reduce_with_url(
        self,
        urls: Iterable[str],
        include_reporter_name: bool,
        parse_fn: Optional[Callable[[str], T]] = None,
        callback_fn: Optional[Callable[[Optional[T]], None]] = None,
    ):
        # Create a semaphore to limit the number of concurrent tasks, a process-pool
        # executor to run `parse_fn` in parallel and a http client session for
        # asynchronous HTTP requests.
        sem = Semaphore(self.concurrent_tasks)
        pool = ProcessPoolExecutor(max_workers=self.num_parsing_processes)
        sess = ClientSession(
            headers=self.request_headers,
            timeout=ClientTimeout(total=self.request_timeout),
        )
        futures = []
        for url in urls:
            await sem.acquire()

            # Create a fetching future.
            f = asyncio.ensure_future(
                self._fetch_and_parse_with_url(sem, pool, sess, url, include_reporter_name, parse_fn)
            )

            # Add done-callback function to the future.
            if callback_fn is not None:
                f.add_done_callback(lambda f: callback_fn(f.result()))

            futures.append(f)

        # Wait for the tasks to be complete and close the http client session and
        # process-pool executor
        await asyncio.wait(futures)
        await sess.close()
        pool.shutdown(wait=True)

    def reduce_to_array(
        self,
        urls: Iterable[str],
        include_reporter_name: bool,
        parse_fn: Optional[Callable[[str], T]] = None,
        update_fn: Optional[Callable[[], None]] = None,
    ) -> List[T]:
        # A callback function to reduce collected data to the array.
        def callback_fn(data: Optional[T]):
            if update_fn is not None:
                update_fn()

            if data is not None:
                results.append(data)

        # Get event loop and set to ignore `SSLError`s from `aiohttp` module.
        loop = asyncio.get_event_loop()
        utils.ignore_aiohttp_ssl_error(loop)

        results = []
        loop.run_until_complete(self._crawl_and_reduce(urls, include_reporter_name, parse_fn, callback_fn))

        return results

    def reduce_to_file(
        self,
        urls: Iterable[str],
        filename: str,
        include_reporter_name: bool,
        parse_fn: Optional[Callable[[str], T]] = None,
        update_fn: Optional[Callable[[], None]] = None,
    ) -> int:
        with open(filename, "w", encoding="utf-8") as fp:
            # A callback function to reduce collected data to the output file.
            def callback_fn(data: Optional[T]):

                if update_fn is not None:
                    update_fn()

                if data[2] is not None:
                    # Increase the counter which indicates the number of actual reduced
                    # items.
                    nonlocal written
                    written += 1

                    doc_id = str(data[0])
                    url = str(data[1])
                    section = str(data[1][-3:])
                    crawl_dt = self.now_time
                    media_com = str(data[2][0])
                    title = str(data[2][1])
                    contents = str(data[2][2])
                    partition_key = crawl_dt[:-3]

                    # fp.write(doc_id + "\t") # doc_id
                    # fp.write(url + "\t") # url
                    # fp.write(section + "\t") # section
                    # fp.write(crawl_dt + "\t") # crawl_dt
                    # fp.write(media_com + "\t") # media_com
                    # fp.write(title + "\t") # title
                    # fp.write(contents + "\t") # contents
                    fp.write(url + "\n")

                    try:
                        conn = connect(
                            host="opyter.iptime.org",
                            port=40000,
                            user="airflow",
                            catalog="dl_iceberg",
                            schema="stg",
                        )

                        cur = conn.cursor()
                        cur.execute(f"""
                        INSERT INTO dl_iceberg.ods.ds_naver_news VALUES('{doc_id}' ,'{url}', '{section}', '{crawl_dt}', '{media_com}', '{title.replace('"',"").replace("'", "")}', '{contents.replace('"',"").replace("'", "")}', '{partition_key}')
                        """)
                    except Exception as e:
                        print(e)


            # Get event loop and set to ignore `SSLError`s from `aiohttp` module.
            loop = asyncio.get_event_loop()
            utils.ignore_aiohttp_ssl_error(loop)

            written = 0
            loop.run_until_complete(self._crawl_and_reduce_with_url(urls, include_reporter_name, parse_fn, callback_fn))

        return written
