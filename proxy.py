import time
import concurrent
import re
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pipe
from multiprocessing import Process

import httpx
import uvicorn
import os
import json
import logging
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import StreamingResponse
from humanfriendly import parse_size
from tqdm import tqdm
from urllib.parse import unquote


class Source:
    @abstractmethod
    def get(self, begin, end):
        pass

    @abstractmethod
    def info(self):
        pass


class URLSource(Source):
    def __init__(self, url, headers, cookies, conns=8):
        self.url = url
        self.headers = headers
        self.cookies = cookies
        self.session = httpx.Client(
            limits=httpx.Limits(max_connections=conns, max_keepalive_connections=conns)
        )
        self.session.headers.update(self.headers)
        self.session.cookies.update(self.cookies)

    def get(self, begin, end):
        return self.session.get(
            self.url,
            headers={'Range': f'bytes={begin}-{end}'}
        ).content, begin, end

    def info(self):
        try:
            resp = self.session.get(self.url, headers={'Range': 'bytes=0-0'}, timeout=10)
            resp.raise_for_status()
            content_type = resp.headers['Content-Type']
            content_disposition = resp.headers.get('Content-Disposition')

            if content_disposition:
                match = re.search(r'filename\*=UTF-8\'\'(.+)', content_disposition)
                if match:
                    file_name = match.group(1)
                    file_name = unquote(file_name)
                else:
                    file_name = os.path.basename(self.url.split("?")[0]) or "downloaded_file"
                try:
                    file_name = unquote(file_name)
                except UnicodeEncodeError:
                    pass
            else:
                file_name = os.path.basename(self.url.split("?")[0]) or "downloaded_file"

            length = int(resp.headers['Content-Range'].split('/')[-1])
            return content_type, file_name, length

        except httpx.RequestError as e:
            logging.error(f"获取文件信息错误: {e}")
            raise
        except httpx.HTTPStatusError as e:
            logging.error(f"获取文件信息HTTP状态码错误: {e}")
            raise


class Selector:
    def __init__(self, targets):
        self.targets = targets

        def loop():
            while True:
                for target in targets:
                    yield target

        self.gen = loop()

    def select(self):
        return next(self.gen)


class SourceGroup(Source):
    def __init__(self, selector: Selector):
        self.selector = selector

    def get(self, begin, end):
        return self.selector.select().get(begin, end)

    def info(self):
        return self.selector.select().info()


class Spliter:
    def __init__(self, *, begin=None, end=None, length=None):
        if begin is not None and end is not None:
            self.begin = begin
            self.length = end - begin + 1
        elif length:
            self.begin = 0
            self.length = length
        else:
            raise Exception('切片器参数不全')

    def iter(self, *, split: int | str = '5M', begin=None, end=None):
        if isinstance(split, str):
            split = parse_size(split, True)
        if not begin:
            begin = self.begin
        if not end:
            end = self.begin + self.length - 1

        def gen():
            left, right = begin, begin + split - 1

            while right <= end:
                yield left, right
                left, right = right + 1, right + split
            if left <= end:
                yield left, end

        return gen()

    def sub_split(self, trunk: int | str = '10M'):
        if isinstance(trunk, str):
            trunk = parse_size(trunk, True)

        def gen():
            for begin, end in self.iter(split=trunk):
                yield Spliter(begin=begin, end=end)

        return gen()


def write_task(pipe, file_name):
    msg = pipe.recv()
    with open(file_name, "rb+") as f:
        while msg is not None:
            data, index = msg
            f.seek(index)
            f.write(data)
            msg = pipe.recv()
    pipe.send(None)


class URLProxy:
    def __init__(
            self,
            urls,
            headers=None,
            cookies=None,
            trunk: str | int = '8M',
            split: str | int = '1M',
            conns=8
    ):
        if cookies is None:
            cookies = dict()
        if headers is None:
            headers = dict()
        if isinstance(trunk, str):
            trunk = parse_size(trunk, True)
        self.trunk = trunk
        if isinstance(split, str):
            split = parse_size(split, True)
        self.split = split
        if isinstance(urls, list):
            self.source = SourceGroup(Selector([URLSource(url, headers, cookies, conns) for url in urls]))
            self.workers = conns * len(urls)
        else:
            self.source = URLSource(urls, headers, cookies, conns)
            self.workers = conns
        self.content_type, self.file_name, self.length = self.source.info()

    def stream(self, *, begin=None, end=None, split=None):
        if not begin:
            begin = 0
        if not end:
            end = self.length - 1
        if not split:
            split = self.split
        spliter = Spliter(begin=begin, end=end)
        executor = ThreadPoolExecutor(max_workers=self.workers)

        for future in concurrent.futures.as_completed(
                [executor.submit(self.source.get, b, e) for b, e in spliter.iter(split=split)]
        ):
            content, b, e = future.result()
            yield content, b, e

    def continuous_stream(self, begin=0):
        next_begin = begin
        while next_begin < self.length:
            end = min(next_begin + self.trunk, self.length) - 1
            yield from self.sorted_stream(begin=next_begin, end=end)
            next_begin = end + 1

    def sorted_stream(self, *, begin=None, end=None, trunk=None, split=None):
        if not begin:
            begin = 0
        if not end:
            end = self.length - 1
        if not trunk:
            trunk = self.trunk
        if not split:
            split = self.split
        spliter = Spliter(begin=begin, end=end)
        for l, r in spliter.iter(split=trunk):
            chunks = {}
            next_pos = l
            executor = ThreadPoolExecutor(max_workers=self.workers)
            try:
                futures = {}
                for b, e in Spliter(begin=l, end=r).iter(split=split):
                    futures[executor.submit(self.source.get, b, e)] = b
                for future in concurrent.futures.as_completed(futures):
                    chunk_data, chunk_begin, _ = future.result()
                    chunks[chunk_begin] = chunk_data
                    while next_pos in chunks:
                        data = chunks.pop(next_pos)
                        yield data
                        next_pos += len(data)
            finally:
                executor.shutdown(wait=False)

    def download(self):
        print('开始下载', self.file_name, '...')
        with open(self.file_name, "wb"):
            pass

        main, sub = Pipe()

        sub_task = Process(target=write_task, args=(sub, self.file_name))
        sub_task.start()
        tqdm_obj = tqdm(total=self.length + 100, unit_scale=True)
        for content, b, e in self.stream(split=self.split):
            main.send((content, b))
            tqdm_obj.update(len(content))
        main.send(None)
        main.recv()
        tqdm_obj.update(100)
        print('下载完成')

__version__ = "0.0.3"

_proxy_cache = {}
_CACHE_TTL = 300


def _get_cached(backend_url):
    entry = _proxy_cache.get(backend_url)
    if entry is None:
        return None
    last_access, proxy = entry
    if time.time() - last_access > _CACHE_TTL:
        try:
            proxy.source.session.close()
        except Exception:
            pass
        del _proxy_cache[backend_url]
        return None
    _proxy_cache[backend_url] = (time.time(), proxy)
    return proxy


def resolve_direct_url(backend_url, headers=None, timeout=15):
    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        if headers:
            client.headers.update(headers)
        resp = client.get(backend_url, headers={'Range': 'bytes=0-0'})
        resp.raise_for_status()
        return str(resp.url)


def create_app(trunk, split, conns, headers):
    app = FastAPI()

    @app.get("/")
    def root():
        return {"service": "Thunder-MT", "version": __version__}

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/stream")
    def stream(request: Request):
        backend_url = request.query_params.get('url')
        if not backend_url:
            raise HTTPException(status_code=400, detail="Missing 'url' parameter")

        proxy = _get_cached(backend_url)
        if proxy is None:
            try:
                direct_url = resolve_direct_url(backend_url, headers)
            except Exception as e:
                logging.error(f"解析直链失败: {e}")
                raise HTTPException(status_code=502, detail="无法解析后端地址")
            proxy = URLProxy(
                urls=direct_url, trunk=trunk, split=split, conns=conns, headers=headers
            )
            _proxy_cache[backend_url] = (time.time(), proxy)
        size = proxy.length

        range_str = request.headers.get("Range")
        if not range_str:
            logging.info(f"无 Range: 连续流 0→{size}, trunk={proxy.trunk}")
            return StreamingResponse(
                proxy.continuous_stream(begin=0),
                headers={
                    'Content-Type': proxy.content_type,
                    'Content-Length': str(size),
                    'Accept-Ranges': 'bytes',
                },
                status_code=200,
            )

        match = re.compile(r'bytes=(\d+)-(\d*)').match(range_str)
        begin, end = match.groups()
        begin = int(begin) if begin else 0
        if end:
            end = min(int(end), begin + proxy.trunk, size - 1)
            length = end - begin + 1
            logging.info(f"Range(B): {range_str} → begin={begin} end={end} length={length}")
            try:
                return StreamingResponse(
                    proxy.sorted_stream(begin=begin, end=end),
                    headers={
                        'Content-Range': f'bytes {begin}-{end}/{size}',
                        'Content-Type': proxy.content_type,
                        'Content-Length': str(length),
                        'Accept-Ranges': 'bytes',
                    },
                    status_code=206,
                )
            except Exception:
                raise HTTPException(status_code=404)
        else:
            logging.info(f"Range(U): {range_str} 连续流 {begin}→{size}")
            try:
                return StreamingResponse(
                    proxy.continuous_stream(begin=begin),
                    headers={
                        'Content-Range': f'bytes {begin}-{size - 1}/{size}',
                        'Content-Type': proxy.content_type,
                        'Content-Length': str(size - begin),
                        'Accept-Ranges': 'bytes',
                    },
                    status_code=206,
                )
            except Exception:
                raise HTTPException(status_code=404)

    return app


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    trunk = os.environ.get('TRUNK', '10M')
    split = os.environ.get('SPLIT', '1M')
    conns = int(os.environ.get('CONNS', '60'))
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', '8010'))
    headers = json.loads(os.environ.get('HEADERS', '{}'))

    app = create_app(trunk, split, conns, headers)
    logging.info(f"Thunder-MT v{__version__} 启动，监听 {host}:{port}")
    logging.info(f"配置: trunk={trunk} split={split} conns={conns}")
    uvicorn.run(app, host=host, port=port)
