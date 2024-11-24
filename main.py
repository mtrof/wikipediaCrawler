import sys
import re
from urllib.request import urlopen
from html.parser import HTMLParser
import sqlite3
from threading import Lock
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

URL = "wikipedia.org"


class MyWikipediaPageParser(HTMLParser):
	def __init__(self):
		super().__init__()
		self._level = 0
		self._wrapper_tag = ""
		self._pattern = re.compile(r"^/wiki/(?!.*:).*")
		self._found_links = set()

	def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
		if self._level > 0 and tag == self._wrapper_tag:
			self._level += 1

		for name, value in attrs:
			if name == "id":
				if value == "bodyContent":
					self._level = 1
					self._wrapper_tag = tag

		if self._level and tag == "a":
			for name, value in attrs:
				if name == "href":
					if self._pattern.match(value):
						self._found_links.add(value)

	def handle_endtag(self, tag: str):
		if self._level > 0 and tag == self._wrapper_tag:
			self._level -= 1

	def get_found_links(self):
		return self._found_links


class DatabaseHandler:
	def __init__(self, db_name: str):
		self.connection = sqlite3.connect(db_name, check_same_thread=False)
		self.cursor = self.connection.cursor()
		self._create_table()

	def _create_table(self):
		self.cursor.execute("""CREATE TABLE IF NOT EXISTS links (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			link TEXT UNIQUE
		)""")
		self.connection.commit()

	def insert_link(self, link: str):
		try:
			self.cursor.execute("INSERT INTO links (link) VALUES (?)", (link,))
			self.connection.commit()
			return True
		except sqlite3.IntegrityError:
			return False

	def get_all_links(self):
		self.cursor.execute("SELECT link FROM links")
		return [row[0] for row in self.cursor.fetchall()]

	def close(self):
		self.cursor.close()
		self.connection.close()


class WikipediaCrawler:
	def __init__(self, start_url: str, max_depth: int, db_name: str, threads_count: int, timeout: int):
		self.start_url = start_url
		self.base_url = self.start_url[:self.start_url.find(URL) + len(URL)]
		self.max_depth = max_depth
		self.threads_count = threads_count
		self.timeout = timeout
		self.task_queue = Queue()
		self.db_handler = DatabaseHandler(db_name)
		self.db_lock = Lock()

	def crawl(self, url: str, depth: int):
		parser = MyWikipediaPageParser()

		with urlopen(url) as response:
			page_html = response.read().decode("utf-8")
			parser.feed(page_html)

		for link in parser.get_found_links():
			full_link = self.base_url + link
			with self.db_lock:
				is_unique = self.db_handler.insert_link(full_link)
			if depth < self.max_depth and is_unique:
				self.task_queue.put((full_link, depth + 1))

	def worker(self):
		while True:
			try:
				url, depth = self.task_queue.get(timeout=self.timeout)
			except Empty:
				break
			self.crawl(url, depth)
			self.task_queue.task_done()

	def run(self):
		self.db_handler.insert_link(self.start_url)
		self.task_queue.put((self.start_url, 1))
		with ThreadPoolExecutor(max_workers=self.threads_count) as executor:
			for _ in range(self.threads_count):
				executor.submit(self.worker)

		self.task_queue.join()

	def __del__(self):
		self.db_handler.close()


if __name__ == "__main__":
	input_url = sys.argv[1]
	crawler = WikipediaCrawler(
		start_url=input_url,
		max_depth=6,
		db_name="database.db",
		threads_count=10,
		timeout=5
	)
	crawler.run()

	for out in crawler.db_handler.get_all_links():
		print(out)
