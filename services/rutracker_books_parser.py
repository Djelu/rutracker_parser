import asyncio
import aiohttp

import bs4.element
from bs4 import BeautifulSoup as BeautifulSoup
from services import database_service as db_service

from configs.proxy_auth_data import login as proxy_login, password as proxy_password, server as proxy_server
from configs.rutracker_auth_data import login_username, login_password, login


class Parser:
    def __init__(self, ids=None):
        self.session = {}
        self.login_url = "https://rutracker.org/forum/login.php"
        self.book_search_url = "https://rutracker.org/forum/tracker.php?f=2387"
        if ids is None:
            self.ids = sorted(db_service.get_ids(), key=int, reverse=True)
        else:
            self.ids = ids
        self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                          "Chrome/102.0.5005.62 Safari/537.36 "
        self.cookie = {"bb_guid": "xiSSh3NZkETu",
                       "bb_ssl": "1",
                       "bb_session": "0-47395776-dNcNUwZtfbJdkPMyVT72",
                       "_ym_uid": "16538481171066421974",
                       "_ym_d": "1653848117",
                       "_ym_isad": "2"}

    async def run(self):
        try:
            book_urls_list = await self.get_book_url_list()
        finally:
            for i in self.session:
                if not self.session[i].closed:
                    await self.session[i].close()
        book_urls_list = book_urls_list[0:2]
        try:
            books_data_list = await asyncio.gather(*[self.get_book_data(book_url) for book_url in book_urls_list])
        finally:
            for book_url in book_urls_list:
                if self.session[book_url] is not None and not self.session[book_url].closed:
                    await self.session[book_url].close()
        await self.export_to_db(books_data_list)

    async def export_to_db(self, books_data):
        if len(books_data) == 0:
            return print("done!")
        res = db_service.json_to_db(books_data)
        if isinstance(res, Exception):
            print("json_to_db exception:\n" + str(res))
        else:
            print("done!")

    async def get_book_url_list(self):
        return self.flatten([await self.get_book_page_urls(page_index) for page_index in range(1, 11)])

    async def get_page_content(self, book_url, session_id, close_session=False):
        header = {'user-agent': self.user_agent}
        if self.session.get(session_id) is None:
            session = aiohttp.ClientSession(cookies=self.cookie, headers=header)
            data = {'login_username': f'{login_username}',
                    'login_password': f'{login_password}',
                    'login': f'{login}'}
            async with session.post(self.login_url, data=data) as login_resp:
                if await login_resp.text():
                    self.session[session_id] = session
                else:
                    await session.close()
        async with self.session.get(session_id).get(book_url) as resp:
            content = await resp.text()
        if close_session and not self.session.get(session_id).closed:
            await self.session.get(session_id).close()
        return content

    def flatten(self, t):
        return [item for sublist in t for item in sublist]

    async def get_url(self, page_index):
        if page_index == 1:
            return self.book_search_url
        content = await self.get_page_content(self.book_search_url, page_index)
        soup = BeautifulSoup(content, "html.parser")
        elems = soup.find_all("a", {"class": "pg"})
        return f'https://rutracker.org/forum/{elems[page_index].attrs["href"]}'

    async def get_book_page_urls(self, page_index):
        content = await self.get_page_content(await self.get_url(page_index), page_index, True)
        soup = BeautifulSoup(content, "html.parser")
        table = soup.find("table", {"id": "tor-tbl"})
        elems = table.find_all("a", {"class": "bold"})
        result = list(map(
            lambda href: f"https://rutracker.org/forum/{href}",
            filter(
                lambda href: not any(href.split("?t=")[1] == id for id in self.ids),
                list(map(
                    lambda el: el.attrs['href'],
                    elems
                ))
            )
        ))
        return result

    async def get_book_data(self, book_page_url):
        book_data = {}
        content = await self.get_page_content(book_page_url, book_page_url, True)
        soup = BeautifulSoup(content, "html.parser")
        root_item = soup.find("div", {"class": "post_body"})
        if root_item is None:
            book_data["no_book"] = True
            return book_data
        book_data["no_book"] = False

        book_data["url"] = book_page_url
        book_data["book_name"] = root_item.find("span").text

        img = root_item.find("img", {"class": "postImg"})
        if img is None:
            img = root_item.find("var", {"class": "postImg"})
        if img is not None:
            if "post-img-broken" in img.attrs['class']:
                img_url = img.attrs['title']
            elif img.attrs.get('src') is not None:
                img_url = img.attrs['src']
            else:
                img_url = img.attrs['title']
            book_data["img_url"] = img_url

        book_page_id = book_page_url.split("?t=")[1]
        book_data["book_page_id"] = book_page_id

        table = soup.find("div", {"class": "post_wrap"}).find("table")
        if table is not None:
            a = table.find("a", {"data-topic_id": book_page_id})
            if a is not None:
                magnet_link = a.attrs['href']
                t = table.find("span", {"id": "tor-size-humn"})
                if t is not None:
                    tor_size = t.text
                    book_data["magnet_link"] = magnet_link
                    book_data["tor_size"] = tor_size

        matches = db_service.matches
        for key in matches.keys():
            book_data[key] = self.get_book_data_by_type(root_item, matches[key])

        return book_data

    def get_book_data_by_type(self, root_item, book_data_type):
        items = root_item.contents

        def cut_off_excess(str):
            if str.startswith(":"):
                str = str.replace(":", "", 1)
            return str.strip()

        for i in range(len(items)):
            item = items[i]
            if str.startswith(str.lstrip(item.text), book_data_type):
                if book_data_type == "Описание":
                    text = str.strip(item.text[10:])
                    if len(text) > 0:
                        val = text
                    else:
                        val = ""
                    for j in range(i + 1, len(items)):
                        it = items[j]
                        if type(it) == bs4.element.NavigableString:
                            val += it.text + "\n\n"
                        else:
                            if it.attrs.get("class") is not None and "post-hr" in it.attrs.get("class"):
                                break
                    return cut_off_excess(val)
                else:
                    return cut_off_excess(items[i + 1].text)
        return ""
