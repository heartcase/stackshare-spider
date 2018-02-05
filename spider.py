# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import bs4
import storage
import time

task = storage.task


class Spider:

    @staticmethod
    def get_html(url: str) -> str:
        r = requests.get(url)
        try:
            r.raise_for_status()
        except Exception as e:
            print(e)
        return r.content.decode('utf-8').replace(u'\xa9', u'')

    @staticmethod
    def get_cats(date):
        html = Spider.get_html("https://stackshare.io/categories/")
        soup = BeautifulSoup(html, 'lxml')
        cats = soup.find_all("div", class_="layer-name-cat-index")
        results = []
        messages = []
        for each in cats:
            url = each.parent.get('href').replace('/', '')
            name = each.string
            result = {'name': name, 'url': url, 'parent_id': 'NULL', 'date': date}
            message = storage.Message()
            message.set_type(task.get_storage().update_group).set_args(result)
            task.get_queue().put(message)
            messages.append(message)
        for message in messages:
            while message.get_lock():
                time.sleep(0)
            results.append(message.get_re())
        return results

    @staticmethod
    def get_sub_cats(cat_url: str, parent_id, date):
        html = Spider.get_html("https://stackshare.io/" + cat_url)
        soup = BeautifulSoup(html, 'lxml')
        cats = soup.find_all("div", class_="collapse in more-cats panel-collapse")[0].children
        results = []
        messages = []
        for each in cats:
            if type(each) is not bs4.NavigableString:
                url = each.get('href').replace('/', '')
                name = each.string
                result = {'name': name, 'url': url, 'parent_id': parent_id, 'date': date}
                message = storage.Message()
                message.set_type(task.get_storage().update_group).set_args(result)
                task.get_queue().put(message)
                messages.append(message)
        for message in messages:
            while message.get_lock():
                time.sleep(0)
            results.append(message.get_re())
        return results

    @staticmethod
    def get_group(cat_url: str, parent_id, date):
        html = Spider.get_html("https://stackshare.io/" + cat_url)
        soup = BeautifulSoup(html, 'lxml')
        cats = soup.find_all("div", class_="collapse more-cats panel-collapse")[0].children
        results = []
        messages = []
        for each in cats:
            if type(each) is not bs4.NavigableString:
                url = each.get('href').replace('/', '')
                name = each.string
                result = {'name': name, 'url': url, 'parent_id': parent_id, 'date': date}
                message = storage.Message()
                message.set_type(task.get_storage().update_group).set_args(result)
                task.get_queue().put(message)
                messages.append(message)
        for message in messages:
            while message.get_lock():
                time.sleep(0)
            results.append(message.get_re())
        return results

    @staticmethod
    def get_tool(sub_cat_url: str, parent_id, date):
        html = Spider.get_html("https://stackshare.io/" + sub_cat_url)
        soup = BeautifulSoup(html, 'lxml')
        cats = soup.find_all('div', class_='landing-stack-name')
        results = []
        messages = []
        for each in cats:
            url = each.a.get('href').replace('/', '')
            name = each.a.span.string
            rating = each.parent.find_all('div', class_='stackup-count')[0].string
            result = {'name': name, 'url': url, 'rating': rating, 'parent_id': parent_id, 'date': date}
            results.append(result)
            message = storage.Message()
            message.set_type(task.get_storage().update_tool).set_args(result)
            task.get_queue().put(message)
            messages.append(message)
        for message in messages:
            while message.get_lock():
                time.sleep(0)
        return results


spider = Spider()
