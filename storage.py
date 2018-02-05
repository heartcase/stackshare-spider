""" Storage Module For StackShare.io Spider Project

Created by HeartCase

Version 2018/2/1

This module define the storage operation message, message queue,

storage abstract class and sqlite implementation storage

this module should be able read request of creating and updating data,

retrieve data, and should automatically remove the no-longer-existed data.

"""
import queue
import threading
import sqlite3
from abc import ABC, abstractmethod


class Message:
    """
    a data operation request message
    """
    def __init__(self):
        self._method = None
        self._args = None
        self._lock = True
        self._re = {}

    def set_type(self, method):
        self._method = method
        return self

    def set_args(self, args: dict ):
        self._args = args
        return self

    def do_action(self):
        self._re = self._method(**self._args)
        self._lock = False

    def get_lock(self):
        return self._lock

    def get_re(self):
        return self._re


class Storage(ABC):
    """
    Abstract class of storage
    """
    @abstractmethod
    def update_group(self, url=None, name=None, parent_id=None, date=None):
        pass

    @abstractmethod
    def update_tool(self, url=None, name=None, parent_id=None, date=None):
        pass

    @abstractmethod
    def read_group(self, name):
        pass

    @abstractmethod
    def read_tool(self, name):
        pass

    @abstractmethod
    def list_groups(self, name):
        pass

    @abstractmethod
    def list_tools(self, name):
        pass

    @abstractmethod
    def run(self):
        pass


class SqliteStorage(Storage):
    """
    SQLite version of implementation of Storage
    """
    def __init__(self, name):
        self._db_name = name
        self._db = None
        self._c = None
        # self._db = sqlite3.connect(name)
        # self._c = self._db.cursor()

    def run(self):
        self._db = sqlite3.connect(self._db_name)
        self._c = self._db.cursor()

    def update_group(self, url=None, name=None, parent_id=None, date=None):
        if url:
            self._has_group_url(url)
        else:
            self._has_group_name(name)
        read_row = self._c.fetchall()
        if len(read_row) is 0:
            # insert
            try:
                self._add_url(url=url, date=date)
                url_id = self._c.lastrowid
                self._add_group(name=name, url_id=url_id, parent_id=parent_id, date=date)
                self._db.commit()
                group_id = self._c.lastrowid
                result = {'group_id': group_id, 'name': name, 'parent_id': parent_id, 'url': url}
                return result
            except 'Exception':
                self._db.rollback()
                return None
        else:
            name = read_row[0][1]
            # update?
            sql = '''
            UPDATE GROUPS
            SET DATE=(?)
            '''
            self._c.execute(sql, (date,))
            self._db.commit()
            group_id = self.read_group(name)[0]
            result = {'group_id': group_id, 'name': name, 'parent_id': parent_id, 'url': url}
            return result

    def update_tool(self, url=None, name=None, parent_id=None, date=None, rating=None):
        if url:
            self._has_tool_url(url)
        else:
            self._has_tool_name(name)
        read_row = self._c.fetchall()
        if len(read_row) is 0:
            # insert
            try:
                self._add_url(url=url, date=date)
                url_id = self._c.lastrowid
                self._add_tool(name=name, url_id=url_id, parent_id=parent_id, date=date, rating=rating)
                self._db.commit()
                return self._c.lastrowid
            except Exception as e:
                print(e)
                print(name, parent_id)
                self._db.rollback()
                return None
        else:
            name = read_row[0][1]
            # update?
            sql = '''
            UPDATE TOOLS
            SET RATING=(?), DATE=(?)
            '''
            self._c.execute(sql, (rating, date))
            self._db.commit()
            return self.read_tool(name)[0]

    def read_group(self, name):
        sql = '''
        SELECT g.*
        FROM GROUPS g
        WHERE g.GROUP_NAME=(?)
        '''
        self._c.execute(sql, (name,))
        result = self._c.fetchone()
        self._db.commit()
        return result

    def read_tool(self, name):
        sql = '''
        SELECT *
        FROM TOOLS
        WHERE TOOLS.TOOL_NAME=(?)
        '''
        self._c.execute(sql, (name,))
        self._db.commit()
        return self._c.fetchone()

    def list_groups(self, name):
        if name != 'NULL':
            sql = '''
            SELECT g.*
            FROM GROUPS g, GROUPS h
            WHERE g.PARENT_ID=h.GROUP_ID AND h.GROUP_NAME=(?)
            '''
        else:
            sql = '''
            SELECT g.*
            FROM GROUPS g
            WHERE g.PARENT_ID=(?)
            '''
        self._c.execute(sql, (name,))
        self._db.commit()
        return self._c.fetchall()

    def list_tools(self, name):
        if name != 'NULL':
            sql = """
                 WITH TEMP(GROUP_ID, PARENT_ID, GROUP_NAME, URL_ID, DATE) AS(            
    
                    SELECT *
                    FROM GROUPS
                    WHERE GROUP_NAME=(?)
    
                    UNION ALL
    
                    SELECT g.*
                    FROM GROUPS g, TEMP t
                    WHERE g.PARENT_ID=t.GROUP_ID
                )
                SELECT DISTINCT TOOLS.*
                FROM TEMP, TOOLS
                WHERE TEMP.GROUP_ID=TOOLS.GROUP_ID
                """
            self._c.execute(sql, (name,))
        else:
            sql = """
            SELECT *
            FROM TOOLS
            """
            self._c.execute(sql)
        self._db.commit()
        return self._c.fetchall()

    def _has_group_url(self, url):
        sql = """
            SELECT GROUPS.*
            FROM GROUPS, URLS
            WHERE GROUPS.URL_ID=URLS.URL_ID AND URLS.URL=(?)
            """
        self._c.execute(sql, (url,))
        self._db.commit()

    def _has_group_name(self, name):
        sql = """
            SELECT *
            FROM GROUPS
            WHERE GROUPS.GROUP_NAME=(?)
            """
        self._c.execute(sql, (name,))
        self._db.commit()

    def _has_tool_name(self, name):
        sql = """
            SELECT *
            FROM TOOLS
            WHERE TOOLS.TOOL_NAME=(?)
            """
        self._c.execute(sql, (name,))
        self._db.commit()

    def _has_tool_url(self, url):
        sql = """
            SELECT TOOLS.*
            FROM TOOLS, URLS
            WHERE TOOLS.URL_ID=URLS.URL_ID AND URLS.URL=(?)
            """
        self._c.execute(sql, (url,))
        self._db.commit()

    def _add_group(self, name, url_id, parent_id, date):
        sql = """INSERT INTO GROUPS VALUES (Null, (?), (?), (?), (?))"""
        self._c.execute(sql, (name, parent_id, url_id, date))

    def _add_tool(self, name, url_id, parent_id, rating, date):
        sql = """INSERT INTO TOOLS VALUES (Null, (?), (?), (?), (?), (?))"""
        self._c.execute(sql, (name, parent_id, rating, url_id, date))

    def _add_url(self, url, date):
        sql = "INSERT INTO URLS VALUES (Null, (?), (?))"
        self._c.execute(sql, (url, date))


class StorageTask(threading.Thread):

    def __init__(self, q: queue.Queue, storage: Storage):
        super(StorageTask, self).__init__()
        self._queue = q
        self._stopped = False
        self._storage = storage

    def run(self):
        self._storage.run()
        self._stopped = False
        while True:
            message = self._queue.get(True)
            if self._stopped:
                break
            if message:
                self._queue.task_done()
                message.do_action()
            else:
                break

    def stop(self):
        self._stopped = True
        self._queue.put(None)

    def get_storage(self):
        return self._storage

    def get_queue(self):
        return self._queue


task = StorageTask(queue.Queue(), SqliteStorage('my_test.db'))
task.start()
