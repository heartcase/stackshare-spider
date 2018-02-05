import os
import sqlite3

os.remove('my_test.db')
db = sqlite3.connect('my_test.db')
c = db.cursor()
c.execute("PRAGMA foreign_keys = ON;")
sql = '''
        CREATE TABLE TOOLS
       (
       TOOL_ID INTEGER PRIMARY KEY,
       TOOL_NAME TEXT UNIQUE,
       GROUP_ID INTEGER,
       RATING REAL,
       URL_ID INTEGER,
       DATE TEXT,
       FOREIGN KEY(GROUP_ID) REFERENCES GROUPS(GROUP_ID),
       FOREIGN KEY(URL_ID) REFERENCES URLS(URL_ID)
       )'''
c.execute(sql)
sql = '''CREATE TABLE GROUPS
       (
       GROUP_ID INTEGER PRIMARY KEY,
       GROUP_NAME TEXT UNIQUE,
       PARENT_ID INTEGER,
       URL_ID INTEGER,
       DATE TEXT,
       FOREIGN KEY(URL_ID) REFERENCES URLS(URL_ID)
       )'''
c.execute(sql)
sql = '''CREATE TABLE URLS
       (
       URL_ID INTEGER PRIMARY KEY,
       URL TEXT UNIQUE,
       DATE TEXT
       )'''
c.execute(sql)
db.commit()
db.close()
