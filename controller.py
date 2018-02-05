import storage
from spider import spider
import threading
import time

REQUEST_WINDOW = 0  # sec of waiting period between requests
task = storage.task


class Controller:

    @staticmethod
    def thread_method(message):
        task.get_queue().put(message)
        while message.get_lock():
            time.sleep(0)

    @staticmethod
    def list_groups(name):
        message = storage.Message()
        message.set_type(task.get_storage().list_groups).set_args({'name': name})
        t = threading.Thread(target=Controller.thread_method, args=(message,))
        t.start()
        t.join()
        return message.get_re()

    @staticmethod
    def list_tools(name):
        message = storage.Message()
        message.set_type(task.get_storage().list_tools).set_args({'name': name})
        t = threading.Thread(target=Controller.thread_method, args=(message,))
        t.start()
        t.join()
        return message.get_re()

    @staticmethod
    def read_tool(name):
        message = storage.Message()
        message.set_type(task.get_storage().read_tool).set_args({'name': name})
        t = threading.Thread(target=Controller.thread_method, args=(message,))
        t.start()
        t.join()
        return message.get_re()

    @staticmethod
    def read_group(name):
        message = storage.Message()
        message.set_type(task.get_storage().read_group).set_args({'name': name})
        t = threading.Thread(target=Controller.thread_method, args=(message,))
        t.start()
        t.join()
        return message.get_re()

    @staticmethod
    def update_all():
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        cats = spider.get_cats(date)
        threads = []
        for cat in cats:
            time.sleep(REQUEST_WINDOW)
            # print("Cat: ", cat['name'])
            t = threading.Thread(target=Controller.update_cat, args=(cat['url'], cat['group_id']))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    @staticmethod
    def update_cat(cat_url, parent_id):
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        sub_cats = spider.get_sub_cats(cat_url, parent_id, date)
        threads = []
        for sub_cat in sub_cats:
            time.sleep(REQUEST_WINDOW)
            # print("    Sub Cat: ", sub_cat['name'])
            t = threading.Thread(target=Controller.update_sub_cat, args=(sub_cat['url'], sub_cat['group_id']))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    @staticmethod
    def update_sub_cat(sub_cat_url, parent_id):
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        groups = spider.get_group(sub_cat_url, parent_id, date)
        threads = []
        for group in groups:
            time.sleep(REQUEST_WINDOW)
            # print("        Group: ", group['name'])
            t = threading.Thread(target=Controller.update_group, args=(group['url'], group['group_id']))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

    @staticmethod
    def update_group(group_url, parent_id):
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        tools = spider.get_tool(group_url, parent_id, date)
        for tool in tools:
            # print("            tools: ", tool['name'])
            pass


controller = Controller()
