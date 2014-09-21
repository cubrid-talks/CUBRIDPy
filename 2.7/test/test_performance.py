# This code was adapted from the official CUBRID Python driver:
# http://www.cubrid.org/wiki_apis/entry/cubrid-python-driver
# See www.cubrid.org for more information.

import threading
import time
from time import ctime
from CUBRIDPy.CUBRIDConnection import CUBRIDConnection

class MyThread(threading.Thread):
    num_rows = 100

    def __init__(self, name = ''):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        self.conn = CUBRIDConnection()
        self.conn.connect()
        self.cur = self.conn.cursor()

        # start db operation using multiple threads
        if self.name == 'insert':
            self.insert_test()
        elif self.name == 'delete':
            self.delete_test()
        elif self.name == 'update':
            self.update_test()
        elif self.name == 'select':
            self.select_test()
        else:
            pass

        print "Test completed."

    def insert_test(self):
        print "Inserting data..."
        each_st_time = time.time()
        for n in range(self.num_rows):
            sql = "insert into tdb values(" + str(n) + ", 'systimestamp +  pefor', systimestamp, " + str(n) + ")"
            self.cur.execute(sql)
            if n % (self.num_rows / 10) == 0:
                print "%s done." % n

        each_ed_time = time.time()
        elapsed_time = each_ed_time - each_st_time
        print ("Operation is INSERT, and the elapsed time for insert is: " + '%f' % elapsed_time + " sec.")
        print

    def delete_test(self):
        print "Deleting data..."
        each_st_time = time.time()
        for n in range(self.num_rows):
            if (n == 0) or (n == 1):
                limit_num = 1
            else:
                limit_num = n * 100
            sql = "delete from tdb where a<" + '%d' % limit_num
            self.cur.execute(sql)
            if n % (self.num_rows / 10) == 0:
                print "%s done." % n

        each_ed_time = time.time()
        elapsed_time = each_ed_time - each_st_time
        print ("Operation is DELETE, and the elapsed time for one commit is: " + '%f' % elapsed_time + " sec.")
        print

    def update_test(self):
        print "Updating data..."
        each_st_time = time.time()
        for n in range(self.num_rows):
            sql = "update tdb set e = e + 10000000 where a=" + '%d' % n
            self.cur.execute(sql)
            if n % (self.num_rows / 10) == 0:
                print "%s done." % n

        each_ed_time = time.time()
        elapsed_time = each_ed_time - each_st_time
        print ("Operation is UPDATE, and the elapsed time for one commit is: " + '%f' % elapsed_time + " sec.")
        print

    def select_test(self):
        print "Selecting data..."
        each_st_time = time.time()
        for n in range(self.num_rows):
            if (n == 0) or (n == 1):
                limit_num = 2
            else:
                limit_num = (n + 1) * 100
            sql = "select * from tdb where a <" + '%d' % limit_num
            self.cur.execute(sql)
            if n % (self.num_rows / 10) == 0:
                print "%s done." % n

        each_ed_time = time.time()
        elapsed_time = each_ed_time - each_st_time
        print ("Operation is SELECT, and the elapsed time is: " + '%f' % elapsed_time + " sec.")
        print


def test_1_thread():
    conn = CUBRIDConnection()
    conn.connect()
    cur = conn.cursor()
    cur.execute('drop table if exists tdb')
    cur.execute('create table tdb(a int, b varchar(20), c timestamp, e int)')

    print 'starting one thread operation at:', ctime()
    t1 = MyThread('insert')
    t1.start()
    t1.join()
    t1 = MyThread('select')
    t1.start()
    t1.join()
    t1 = MyThread('update')
    t1.start()
    t1.join()
    t1 = MyThread('delete')
    t1.start()
    t1.join()
    time.sleep(1)
    cur.execute('drop table if exists tdb')
    conn.close()


def test_many_threads(threads_count):
    conn = CUBRIDConnection()
    conn.connect()
    cur = conn.cursor()
    cur.execute('drop table if exists tdb')
    cur.execute('create table tdb(a int, b varchar(20), c timestamp, e int)')

    print 'Starting threads for INSERT operations at: ', ctime()
    threads_insert = []
    for i in range(threads_count):
        t = MyThread('insert')
        threads_insert.append(t)
    for n in range(threads_count):
        threads_insert[n].start()
    for j in range(threads_count):
        threads_insert[j].join()
    print 'End insert!'

    print 'Starting threads for SELECT operations at: ', ctime()
    threads_select = []
    for i in range(threads_count):
        t1 = MyThread('select')
        threads_select.append(t1)
    for n in range(threads_count):
        threads_select[n].start()
    for j in range(threads_count):
        threads_select[j].join()
    print 'End select!'

    print 'Starting threads for UPDATE operations at: ', ctime()
    threads_update = []
    for i in range(threads_count):
        t2 = MyThread('update')
        threads_update.append(t2)
    for n in range(threads_count):
        threads_update[n].start()
    for j in range(threads_count):
        threads_update[j].join()
    print 'End update!'

    print 'Starting threads for DELETE operations at: ', ctime()
    threads_delete = []
    for i in range(threads_count):
        t3 = MyThread('delete')
        threads_delete.append(t3)
    for n in range(threads_count):
        threads_delete[n].start()
    for j in range(threads_count):
        threads_delete[j].join()
    print 'End delete!'

    cur.execute('drop table if exists tdb')
    conn.close()


if __name__ == '__main__':
    #test_1_thread()
    test_many_threads(3)
