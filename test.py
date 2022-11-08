import connector


def test():
    cursor = connector.connect('http://root:@localhost:8081').cursor()
    cursor.execute('SELECT * FROM test')
    # print(cursor.fetchone())
    # print(cursor.fetchall())

    for i in cursor.next():
        print(i)


if __name__ == '__main__':
    test()
