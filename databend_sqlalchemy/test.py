from databend_sqlalchemy import connector


def test():
    cursor = connector.connect('http://root:@localhost:8081').cursor()
    cursor.execute("select null as db, name as name, database as schema, if(engine = 'VIEW', 'view', 'table') as type from system.tables where database = 'default';")
    # print(cursor.fetchone())
    print(cursor.fetchall())
    print(cursor.description)

    # for i in cursor.next():
    #     print(i)


if __name__ == '__main__':
    test()
