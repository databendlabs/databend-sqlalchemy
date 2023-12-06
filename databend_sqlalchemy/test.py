from databend_sqlalchemy import connector


def test():
    conn = connector.connect(
        "databend://databend:databend@localhost:8000/default?sslmode=disable"
    )
    cursor = conn.cursor()
    cursor.execute(
        "select null as db, name as name, database as schema, if(engine = 'VIEW', 'view', 'table') "
        "as type from system.tables where database = 'default';"
    )
    # print(cursor.fetchone())
    print(cursor.fetchall())
    print(cursor.description)
    conn.close()


if __name__ == "__main__":
    test()
