import mysql.connector
import psycopg2


mysql_db = mysql.connector.connect(
    host='localhost',
    user=' root',
    password='root123',
    database='ikm_test'
)
sql_cursor = mysql_db.cursor()

poistgres_db = psycopg2.connect(
    host="localhost",
    database="ikm",
    user="root",
    password="root@123"
)
postgs_cursor = poistgres_db.cursor()

sql_cursor.execute("SHOW TABLES")
tables = sql_cursor.fetchall()

postgs_cursor.execute("SHOW TABLES")
post_tables = postgs_cursor.fetchall()
post_tables = [table[0] for table in post_tables]


for table in tables:
    table = table[0]
    if table not in post_tables:
        create_query = 'CREATE TABLE {} SELECT * FROM {}'.format(table, table)
        postgs_cursor.execute(create_query)
        poistgres_db.commit()
        
    else:
        coulms = sql_cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =" + table)
        coulms = [val[0]  for val in sql_cursor.fetchall()]
        data = 'SELECT * FROM {}'.format(table)
        sql_cursor.execute(data)
        sql_data = sql_cursor.fetchall()
        
        for item in sql_data:
            exist_query = 'SELECT * FROM {} WHERE EXISTS({})'.format(table, item)
            sql_cursor.execute(exist_query)
            rows=sql_cursor.fetchall()
            if not rows:
                insert_query = 'INSERT INTO {} ({}) VALUES({})'.format(table, coulms, item)
                postgs_cursor.execute(insert_query)
                poistgres_db.commit()


      
        









sql_cursor.close()
postgs_cursor.close()



