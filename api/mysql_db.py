import json
import pymysql

class Database:

    def __init__(self, db_uri):
        self.db_uri = db_uri
        self.conn = None

    def connect(self):
        if not self.conn:
            db_info = self.db_uri.split('/')
            db_engine = db_info[0]

            db_credentials = db_info[2].split('@')[0]
            db_username, db_password = db_credentials.split(':')

            db_location = db_info[2].split('@')[1].split('/')[0]
            db_host, db_port = db_location.split(':')

            db_name = db_info[3]

            self.conn = pymysql.connect(
                host=db_host,
                user=db_username,
                password=db_password,
                database=db_name,
                port=int(db_port)
            )

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def query(self, sql):
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()

        return result

    def get_all_tables(self):
        tables = self.query("SHOW TABLES")
        table_list = [table[0] for table in tables]
        return table_list
