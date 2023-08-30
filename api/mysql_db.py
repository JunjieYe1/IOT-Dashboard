import json
import pymysql
import decimal
import datetime

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

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
        print(sql)
        self.connect()
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            column_names = [desc[0] for desc in cursor.description]
            result = cursor.fetchall()

        return column_names, result

    # 获取所有表名
    def get_all_tables(self):
        tables = self.query("SHOW TABLES")
        table_list = [table[0] for table in tables]
        return table_list

    # 获取表的所有列名
    def get_table_columns(self, table_name):
        columns = self.query(f"SHOW COLUMNS FROM {table_name}")
        column_list = [column[0] for column in columns]
        return column_list

    # 获取表的所有数据，行数限制，降序排列，过滤条件
    def get_table_data(self, table_name, limit=100, order_by=None, where=None):
        sql = f"SELECT * FROM {table_name}"
        if where:
            sql += f" WHERE {where}"
        if order_by:
            sql += f" ORDER BY {order_by} DESC"
        if limit:
            sql += f" LIMIT {limit}"
        column_names, raw_data = self.query(sql)
        data = [dict(zip(column_names, row)) for row in raw_data]
        json_data = json.dumps(data, cls=DecimalEncoder, ensure_ascii=False)
        return json.loads(json_data)

    # 分组查询
    def get_table_data_group(self, table_name, group_by=None, where=None):
        sql = f"SELECT * FROM {table_name}"
        if where:
            sql += f" WHERE {where}"
        if group_by:
            sql += f" GROUP BY {group_by}"
        column_names, raw_data = self.query(sql)
        data = [dict(zip(column_names, row)) for row in raw_data]

        return data
