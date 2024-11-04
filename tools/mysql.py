import pymysql.cursors
import os


def get_conn():
    connection = pymysql.connect(host=os.getenv("HOST"),
                                 port=int(os.getenv("PORT")),
                                 user=os.getenv("USER"),
                                 password=os.getenv("PWD"),
                                 database=os.getenv("DB_NAME"),
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection


def get_binlog_info():
    conn = get_conn()

    with conn.cursor() as cursor:
        sql = 'show master status;'
        cursor.execute(sql)
        conn.commit()
        t = cursor.fetchone()
        return t['File'], t['Position']


def fetch_one(sql: str):
    conn = get_conn()

    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
        t = cursor.fetchone()
        return t


def fetch(sql: str):
    conn = get_conn()

    with conn.cursor() as cursor:
        cursor.execute(sql)
        conn.commit()
        t = cursor.fetchall()
        return t
