import pymysql.cursors
import os


def get_conn(cluster_type="source"):
    if cluster_type == "source":
        connection = pymysql.connect(host=os.getenv("SOURCE_HOST"),
                                    port=int(os.getenv("SOURCE_PORT")),
                                    user=os.getenv("SOURCE_USER"),
                                    password=os.getenv("SOURCE_PWD"),
                                    database=os.getenv("SOURCE_DB_NAME"),
                                    cursorclass=pymysql.cursors.DictCursor)
    else:
        connection = pymysql.connect(host=os.getenv("TARGET_HOST"),
                                    port=int(os.getenv("TARGET_PORT")),
                                    user=os.getenv("TARGET_USER"),
                                    password=os.getenv("TARGET_PWD"),
                                    database=os.getenv("TARGET_DB_NAME"),
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
