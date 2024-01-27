import mysql.connector
import os


def open_connection():
    mysql_password = os.environ.get('MYSQLPASS')

    try:
        stage_cnx = mysql.connector.connect(user='root', password=mysql_password,
                                            host='127.0.0.1', database='stage')

        if stage_cnx.is_connected():
            print("Connected to MySQL STAGE database")

        dwh_cnx = mysql.connector.connect(user='root', password=mysql_password,
                                          host='127.0.0.1', database='dwh')

        if stage_cnx.is_connected():
            print("Connected to MySQL DWH database")

    except mysql.connector.Error as err:
        print(err)
    return stage_cnx, dwh_cnx


def close_connecttion(cnx):
    if cnx.is_connected():
        cnx.close()
        print("MySQL connection closed")
