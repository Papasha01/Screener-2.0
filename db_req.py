from mysql.connector import MySQLConnection, Error
from loguru import logger

def select_user_id(user_id):
    query = """SELECT * from user_id where user_id = %s"""
    try:
        conn = MySQLConnection(host="localhost", user="root", db="screener_v2_0")
        cursor = conn.cursor()
        cursor.execute(query, (user_id,))
        result = cursor.fetchall()
        if len(result) > 0:
            return(result[0])
        else:
            return(False)
    except Error as error:
        logger.error(error)
    finally:
        conn.close()

def select_all_user_id():
    query = """SELECT user_id from user_id """
    try:
        conn = MySQLConnection(host="localhost", user="root", db="screener_v2_0")
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result) > 0:
            return(result)
        else:
            return(False)
    except Error as error:
        logger.error(error)
    finally:
        conn.close()


def insert_user_id(user_id):
    query = """INSERT INTO user_id
        (user_id)
        VALUES (%s);"""
    try:
        conn = MySQLConnection(host="localhost", user="root", db="screener_v2_0")
        cursor = conn.cursor()
        cursor.execute(query, (user_id,))
        conn.commit()
    except Error as error:
        logger.error(error)
    finally:
        conn.close()    

