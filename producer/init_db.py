import mysql.connector
from mysql.connector import Error


def create_connection_to_server(host_name, user_name, user_password):
    """
    Создаёт connector для подключения к серверу с базами данных.
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


def create_connection(host_name, user_name, user_password, db_name):
    """
    Создаёт connector для подключение к базе данных db_name.
    """
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


def create_database(connection, query):

    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


def create_table_with_words(connection, query):
     cursor = connection.cursor()
     try:
         cursor.execute(query)
         connection.commit()
         print("Query executed successfully")
     except Error as e:
         print(f"The error '{e}' occurred")


if __name__ == "__main__":
    connection = create_connection_to_server("database", "root", "1")
    query = "CREATE DATABASE words_database"
    create_database(connection, query)
    connection.close()
    
    conn = create_connection("database", "root", "1", "words_database")
    query = """
            CREATE TABLE IF NOT EXISTS words_counter (
                id INT AUTO_INCREMENT, 
                words TEXT NOT NULL, 
                num INT, 
                file_name TEXT,
                PRIMARY KEY (id)
            ) ENGINE = InnoDB
            """
    create_table_with_words(conn, query)
    conn.close()