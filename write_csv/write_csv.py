import mysql.connector
from mysql.connector import Error
import csv
import pika



def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection


def write_to_csv(connection, n):
    """
    Запрашивает записи слов из базы данных, количество которых больше n. 
    Записывает записи слов из базы данных, количество которых больше n в csv файл.
    Удаляет записи слов из базы данных, количество которых больше n.
    """
    query = """
            SELECT words, file_name
            FROM words_counter
            WHERE num >= {}
            """.format(n)
    cursor = connection.cursor()
    
    try:
        cursor.execute(query)
        result = cursor.fetchall()

        # предобработка для записи
        data = []
        for rec in result:
            record = rec[1].split(';')[:-1]
            record.insert(0, rec[0])
            data.append(record)
            
        # Запись слов и имён файлов в csv файл
        with open('csv_files/file.csv', "a", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(data) 
            print('Слова записаны')     
               
    except Error as e:
        print(f"The error '{e}' occurred")
    
    # Удаление записей из базы данных
    try:
        query = """
        DELETE FROM words_counter
        WHERE num >= {}
        """.format(n)
        cursor.execute(query)
        connection.commit()
        print('удОлено')
    except Error as e:
        print(f"The error '{e}' occurred")
    
    
def callback(ch, method, properties, body):
    conn = create_connection("database", "root", "1", "words_database")
    print(body)
    write_to_csv(conn, body)
    
    
def start_consuming():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_filename', exchange_type='direct')
    channel.queue_declare(queue='write_csv')
    channel.queue_bind(exchange='direct_filename', queue='write_csv', routing_key='for_write_csv')
    print(' [*] Waiting for file. To exit press CTRL+C')
    channel.basic_consume(queue='write_csv', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()
    
    
if __name__ == "__main__":
    
    start_consuming()

        
        
    