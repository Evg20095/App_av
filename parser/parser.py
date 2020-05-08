import pika, sys, os, time
import mysql.connector
from mysql.connector import Error



""" функции отправки сообщения для проверки db и записи в scv"""

def emit_message(message, routing_key):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_filename2', exchange_type='direct')
    channel.basic_publish(exchange='direct_filename2', routing_key=routing_key, body=message)
    print(" [x] Sent %r" % (message))
    connection.close()



""" стандартные функции для работы с базой данных"""

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

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_query(connection, query):
     cursor = connection.cursor()
     try:
         cursor.execute(query)
         connection.commit()
     except Error as e:
         print(f"The error '{e}' occurred")
    
def executemany_query(connection, query, values):
     cursor = connection.cursor()
     try:
         cursor.executemany(query, values)
         connection.commit()
         print("New words added")
     except Error as e:
         print(f"The error '{e}' occurred")



""" Составные функции """

def update_num_in_db(connection, word, number):
    """
    Функция обновляет количество слова в базе данных
    """    
    query = """
            UPDATE words_counter
            SET num = num + {0}
            WHERE words = '{1}'
            """.format(number, word)
    execute_query(connection, query)

def add_new_words(connection, new_words):
    """
    Функция добавляет новые слова из файла в базу данных
    """
    query = """
            INSERT INTO words_counter ( words, num, file_name )
            VALUES ( %s, %s, %s )
            """
    executemany_query(connection, query, new_words)

def add_file_name(connection, word, file_name):
    """
    Функция обновляет список файлов для слов.
    """
    query = """
            UPDATE words_counter
            SET file_name = CONCAT(file_name, '{0}' )
            WHERE words = '{1}'
            """.format(file_name, word)
    execute_query(connection, query)
    


"""функции для обработки файла"""

def get_words(path):
    
    """    
    Принимает путь до файла
    Возвращает список слов из файла
    
    """
    punctuation_marks = [',', '.', '?', '!', ':', ';', '"', "'"]
    with open(path) as file:
        text = file.read()
    text = text.replace("\n", " ")
    for mark in punctuation_marks:
        text = text.replace(mark, " ")
    text = text.lower()
    list_word = text.split()
    words = []
    for word in list_word:
        if word.isalpha():
            words.append(word)
    return words

def get_words_dict(words):
    
    """
    Принимает список слов
    Возвращает словарь с уникальными словами и их количеством    
    
    """
    words_dict = dict()
    for word in words:
        if word in words_dict:
            words_dict[word] = words_dict[word] + 1
        else:
            words_dict[word] = 1
    return words_dict




def callback(ch, method, properties, body):
    
    words = get_words(body)                               # получаем список уникальных слов
    words_dic = get_words_dict(words)                     # получаем словрь слов и их количество
    file_name = os.path.split(body)[-1].decode() + ';'    # название файла


    conn = create_connection("database", "root", "1", "words_database")
    query = """
            SELECT words
            FROM words_counter
            WHERE words IN {}
            """.format(tuple(words_dic.keys()))
    data = execute_read_query(conn, query)                # те слова, которые уже есть в базе данных [('apple', ), ...]


    # Обновляем количества слово и список файлов в db 
    for record in data:
        update_num_in_db(conn, record[0], words_dic[record[0]])
        add_file_name(conn, record[0], file_name)
    
    
    
    data_set = {word[0] for word in data}                 # {'apple', ...} слова из db в множество переводим
    words_items = words_dic.items()                       # [('apple', 5), ...] слова из файла в формат для передачи sql запроса перевидом
    new_words_for_db = [ (item[0], item[1], file_name) for item in words_items if item[0] not in data_set ]  # новые слова которых нет в базе данных [('apple', 5, b'file_1.txt'), ...]
    
    
    # добавляем новые слова в db
    add_new_words(conn, new_words_for_db)
    
    # отправляем сообщение в write_csv для записи файлов на csv
    emit_message(n, 'for_write_csv')

    
    conn.close()

def start_consuming():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_filename', exchange_type='direct')
    channel.queue_declare(queue='parser')
    channel.queue_bind(exchange='direct_filename', queue='parser', routing_key='for_parser')
    print(' [*] Waiting for file. To exit press CTRL+C')
    channel.basic_consume(queue='parser', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


if __name__ == "__main__":
     
    print('Start parser')
    n = str(sys.argv[1])
    print(n)
    while True:
            try:
                start_consuming()
            except:
                print('Ждём кролика')
                time.sleep(7)


    
