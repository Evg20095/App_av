import pika, sys, os, shutil, time


def emit_message(message, routing_key):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_filename', exchange_type='direct')
    channel.basic_publish(exchange='direct_filename', routing_key=routing_key, body=message)
    print(" [x] Sent %r" % (message))
    connection.close()


def init(func):
    """
    Декоратор для инициализации корутины
    """
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return wrapper


def find_file(next_coroutine):
    """
    Функция обходит рекурсивно обходит папку и передаёт путь до файла в корутину.
    """
    texts = []
    for root, subfolders, files in os.walk(os.getcwd() + '/container'):
        for file in files:
            next_coroutine.send(os.path.join(root, file))
    next_coroutine.close()


@init
def filter_file():
    """
    Если файл текстовые корутина перемещает файл в parser_container и отправляет путь в очередь for_parser
    Если файл не текстовы корутина перемещает файл в other_files и отправляет путь в очередь for_error_handler
    """
    text_extensions = ['txt']
    base_path = os.getcwd()
    try:
        while True:
            file_path = yield
            ext = file_path.split('.')[-1]
            
            if ext in text_extensions:
                new_file_path = shutil.move(file_path, base_path + '/parser_container/' + os.path.split(file_path)[-1])
                message = new_file_path
                emit_message(message, 'for_parser')
            else: 
                new_file_path = shutil.move(file_path, base_path + '/other_files/' + os.path.split(file_path)[-1])
                message = os.path.split(new_file_path)[-1]
                emit_message(message, 'for_error_handler')
            
    except GeneratorExit: 
        pass    # обязательно вернуться сюда и нормально описать исключение


def pipeline():
    text = filter_file()
    find_file(text)



if __name__ == "__main__":
    
    print( '  [*] Start producer. To exit press CTRL+C' )
    while True:
        for root, subfolders, files in os.walk(os.getcwd() + '/container'):
            if files:
                pipeline()
        time.sleep(5)