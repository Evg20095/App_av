import pika
import smtplib, ssl, sys

def send_message_to_email(sender_email, email_password, receiver_email, body):
    """
    Функция принимает аргументы указываемые в DockerFile и body из rabbitmq.
    Логинится под аккаунтом gmail и отправляет письмо.
    """
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    context = ssl.create_default_context ()
    message = """\
    Subject: Error handler

    Body: {} file is not a text file. Moved to a folder other_files """ .format(body)
    
    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, email_password)
            server.sendmail(sender_email, receiver_email, message)
            print('E-mail sent')
    except:
        print('e-mail not send. Restert handler with the correct login and password. ')


def callback(ch, method, properties, body):
    send_message_to_email(sender_email, email_password, receiver_email, body)


def start_consuming():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.exchange_declare(exchange='direct_filename', exchange_type='direct')
    channel.queue_declare(queue='error_handler')
    channel.queue_bind(exchange='direct_filename', queue='error_handler',
                       routing_key='for_error_handler')
    channel.basic_consume(queue='error_handler', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    
    print(' [*] Start error handler. To exit press CTRL+C')

    sender_email = sys.argv[1]
    email_password = sys.argv[2]
    receiver_email = sys.argv[3]

    # sender_email = input('Введите gmail почту для отправки писем: ')
    # password = input('Введите пароль: ')
    # receiver_email = input('Введите gmail почту для приёма писем: ')
    start_consuming()