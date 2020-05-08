FROM python:3.7.4

RUN mkdir -p /usr/app/
WORKDIR /usr/app/

COPY . /usr/app/

RUN pip install --no-cache-dir -r requirements.txt 


CMD sleep 1 && python write_csv.py

# (python parser.py n & python error_handler.py sender_email sender_password receiver_email & python producer.py)