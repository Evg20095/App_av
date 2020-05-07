Предпосылки
=========

В parser/Dockerfile нужно установить занчение n \
\
В error_handler/Dockerfile нужно установить значения: \
sender_email — почта для отправки писем \
sender_password — пароль от почты для отправки \
receiver_email — почта для приёма писем


Обработка ошибок
=========
В случае если ен удаётся подключить к базе данных:\
docker exec -it db_container_id bash\
mysql -u root -p\
CREATE USER 'root'@'%' IDENTIFIED BY '1';\
GRANT SELECT ON *.* TO 'root'@'%';
