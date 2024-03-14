# airflow_clickhouse_test

# delete non-active image
    docker image prune 

# build 
    sudo docker compose up --build

# start scheduler
    docker ps
    sudo docker exec -it project-webserver-1 /bin/bash
    airflow scheduler

# steps
    1 - скачивание и распаковка файла с данными
    2 - фильтрация данных
    3 - деление на блоки
    4 - загрузка данных в ClickHouse

# sql_query.py
    Запрос, который выводит все "area"...

# что еще можно сделать 
    сделать в дату и добавить партиции по дате