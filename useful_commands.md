## Полезные команды

---
Команда для бэкапа базы из контейнера:
Образец:
```
Образец:
docker exec -i pg_container_name /bin/bash -c "PGPASSWORD=pg_password pg_dump --username pg_username database_name" > /desired/path/on/your/machine/dump.sql
Пример:
docker exec -i postgres /bin/bash -c "PGPASSWORD=123qwe pg_dump --username app movies_database" > $HOME/YandexPracticum/new_admin_panel_spr
int_3/etl/create_db/dump.sql
```

Команда для восстановления базы в контейнере:
Образец:
```
Образец:
docker exec -i pg_container_name /bin/bash -c "PGPASSWORD=pg_password psql --username pg_username database_name" < /path/on/your/machine/dump.sql
Пример:
docker exec -i postgres /bin/bash -c "PGPASSWORD=123qwe psql --username app movies_database" < $HOME/YandexPracticum/new_admin_panel_spr
int_3/etl/create_db/dump.sql
```

