Управление топиками и их настройками
--------------------------------------------

Структура директорий:
```
clusters/
    <cluster id>/
        cluster.yml
        rest.yml     #todo
        topics.yml   #todo
```

Required Kafka ACL:
===================

`${topic_prefix}` -- cluster user topic prefix

```
User:kafka_services_prod@sre-topic-config,Topic,PREFIXED,${topic_prefix},Describe,Allow,*
User:kafka_services_prod@sre-topic-config,Topic,PREFIXED,${topic_prefix},DescribeConfigs,Allow,*
User:kafka_services_prod@sre-topic-config,Topic,PREFIXED,${topic_prefix},AlterConfigs,Allow,*
```

DEV
===

To start:

```bash
python3 -m venv venv
source ./venv/bin/activate
python -m pip install -r requirements.txt
```

Freeze dependencies
```bash
python -m pip freeze > requirements.txt
```

https://medium.com/swlh/python-yaml-configuration-with-environment-variables-parsing-77930f4273ac

TODO
====

- [ ] удаление пустых топиков(+ схем?[как опция])
- [ ] проверка размера топика и применение экстренных настроек, 
  если размер выше порога. Нету API =( (https://github.com/dpkp/kafka-python/issues/2163)
- [ ] удаление топиков с неправильным именем
- [ ] создание топиковx
- [ ] авторизация в кафка
- [ ] поменть параметры скопом
- [ ] dry run

Help
----

* [kafka admin api example](https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py)
* [logging best practice](https://www.datadoghq.com/blog/python-logging-best-practices/)
* https://banzaicloud.com/blog/kafka-acl/
* https://medium.com/@bigdataschool/борьба-со-сложностью-acl-настроек-в-apache-kafka-или-self-service-авторизации-в-booking-com-85dbbb5e06ff
* https://devshawn.com/blog/automating-kafka-topic-and-acl-mangement/
