#!/usr/bin/env python
# -*- coding: utf-8 -*-
##################################################
## Управление настройками топиков кластеров Kafka
##################################################
## Author: Ilya Makarov
## Version: 22.05.01
## Email: im@e11it.ru
## Status: DEV
##################################################
import os
import sys
import re
from pyaml_env import parse_config
from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka import KafkaException
import logging
from dotenv import load_dotenv

load_dotenv()


class MaskRule:
    def __init__(self, obj):
        self.config = dict()
        if 'mask' in obj:
            self.mask_str = obj['mask']
            self.mask = re.compile(self.mask_str)
        else:
            raise ValueError('mask attribute not present')
        for k, v in obj.items():
            if k not in 'mask':
                logger.debug("Set mask={} config={}".format(obj['mask'], k))
                self.config[k] = v


class SR:
    # Хранит информацию об имеющихся схемах кластера и позволяет производить операции над ними
    conn_config = None
    admin_client = None
    subjects = None

    def __init__(self, config):
        self.conn_config = config
        self.admin_client = SchemaRegistryClient(config)
        self.subjects = self.admin_client.get_subjects()
        logger.debug("loaded {} schema subjets from schema registry".format(len(self.subjects)))

    def delete_topic_schemas(self, topic_name, permanent=True):
        # Удаляем схемы для ключа и занчения
        if f"{topic_name}-key" in self.subjects:
            versions = self.admin_client.delete_subject(f"{topic_name}-key", permanent=permanent)
            logger.debug(f"deleted schema {topic_name}-key versions: {*versions,}")
            self.subjects.remove(f"{topic_name}-key")

        if f"{topic_name}-value" in self.subjects:
            versions = self.admin_client.delete_subject(f"{topic_name}-value", permanent=permanent)
            logger.debug(f"deleted schema {topic_name}-value versions: {*versions,}")
            self.subjects.remove(f"{topic_name}-value")


class Cluster:
    config = None
    masks = []
    topics = None
    admin_client = None
    sr_client = None  # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/_modules/confluent_kafka/schema_registry/schema_registry_client.html

    def __init__(self, obj):
        if obj is None:
            raise ValueError('No cluster settings found')
        if 'config' not in obj:
            raise ValueError('Cluster connection "config" is required')
        self.config = obj['config']
        self.create_admin_client()
        if 'schema_registry' in obj:
            self.sr_client = SR(obj['schema_registry'])

        if 'validate_regexp' in obj:
            self.validate_regexp = re.compile(obj['validate_regexp'])
        if 'masks_settings' in obj:
            for mask in obj['masks_settings']:
                logger.debug("=+= {}".format(mask))
                self.masks.append(MaskRule(mask))

        self.dry_run = False
        if 'dry_run' in obj:
            # self.dry_run = obj['dry_run'].lower()  in ("yes", "true", "t", "1")
            self.dry_run = obj['dry_run']

        self._delete_invalid_topics = False
        if 'delete_invalid_topics' in obj:
            self._delete_invalid_topics = obj['delete_invalid_topics']
        self.load_existing_topics()

    def create_admin_client(self):
        #logger.debug(self.config)
        self.admin_client = AdminClient(self.config)

    def load_schemas_from_sr(self):
        subjects = self.sr_admin_client.get_subjects()
        for e in subjects:
            print(e)

    def load_existing_topics(self):
        metadata = self.admin_client.list_topics(timeout=60)
        logger.debug("Loaded metadata for {} topics".format(len(metadata.topics)))
        self.topics = Topics(metadata)
        if self.validate_regexp is not None:
            self.topics.validate_names(self.validate_regexp)

        fs = self.admin_client.describe_configs(self.topics.resources)
        # Wait for operation to finish.
        for res, f in fs.items():
            try:
                config = f.result()
                self.topics.set_config_for_topic(res.name, config)
            except KafkaException as e:
                print("Failed to describe {}: {}".format(res, e))
            except Exception:
                raise

    def clean_sr(self):
        self.topics.topics

    def delete_invalid_topics(self):
        invalid_topics = self.topics.invalid_topics
        logger.info("Check for topics with invalid names")
        if self.dry_run or not self._delete_invalid_topics:
            for topic in invalid_topics:
                logger.info("[del:dry] {}".format(topic))
            return
        if not self._delete_invalid_topics:
            return
        _obj = self.admin_client.delete_topics(self.topics.invalid_topics, operation_timeout=60)
        for topic, f in _obj.items():
            try:
                f.result()  # The result itself is None
                logger.info("[deleted] {}".format(topic))
            except Exception as e:
                logger.error("Failed to delete topic {}: {}".format(topic, e))

    ''' Применяет настройки кластера для топиков'''
    def apply_cluster_configs(self):
        self.topics.apply_masks(self.masks)
        topics = self.topics.get_valid_topics
        for topic in topics:
            resource = topic.resource_with_config()
            if resource is None:
                continue

            logger.debug("Will be updated: {} with {}".format(topic.name, resource.set_config_dict))
            if self.dry_run:
                continue
            obj = self.admin_client.alter_configs([resource])
            # Wait for operation to finish.
            for res, f in obj.items():
                try:
                    f.result()  # empty, but raises exception on failure
                    logger.info("{} configuration successfully altered".format(res))
                except Exception as e:
                    logger.exception(e)


class Topic:
    def __init__(self, name: str, partitions=1):
        self.name = name
        self.partitions = partitions
        self.options = {}
        self.is_valid_name = False
        self._config = None
        self._custom_config = {}
        self._need_cfg_update = False

    @property
    def resource(self):
        return ConfigResource('TOPIC', self.name)

    def resource_with_config(self):
        config = self.merged_config()
        if config is None:
            return None
        return ConfigResource('TOPIC', self.name, set_config=config)

    @property
    def config(self) -> dict():
        return self._config.copy()

    @config.setter
    def config(self, config):
        if self._config is None:
            self._config = config.copy()
            if logger.isEnabledFor(level=logging.DEBUG):
                logger.debug("Topic: {}".format(self.name))
                for k, v in self._config.items():
                    # Выводим измененные параметры для топика
                    if not v.is_default and v.source == ConfigSource.DYNAMIC_TOPIC_CONFIG.value:
                        logger.debug("loaded cfg> {} = {}".format(k, v.value))

        else:
            logger.exception("Config override")

    def need_cfg_update(self) -> bool:
        for k, v in self.merged_config().items():
            if k in self._config and self._config[k].value == v and not self._config[k].is_default:
                continue
            return True

        return False

    @property
    def custom_config(self):
        return self._custom_config

    def merged_config(self, allow_manual=False) -> dict():
        # Функция возвращает конфиг/отличающиеся параметры между текущим и что ожидалось
        # allow_manual - разрешает наличие параметров, не описанных в конфигурации
        # Return: None - если нечего применять или новый конфиг

        if len(self._custom_config) == 0 and allow_manual:
            return None

        # Получаем настройки различающиеся с описанными
        config_delta = {}
        for k, v in self._custom_config.items():
            if k in self._config and str(self._config[k].value) == str(v):
                continue
            config_delta[k] = v

        if len(config_delta) == 0 and allow_manual:
            return None

        '''кастомные значения, установленные руками'''
        restore_default = False

        for k, entry in [(k, v) for k, v in self._config.items() if not v.is_default and v.source == ConfigSource.DYNAMIC_TOPIC_CONFIG.value ]:
            if k not in config_delta:
                if allow_manual:
                    logger.warning("[manual] topic {} {}: {}".format(self.name, k, entry.value))
                    config_delta[k] = entry.value
                elif k not in self._custom_config:
                    restore_default = True

        if len(config_delta) == 0 and not restore_default:
            return None

        config_delta.update(self._custom_config)
        return config_delta

    @custom_config.setter
    def custom_config(self, config: dict):
        if self._custom_config is None:
            self._custom_config = config
        else:
            for k, v in config.items():
                if k in self._custom_config and logger.isEnabledFor(level=logging.DEBUG):
                    logger.debug("Topic: {}. Overwrite setting {}: {} -> {}".format(
                        self.name,
                        k, self._custom_config[k], v
                    ))
                self._custom_config[k] = v

    def custom(self, obj):
        if obj is None:
            raise ValueError('No cluster settings found')
        self.name = obj['name']
        self.format = obj['format']
        for k, v in obj.items():
            if k not in ('name', 'format'):
                self.options[k] = v


class Topics:
    topics = {}

    def __init__(self, metadata):
        sys_topics = re.compile("^_")
        for item in iter(metadata.topics.values()):
            # load all topics except systems(start with _)
            if not sys_topics.match(item.topic):
                self.topics[item.topic] = Topic(item.topic, len(item.partitions))

    @property
    def resources(self) -> dict():
        return [topic.resource for topic in iter(self.topics.values())]

    @property
    def invalid_topics(self) -> dict():
        return [topic.name for topic in iter(self.topics.values()) if not topic.is_valid_name]

    @property
    def get_valid_topics(self) -> list:
        return [topic for topic in iter(self.topics.values()) if topic.is_valid_name]

    def set_config_for_topic(self, name: str, config: dict):
        self.topics[name].config = config

    def load_config(self, obj):
        if obj is None:
            logging.warning("No topics configuration found")
            return
        for topic in obj:
            self.topics.append(Topic(topic))

    def apply_masks(self, masks: dict()):
        for topic_name, topic in self.topics.items():
            for mask_rules in masks:
                if mask_rules.mask.match(topic_name):
                    topic.custom_config = mask_rules.config

    def validate_names(self, regexp: re):
        for name in self.topics:
            if regexp.match(name):
                self.topics[name].is_valid_name = True
            else:
                logger.warning("Invalid topic name: {}".format(name))


if __name__ == '__main__':
    format_def = logging.Formatter('%(asctime)s %(name)s %(levelname)s:%(message)s')
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    if 'CLUSTER_NAME' in os.environ:
        cluster_name = os.getenv('CLUSTER_NAME')
    elif len(sys.argv) == 2:
        cluster_name = sys.argv[1]
    else:
        logging.error("Usage: {} <cluster_id>".format(sys.argv[0]))
        logging.error("\tor set CLUSTER_NAME env variable")
        sys.exit(1)

    logging.info(f"Cluster {cluster_name}")
    cluster = Cluster(parse_config(f"clusters/{cluster_name}/cluster.yml"))
    cluster.delete_invalid_topics()
    cluster.apply_cluster_configs()

    # with open(cluster_name+'/topics.yml', 'r') as stream:
    #    topics = Topics(yaml.safe_load(stream))
