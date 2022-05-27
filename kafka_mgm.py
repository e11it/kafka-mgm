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
import copy
import logging
import os
import re
import sys
from re import Pattern
from typing import Dict, List, Optional

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import (
    AdminClient,
    ClusterMetadata,
    ConfigEntry,
    ConfigResource,
    ConfigSource,
)
from confluent_kafka.schema_registry import SchemaRegistryError
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from dotenv import load_dotenv
from pyaml_env import parse_config

load_dotenv()

format_def = logging.Formatter("%(asctime)s %(name)s %(levelname)s:%(message)s")
logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MaskRule:
    def __init__(self, obj: Dict):
        if "mask" not in obj:
            raise ValueError('Key "mask" is not present in {}'.format(obj))
        self.mask_str: str = obj["mask"]
        self.mask: Pattern = re.compile(self.mask_str)
        self.config: Dict = self._get_config(obj)

    @staticmethod
    def _get_config(obj: Dict) -> Dict:
        config = {}
        for k, v in obj.items():
            if k == "mask":
                continue
            logger.debug("Set mask={} config={}".format(obj.get("mask"), k))
            config[k] = v
        return config


class SR:
    # Хранит информацию об имеющихся схемах кластера и позволяет производить операции над ними
    def __init__(self, config: Dict):
        if config is None:
            raise ValueError("Config is not found")
        self.conn_config: Dict = config
        self.admin_client: SchemaRegistryClient = self.get_sr_client(config)
        self.subjects: List[str] = self.admin_client.get_subjects()
        logger.debug(
            "loaded {} schema subjects from schema registry".format(len(self.subjects))
        )

    @staticmethod
    def get_sr_client(config: Dict) -> SchemaRegistryClient:
        return SchemaRegistryClient(config)

    def delete_topic_schemas(self, topic_name: str, permanent: bool = True):
        # Удаляем схемы для ключа и значения
        if f"{topic_name}-key" in self.subjects:
            versions = self.admin_client.delete_subject(f"{topic_name}-key", permanent=permanent)
            logger.debug(f"deleted schema {topic_name}-key versions: {*versions,}")
            self.subjects.remove(f"{topic_name}-key")

        if f"{topic_name}-value" in self.subjects:
            versions = self.admin_client.delete_subject(f"{topic_name}-value", permanent=permanent)
            logger.debug(f"deleted schema {topic_name}-value versions: {*versions,}")
            self.subjects.remove(f"{topic_name}-value")


class Cluster:
    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/_modules/confluent_kafka/schema_registry/schema_registry_client.html
    def __init__(self, obj: Dict):
        if obj is None:
            raise ValueError("No cluster settings are found")
        if obj.get("config") is None:
            raise ValueError('Cluster connection "config" is required')

        self.config: Dict = obj["config"]
        self.config.setdefault('group.id', 'group-id')

        self.admin_client: AdminClient = self.create_admin_client(self.config)
        self.consumer: Consumer = self.create_consumer(self.config)
        self.dry_run: bool = obj.get("dry_run", False)
        self._delete_invalid_topics: bool = obj.get("delete_invalid_topics", False)
        self._delete_empty_topics: bool = obj.get("delete_empty_topics", False)

        self.sr_client: Optional[SR] = None
        if obj.get("schema_registry"):
            self.sr_client = SR(obj["schema_registry"])

        self.validate_regexp: Optional[Pattern] = None
        if obj.get("validate_regexp"):
            self.validate_regexp = re.compile(obj["validate_regexp"])

        self.masks: List = []
        for mask in obj.get("masks_settings", []):
            logger.debug("=+= {}".format(mask))
            self.masks.append(MaskRule(mask))

        self.topics: Topics = self.load_existing_topics()

    @staticmethod
    def create_admin_client(config: Dict) -> AdminClient:
        return AdminClient(config)

    @staticmethod
    def create_consumer(config: Dict) -> Consumer:
        return Consumer(config)

    def load_schemas_from_sr(self):
        if self.sr_client is None:
            raise SchemaRegistryError(
                http_status_code=404,
                error_code=404,
                error_message="Schema Registry is not found: {}".format(self.sr_client),
            )
        subjects = self.sr_client.subjects
        for subject in subjects:
            logger.info(subject)

    def load_existing_topics(self):
        metadata = self.admin_client.list_topics(timeout=60)
        topics = Topics(metadata)
        logger.debug("Loaded metadata for {} topics".format(len(metadata.topics)))

        if self.validate_regexp is not None:
            topics.validate_names(self.validate_regexp)

        fs = self.admin_client.describe_configs(topics.resources)
        # Wait for operation to finish.
        for res, f in fs.items():
            try:
                config = f.result()
                topics.set_config_for_topic(res.name, config)
            except KafkaException as e:
                logger.fatal("Failed to describe {}: {}".format(res, e))

        return topics

    def _delete_topics(self, topics: List[str]):
        if self.dry_run:
            for topic in topics:
                logger.info(f"[dry run: soft delete topic] {topic}")
            return

        _obj = self.admin_client.delete_topics(topics, operation_timeout=60)
        for topic, f in _obj.items():
            try:
                f.result()  # The result itself is None
                logger.info(f"[deleted] {topic}")
            except Exception as e:
                logger.fatal(f"Failed to delete topic {topic}: {e}")

    def delete_invalid_topics(self):
        if not self._delete_invalid_topics:
            return

        invalid_topics = self.topics.invalid_topics
        logger.info("Delete topics with invalid names")
        self._delete_topics(invalid_topics)

    def delete_empty_topics(self):
        if not self._delete_empty_topics:
            return

        if self.consumer is None:
            raise KafkaException(
                "Kafka consumer is not found: {}".format(self.consumer)
            )
        if self.sr_client is None:
            raise SchemaRegistryError(
                http_status_code=404,
                error_code=404,
                error_message="Schema Registry is not found: {}".format(self.sr_client),
            )

        empty_topics = self.topics.get_empty_topics(self.consumer)
        if not empty_topics:
            return
        logger.info("Delete empty topics")
        self._delete_topics(empty_topics)
        logger.info("Delete empty topics schemas")
        for topic_name in empty_topics:
            self.sr_client.delete_topic_schemas(topic_name)

    """ Применяет настройки кластера для топиков"""

    def apply_cluster_configs(self):
        self.topics.apply_masks(self.masks)
        topics = self.topics.get_valid_topics
        for topic in topics:
            resource = topic.resource_with_config()
            if resource is None:
                continue

            logger.debug(
                "Will be updated: {} with {}".format(
                    topic.name, resource.set_config_dict
                )
            )
            if self.dry_run:
                continue

            obj = self.admin_client.alter_configs([resource])
            # Wait for operation to finish.
            for res, f in obj.items():
                try:
                    f.result()  # empty, but raises exception on failure
                    logger.info("{} configuration successfully altered".format(res))
                except Exception as e:
                    logger.error(e)


class Topic:
    def __init__(self, name: str, partition_count: int = 1):
        self.name: str = name
        self.partition_count: int = partition_count
        self.options: Dict = {}
        self.is_valid_name: bool = False
        self._config: Dict[str, ConfigEntry] = {}
        self._custom_config: Dict = {}
        self._need_cfg_update: bool = False
        self.format: Optional[str] = None

    @property
    def resource(self) -> ConfigResource:
        return ConfigResource("TOPIC", self.name)

    def resource_with_config(self) -> Optional[ConfigResource]:
        config = self.merged_config()
        if config is None:
            return None
        return ConfigResource("TOPIC", self.name, set_config=config)

    @property
    def config(self) -> Dict:
        return copy.deepcopy(self._config)

    @config.setter
    def config(self, config: Dict[str, ConfigEntry]):
        if self._config:
            logger.exception("Config override")
            return

        self._config = copy.deepcopy(config)
        if logger.isEnabledFor(level=logging.DEBUG):
            logger.debug("Topic: {}".format(self.name))
            for k, v in self._config.items():
                # Выводим измененные параметры для топика
                if (
                    not v.is_default
                    and v.source == ConfigSource.DYNAMIC_TOPIC_CONFIG.value
                ):
                    logger.debug(f"\tcfg> {k} = {v.value}")

    def need_cfg_update(self) -> bool:
        for k, v in self.merged_config().items():
            if (
                k in self._config
                and self._config[k].value == v
                and not self._config[k].is_default
            ):
                continue
            return True

        return False

    @property
    def custom_config(self):
        return self._custom_config

    @custom_config.setter
    def custom_config(self, config: Dict):
        if self._custom_config is None:
            self._custom_config = copy.deepcopy(config)
            return

        for k, v in config.items():
            if k in self._custom_config and logger.isEnabledFor(level=logging.DEBUG):
                logger.debug(
                    "Topic: {}. Overwrite setting {}: {} -> {}".format(
                        self.name,
                        k,
                        self._custom_config[k],
                        v,
                    )
                )
            self._custom_config[k] = str(v)

    @property
    def config_delta(self):
        return {
            k: v
            for k, v in self._custom_config.items()
            if k in self._config and str(self._config[k].value) == str(v)
        }

    @property
    def dynamic_config(self):
        return {
            k: v.value
            for k, v in self._config.items()
            if not v.is_default and v.source == ConfigSource.DYNAMIC_TOPIC_CONFIG.value
        }

    def merged_config(self, allow_manual: bool = False) -> Optional[Dict]:
        """Функция возвращает новый конфиг из переданного custom_config.
        Если установлен флаг allow_manual, добавляются параметры из config.
        custom_config должен полностью перезаписать config.

        Args:
            allow_manual: Если True, то к custom_config добавляются параметры из custom.
            Если False, то применяются только переданные параметры custom_config. В этом случае
            все параметры, которых нет в custom_config, но есть в config, очистятся.

        Returns:
            None - если нечего изменять.
            Новый конфиг - если переданы новые настройки.
        """

        if not allow_manual:
            if self._custom_config == self.dynamic_config:
                return None
            return self._custom_config

        if not self._custom_config:
            return None

        # Получаем настройки, которые были выставлены не по умолчанию
        merged_config = self.dynamic_config
        merged_config.update(self._custom_config)
        if merged_config == self.dynamic_config:
            return None
        return merged_config

    def custom(self, obj: Dict):
        if obj is None:
            raise ValueError("No cluster settings found")
        self.name = obj["name"]
        self.format = obj["format"]
        for k, v in obj.items():
            if k not in ("name", "format"):
                self.options[k] = v

    def empty_topic(self, consumer: Consumer) -> bool:
        message_sum = 0

        try:
            for partition in range(self.partition_count):
                tp = TopicPartition(self.name, partition)
                offsets = consumer.get_watermark_offsets(tp)
                low_offset, high_offset = offsets
                diff = high_offset - low_offset
                message_sum += diff

                if not message_sum == 0:
                    return False
        except Exception as e:
                logger.fatal(f"Failed to get watermark. Topic: {self.name}, partition{partition}. Exception: {e}")

        return True


class Topics:
    def __init__(self, metadata: ClusterMetadata):
        self.topics: Dict[str, Topic] = {}
        sys_topics = re.compile("^_")
        for item in iter(metadata.topics.values()):
            # load all topics except systems(start with _)
            if not sys_topics.match(item.topic):
                self.topics[item.topic] = Topic(item.topic, len(item.partitions))

    @property
    def resources(self) -> List[ConfigResource]:
        return [topic.resource for topic in iter(self.topics.values())]

    @property
    def invalid_topics(self) -> List[str]:
        return [
            topic.name
            for topic in iter(self.topics.values())
            if not topic.is_valid_name
        ]

    @property
    def get_valid_topics(self) -> List[Topic]:
        return [topic for topic in iter(self.topics.values()) if topic.is_valid_name]

    def get_empty_topics(self, consumer: Consumer) -> List[str]:
        return [
            topic.name
            for topic in iter(self.topics.values())
            if topic.is_valid_name and topic.empty_topic(consumer)
        ]

    def set_config_for_topic(self, name: str, config: Dict):
        self.topics[name].config = config

    def load_config(self, obj: List[str]):
        if obj is None:
            logging.warning("No topics configuration found")
            return
        for topic in obj:
            self.topics[topic] = Topic(topic)

    def apply_masks(self, masks: List[MaskRule]):
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


if __name__ == "__main__":
    if "CLUSTER_NAME" in os.environ:
        cluster_name = os.getenv("CLUSTER_NAME")
    elif len(sys.argv) == 2:
        cluster_name = sys.argv[1]
    else:
        logging.error("Usage: {} <cluster_id>".format(sys.argv[0]))
        logging.error("\tor set CLUSTER_NAME env variable")
        sys.exit(1)

    logging.info(f"Cluster {cluster_name}")
    cluster = Cluster(parse_config(f"clusters/{cluster_name}/cluster.yml"))
    cluster.delete_invalid_topics()
    cluster.delete_empty_topics()
    cluster.apply_cluster_configs()
