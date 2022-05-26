from unittest.mock import MagicMock

import pytest

from tests.conftest import TEST_CONFIG
from confluent_kafka.admin import ConfigEntry,ConfigSource


class TestTopic:
    """Экземпляр класса Topic"""
    _discovered_config = {
            # Значение по дефолту
            "compression.type": ConfigEntry("compression.type","producer",
                                         is_default=True,source=ConfigSource.DEFAULT_CONFIG.value),
            # Измененная настройка на брокере
            "retention.bytes": ConfigEntry("retention.bytes","3221225472",
                                          is_default=False,source=ConfigSource.STATIC_BROKER_CONFIG.value),
            # Измененные значения для топика
            "segment.bytes": ConfigEntry("segment.bytes","1073741824",
                                         is_default=False,source=ConfigSource.DYNAMIC_TOPIC_CONFIG.value),
            "cleanup.policy": ConfigEntry("cleanup.policy","compact",
                                          is_default=False,source=ConfigSource.DYNAMIC_TOPIC_CONFIG.value),
    }

    def test_resource_with_config_none(self, topic, mocker):
        """Экземпляр метода resource_with_config"""

        mocker.patch("kafka_mgm.Topic.merged_config", return_value=None)
        assert topic.resource_with_config() is None

    def test_resource_with_config(self, topic, mocker):
        """Экземпляр метода resource_with_config"""

        mocker.patch("kafka_mgm.Topic.merged_config", return_value={"test": "test"})
        mocker.patch("kafka_mgm.ConfigResource.__new__", return_value="test")
        assert topic.resource_with_config() == "test"

    def test_config(self, topic):
        """Экземпляр метода config"""

        topic.config = TEST_CONFIG
        assert list(topic.config.keys()) == ["test"]
        topic.config = {"wrong": "wrong"}
        assert list(topic.config.keys()) == ["test"]

    def test_need_cfg_update_false(self, topic, mocker):
        """Экземпляр метода need_cfg_update"""

        mocker.patch("kafka_mgm.Topic.merged_config", return_value={"test": "test"})

        topic.config = TEST_CONFIG
        assert topic.need_cfg_update() is False

    def test_need_cfg_update_true(self, topic, mocker):
        """Экземпляр метода need_cfg_update"""

        mocker.patch("kafka_mgm.Topic.merged_config", return_value={"default": "default"})

        topic.config = TEST_CONFIG
        assert topic.need_cfg_update() is True

    def test_custom_config(self, topic):
        """Экземпляр метода custom_config"""

        topic.custom_config = {"test": "test"}
        assert topic.custom_config == {"test": "test"}
        topic.custom_config = {"test2": "test2"}
        assert topic.custom_config == {"test": "test", "test2": "test2"}

    def test_merged_config_empty(self, topic):
        """Экземпляр метода merged_config"""

        assert topic.merged_config(allow_manual=True) is None

    def test_merged_config_empty_delta(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test"}
        topic.config = TEST_CONFIG
        assert topic.merged_config(allow_manual=True) is None

    def test_merged_config_exist_delta(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test"}
        assert topic.merged_config(allow_manual=True) == {"test": "test"}

    def test_merged_config_false_manual(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test"}
        topic.config = TEST_CONFIG
        assert topic.merged_config() is None

    def test_merged_config_false_manual_and_exist_delta(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test"}
        assert topic.merged_config() == {"test": "test"}

    def test_merged_config_false_manual_and_great_custom_config(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test", "test2": "test2"}
        topic.config = {"test": MagicMock(is_default=False, source=1, value="test")}
        assert topic.merged_config() == {'test': 'test', 'test2': 'test2'}

    def test_merged_config_true_manual_and_great_custom_config(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test", "test2": "test2"}
        topic.config = {"test": MagicMock(is_default=False, source=1, value="test")}
        assert topic.merged_config(allow_manual=True) == {'test': 'test', 'test2': 'test2'}

    def test_merged_config_false_manual_and_great_config(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test"}
        topic.config = {
            "test": MagicMock(is_default=False, source=1, value="test"),
            "test2": MagicMock(is_default=False, source=1, value="test2"),
        }
        assert topic.merged_config() == {'test': 'test'}

    def test_merged_config_true_manual_and_great_config(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test"}
        topic.config = {
            "test": MagicMock(is_default=False, source=1, value="test"),
            "test2": MagicMock(is_default=False, source=1, value="test2"),
        }
        assert topic.merged_config(allow_manual=True) is None

    def test_merged_config_false_manual_and_both_configs_have_differ(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test", "test2": "test2"}
        topic.config = {
            "test": MagicMock(is_default=False, source=1, value="test"),
            "test3": MagicMock(is_default=False, source=1, value="test3"),
        }
        assert topic.merged_config() == {'test': 'test', 'test2': 'test2'}

    def test_merged_config_true_manual_and_both_configs_have_differ(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test", "test2": "test2"}
        topic.config = {
            "test": MagicMock(is_default=False, source=1, value="test"),
            "test3": MagicMock(is_default=False, source=1, value="test3"),
        }
        assert topic.merged_config(allow_manual=True) == {'test': 'test', 'test2': 'test2', 'test3': 'test3'}

    def test_merged_config_false_manual_and_both_configs_differ(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test"}
        topic.config = {
            "test1": MagicMock(is_default=False, source=1, value="test1"),
        }
        assert topic.merged_config() == {'test': 'test'}

    def test_merged_config_true_manual_and_both_configs_differ(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"test": "test"}
        topic.config = {
            "test1": MagicMock(is_default=False, source=1, value="test1"),
        }
        assert topic.merged_config(allow_manual=True) == {'test': 'test', 'test1': 'test1'}

    def test_custom(self, topic):
        """Экземпляр метода custom"""

        topic.custom({"name": "test", "format": "test", "test": "test"})
        assert topic.name == "test"
        assert topic.format == "test"
        assert topic.options == {"test": "test"}

    def test_custom_error(self, topic):
        """Экземпляр метода custom"""

        with pytest.raises(ValueError):
            topic.custom(None)

    def test_empty_topic(self, topic, consumer):
        """Экземпляр метода empty_topic"""

        assert topic.empty_topic(consumer) is False

    def test_empty_topic_true(self, topic, consumer):
        """Экземпляр метода empty_topic"""

        consumer.get_watermark_offsets.return_value = (1, 1,)
        assert topic.empty_topic(consumer) is True
