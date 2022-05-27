import pytest
from confluent_kafka.admin import ConfigEntry, ConfigSource


class TestTopic:
    """Экземпляр класса Topic"""

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

        assert list(topic.config.keys()) == ["compression.type", "retention.bytes", "segment.bytes", "cleanup.policy"]
        topic.config = {
            "flush.ms": ConfigEntry("flush.ms", "1000000",
                                    is_default=True, source=ConfigSource.DEFAULT_CONFIG.value)
        }
        assert list(topic.config.keys()) == ["compression.type", "retention.bytes", "segment.bytes", "cleanup.policy"]

    def test_need_cfg_update_false(self, topic):
        """Экземпляр метода need_cfg_update"""

        assert topic.need_cfg_update() is False

    def test_need_cfg_update_true(self, topic, mocker):
        """Экземпляр метода need_cfg_update"""

        mocker.patch("kafka_mgm.Topic.merged_config", return_value={"default": "default"})
        assert topic.need_cfg_update() is True

    def test_custom_config(self, topic):
        """Экземпляр метода custom_config"""

        topic.custom_config = {"cleanup.policy": "delete"}
        assert topic.custom_config == {"cleanup.policy": "delete"}
        topic.custom_config = {"segment.bytes": "1024"}
        assert topic.custom_config == {"cleanup.policy": "delete", "segment.bytes": "1024"}

    def test_merged_config_empty_custom_config_true(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {}
        assert topic.merged_config(allow_manual=True) is None

    def test_merged_config_empty_custom_config_false(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {}
        assert topic.merged_config(allow_manual=False) == {}

    def test_merged_config_old_values_true(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"segment.bytes": "1073741824", "cleanup.policy": "compact"}  # старые значения
        assert topic.merged_config(allow_manual=True) is None

    def test_merged_config_old_values_false(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"segment.bytes": "1073741824", "cleanup.policy": "compact"}  # старые значения
        assert topic.merged_config(allow_manual=False) is None

    def test_merged_config_not_full_values_true(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"segment.bytes": "1073741824"}  # старые неполные значения
        assert topic.merged_config(allow_manual=True) is None

    def test_merged_config_not_full_values_false(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"segment.bytes": "1073741824"}  # старые неполные значения
        assert topic.merged_config(allow_manual=False) == {"segment.bytes": "1073741824"}

    def test_merged_config_new_value_true(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"segment.bytes": "1"}  # новое значение у segment.bytes
        assert topic.merged_config(allow_manual=True) == {"segment.bytes": "1", 'cleanup.policy': 'compact'}

    def test_merged_config_new_value_false(self, topic):
        """Экземпляр метода merged_config"""

        topic.custom_config = {"segment.bytes": "1"}  # новое значение у segment.bytes
        assert topic.merged_config(allow_manual=False) == {"segment.bytes": "1"}

    def test_merged_config_new_and_old_values_true(self, topic):
        """Экземпляр метода merged_config"""

        # новое значение у segment.bytes
        # старое значение у cleanup.policy
        topic.custom_config = {"segment.bytes": "1", 'cleanup.policy': 'compact'}
        assert topic.merged_config(allow_manual=True) == {"segment.bytes": "1", 'cleanup.policy': 'compact'}

    def test_merged_config_new_and_old_values_false(self, topic):
        """Экземпляр метода merged_config"""

        # новое значение у segment.bytes
        # старое значение у cleanup.policy
        topic.custom_config = {"segment.bytes": "1", 'cleanup.policy': 'compact'}
        assert topic.merged_config(allow_manual=False) == {"segment.bytes": "1", 'cleanup.policy': 'compact'}

    def test_merged_config_new_param_true(self, topic):
        """Экземпляр метода merged_config"""

        # новый параметр flush.ms
        topic.custom_config = {"flush.ms": "1000000"}
        assert topic.merged_config(allow_manual=True) == {
            'cleanup.policy': 'compact', "flush.ms": "1000000", "segment.bytes": "1073741824"
        }

    def test_merged_config_new_param_false(self, topic):
        """Экземпляр метода merged_config"""

        # новый параметр flush.ms
        topic.custom_config = {"flush.ms": "1000000"}
        assert topic.merged_config(allow_manual=False) == {"flush.ms": "1000000"}

    def test_merged_config_new_param_and_new_value_true(self, topic):
        """Экземпляр метода merged_config"""

        # новый параметр flush.ms
        # новое значение у cleanup.policy
        topic.custom_config = {"cleanup.policy": "delete", "flush.ms": "1000000"}
        assert topic.merged_config(allow_manual=True) == {
            'cleanup.policy': 'delete', "flush.ms": "1000000", "segment.bytes": "1073741824"
        }

    def test_merged_config_new_param_and_new_value_false(self, topic):
        """Экземпляр метода merged_config"""

        # новый параметр flush.ms
        # новое значение у cleanup.policy
        topic.custom_config = {"cleanup.policy": "delete", "flush.ms": "1000000"}
        assert topic.merged_config(allow_manual=False) == {'cleanup.policy': 'delete', "flush.ms": "1000000"}

    def test_merged_config_new_param_and_old_value_true(self, topic):
        """Экземпляр метода merged_config"""

        # новый параметр flush.ms
        # старое значение у cleanup.policy
        topic.custom_config = {"cleanup.policy": "compact", "flush.ms": "1000000"}
        assert topic.merged_config(allow_manual=True) == {
            'cleanup.policy': 'compact', "flush.ms": "1000000", "segment.bytes": "1073741824"
        }

    def test_merged_config_new_param_and_old_value_false(self, topic):
        """Экземпляр метода merged_config"""

        # новый параметр flush.ms
        # старое значение у cleanup.policy
        topic.custom_config = {"cleanup.policy": "compact", "flush.ms": "1000000"}
        assert topic.merged_config(allow_manual=False) == {"cleanup.policy": "compact", "flush.ms": "1000000"}

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
