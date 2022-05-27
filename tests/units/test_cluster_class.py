import pytest
from confluent_kafka import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryError

from tests.conftest import TEST_TOPIC_NAME


class TestCluster:
    """Экземпляр класса Cluster"""

    def test_load_schemas_from_sr(self, cluster):
        """Экземпляр метода load_schemas_from_sr"""

        cluster.load_schemas_from_sr()
        assert len(cluster.sr_client.subjects) == 2

    def test_load_schemas_from_sr_error(self, cluster):
        """Экземпляр метода load_schemas_from_sr"""

        cluster.sr_client = None
        with pytest.raises(SchemaRegistryError):
            cluster.load_schemas_from_sr()

    def test_load_existing_topics(self, cluster):
        """Экземпляр метода load_existing_topics"""

        topics = cluster.load_existing_topics()
        assert list(topics.topics.keys()) == [TEST_TOPIC_NAME]

    def test_delete_topics(self, cluster):
        """Экземпляр метода _delete_topics"""

        cluster._delete_topics([TEST_TOPIC_NAME])
        assert cluster.admin_client.delete_topics.call_count == 1

    def test_delete_invalid_topics(self, cluster):
        """Экземпляр метода delete_invalid_topics"""

        cluster._delete_invalid_topics = True
        cluster.delete_invalid_topics()
        assert cluster.admin_client.delete_topics.call_count == 1

    def test_delete_invalid_topics_false(self, cluster):
        """Экземпляр метода delete_invalid_topics"""

        cluster._delete_invalid_topics = False
        cluster.delete_invalid_topics()
        assert cluster.admin_client.delete_topics.call_count == 0

    def test_delete_empty_topics_false(self, cluster):
        """Экземпляр метода delete_empty_topics"""

        cluster._delete_empty_topics = False
        cluster.delete_invalid_topics()
        assert cluster.admin_client.delete_topics.call_count == 0

    def test_delete_empty_topics(self, cluster):
        """Экземпляр метода delete_empty_topics"""

        cluster._delete_empty_topics = True
        cluster.delete_empty_topics()
        assert cluster.admin_client.delete_topics.call_count == 0
        assert cluster.sr_client.admin_client.delete_subject.call_count == 0

    def test_delete_empty_topics_exist(self, cluster):
        """Экземпляр метода delete_empty_topics"""

        cluster._delete_empty_topics = True
        cluster.consumer.get_watermark_offsets.return_value = (1, 1,)
        cluster.delete_empty_topics()
        assert cluster.admin_client.delete_topics.call_count == 1
        assert cluster.sr_client.admin_client.delete_subject.call_count == 2

    def test_delete_empty_topics_error_consumer(self, cluster):
        """Экземпляр метода delete_empty_topics"""

        cluster._delete_empty_topics = True
        cluster.consumer = None
        with pytest.raises(KafkaException):
            cluster.delete_empty_topics()

    def test_delete_empty_topics_error_sr(self, cluster):
        """Экземпляр метода delete_empty_topics"""

        cluster._delete_empty_topics = True
        cluster.sr_client = None
        with pytest.raises(SchemaRegistryError):
            cluster.delete_empty_topics()

    def test_apply_cluster_configs(self, cluster):
        """Экземпляр метода apply_cluster_configs"""

        cluster.apply_cluster_configs()
        assert cluster.admin_client.alter_configs.call_count == 1

    def test_apply_cluster_configs_not_resource(self, cluster):
        """Экземпляр метода apply_cluster_configs"""

        cluster.topics.topics[TEST_TOPIC_NAME].custom_config = {
            "segment.bytes": "1073741824", "cleanup.policy": "compact"
        }
        cluster.apply_cluster_configs()
        assert cluster.admin_client.alter_configs.call_count == 0
