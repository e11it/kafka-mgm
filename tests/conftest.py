from unittest.mock import MagicMock

import pytest
from confluent_kafka.admin import ClusterMetadata, TopicMetadata, PartitionMetadata

from kafka_mgm import MaskRule, SR, Topic, Topics, Cluster

TEST_TOPIC_NAME = "test.topic"
TEST_SR_SUBJECTS = [f"{TEST_TOPIC_NAME}-key", f"{TEST_TOPIC_NAME}-value"]
TEST_PARTITION_ID = 0
TEST_MASK = r"test\."
TEST_VERSION = "version_test"
TEST_CONFIG = {
    "config": "test",
    "dry_run": False,
    "delete_invalid_topics": False,
    "validate_regexp": r"^\d{3}-\d(-\d{3}-\d)?\.[a-z0-9-]+\.(db|cdc|bin|cmd|sys|log|tmp)\.[a-z0-9-.]+\.\d+$",
}


@pytest.fixture(scope="function")
def mask_rule():
    """Экземпляр класса MaskRule"""

    mask = MaskRule({"mask": TEST_MASK})
    return mask


@pytest.fixture(scope="function")
def sr(mocker):
    """Экземпляр класса SR"""

    sr_mock = MagicMock()
    sr_mock.get_subjects.return_value = TEST_SR_SUBJECTS
    sr_mock.delete_subject.return_value = [TEST_VERSION]
    mocker.patch("kafka_mgm.SR.get_sr_client", return_value=sr_mock)

    sr = SR({"test": "test"})
    return sr


@pytest.fixture(scope="function")
def topic(mocker):
    """Экземпляр класса Topic"""

    resource_mock = MagicMock(return_value="test")
    mocker.patch("kafka_mgm.Topic.resource", return_value=resource_mock)

    topic = Topic(TEST_TOPIC_NAME)
    return topic


@pytest.fixture(scope="function")
def metadata():
    """Экземпляр класса ClusterMetadata"""

    meta_partition = PartitionMetadata()
    meta_partition.id = TEST_PARTITION_ID
    meta_topic = TopicMetadata()
    meta_topic.topic = TEST_TOPIC_NAME
    meta_topic.partitions = {TEST_PARTITION_ID: meta_partition}
    meta_cluster = ClusterMetadata()
    meta_cluster.topics = {TEST_TOPIC_NAME: meta_topic}

    return meta_cluster


@pytest.fixture(scope="function")
def topics(metadata):
    """Экземпляр класса Topics"""

    topics = Topics(metadata)
    return topics


@pytest.fixture(scope="function")
def consumer():
    """Экземпляр класса Consumer"""

    consumer_mock = MagicMock()
    consumer_mock.get_watermark_offsets.return_value = (0, 1,)
    return consumer_mock


@pytest.fixture(scope="function")
def cluster(mocker, sr, mask_rule, metadata, consumer):
    """Экземпляр класса Cluster"""

    admin_mock = MagicMock()
    admin_mock.list_topics.return_value = metadata
    admin_mock.describe_configs.return_value = {"test": MagicMock(return_value={"test": "test"})}
    admin_mock.delete_topics.return_value = {"test": MagicMock(return_value={"test": "test"})}
    admin_mock.alter_configs.return_value = {"test": MagicMock(return_value={"test": "test"})}

    mocker.patch("kafka_mgm.Cluster.create_admin_client", return_value=admin_mock)
    mocker.patch("kafka_mgm.Cluster.create_consumer", return_value=consumer)

    cluster = Cluster(TEST_CONFIG)
    cluster.sr_client = sr
    cluster.masks.append(mask_rule)
    return cluster
