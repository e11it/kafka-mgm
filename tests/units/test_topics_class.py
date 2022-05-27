import re

from tests.conftest import TEST_TOPIC_NAME, TEST_MASK


class TestTopics:
    """Экземпляр класса Topics"""

    def test_load_config(self, topics):
        """Экземпляр метода load_config"""

        assert list(topics.topics.keys()) == [TEST_TOPIC_NAME]
        topics.load_config(['new_test'])
        assert list(topics.topics.keys()) == [TEST_TOPIC_NAME, 'new_test']

    def test_apply_masks(self, topics, mask_rule):
        """Экземпляр метода apply_masks"""

        mask_rule.config = {"test": "test"}
        assert list(topics.topics.keys()) == [TEST_TOPIC_NAME]
        assert topics.topics[TEST_TOPIC_NAME].custom_config == {}
        topics.apply_masks([mask_rule])
        assert topics.topics[TEST_TOPIC_NAME].custom_config == {"test": "test"}

    def test_apply_masks_wrong(self, topics, mask_rule):
        """Экземпляр метода apply_masks"""

        mask_rule.mask_str = r"wrong\."
        mask_rule.mask = re.compile(r"wrong\.")
        mask_rule.config = {"test": "test"}
        assert list(topics.topics.keys()) == [TEST_TOPIC_NAME]
        assert topics.topics[TEST_TOPIC_NAME].custom_config == {}
        topics.apply_masks([mask_rule])
        assert topics.topics[TEST_TOPIC_NAME].custom_config == {}

    def test_validate_names_wrong(self, topics):
        """Экземпляр метода validate_names"""

        regexp = re.compile(r"wrong\.")
        assert list(topics.topics.keys()) == [TEST_TOPIC_NAME]
        assert topics.topics[TEST_TOPIC_NAME].is_valid_name is False
        topics.validate_names(regexp)
        assert topics.topics[TEST_TOPIC_NAME].is_valid_name is False

    def test_validate_names(self, topics):
        """Экземпляр метода validate_names"""

        regexp = re.compile(TEST_MASK)
        assert list(topics.topics.keys()) == [TEST_TOPIC_NAME]
        assert topics.topics[TEST_TOPIC_NAME].is_valid_name is False
        topics.validate_names(regexp)
        assert topics.topics[TEST_TOPIC_NAME].is_valid_name is True
