from tests.conftest import TEST_TOPIC_NAME


class TestSR:
    """Экземпляр класса SR"""

    def test_delete_topic_schemas(self, sr):
        """Экземпляр метода delete_topic_schemas"""

        sr.subjects = [f"{TEST_TOPIC_NAME}-key", f"{TEST_TOPIC_NAME}-value"]
        sr.delete_topic_schemas(TEST_TOPIC_NAME)
        assert sr.subjects == []
        assert sr.admin_client.delete_subject.call_count == 2

    def test_delete_topic_schemas_not_found(self, sr):
        """Экземпляр метода delete_topic_schemas"""

        sr.subjects = [f"{TEST_TOPIC_NAME}-key", f"{TEST_TOPIC_NAME}-value"]
        sr.delete_topic_schemas("wrong")
        assert sr.subjects == [f"{TEST_TOPIC_NAME}-key", f"{TEST_TOPIC_NAME}-value"]
        assert sr.admin_client.delete_subject.call_count == 0
