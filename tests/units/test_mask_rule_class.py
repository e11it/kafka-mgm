class TestMaskRule:
    """Экземпляр класса MaskRule"""

    def test_get_config(self, mask_rule):
        """Экземпляр метода get_config"""

        assert mask_rule.config == {}
        assert mask_rule._get_config({"mask": "mask", "test": "test"}) == {"test": "test"}
