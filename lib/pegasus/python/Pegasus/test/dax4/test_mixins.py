import pytest

from Pegasus.dax4.mixins import MetadataMixin
from Pegasus.dax4.mixins import HookMixin
from Pegasus.dax4.mixins import ProfileMixin
from Pegasus.dax4.errors import DuplicateError
from Pegasus.dax4.errors import NotFoundError


@pytest.fixture(scope="function")
def mixin_obj():
    def _mixin_obj():
        class MixinObj(MetadataMixin):
            def __init__(self):
                self.metadata = dict()

        return MixinObj()

    return _mixin_obj()


class TestMetadataMixin:
    def test_add_metadata(self, mixin_obj):
        mixin_obj.add_metadata("key", "value")
        assert mixin_obj.metadata["key"] == "value"

    def test_add_duplicate_metadata(self, mixin_obj):
        mixin_obj.add_metadata("key", "value")
        with pytest.raises(DuplicateError):
            mixin_obj.add_metadata("key", "value")

    def test_has_metadata(self, mixin_obj):
        mixin_obj.add_metadata("key", "value")
        assert mixin_obj.has_metadata("key") == True

    def test_remove_metadata(self, mixin_obj):
        mixin_obj.add_metadata("key", "value")
        mixin_obj.remove_metadata("key")
        assert "key" not in mixin_obj.metadata
        assert len(mixin_obj.metadata) == 0

    def test_remove_invalid_metadata(self, mixin_obj):
        with pytest.raises(NotFoundError):
            mixin_obj.remove_metadata("key")

    def test_clear_metadata(self, mixin_obj):
        mixin_obj.add_metadata("key1", "value")
        mixin_obj.add_metadata("key2", "value")
        assert len(mixin_obj.metadata) == 2

        mixin_obj.clear_metadata()
        assert len(mixin_obj.metadata) == 0

    def test_chaining(self, mixin_obj):
        (mixin_obj.add_metadata("key1", "value").add_metadata("key2", "value"))

        assert len(mixin_obj.metadata) == 2

        (mixin_obj.remove_metadata("key1").remove_metadata("key2"))

        assert len(mixin_obj.metadata) == 0

        (mixin_obj.clear_metadata().add_metadata("key1", "value1"))

        assert mixin_obj.has_metadata("key1") == True


class TestHookMixin:
    pass


class TestProfileMixin:
    pass

