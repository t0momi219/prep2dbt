import pytest

from prep2dbt.converters.factory import VersionMappingRegistory
from prep2dbt.models.graph import DAG
from prep2dbt.protocols.converter import Converter


def __get_all_converters():
    for c in VersionMappingRegistory.version_converters.values():
        yield c


@pytest.mark.parametrize("converter", __get_all_converters())
class TestConverterImplCheck:
    """
    登録されたコンバータークラスが、期待通りの実装になっているかチェックするためのテストです。
    """

    def test(self, converter: Converter):
        assert hasattr(converter, "validate")
        assert hasattr(converter, "calculate_columns")
        assert hasattr(converter, "generate_dbt_models")

        try:
            converter.validate({})
        except Exception as e:
            assert type(e) != NotImplementedError

        try:
            converter.calculate_columns("", DAG())
        except Exception as e:
            assert type(e) != NotImplementedError

        try:
            converter.generate_dbt_models("", DAG())
        except Exception as e:
            assert type(e) != NotImplementedError
