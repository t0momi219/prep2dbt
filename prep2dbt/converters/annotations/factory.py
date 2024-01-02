from prep2dbt.converters.annotations.add_column.add_column import \
    AddColumnAnnotationConverter
from prep2dbt.converters.annotations.change_column_type.change_column_type import \
    ChangeColumnTypeAnnotationConverter
from prep2dbt.converters.annotations.duplicate_column.duplicate_column import \
    DuplicateColumnAnnotationConverter
from prep2dbt.converters.annotations.filter_operation.filter_operation import \
    FilterOperationAnnotationConverter
from prep2dbt.converters.annotations.keep_only_columns.keep_only_columns import \
    KeepOnlyColumnAnnotationConverter
from prep2dbt.converters.annotations.quick_calc_column.quick_calc_column import \
    QuickCalcColumnAnnotationConverter
from prep2dbt.converters.annotations.remap.remap import \
    RemapAnnotationConverter
from prep2dbt.converters.annotations.remove_columns.remove_columns import \
    RemoveColumnsAnnotationConverter
from prep2dbt.converters.annotations.rename_column.rename_column import \
    RenameColumnAnnotationConverter
from prep2dbt.converters.annotations.unknown.unknown import \
    UnknownAnnotationConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.protocols.converter import AnnotationConverter


class AnnotationVersionMappingRegistory:
    version_converters: dict[str, type[AnnotationConverter]] = {
        ".v1.AddColumn": AddColumnAnnotationConverter,
        ".v1.ChangeColumnType": ChangeColumnTypeAnnotationConverter,
        ".v2019_2_3.DuplicateColumn": DuplicateColumnAnnotationConverter,
        ".v1.FilterOperation": FilterOperationAnnotationConverter,
        ".v2019_2_2.KeepOnlyColumns": KeepOnlyColumnAnnotationConverter,
        ".v2018_3_3.QuickCalcColumn": QuickCalcColumnAnnotationConverter,
        ".v2019_1_4.Remap": RemapAnnotationConverter,
        ".v1.RemoveColumns": RemoveColumnsAnnotationConverter,
        ".v1.RenameColumn": RenameColumnAnnotationConverter,
        "unknown": UnknownAnnotationConverter,
    }


class AnnotationConverterFactory:
    """
    AnnotationConverterのfactoryクラス
    """

    @classmethod
    def by_type(cls, type_name: str) -> AnnotationConverter:
        """該当バージョンの変換仕様を探して返却する"""
        if type_name in AnnotationVersionMappingRegistory.version_converters.keys():
            return AnnotationVersionMappingRegistory.version_converters[type_name]

        raise UnknownNodeException("{}は見つかりませんでした。".format(type_name))

    @classmethod
    def find_all_versions(cls, type_name: str) -> list[AnnotationConverter]:
        """同じタイプで違うバージョンの変換仕様を探して返却する"""
        # NodeTypeは、最後にタイプの基本名がついていることが多い
        type_name_suffix = type_name.split(".")[-1]
        res: list[AnnotationConverter] = []
        for (
            other_version_key
        ) in AnnotationVersionMappingRegistory.version_converters.keys():
            if other_version_key.endswith(type_name_suffix):
                res.append(
                    AnnotationVersionMappingRegistory.version_converters[
                        other_version_key
                    ]
                )

        if len(res) != 0:
            return res

        raise UnknownNodeException("{}は見つかりませんでした。".format(type_name))

    @classmethod
    def get_unknown(cls) -> AnnotationConverter:
        """未知のノード用の変換仕様を返却する"""
        return AnnotationVersionMappingRegistory.version_converters["unknown"]

    @classmethod
    def get_annotation_converter_by_type(cls, type_name: str) -> AnnotationConverter:
        try:
            converter = AnnotationConverterFactory.by_type(type_name)
        except UnknownNodeException:
            # もし変換仕様が見つからなければ、別バージョンで定義済みの変換仕様を探す
            try:
                converters = AnnotationConverterFactory.find_all_versions(type_name)
                converter = converters.pop()
            except UnknownNodeException:
                # それでも見つからなければ、未知のノード用の変換仕様を返す
                converter = AnnotationConverterFactory.get_unknown()

        return converter
