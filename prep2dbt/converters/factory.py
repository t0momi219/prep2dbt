from prep2dbt.converters.container.converter import ContainerConverter
from prep2dbt.converters.load_csv.converter import LoadCsvConverter
from prep2dbt.converters.load_csv_input_union.converter import \
    LoadCsvInputUnionConverter
from prep2dbt.converters.load_excel.converter import LoadExcelConverter
from prep2dbt.converters.load_sql.converter import LoadSqlConverter
from prep2dbt.converters.load_sql_proxy.converter import LoadSqlProxyConverter
from prep2dbt.converters.superaggregate.converter import \
    SuperAggregateConverter
from prep2dbt.converters.superjoin.converter import SuperJoinConverter
from prep2dbt.converters.supertransform.converter import \
    SuperTransformConverter
from prep2dbt.converters.unknown.converter import UnknownConverter
from prep2dbt.converters.write_to_hyper.converter import WriteToHyperConverter
from prep2dbt.exceptions import UnknownNodeException
from prep2dbt.protocols.converter import Converter


class VersionMappingRegistory:
    version_converters: dict[str, type[Converter]] = {
        ".v1.LoadSql": LoadSqlConverter,
        ".v2019_3_1.LoadSqlProxy": LoadSqlProxyConverter,
        ".v1.LoadCsv": LoadCsvConverter,
        ".v1.LoadCsvInputUnion": LoadCsvInputUnionConverter,
        ".v1.LoadExcel": LoadExcelConverter,
        ".v1.Container": ContainerConverter,
        ".v1.WriteToHyper": WriteToHyperConverter,
        ".v2018_2_3.SuperAggregate": SuperAggregateConverter,
        ".v2018_2_3.SuperJoin": SuperJoinConverter,
        ".v2018_2_3.SuperTransform": SuperTransformConverter,
        "unknown": UnknownConverter,
    }


class ConverterFactory:
    """
    Converterのfactoryクラス
    """

    @classmethod
    def by_type(cls, type_name: str) -> Converter:
        """該当バージョンの変換仕様を探して返却する"""
        if type_name in VersionMappingRegistory.version_converters.keys():
            return VersionMappingRegistory.version_converters[type_name]

        raise UnknownNodeException("{}は見つかりませんでした。".format(type_name))

    @classmethod
    def find_all_versions(cls, type_name: str) -> list[Converter]:
        """同じタイプで違うバージョンの変換仕様を探して返却する"""
        # NodeTypeは、最後にタイプの基本名がついていることが多い
        type_name_suffix = type_name.split(".")[-1]
        res: list[Converter] = []
        for other_version_key in VersionMappingRegistory.version_converters.keys():
            if other_version_key.endswith(type_name_suffix):
                res.append(
                    VersionMappingRegistory.version_converters[other_version_key]
                )

        if len(res) != 0:
            return res

        raise UnknownNodeException("{}は見つかりませんでした。".format(type_name))

    @classmethod
    def get_unknown(cls) -> Converter:
        """未知のノード用の変換仕様を返却する"""
        return VersionMappingRegistory.version_converters["unknown"]

    @classmethod
    def get_converter_by_type(cls, type_name: str) -> Converter:
        try:
            converter = ConverterFactory.by_type(type_name)
        except UnknownNodeException:
            # もし変換仕様が見つからなければ、別バージョンで定義済みの変換仕様を探す
            try:
                converters = ConverterFactory.find_all_versions(type_name)
                converter = converters.pop()
            except UnknownNodeException:
                # それでも見つからなければ、未知のノード用の変換仕様を返す
                converter = ConverterFactory.get_unknown()

        return converter
