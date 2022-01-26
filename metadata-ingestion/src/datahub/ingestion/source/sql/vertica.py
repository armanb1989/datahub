# This import verifies that the dependencies are available.
import sqlalchemy.dialects.postgresql as custom_types
import verticapy  # noqa: F401
from sqla_vertica_python.vertica_python import VerticaDialect

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    make_sqlalchemy_type,
    register_custom_type,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BytesTypeClass,
    MapTypeClass,
)

GEOMETRY = make_sqlalchemy_type("GEOMETRY")
GEOGRAPHY = make_sqlalchemy_type("GEOGRAPHY")

register_custom_type(custom_types.ARRAY, ArrayTypeClass)
register_custom_type(custom_types.JSON, BytesTypeClass)
register_custom_type(custom_types.JSONB, BytesTypeClass)
register_custom_type(custom_types.HSTORE, MapTypeClass)
register_custom_type(GEOMETRY)
register_custom_type(GEOGRAPHY)

VerticaDialect.ischema_names["geometry"] = GEOMETRY
VerticaDialect.ischema_names["geography"] = GEOGRAPHY


class VerticaConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "vertica+verticapy"

    def get_identifier(self: BasicSQLAlchemyConfig, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{regular}"
        if self.database:
            return f"{self.database}.{regular}"
        return regular


class VerticaSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "vertica")

    @classmethod
    def create(cls, config_dict, ctx):
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)
