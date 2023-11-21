import logging

from datetime import datetime
from typing import Dict, List, Optional, Type

from flask_babel import gettext as __
from marshmallow import Schema, fields
from marshmallow.validate import Range
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.sql.type_api import TypeEngine
from superset.db_engine_specs.base import (
    BaseEngineSpec,
    BasicParametersType,
    BasicParametersMixin,
)
from superset.db_engine_specs.exceptions import SupersetDBAPIDatabaseError
from superset.errors import SupersetError, SupersetErrorType, ErrorLevel
from superset.utils.network import is_hostname_valid, is_port_open
from superset.models.core import Database

from databend_sqlalchemy.databend_dialect import ischema_names

logger = logging.getLogger(__name__)


class DatabendParametersSchema(Schema):
    username = fields.String(allow_none=True, description=__("Username"))
    password = fields.String(allow_none=True, description=__("Password"))
    host = fields.String(required=True, description=__("Hostname or IP address"))
    port = fields.Integer(
        allow_none=True,
        description=__("Database port"),
        validate=Range(min=0, max=65535),
    )
    database = fields.String(allow_none=True, description=__("Database name"))
    encryption = fields.Boolean(
        default=True, description=__("Use an encrypted connection to the database")
    )
    query = fields.Dict(
        keys=fields.Str(), values=fields.Raw(), description=__("Additional parameters")
    )


class DatabendEngineSpec(BaseEngineSpec, BasicParametersMixin):
    """
    See :py:class:`superset.db_engine_specs.base.BaseEngineSpec`
    """

    engine = "databend"
    engine_name = "Databend Sqlalchemy"

    time_secondary_columns = True
    time_groupby_inline = True
    _function_names = []

    _time_grain_expressions = {
        None: "{col}",
        "PT1M": "to_start_of_minute(TO_DATETIME({col}))",
        "PT5M": "to_start_of_five_minutes(TO_DATETIME({col}))",
        "PT10M": "to_start_of_ten_minutes(TO_DATETIME({col}))",
        "PT15M": "to_start_of_fifteen_minutes(TO_DATETIME({col}))",
        "PT30M": "TO_DATETIME(intDiv(toUInt32(TO_DATETIME({col})), 1800)*1800)",
        "PT1H": "to_start_of_hour(TO_DATETIME({col}))",
        "P1D": "to_start_of_day(TO_DATETIME({col}))",
        "P1W": "to_monday(TO_DATETIME({col}))",
        "P1M": "to_start_of_month(TO_DATETIME({col}))",
        "P3M": "to_start_of_quarter(TO_DATETIME({col}))",
        "P1Y": "to_start_of_year(TO_DATETIME({col}))",
    }

    sqlalchemy_uri_placeholder = (
        "databend://user:password@host[:port][/dbname][?key=value...]"
    )
    parameters_schema = DatabendParametersSchema()
    encryption_parameters = {"secure": True}

    @classmethod
    def epoch_to_dttm(cls) -> str:
        return "{col}"

    @classmethod
    def get_dbapi_exception_mapping(cls) -> Dict[Type[Exception], Type[Exception]]:
        return {}

    @classmethod
    def get_dbapi_mapped_exception(cls, exception: Exception) -> Exception:
        new_exception = cls.get_dbapi_exception_mapping().get(type(exception))
        if new_exception == SupersetDBAPIDatabaseError:
            return SupersetDBAPIDatabaseError("Connection failed")
        if not new_exception:
            return exception
        return new_exception(str(exception))

    @classmethod
    def convert_dttm(
        cls, target_type: str, dttm: datetime, *_args, **_kwargs
    ) -> Optional[str]:
        if target_type.upper() == "DATE":
            return f"'{dttm.date().isoformat()}'"
        if target_type.upper() == "DATETIME":
            return f"""'{dttm.isoformat(sep=" ", timespec="seconds")}'"""
        return None

    @classmethod
    def get_function_names(cls, database: Database) -> List[str]:
        if cls._function_names:
            return cls._function_names
        try:
            names = database.get_df("SELECT name FROM system.functions;")[
                "name"
            ].tolist()
            cls._function_names = names
            return names
        except Exception:
            logger.exception("Error retrieving system.functions")
            return []

    @classmethod
    def get_datatype(cls, type_code: str) -> str:
        return type_code

    @classmethod
    def get_sqla_column_type(cls, type_: str) -> Optional[TypeEngine]:
        return ischema_names.get(type_, None)

    @classmethod
    def build_sqlalchemy_uri(cls, parameters: BasicParametersType, *_args):
        url_params = parameters.copy()
        if url_params.get("encryption"):
            query = parameters.get("query", {}).copy()
            query.update(cls.encryption_parameters)
            url_params["query"] = query
        if not url_params.get("database"):
            url_params["database"] = "__default__"
        url_params.pop("encryption", None)
        return str(URL(f"{cls.engine}", **url_params))

    @classmethod
    def get_parameters_from_uri(
        cls, uri: str, *_args, **_kwargs
    ) -> BasicParametersType:
        url = make_url(uri)
        query = url.query
        if "secure" in query:
            encryption = url.query.get("secure") == "true"
            query.pop("secure")
        else:
            encryption = False
        return BasicParametersType(
            username=url.username,
            password=url.password,
            host=url.host,
            port=url.port,
            database=None if url.database == "__default__" else url.database,
            query=query,
            encryption=encryption,
        )

    @classmethod
    def default_port(interface: str, secure: bool):
        if interface.startswith("http"):
            return 443 if secure else 8000
        raise ValueError("Unrecognized Databend interface")

    @classmethod
    # pylint: disable=arguments-renamed
    def validate_parameters(cls, properties) -> List[SupersetError]:
        # The newest versions of superset send a "properties" object with a parameters key, instead of just
        # the parameters, so we hack to be compatible
        parameters = properties.get("parameters", properties)
        host = parameters.get("host", None)
        if not host:
            return [
                SupersetError(
                    "Hostname is required",
                    SupersetErrorType.CONNECTION_MISSING_PARAMETERS_ERROR,
                    ErrorLevel.WARNING,
                    {"missing": ["host"]},
                )
            ]
        if not is_hostname_valid(host):
            return [
                SupersetError(
                    "The hostname provided can't be resolved.",
                    SupersetErrorType.CONNECTION_INVALID_HOSTNAME_ERROR,
                    ErrorLevel.ERROR,
                    {"invalid": ["host"]},
                )
            ]
        port = parameters.get("port")
        if port is None:
            port = cls.default_port("http", parameters.get("encryption", False))
        try:
            port = int(port)
        except (ValueError, TypeError):
            port = -1
        if port <= 0 or port >= 65535:
            return [
                SupersetError(
                    "Port must be a valid integer between 0 and 65535 (inclusive).",
                    SupersetErrorType.CONNECTION_INVALID_PORT_ERROR,
                    ErrorLevel.ERROR,
                    {"invalid": ["port"]},
                )
            ]
        if not is_port_open(host, port):
            return [
                SupersetError(
                    "The port is closed.",
                    SupersetErrorType.CONNECTION_PORT_CLOSED_ERROR,
                    ErrorLevel.ERROR,
                    {"invalid": ["port"]},
                )
            ]
        return []
