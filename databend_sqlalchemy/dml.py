#!/usr/bin/env python
#
# Note: parts of the file come from https://github.com/snowflakedb/snowflake-sqlalchemy
#       licensed under the same Apache 2.0 License
from enum import Enum
from types import NoneType
from urllib.parse import urlparse

from sqlalchemy.sql.selectable import Select, Subquery, TableClause
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.roles import FromClauseRole


class _OnMergeBaseClause(ClauseElement):
    # __visit_name__ = "on_merge_base_clause"

    def __init__(self):
        self.set = {}
        self.predicate = None

    def __repr__(self):
        return f" AND {str(self.predicate)}" if self.predicate is not None else ""

    def values(self, **kwargs):
        self.set = kwargs
        return self

    def where(self, expr):
        self.predicate = expr
        return self


class WhenMergeMatchedUpdateClause(_OnMergeBaseClause):
    __visit_name__ = "when_merge_matched_update"

    def __repr__(self):
        case_predicate = super()
        update_str = f"WHEN MATCHED{case_predicate} THEN UPDATE"
        if not self.set:
            return f"{update_str} *"

        set_values = ", ".join(
            [f"{set_item[0]} = {set_item[1]}" for set_item in self.set.items()]
        )
        return f"{update_str} SET {str(set_values)}"


class WhenMergeMatchedDeleteClause(_OnMergeBaseClause):
    __visit_name__ = "when_merge_matched_delete"

    def __repr__(self):
        case_predicate = super()
        return f"WHEN MATCHED{case_predicate} THEN DELETE"


class WhenMergeUnMatchedClause(_OnMergeBaseClause):
    __visit_name__ = "when_merge_unmatched"

    def __repr__(self):
        case_predicate = super()
        insert_str = f"WHEN NOT MATCHED{case_predicate} THEN INSERT"
        if not self.set:
            return f"{insert_str} *"

        sets, sets_tos = zip(*self.set.items())
        return "{} ({}) VALUES ({})".format(
            insert_str,
            ", ".join(sets),
            ", ".join(map(str, sets_tos)),
        )


class Merge(UpdateBase):
    __visit_name__ = "merge"
    _bind = None

    inherit_cache = False

    def __init__(self, target, source, on):
        if not isinstance(source, (TableClause, Select, Subquery)):
            raise Exception(f"Invalid type for merge source: {source}")
        self.target = target
        self.source = source
        self.on = on
        self.clauses = []

    def __repr__(self):
        clauses = " ".join([repr(clause) for clause in self.clauses])
        return (
            f"MERGE INTO {self.target} USING ({select(self.source)}) AS {self.source.name} ON {self.on}"
            + (f" {clauses}" if clauses else "")
        )

    def when_matched_then_update(self):
        clause = WhenMergeMatchedUpdateClause()
        self.clauses.append(clause)
        return clause

    def when_matched_then_delete(self):
        clause = WhenMergeMatchedDeleteClause()
        self.clauses.append(clause)
        return clause

    def when_not_matched_then_insert(self):
        clause = WhenMergeUnMatchedClause()
        self.clauses.append(clause)
        return clause


class _CopyIntoBase(UpdateBase):
    __visit_name__ = "copy_into"
    _bind = None

    def __init__(
        self,
        target: ["TableClause", "StageClause", "_StorageClause"],
        from_,
        file_format: "CopyFormat" = None,
        options: ["CopyIntoLocationOptions", "CopyIntoTableOptions"] = None,
    ):
        self.target = target
        self.from_ = from_
        self.file_format = file_format
        self.options = options

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in base.py
        """
        val = f"COPY INTO {self.target} FROM {repr(self.from_)}"
        return val + f" {repr(self.file_format)} ({self.options})"

    def bind(self):
        return None


class CopyIntoLocation(_CopyIntoBase):
    inherit_cache = False

    def __init__(
        self,
        *,
        target: ["StageClause", "_StorageClause"],
        from_,
        file_format: "CopyFormat" = None,
        options: "CopyIntoLocationOptions" = None,
    ):
        super().__init__(target, from_, file_format, options)


class CopyIntoTable(_CopyIntoBase):
    inherit_cache = False

    def __init__(
        self,
        *,
        target: [TableClause],
        from_: ["StageClause", "_StorageClause", "FileColumnClause"],
        files: list = None,
        pattern: str = None,
        file_format: "CopyFormat" = None,
        options: "CopyIntoTableOptions" = None,
    ):
        super().__init__(target, from_, file_format, options)
        self.files = files
        self.pattern = pattern


class _CopyIntoOptions(ClauseElement):
    __visit_name__ = "copy_into_options"

    def __init__(self):
        self.options = dict()

    def __repr__(self):
        return "\n".join([f"{k} = {v}" for k, v in self.options.items()])


class CopyIntoLocationOptions(_CopyIntoOptions):
    # __visit_name__ = "copy_into_location_options"

    def __init__(
        self,
        *,
        single: bool = None,
        max_file_size_bytes: int = None,
        overwrite: bool = None,
        include_query_id: bool = None,
        use_raw_path: bool = None,
    ):
        super().__init__()
        if not isinstance(single, NoneType):
            self.options["SINGLE"] = "TRUE" if single else "FALSE"
        if not isinstance(max_file_size_bytes, NoneType):
            self.options["MAX_FILE_SIZE "] = max_file_size_bytes
        if not isinstance(overwrite, NoneType):
            self.options["OVERWRITE"] = "TRUE" if overwrite else "FALSE"
        if not isinstance(include_query_id, NoneType):
            self.options["INCLUDE_QUERY_ID"] = "TRUE" if include_query_id else "FALSE"
        if not isinstance(use_raw_path, NoneType):
            self.options["USE_RAW_PATH"] = "TRUE" if use_raw_path else "FALSE"


class CopyIntoTableOptions(_CopyIntoOptions):
    # __visit_name__ = "copy_into_table_options"

    def __init__(
        self,
        *,
        size_limit: int = None,
        purge: bool = None,
        force: bool = None,
        disable_variant_check: bool = None,
        on_error: str = None,
        max_files: int = None,
        return_failed_only: bool = None,
        column_match_mode: str = None,
    ):
        super().__init__()
        if not isinstance(size_limit, NoneType):
            self.options["SIZE_LIMIT"] = size_limit
        if not isinstance(purge, NoneType):
            self.options["PURGE "] = "TRUE" if purge else "FALSE"
        if not isinstance(force, NoneType):
            self.options["FORCE"] = "TRUE" if force else "FALSE"
        if not isinstance(disable_variant_check, NoneType):
            self.options["DISABLE_VARIANT_CHECK"] = (
                "TRUE" if disable_variant_check else "FALSE"
            )
        if not isinstance(on_error, NoneType):
            self.options["ON_ERROR"] = on_error
        if not isinstance(max_files, NoneType):
            self.options["MAX_FILES"] = max_files
        if not isinstance(return_failed_only, NoneType):
            self.options["RETURN_FAILED_ONLY"] = return_failed_only
        if not isinstance(column_match_mode, NoneType):
            self.options["COLUMN_MATCH_MODE"] = column_match_mode


class Compression(Enum):
    NONE = "NONE"
    AUTO = "AUTO"
    GZIP = "GZIP"
    BZ2 = "BZ2"
    BROTLI = "BROTLI"
    ZSTD = "ZSTD"
    DEFLATE = "DEFLATE"
    RAW_DEFLATE = "RAW_DEFLATE"
    XZ = "XZ"
    SNAPPY = "SNAPPY"
    ZIP = "ZIP"


class CopyFormat(ClauseElement):
    """
    Base class for Format specifications inside a COPY INTO statement. May also
    be used to create a named format.
    """

    __visit_name__ = "copy_format"

    def __init__(self, format_name=None):
        self.options = dict()
        if format_name:
            self.options["format_name"] = format_name

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the respective visitor in the dialect
        """
        return f"FILE_FORMAT=({self.options})"


class CSVFormat(CopyFormat):
    format_type = "CSV"

    def __init__(
        self,
        *,
        record_delimiter: str = None,
        field_delimiter: str = None,
        quote: str = None,
        escape: str = None,
        skip_header: int = None,
        nan_display: str = None,
        null_display: str = None,
        error_on_column_mismatch: bool = None,
        empty_field_as: str = None,
        output_header: bool = None,
        binary_format: str = None,
        compression: Compression = None,
    ):
        super().__init__()
        if record_delimiter:
            if (
                len(str(record_delimiter).encode().decode("unicode_escape")) != 1
                and record_delimiter != "\r\n"
            ):
                raise TypeError("Record Delimiter should be a single character.")
            self.options["RECORD_DELIMITER"] = f"{repr(record_delimiter)}"
        if field_delimiter:
            if len(str(field_delimiter).encode().decode("unicode_escape")) != 1:
                raise TypeError("Field Delimiter should be a single character")
            self.options["FIELD_DELIMITER"] = f"{repr(field_delimiter)}"
        if quote:
            if quote not in ["'", '"', "`"]:
                raise TypeError("Quote character must be one of [', \", `].")
            self.options["QUOTE"] = f"{repr(quote)}"
        if escape:
            if escape not in ["\\", ""]:
                raise TypeError('Escape character must be "\\" or "".')
            self.options["ESCAPE"] = f"{repr(escape)}"
        if skip_header:
            if skip_header < 0:
                raise TypeError("Skip header must be positive integer.")
            self.options["SKIP_HEADER"] = skip_header
        if nan_display:
            if nan_display not in ["NULL", "NaN"]:
                raise TypeError('NaN Display should be "NULL" or "NaN".')
            self.options["NAN_DISPLAY"] = f"'{nan_display}'"
        if null_display:
            self.options["NULL_DISPLAY"] = f"'{null_display}'"
        if error_on_column_mismatch:
            self.options["ERROR_ON_COLUMN_MISMATCH"] = str(
                error_on_column_mismatch
            ).upper()
        if empty_field_as:
            if empty_field_as not in ["NULL", "STRING", "FIELD_DEFAULT"]:
                raise TypeError(
                    'Empty Field As should be "NULL", "STRING" for "FIELD_DEFAULT".'
                )
            self.options["EMPTY_FIELD_AS"] = f"{empty_field_as}"
        if output_header:
            self.options["OUTPUT_HEADER"] = str(output_header).upper()
        if binary_format:
            if binary_format not in ["HEX", "BASE64"]:
                raise TypeError('Binary Format should be "HEX" or "BASE64".')
            self.options["BINARY_FORMAT"] = binary_format
        if compression:
            self.options["COMPRESSION"] = compression.value


class TSVFormat(CopyFormat):
    format_type = "TSV"

    def __init__(
        self,
        *,
        record_delimiter: str = None,
        field_delimiter: str = None,
        compression: Compression = None,
    ):
        super().__init__()
        if record_delimiter:
            if (
                len(str(record_delimiter).encode().decode("unicode_escape")) != 1
                and record_delimiter != "\r\n"
            ):
                raise TypeError("Record Delimiter should be a single character.")
            self.options["RECORD_DELIMITER"] = f"{repr(record_delimiter)}"
        if field_delimiter:
            if len(str(field_delimiter).encode().decode("unicode_escape")) != 1:
                raise TypeError("Field Delimiter should be a single character")
            self.options["FIELD_DELIMITER"] = f"{repr(field_delimiter)}"
        if compression:
            self.options["COMPRESSION"] = compression.value


class NDJSONFormat(CopyFormat):
    format_type = "NDJSON"

    def __init__(
        self,
        *,
        null_field_as: str = None,
        missing_field_as: str = None,
        compression: Compression = None,
    ):
        super().__init__()
        if null_field_as:
            if null_field_as not in ["NULL", "FIELD_DEFAULT"]:
                raise TypeError('Null Field As should be "NULL" or "FIELD_DEFAULT".')
            self.options["NULL_FIELD_AS"] = f"{null_field_as}"
        if missing_field_as:
            if missing_field_as not in [
                "ERROR",
                "NULL",
                "FIELD_DEFAULT",
                "TYPE_DEFAULT",
            ]:
                raise TypeError(
                    'Missing Field As should be "ERROR", "NULL", "FIELD_DEFAULT" or "TYPE_DEFAULT".'
                )
            self.options["MISSING_FIELD_AS"] = f"{missing_field_as}"
        if compression:
            self.options["COMPRESSION"] = compression.value


class ParquetFormat(CopyFormat):
    format_type = "PARQUET"

    def __init__(
        self,
        *,
        missing_field_as: str = None,
        compression: Compression = None,
    ):
        super().__init__()
        if missing_field_as:
            if missing_field_as not in ["ERROR", "FIELD_DEFAULT"]:
                raise TypeError(
                    'Missing Field As should be "ERROR" or "FIELD_DEFAULT".'
                )
            self.options["MISSING_FIELD_AS"] = f"{missing_field_as}"
        if compression:
            if compression not in [Compression.ZSTD, Compression.SNAPPY]:
                raise TypeError(
                    'Compression should be None, ZStd, or Snappy.'
                )
            self.options["COMPRESSION"] = compression.value


class AVROFormat(CopyFormat):
    format_type = "AVRO"

    def __init__(
        self,
        *,
        missing_field_as: str = None,
    ):
        super().__init__()
        if missing_field_as:
            if missing_field_as not in ["ERROR", "FIELD_DEFAULT"]:
                raise TypeError(
                    'Missing Field As should be "ERROR" or "FIELD_DEFAULT".'
                )
            self.options["MISSING_FIELD_AS"] = f"{missing_field_as}"


class ORCFormat(CopyFormat):
    format_type = "ORC"

    def __init__(
        self,
        *,
        missing_field_as: str = None,
    ):
        super().__init__()
        if missing_field_as:
            if missing_field_as not in ["ERROR", "FIELD_DEFAULT"]:
                raise TypeError(
                    'Missing Field As should be "ERROR" or "FIELD_DEFAULT".'
                )
            self.options["MISSING_FIELD_AS"] = f"{missing_field_as}"


class StageClause(ClauseElement, FromClauseRole):
    """Stage Clause"""

    __visit_name__ = "stage"
    _hide_froms = ()

    def __init__(self, *, name, path=None):
        self.name = name
        self.path = path

    def __repr__(self):
        return f"@{self.name}/{self.path}"


class FileColumnClause(ClauseElement, FromClauseRole):
    """Clause for selecting file columns from a Stage/Location"""

    __visit_name__ = "file_column"

    def __init__(self, *, columns, from_: ["StageClause", "_StorageClause"]):
        # columns need to be expressions of column index, e.g. $1, IF($1 =='t', True, False), or string of these expressions that we just use
        self.columns = columns
        self.from_ = from_

    def __repr__(self):
        return (
            f"SELECT {self.columns if isinstance(self.columns, str) else ','.join(repr(col) for col in self.columns)}"
            f" FROM {repr(self.from_)}"
        )


class _StorageClause(ClauseElement):
    pass


class AmazonS3(_StorageClause):
    """Amazon S3"""

    __visit_name__ = "amazon_s3"

    def __init__(
        self,
        uri: str,
        access_key_id: str,
        secret_access_key: str,
        endpoint_url: str = None,
        enable_virtual_host_style: bool = None,
        master_key: str = None,
        region: str = None,
        security_token: str = None,
    ):
        r = urlparse(uri)
        if r.scheme != "s3":
            raise ValueError(f"Invalid S3 URI: {uri}")

        self.uri = uri
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.bucket = r.netloc
        self.path = r.path
        if endpoint_url:
            self.endpoint_url = endpoint_url
        if enable_virtual_host_style:
            self.enable_virtual_host_style = enable_virtual_host_style
        if master_key:
            self.master_key = master_key
        if region:
            self.region = region
        if security_token:
            self.security_token = security_token

    def __repr__(self):
        return (
            f"'{self.uri}' \n"
            f"CONNECTION = (\n"
            f"  ENDPOINT_URL = '{self.endpoint_url}' \n"
            if self.endpoint_url
            else (
                ""
                f"  ACCESS_KEY_ID = '{self.access_key_id}' \n"
                f"  SECRET_ACCESS_KEY  = '{self.secret_access_key}'\n"
                f"  ENABLE_VIRTUAL_HOST_STYLE  = '{self.enable_virtual_host_style}'\n"
                if self.enable_virtual_host_style
                else (
                    "" f"  MASTER_KEY  = '{self.master_key}'\n"
                    if self.master_key
                    else (
                        "" f"  REGION  = '{self.region}'\n"
                        if self.region
                        else (
                            "" f"  SECURITY_TOKEN  = '{self.security_token}'\n"
                            if self.security_token
                            else "" f")"
                        )
                    )
                )
            )
        )


class AzureBlobStorage(_StorageClause):
    """Microsoft Azure Blob Storage"""

    __visit_name__ = "azure_blob_storage"

    def __init__(self, *, uri: str, account_name: str, account_key: str):
        r = urlparse(uri)
        if r.scheme != "azblob":
            raise ValueError(f"Invalid Azure URI: {uri}")

        self.uri = uri
        self.account_name = account_name
        self.account_key = account_key
        self.container = r.netloc
        self.path = r.path

    def __repr__(self):
        return (
            f"'{self.uri}' \n"
            f"CONNECTION = (\n"
            f"  ENDPOINT_URL = 'https://{self.account_name}.blob.core.windows.net' \n"
            f"  ACCOUNT_NAME = '{self.account_name}' \n"
            f"  ACCOUNT_KEY = '{self.account_key}'\n"
            f")"
        )


class GoogleCloudStorage(_StorageClause):
    """Google Cloud Storage"""

    __visit_name__ = "google_cloud_storage"

    def __init__(self, *, uri, credentials):
        r = urlparse(uri)
        if r.scheme != "gcs":
            raise ValueError(f"Invalid Google Cloud Storage URI: {uri}")

        self.uri = uri
        self.credentials = credentials

    def __repr__(self):
        return (
            f"'{self.uri}' \n"
            f"CONNECTION = (\n"
            f"  ENDPOINT_URL = 'https://storage.googleapis.com' \n"
            f"  CREDENTIAL = '{self.credentials}' \n"
            f")"
        )
