#!/usr/bin/env python


VERSION = (0, 5, 0)
__version__ = ".".join(str(x) for x in VERSION)


from .dml import (
    Merge,
    WhenMergeUnMatchedClause,
    WhenMergeMatchedDeleteClause,
    WhenMergeMatchedUpdateClause,
    CopyIntoTable,
    CopyIntoLocation,
    CopyIntoTableOptions,
    CopyIntoLocationOptions,
    CSVFormat,
    TSVFormat,
    NDJSONFormat,
    ParquetFormat,
    ORCFormat,
    AmazonS3,
    AzureBlobStorage,
    GoogleCloudStorage,
    FileColumnClause,
    StageClause,
    Compression,
)
