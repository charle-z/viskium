"""Bounded local storage adapters and explicit data-root layout."""

from .layout import (
    DATA_CATEGORIES,
    DATA_ROOT_MARKER,
    DATA_ROOT_SCHEMA_VERSION,
    DataRootLayout,
    StorageLayoutError,
    initialize_data_root,
    verify_data_root,
)
from .sqlite_store import (
    PurgeReport,
    SQLiteStore,
    SQLiteStoreError,
    SQLiteStoreIntegrityError,
    SQLiteStoreReadOnlyError,
    StoredObservation,
    StoreFootprint,
    StoreHealth,
)
from .writer import (
    ObservationWriter,
    SubmissionResult,
    SubmissionStatus,
    WriterMetrics,
    WriterReceiptMetadata,
    WriterStartReport,
    WriterState,
    WriterStopReport,
)

__all__ = [
    "DATA_CATEGORIES",
    "DATA_ROOT_MARKER",
    "DATA_ROOT_SCHEMA_VERSION",
    "DataRootLayout",
    "ObservationWriter",
    "PurgeReport",
    "SQLiteStore",
    "SQLiteStoreError",
    "SQLiteStoreIntegrityError",
    "SQLiteStoreReadOnlyError",
    "StorageLayoutError",
    "StoreFootprint",
    "StoreHealth",
    "StoredObservation",
    "SubmissionResult",
    "SubmissionStatus",
    "WriterMetrics",
    "WriterReceiptMetadata",
    "WriterStartReport",
    "WriterState",
    "WriterStopReport",
    "initialize_data_root",
    "verify_data_root",
]
