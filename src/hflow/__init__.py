"""hflow: an open-source robotics data pipeline.

Ingest, quality-check, enrich, and curate robot episode data. See README.md
and docs/ARCHITECTURE.md; the primary source is Dyna Robotics'
"Training Dyna-2 at million-hour scale, repeatably".
"""

from importlib.metadata import PackageNotFoundError, version

from hflow import checks, ffmpeg, providers, testing
from hflow.app import (
    App,
    CheckRunReport,
    CheckStatus,
    EnrichmentRunReport,
    TestReport,
)
from hflow.batching import PlannedBatch, plan_batches, plan_batches_from_files
from hflow.catalog import AppendResult, Catalog, CheckRunRow
from hflow.curation import (
    CheckCoverage,
    CurationReport,
    StaleEpisode,
    curate,
    open_catalog_connection,
    stale_episodes,
)
from hflow.doctor import DiagnosticLevel, DoctorReport, Finding, diagnose
from hflow.episode import ChannelData, Episode, ExtractedFrame
from hflow.format import GopPreset
from hflow.mcap_writer import CanonicalMcapWriter
from hflow.reader import (
    EpisodeReader,
    MessageBatch,
    PythonMcapEpisodeReader,
    TopicInfo,
    open_reader,
)
from hflow.resample import DerivedSeries, ResamplePolicy, to_grid
from hflow.steps import (
    RUN_PROFILES,
    CheckResult,
    DerivedChannel,
    EnrichmentResult,
    Interval,
    MeasurementValue,
    RegisteredCheck,
    RegisteredEnrichment,
    Stage,
    stages_for_profile,
)
from hflow.storage import (
    BucketStorageRoot,
    LocalStorageRoot,
    StorageListing,
    StorageRoot,
    fetch_uri,
    is_bucket_url,
    parse_storage_root,
)
from hflow.transform import EpisodeStamps, TransformConfig, write_canonical_episode

try:
    __version__ = version("hflow")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "0.0.0"

__all__ = [
    "RUN_PROFILES",
    "App",
    "AppendResult",
    "BucketStorageRoot",
    "CanonicalMcapWriter",
    "Catalog",
    "ChannelData",
    "CheckCoverage",
    "CheckResult",
    "CheckRunReport",
    "CheckRunRow",
    "CheckStatus",
    "CurationReport",
    "DerivedChannel",
    "DerivedSeries",
    "DiagnosticLevel",
    "DoctorReport",
    "EnrichmentResult",
    "EnrichmentRunReport",
    "Episode",
    "EpisodeReader",
    "EpisodeStamps",
    "ExtractedFrame",
    "Finding",
    "GopPreset",
    "Interval",
    "LocalStorageRoot",
    "MeasurementValue",
    "MessageBatch",
    "PlannedBatch",
    "PythonMcapEpisodeReader",
    "RegisteredCheck",
    "RegisteredEnrichment",
    "ResamplePolicy",
    "Stage",
    "StaleEpisode",
    "StorageListing",
    "StorageRoot",
    "TestReport",
    "TopicInfo",
    "TransformConfig",
    "__version__",
    "checks",
    "curate",
    "diagnose",
    "fetch_uri",
    "ffmpeg",
    "is_bucket_url",
    "open_catalog_connection",
    "open_reader",
    "parse_storage_root",
    "plan_batches",
    "plan_batches_from_files",
    "providers",
    "stages_for_profile",
    "stale_episodes",
    "testing",
    "to_grid",
    "write_canonical_episode",
]
