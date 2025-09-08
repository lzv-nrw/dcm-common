"""Tests for the `JobInfo`- and related data models."""

from datetime import datetime
import pickle

from dcm_common.models.data_model import get_model_serialization_test
from dcm_common.orchestra.models import (
    JobConfig,
    MetadataRecord,
    JobMetadata,
    JobInfo,
    Token,
    Report,
)


test_job_config_json = get_model_serialization_test(
    JobConfig,
    (
        (("a", {"reqest": 0}, {"original": 1}), {}),
        (("a", {"reqest": 0}, {"original": 1}, {"property": 2}), {}),
    ),
)


test_metadata_record_json = get_model_serialization_test(
    MetadataRecord,
    (
        ((), {}),
        (("a", datetime.now().isoformat()), {}),
    ),
)


test_job_metadata_json = get_model_serialization_test(
    JobMetadata,
    (
        ((), {}),
        (
            (
                MetadataRecord("a"),
                MetadataRecord("b"),
                MetadataRecord("c"),
                MetadataRecord("d"),
            ),
            {},
        ),
    ),
)


def test_job_metadata_helpers():
    """Test helper-methods of `JobMetadata`."""
    metadata = JobMetadata()

    assert metadata.produced is None
    metadata.produce("a")
    assert metadata.produced is not None
    assert metadata.produced.by == "a"

    assert metadata.consumed is None
    metadata.consume("b")
    assert metadata.consumed is not None
    assert metadata.consumed.by == "b"

    assert metadata.aborted is None
    metadata.abort("c")
    assert metadata.aborted is not None
    assert metadata.aborted.by == "c"

    assert metadata.completed is None
    metadata.complete("d")
    assert metadata.completed is not None
    assert metadata.completed.by == "d"


def test_job_info_json():
    """
    Test `JobInfo.json` (manually due to behavior regarding Report).
    """
    info = JobInfo(JobConfig("a", {}, {}), Token("b"), JobMetadata(), Report())

    info_json = info.json
    info_from_json = JobInfo.from_json(info_json)

    assert info_json == info_from_json.json

    # explicitly parse report (JSON) as Report-instance
    info_from_json.report = Report.from_json(info_from_json.report)

    assert pickle.dumps(info) == pickle.dumps(info_from_json)
