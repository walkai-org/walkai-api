from datetime import UTC, datetime
from io import BytesIO

import boto3
from botocore.response import StreamingBody
from botocore.stub import Stubber

from app.core.aws import get_s3_client
from app.main import app
from app.services import job_service


def _create_volume_with_prefix(db_session, *, prefix: str):
    volume = job_service.create_volume(db_session, storage=4, is_input=False)
    volume.key_prefix = prefix
    db_session.commit()
    db_session.refresh(volume)
    return volume


def test_list_volume_objects_returns_files(auth_client, db_session):
    client, _ = auth_client
    prefix = "users/10/jobs/20/30/outputs"
    volume = _create_volume_with_prefix(db_session, prefix=prefix)

    s3_client = boto3.client(
        "s3",
        region_name="us-test-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(s3_client)
    response = {
        "IsTruncated": False,
        "KeyCount": 2,
        "Contents": [
            {
                "Key": f"{prefix}/logs/main.log",
                "Size": 42,
                "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                "ETag": '"etag1"',
            },
            {
                "Key": f"{prefix}/results/output.json",
                "Size": 128,
                "LastModified": datetime(2024, 1, 2, tzinfo=UTC),
                "ETag": '"etag2"',
            },
        ],
        "CommonPrefixes": [
            {"Prefix": f"{prefix}/results/"},
        ],
        "Name": "test-bucket",
        "Prefix": f"{prefix}/",
        "MaxKeys": 1000,
    }
    stubber.add_response(
        "list_objects_v2",
        response,
        {"Bucket": "test-bucket", "Prefix": f"{prefix}/", "Delimiter": "/"},
    )
    stubber.activate()

    app.dependency_overrides[get_s3_client] = lambda: s3_client
    try:
        resp = client.get(f"/volumes/{volume.id}/objects")
    finally:
        app.dependency_overrides.pop(get_s3_client, None)
        stubber.assert_no_pending_responses()
        stubber.deactivate()

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["prefix"] == prefix
    assert payload["directories"] == ["results/"]
    assert payload["truncated"] is False
    assert payload["next_continuation_token"] is None
    objects = payload["objects"]
    assert {obj["key"] for obj in objects} == {
        "logs/main.log",
        "results/output.json",
    }


def test_list_volume_objects_handles_empty(auth_client, db_session):
    client, _ = auth_client
    prefix = "users/11/jobs/22/33/outputs"
    volume = _create_volume_with_prefix(db_session, prefix=prefix)

    s3_client = boto3.client(
        "s3",
        region_name="us-test-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(s3_client)
    stubber.add_response(
        "list_objects_v2",
        {
            "IsTruncated": False,
            "KeyCount": 0,
            "Name": "test-bucket",
            "Prefix": f"{prefix}/",
            "MaxKeys": 1000,
        },
        {"Bucket": "test-bucket", "Prefix": f"{prefix}/", "Delimiter": "/"},
    )
    stubber.activate()

    app.dependency_overrides[get_s3_client] = lambda: s3_client
    try:
        resp = client.get(f"/volumes/{volume.id}/objects")
    finally:
        app.dependency_overrides.pop(get_s3_client, None)
        stubber.assert_no_pending_responses()
        stubber.deactivate()

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["objects"] == []
    assert payload["directories"] == []
    assert payload["truncated"] is False


def test_list_volume_objects_requires_prefix(auth_client, db_session):
    client, _ = auth_client
    volume = job_service.create_volume(db_session, storage=2, is_input=False)
    db_session.commit()

    resp = client.get(f"/volumes/{volume.id}/objects")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Volume is not stored in object storage"


def test_download_volume_file(auth_client, db_session):
    client, _ = auth_client
    prefix = "users/15/jobs/25/35/outputs"
    volume = _create_volume_with_prefix(db_session, prefix=prefix)

    file_bytes = b"example-data"
    key = "artifacts/output.bin"
    s3_client = boto3.client(
        "s3",
        region_name="us-test-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(s3_client)
    stubber.add_response(
        "get_object",
        {
            "Body": StreamingBody(BytesIO(file_bytes), len(file_bytes)),
            "ContentType": "application/octet-stream",
            "ContentLength": len(file_bytes),
            "ETag": '"etag-file"',
        },
        {"Bucket": "test-bucket", "Key": f"{prefix}/{key}"},
    )
    stubber.activate()

    app.dependency_overrides[get_s3_client] = lambda: s3_client
    try:
        resp = client.get(
            f"/volumes/{volume.id}/file",
            params={"key": key},
        )
    finally:
        app.dependency_overrides.pop(get_s3_client, None)
        stubber.assert_no_pending_responses()
        stubber.deactivate()

    assert resp.status_code == 200
    assert resp.content == file_bytes
    assert resp.headers["etag"] == '"etag-file"'
    assert resp.headers["content-disposition"] == 'attachment; filename="output.bin"'


def test_download_volume_file_missing(auth_client, db_session):
    client, _ = auth_client
    prefix = "users/16/jobs/26/36/outputs"
    volume = _create_volume_with_prefix(db_session, prefix=prefix)

    key = "logs/main.log"
    s3_client = boto3.client(
        "s3",
        region_name="us-test-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    stubber = Stubber(s3_client)
    stubber.add_client_error(
        "get_object",
        service_error_code="NoSuchKey",
        service_message="Not found",
        http_status_code=404,
        expected_params={"Bucket": "test-bucket", "Key": f"{prefix}/{key}"},
    )
    stubber.activate()

    app.dependency_overrides[get_s3_client] = lambda: s3_client
    try:
        resp = client.get(f"/volumes/{volume.id}/file", params={"key": key})
    finally:
        app.dependency_overrides.pop(get_s3_client, None)
        stubber.assert_no_pending_responses()
        stubber.deactivate()

    assert resp.status_code == 404
    assert resp.json()["detail"] == "File not found"


def test_download_volume_file_invalid_path(auth_client, db_session):
    client, _ = auth_client
    prefix = "users/17/jobs/27/37/outputs"
    volume = _create_volume_with_prefix(db_session, prefix=prefix)

    class _StubClient:
        def get_object(self, *args, **kwargs):
            raise AssertionError("S3 client should not be used")

    app.dependency_overrides[get_s3_client] = _StubClient
    try:
        resp = client.get(
            f"/volumes/{volume.id}/file",
            params={"key": "../secrets.txt"},
        )
    finally:
        app.dependency_overrides.pop(get_s3_client, None)

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Invalid file path"
