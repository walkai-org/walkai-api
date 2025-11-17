from datetime import UTC, datetime
from io import BytesIO

import boto3
from botocore.response import StreamingBody
from botocore.stub import Stubber

from app.core.aws import get_s3_client
from app.main import app
from app.models.jobs import Volume
from app.services import job_service


def _create_volume_with_prefix(db_session, *, prefix: str):
    volume = job_service.create_volume(db_session, storage=4, is_input=False)
    volume.key_prefix = prefix
    db_session.commit()
    db_session.refresh(volume)
    return volume


def test_list_volumes(auth_client, db_session):
    client, user = auth_client
    input_volume = job_service.create_input_volume_with_upload(
        db_session, user=user, storage=3
    )
    output_volume = job_service.create_volume(db_session, storage=5, is_input=False)
    output_volume.key_prefix = "users/99/jobs/123/outputs"
    db_session.commit()
    db_session.refresh(output_volume)

    resp = client.get("/volumes")

    assert resp.status_code == 200
    payload = resp.json()
    assert {vol["id"] for vol in payload} == {input_volume.id, output_volume.id}

    input_payload = next(vol for vol in payload if vol["id"] == input_volume.id)
    assert input_payload["is_input"] is True
    assert input_payload["size"] == input_volume.size
    assert input_payload["key_prefix"] == input_volume.key_prefix
    assert input_payload["pvc_name"] == input_volume.pvc_name

    output_payload = next(vol for vol in payload if vol["id"] == output_volume.id)
    assert output_payload["is_input"] is False
    assert output_payload["size"] == output_volume.size
    assert output_payload["key_prefix"] == output_volume.key_prefix
    assert output_payload["pvc_name"] == output_volume.pvc_name


def test_list_volumes_filters_by_is_input(auth_client, db_session):
    client, user = auth_client
    input_volume = job_service.create_input_volume_with_upload(
        db_session, user=user, storage=2
    )
    output_volume = job_service.create_volume(db_session, storage=4, is_input=False)
    db_session.commit()

    resp_inputs = client.get("/volumes", params={"is_input": True})
    assert resp_inputs.status_code == 200
    assert [vol["id"] for vol in resp_inputs.json()] == [input_volume.id]

    resp_outputs = client.get("/volumes", params={"is_input": False})
    assert resp_outputs.status_code == 200
    assert [vol["id"] for vol in resp_outputs.json()] == [output_volume.id]


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
        {"Bucket": "test-bucket", "Prefix": f"{prefix}/"},
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
        {"Bucket": "test-bucket", "Prefix": f"{prefix}/"},
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
    assert payload["truncated"] is False


def test_list_volume_objects_lists_nested_directories(auth_client, db_session):
    client, _ = auth_client
    prefix = "users/12/jobs/24/36/outputs"
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
        "KeyCount": 1,
        "Contents": [
            {
                "Key": f"{prefix}/artifacts/nested/item.txt",
                "Size": 16,
                "LastModified": datetime(2024, 2, 2, tzinfo=UTC),
                "ETag": '"etag-nested"',
            }
        ],
        "Name": "test-bucket",
        "Prefix": f"{prefix}/",
        "MaxKeys": 1000,
    }
    stubber.add_response(
        "list_objects_v2",
        response,
        {"Bucket": "test-bucket", "Prefix": f"{prefix}/"},
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
    assert payload["objects"][0]["key"] == "artifacts/nested/item.txt"


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


def test_create_input_volume_returns_volume_data(auth_client, db_session):
    client, user = auth_client

    resp = client.post(
        "/volumes/inputs",
        json={"storage": 3},
    )

    assert resp.status_code == 201
    payload = resp.json()
    volume = payload["volume"]

    assert volume["is_input"] is True
    assert volume["size"] == 3
    assert volume["pvc_name"].startswith("input-")
    assert volume["key_prefix"].startswith(f"users/{user.id}/inputs/")

    db_volume = db_session.get(Volume, volume["id"])
    assert db_volume is not None
    assert db_volume.key_prefix == volume["key_prefix"]
    assert db_volume.is_input is True


def test_upload_file_returns_presigned_urls(auth_client, db_session, monkeypatch):
    client, user = auth_client
    input_volume = job_service.create_input_volume_with_upload(
        db_session, user=user, storage=2
    )

    calls: list[dict[str, object]] = []

    def _fake_presign(s3_client, *, key, method="PUT", expires=3600):
        calls.append({"key": key, "method": method})
        return f"https://example.com/{key}"

    monkeypatch.setattr("app.api.volumes.presign_url", _fake_presign)

    class _StubS3:
        pass

    app.dependency_overrides[get_s3_client] = lambda: _StubS3()
    try:
        resp = client.post(
            "/volumes/inputs/presigneds",
            json={"volume_id": input_volume.id, "file_names": ["aaa.txt", "bbb.txt"]},
        )
    finally:
        app.dependency_overrides.pop(get_s3_client, None)

    assert resp.status_code == 201
    assert resp.json() == {
        "presigneds": [
            f"https://example.com/{input_volume.key_prefix}/aaa.txt",
            f"https://example.com/{input_volume.key_prefix}/bbb.txt",
        ]
    }
    assert len(calls) == 2


def test_upload_file_rejects_non_input_volume(auth_client, db_session):
    client, _ = auth_client
    volume = job_service.create_volume(db_session, storage=1, is_input=False)
    volume.key_prefix = "users/1/jobs/1/outputs"
    db_session.commit()

    class _StubS3:
        pass

    app.dependency_overrides[get_s3_client] = lambda: _StubS3()
    try:
        resp = client.post(
            "/volumes/inputs/presigneds",
            json={"volume_id": volume.id, "file_names": ["aaa.txt"]},
        )
    finally:
        app.dependency_overrides.pop(get_s3_client, None)

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Volume must be input vol"
