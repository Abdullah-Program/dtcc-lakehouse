#!/usr/bin/env python3
"""Integration tests for Garage S3 Object Store.

Validates S3 API compatibility using AWS SigV4 signatures over pure Python
stdlib (no external pip dependencies). Verifies PUT, GET, and LIST operations
against the dtcc-lakehouse bucket.
"""
import datetime
import hashlib
import hmac
import json
import os
import unittest
import urllib.error
import urllib.request

ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "GK135da1acc67448e5f988c2d9")
SECRET_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY",
    "8ebd487cc54261411c33d336465f8a26ba58c064bddb3c56692189b2b4a21eba",
)
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://127.0.0.1:3900")
S3_REGION = os.getenv("AWS_REGION", "garage")
BUCKET_NAME = "dtcc-lakehouse"


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _get_signature_key(
    key: str, date_stamp: str, region_name: str, service_name: str
) -> bytes:
    k_date = _sign(("AWS4" + key).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region_name)
    k_service = _sign(k_region, service_name)
    k_signing = _sign(k_service, "aws4_request")
    return k_signing


def s3_request(
    method: str,
    path: str,
    payload: bytes = b"",
    content_type: str = "application/octet-stream",
) -> tuple[int, bytes]:
    """Execute an AWS SigV4 signed HTTP request against Garage S3."""
    host = S3_ENDPOINT.replace("http://", "").replace("https://", "")
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    payload_hash = hashlib.sha256(payload).hexdigest()

    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = (
        f"{method}\n"
        f"{path}\n"
        f"\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{payload_hash}"
    )

    credential_scope = f"{date_stamp}/{S3_REGION}/s3/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n"
        f"{amz_date}\n"
        f"{credential_scope}\n"
        f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    )

    signing_key = _get_signature_key(SECRET_KEY, date_stamp, S3_REGION, "s3")
    signature = hmac.new(
        signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
    ).hexdigest()

    auth_header = (
        f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    req = urllib.request.Request(
        f"{S3_ENDPOINT}{path}",
        data=payload if method in ("PUT", "POST") else None,
        headers={
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
            "Authorization": auth_header,
            "Content-Type": content_type,
        },
        method=method,
    )

    with urllib.request.urlopen(req) as resp:
        return resp.status, resp.read()


class TestGarageS3(unittest.TestCase):
    """Test suite validating Garage S3 API operations."""

    def test_01_list_bucket(self):
        """Verify the dtcc-lakehouse bucket is reachable and listable."""
        status, body = s3_request("GET", f"/{BUCKET_NAME}/")
        self.assertEqual(status, 200)
        self.assertIn(b"ListBucketResult", body)

    def test_02_put_and_get_object(self):
        """Verify object write and read-back integrity."""
        test_payload = json.dumps(
            {
                "engine": "garage-s3",
                "table_format": "apache-iceberg",
                "lakehouse": "dtcc",
                "status": "operational",
            }
        ).encode("utf-8")

        # 1. PUT Object
        put_status, _ = s3_request(
            "PUT",
            f"/{BUCKET_NAME}/smoke_test/status.json",
            payload=test_payload,
            content_type="application/json",
        )
        self.assertEqual(put_status, 200)

        # 2. GET Object
        get_status, get_body = s3_request(
            "GET", f"/{BUCKET_NAME}/smoke_test/status.json"
        )
        self.assertEqual(get_status, 200)
        self.assertEqual(get_body, test_payload)

        # 3. Verify parsed JSON content
        data = json.loads(get_body.decode("utf-8"))
        self.assertEqual(data["lakehouse"], "dtcc")
        self.assertEqual(data["status"], "operational")


if __name__ == "__main__":
    unittest.main()
