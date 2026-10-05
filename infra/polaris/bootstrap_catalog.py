#!/usr/bin/env python3
"""Bootstrap script for Apache Polaris REST Catalog.

Obtains an OAuth2 bearer token using root credentials and provisions
the 'dtcc_catalog' mapped to the Garage S3 bucket (s3://dtcc-lakehouse/).
"""
import json
import os
import sys
import urllib.error
import urllib.request

POLARIS_HOST = os.getenv("POLARIS_HOST", "127.0.0.1")
POLARIS_PORT = os.getenv("POLARIS_PORT", "8181")
POLARIS_REALM = os.getenv("POLARIS_REALM", "POLARIS")
CLIENT_ID = os.getenv("POLARIS_CLIENT_ID", "root")
CLIENT_SECRET = os.getenv("POLARIS_CLIENT_SECRET", "s3cr3t")

TOKEN_URL = f"http://{POLARIS_HOST}:{POLARIS_PORT}/api/catalog/v1/oauth/tokens"
CATALOGS_URL = f"http://{POLARIS_HOST}:{POLARIS_PORT}/api/management/v1/catalogs"
GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://garage:3900")
S3_BUCKET = os.getenv("S3_BUCKET", "dtcc-lakehouse")


def get_oauth_token() -> str:
    """Request an OAuth2 Bearer token from Polaris."""
    data = (
        f"grant_type=client_credentials&client_id={CLIENT_ID}"
        f"&client_secret={CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL"
    ).encode("utf-8")

    req = urllib.request.Request(
        TOKEN_URL,
        data=data,
        headers={
            "Polaris-Realm": POLARIS_REALM,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        return body["access_token"]


def delete_catalog_if_exists(token: str, catalog_name: str = "dtcc_catalog") -> None:
    """Delete an existing catalog to allow fresh re-configuration."""
    req = urllib.request.Request(
        f"{CATALOGS_URL}/{catalog_name}",
        headers={
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": POLARIS_REALM,
        },
        method="DELETE",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"Existing catalog '{catalog_name}' deleted (HTTP {resp.status}).")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"Notice on delete (HTTP {e.code}): {e.read().decode()}")


def create_catalog(token: str, catalog_name: str = "dtcc_catalog") -> dict:
    """Register an internal S3-backed catalog in Polaris with region specified."""
    payload = {
        "catalog": {
            "name": catalog_name,
            "type": "INTERNAL",
            "readOnly": False,
            "properties": {
                "default-base-location": f"s3://{S3_BUCKET}/",
            },
            "storageConfigInfo": {
                "storageType": "S3",
                "allowedLocations": [f"s3://{S3_BUCKET}/"],
                "endpoint": GARAGE_ENDPOINT,
                "region": "garage",
                "stsUnavailable": True,
                "pathStyleAccess": True,
            },
        }
    }

    req = urllib.request.Request(
        CATALOGS_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": POLARIS_REALM,
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        data = resp.read().decode("utf-8")
        return json.loads(data) if data else {"status": resp.status}


def list_catalogs(token: str) -> list:
    """List all registered catalogs in Polaris."""
    req = urllib.request.Request(
        CATALOGS_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "Polaris-Realm": POLARIS_REALM,
        },
        method="GET",
    )
    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        return body.get("catalogs", [])


def main():
    print(f"Authenticating with Polaris at http://{POLARIS_HOST}:{POLARIS_PORT}...")
    token = get_oauth_token()
    print("Obtained OAuth2 token.")

    print("Re-provisioning 'dtcc_catalog' with region 'us-east-1'...")
    delete_catalog_if_exists(token, "dtcc_catalog")
    result = create_catalog(token, "dtcc_catalog")
    print(f"Catalog registration result: {result}")

    catalogs = list_catalogs(token)
    catalog_names = [c.get("name") for c in catalogs]
    print(f"Active Polaris Catalogs: {catalog_names}")
    assert "dtcc_catalog" in catalog_names, "dtcc_catalog was not found!"
    print("Polaris bootstrap verification successful!")


if __name__ == "__main__":
    main()
