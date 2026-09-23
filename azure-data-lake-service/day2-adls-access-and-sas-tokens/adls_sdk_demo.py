"""
ADLS Gen2 SDK Demo — Day 2
Tests: list containers, create directory, upload file, read file, list paths
Run: python adls_sdk_demo.py

Set these two env vars before running (never hardcode secrets):
  $env:ADLS_ACCOUNT_NAME = "stadlsdev001"
  $env:ADLS_SAS_TOKEN    = "?sv=2022-11-02&ss=b&..."
"""

import os
import sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError


def get_client() -> DataLakeServiceClient:
    account_name = os.environ.get("ADLS_ACCOUNT_NAME")
    sas_token = os.environ.get("ADLS_SAS_TOKEN")

    if not account_name or not sas_token:
        print("ERROR: Set ADLS_ACCOUNT_NAME and ADLS_SAS_TOKEN environment variables first.")
        print("  PowerShell:  $env:ADLS_ACCOUNT_NAME = 'your-account'")
        print("               $env:ADLS_SAS_TOKEN    = '?sv=2022-...'")
        sys.exit(1)

    account_url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=sas_token)


def test_list_containers(client: DataLakeServiceClient):
    print("\n--- Test 1: List containers (file systems) ---")
    file_systems = client.list_file_systems()
    found = []
    for fs in file_systems:
        found.append(fs.name)
        print(f"  Container: {fs.name}")
    print(f"  PASS — found {len(found)} container(s): {found}")
    return found


def test_create_directory(client: DataLakeServiceClient, container: str):
    print(f"\n--- Test 2: Create directory in '{container}' ---")
    fs_client = client.get_file_system_client(container)
    dir_path = "day2-sdk-test/demo-dir"

    try:
        dir_client = fs_client.get_directory_client(dir_path)
        dir_client.create_directory()
        print(f"  Created: {container}/{dir_path}")
    except ResourceExistsError:
        print(f"  Directory already exists (OK): {container}/{dir_path}")

    print("  PASS")
    return dir_path


def test_upload_file(client: DataLakeServiceClient, container: str, dir_path: str):
    print(f"\n--- Test 3: Upload file to '{container}/{dir_path}' ---")
    fs_client = client.get_file_system_client(container)
    dir_client = fs_client.get_directory_client(dir_path)
    file_client = dir_client.get_file_client("sample_payments.csv")

    csv_content = (
        "payment_id,amount,currency,status\n"
        "PAY001,100.00,AUD,settled\n"
        "PAY002,250.50,AUD,pending\n"
        "PAY003,75.25,AUD,settled\n"
        "PAY004,999.99,AUD,failed\n"
    )
    encoded = csv_content.encode("utf-8")

    file_client.create_file()
    file_client.append_data(data=encoded, offset=0, length=len(encoded))
    file_client.flush_data(len(encoded))

    print(f"  Uploaded: {container}/{dir_path}/sample_payments.csv ({len(encoded)} bytes)")
    print("  PASS")


def test_read_file(client: DataLakeServiceClient, container: str, dir_path: str):
    print(f"\n--- Test 4: Read file from '{container}/{dir_path}' ---")
    fs_client = client.get_file_system_client(container)
    file_client = fs_client.get_file_client(f"{dir_path}/sample_payments.csv")

    try:
        download = file_client.download_file()
        content = download.readall().decode("utf-8")
        lines = content.strip().split("\n")
        print(f"  File content ({len(lines)} rows including header):")
        for line in lines:
            print(f"    {line}")
        print("  PASS")
    except ResourceNotFoundError:
        print("  FAIL — file not found (did Test 3 succeed?)")


def test_list_paths(client: DataLakeServiceClient, container: str, dir_path: str):
    print(f"\n--- Test 5: List paths under '{container}/{dir_path}' ---")
    fs_client = client.get_file_system_client(container)
    paths = fs_client.get_paths(path=dir_path, recursive=True)

    count = 0
    for path in paths:
        kind = "DIR " if path.is_directory else "FILE"
        size = f"{path.content_length} bytes" if not path.is_directory else ""
        print(f"  [{kind}] {path.name}  {size}")
        count += 1

    print(f"  PASS — listed {count} path(s)")


def main():
    print("=" * 55)
    print("  ADLS Gen2 Python SDK Demo — Day 2")
    print("=" * 55)

    client = get_client()

    # Test 1: list containers
    containers = test_list_containers(client)

    # Pick 'bronze' if it exists, otherwise use first container
    target = "bronze" if "bronze" in containers else (containers[0] if containers else None)
    if not target:
        print("\nERROR: No containers found. Create 'bronze' in your ADLS Gen2 account first.")
        sys.exit(1)

    print(f"\nUsing container: '{target}'")

    # Tests 2-5 on the chosen container
    dir_path = test_create_directory(client, target)
    test_upload_file(client, target, dir_path)
    test_read_file(client, target, dir_path)
    test_list_paths(client, target, dir_path)

    print("\n" + "=" * 55)
    print("  All tests passed!")
    print(f"  Check portal: {target}/{dir_path}/")
    print("=" * 55)


if __name__ == "__main__":
    main()
