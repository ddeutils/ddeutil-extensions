# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import httpx
import requests
from requests.auth import HTTPBasicAuth


def make_request(url, timeout=5.0):
    try:
        response = httpx.get(url, timeout=timeout)
        response.raise_for_status()  # Check for HTTP errors
        try:
            return response.json()
        except ValueError:
            return response.text  # Return raw text if not JSON
    except httpx.TimeoutException:
        print(f"Request timed out after {timeout} seconds")
    except httpx.HTTPStatusError as e:
        print(f"HTTP error occurred: {e}")
        print(f"Status code: {e.response.status_code}")
    except httpx.ConnectError:
        print("Failed to connect to the server")
    except httpx.RequestError as e:
        print(f"Request failed: {e}")
    return None


def upload_file():
    # Create a sample file if it doesn't exist
    sample_file = Path("sample.txt")
    if not sample_file.exists():
        sample_file.write_text("This is a sample file made by Yang Zhou.")

    # Example 1: Single file upload
    print("Uploading single file...")
    with open("sample.txt", "rb") as f:
        files = {"file": ("sample.txt", f, "text/plain")}
        response = httpx.post("https://httpbin.org/post", files=files)
        print(f"Response: {response.json()}")

    # Example 2: Multiple files upload
    print("\nUploading multiple files...")
    with open("sample.txt", "rb") as f1, open("sample.txt", "rb") as f2:
        files = {
            "file1": ("file1.txt", f1, "text/plain"),
            "file2": ("file2.txt", f2, "text/plain"),
        }
        response = httpx.post("https://httpbin.org/post", files=files)
        print(f"Response: {response.json()}")

    # Example 3: File with additional form data
    print("\nUploading file with form data...")
    with open("sample.txt", "rb") as f:
        data = {"description": "Sample file upload"}
        files = {"file": ("sample.txt", f, "text/plain")}
        response = httpx.post(
            "https://httpbin.org/post", data=data, files=files
        )
        print(f"Response: {response.json()}")


def download_file(url, output_path, chunk_size=1024):
    """
    Download a large file with streaming.

    Args:
        url (str): URL of the file to download
        output_path (str): Path where the file will be saved
        chunk_size (int): Size of chunks to read at a time
    """
    try:
        with httpx.stream("GET", url) as response:
            response.raise_for_status()

            # Stream the file to disk
            with open(output_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

            print(f"Download completed: {output_path}")

    except httpx.RequestError as e:
        print(f"Download failed: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
    except Exception as e:
        print(f"An error occurred: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)


async def fetch_single(client, url, delay=0):
    """Fetch a single URL with optional delay"""
    print(f"Starting request to {url}")
    response = await client.get(url)
    print(f"Completed request to {url}")
    return response.json()


async def async_request():
    # Create async client
    async with httpx.AsyncClient() as client:
        # Example 1: Sequential requests
        print("Making sequential requests...")
        start_time = time.time()

        response1 = await fetch_single(client, "https://httpbin.org/delay/1")
        response2 = await fetch_single(client, "https://httpbin.org/delay/2")
        _ = response1
        _ = response2

        sequential_time = time.time() - start_time
        print(f"Sequential requests took {sequential_time:.2f} seconds")

        # Example 2: Concurrent requests
        print("\nMaking concurrent requests...")
        start_time = time.time()

        responses = await asyncio.gather(
            fetch_single(client, "https://httpbin.org/delay/1"),
            fetch_single(client, "https://httpbin.org/delay/2"),
            fetch_single(client, "https://httpbin.org/delay/3"),
        )
        _ = responses

        concurrent_time = time.time() - start_time
        print(f"Concurrent requests took {concurrent_time:.2f} seconds")
        print(f"Time saved: {sequential_time - concurrent_time:.2f} seconds")

        # Example 3: Handling multiple different endpoints
        print("\nFetching from multiple endpoints...")
        tasks = [
            fetch_single(client, "https://httpbin.org/get"),
            fetch_single(client, "https://httpbin.org/headers"),
            fetch_single(client, "https://httpbin.org/ip"),
        ]

        results = await asyncio.gather(*tasks)
        for i, result in enumerate(results, 1):
            print(f"Response {i}: {result}")


# Base URL for the API
base_url = "https://jsonplaceholder.typicode.com/posts"


# 1. API Key Authentication
def api_key_auth():
    headers = {"Authorization": "Api-Key your_api_key"}
    response = requests.get(base_url, headers=headers)
    print("API Key Auth Response:", response.status_code, response.json())


# 2. Bearer Token Authentication
def bearer_token_auth():
    headers = {"Authorization": "Bearer your_bearer_token_here"}
    response = requests.get(base_url, headers=headers)
    print("Bearer Token Auth Response:", response.status_code, response.json())


# 3. Basic Authentication
def basic_auth():
    response = requests.get(
        base_url, auth=HTTPBasicAuth("username", "password")
    )
    print("Basic Auth Response:", response.status_code, response.json())


# 4. OAuth 2.0 Authentication (Client Credentials Flow)
def oauth2_auth():
    # Step 1: Obtain access token
    token_url = "https://auth.example.com/oauth/token"
    data = {
        "client_id": "your_client_id",
        "client_secret": "your_client_secret",
        "grant_type": "client_credentials",
    }
    token_response = requests.post(token_url, data=data)
    access_token = token_response.json().get("access_token")

    # Step 2: Use the access token for API requests
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(base_url, headers=headers)
    print("OAuth 2.0 Auth Response:", response.status_code, response.json())


# 5. JWT (JSON Web Token) Authentication
def jwt_auth():
    jwt_token = "your_jwt_token_here"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    response = requests.get(base_url, headers=headers)
    print("JWT Auth Response:", response.status_code, response.json())
