# S3 Browser (Python/Tkinter)

A Linux-native S3 browser with endpoint management, bucket/object navigation, version awareness, and built-in tools for test data and bulk cleanup.

## Features

- Endpoint manager with per-endpoint HTTP/HTTPS selection and SSL verification toggle.
- Bucket list, create/delete buckets, and lifecycle policy management.
- Object browsing with prefix filter, folder navigation, flat view, and metadata/tags.
- Multi-select download/delete for objects.
- Versions tab with:
  - Load all versions or selected object versions.
  - Filters by prefix and text/regex.
  - Toggle display of versions vs delete markers.
  - Multi-select download/delete for versions.
  - Summary counts for objects, versions, and delete markers.
- Safety settings for retries, delays, and timeouts.
- Tools menu to run `put-testdata.py` and `delete-all.py` with GUI options and cancel support.

## Requirements

- Python 3
- `boto3`
- Tkinter (usually bundled with Python on Linux)

Install dependencies:

```bash
pip install boto3
```

## Run

```bash
python s3_browser.py
```

## Endpoint Configuration

Use `File -> Configure Endpoints` to add or edit S3 endpoints.

Fields:
- Name
- Endpoint URL (host:port or full URL; scheme is chosen separately)
- Access Key / Secret Key
- Region
- Scheme: `https` or `http`
- Verify SSL: disable for self-signed endpoints or to bypass certificate checks

If you see `ssl Wrong_version_number`, verify the scheme or disable SSL verification.

## Object Browsing

- Select a bucket to list objects.
- Use the Prefix filter to limit object listing.
- Right-click an object to download, delete, or copy its key.
- Select multiple objects to download or delete in bulk.

## Versions Tab

- `Show All Versions` loads all versions for the current bucket (prefix filter is applied server-side if set).
- Selecting an object loads only versions for that key.
- Filters:
  - Prefix: filters by object key prefix.
  - Text/Regex: filters by key or version id (toggle regex).
  - Toggle Versions/Delete Markers to narrow what you see.
- Summary label shows total entries, object count, versions, and delete markers.

## Safety Settings

`Settings -> Safety Settings` provides:
- Max retries
- Retry base/max delay
- Optional per-request delay
- Connect/read timeouts

These settings are applied to new S3 client connections and used to add retry/backoff behavior around API calls.

## Tools

### Put Test Data
`Tools -> Put Test Data...`

GUI options mirror `put-testdata.py` arguments, including:
- Bucket, endpoint, access key, secret key
- Object size, versions, objects count
- Prefix, thread count, checksum, debug

### Delete All
`Tools -> Delete All...`

GUI options mirror `delete-all.py` arguments, including:
- Bucket, endpoint, access key, secret key
- Batch size, max workers, retries, retry mode
- Connections, pipeline size, list max keys
- Immediate deletion toggle, deletion delay, debug

Both tools support cancel and show output in the GUI.

## Files

- `s3_browser.py`: main GUI application
- `put-testdata.py`: test data generator
- `delete-all.py`: high-performance delete tool
- `s3-browser-launcher.sh`: launcher script
