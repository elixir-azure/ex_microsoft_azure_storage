# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## 0.3.4 - 2021-10-25

- `Blob.put_blob_from_url` content type warning is now suppressible in config.

## 0.3.3 - 2021-10-06

### Fixed

- `Blob.get_blob_properties` no longer raises when the blob is not found.

## 0.3.2 - 2021-08-08

### Fixed

- `BlobStorage.get_blob_service_properties` now works correctly with the Azure Storage simulator
  Azurite.

## 0.3.1 - 2021-07-12

### Fixed

- #3 Workaround now preserves all blob properties, or attempts to at least.

## 0.3.0 - 2021-07-12

### Added

- Support for Azure Storage connection strings
- Support for getting, setting and updating blob properties
- #3 Workaround for loss of `content-type` when using `Blob.put_blob_from_url/2`

## 0.2.3 - 2021-07-05

### Added

- Generate SAS tokens overriding content-disposition, content-encoding, content-language and so on.

## 0.2.2 - 2021-07-05

### Fixed

- SAS tokens now correctly generated to API version `2020-04-08`

## 0.2.1 - 2021-07-05

### Fixed

- `BlobStorage.get_blob_service_properties` and `BlobStorage.get_blob_service_stats` now function as expected.

## 0.2.0 - 2021-07-05

### Added

- Add `Blob.put_blob` and `Blob.put_blob_from_url` APIs

### Changed

- Bump API version from `2018-03-28` to `2020-04-08`

## 0.1.0 - 2021-07-05

### Added

- Pull in work from original [ex_microsoft_azure_storage](https://github.com/chgeuer/ex_microsoft_azure_storage).
