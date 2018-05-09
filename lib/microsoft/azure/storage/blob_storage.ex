defmodule Microsoft.Azure.Storage.BlobStorage do
  import SweetXml
  alias Microsoft.Azure.Storage.ApiVersion.Models.BlobStorageSignedFields, as: SignedData
  alias Microsoft.Azure.Storage.BlobClient
  alias Microsoft.Azure.Storage.DateTimeUtils

  def create_container(account_name, account_key, container_name) do
    container_name = container_name |> String.downcase()

    cloudEnvironmentSuffix = "core.windows.net"
    host = "#{account_name}.blob.#{cloudEnvironmentSuffix}"
    resourcePath = "/#{container_name}/"
    query = %{restype: "container"} |> URI.encode_query()
    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()
    base_uri = "#{uri.scheme}://#{uri.host}:#{uri.port}"
    request_path = "#{uri.path}?#{uri.query}"

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => "2015-04-05"
    }

    hdr = fn headers, name -> "#{name}:" <> headers[name] end

    signature =
      SignedData.new()
      |> Map.put(:verb, "PUT")
      |> Map.put(
        :canonicalizedHeaders,
        hdr.(headers, "x-ms-date") <> "\n" <> hdr.(headers, "x-ms-version")
      )
      |> Map.put(
        :canonicalizedResource,
        "/#{account_name}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> SignedData.sign(account_key)

    client =
      BlobClient.new(
        base_uri,
        headers |> Map.put("Authorization", "SharedKey #{account_name}:#{signature}")
      )

    %{status: 201} = client |> BlobClient.put(request_path, "")
  end

  def list_containers(account_name, account_key) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authentication-for-the-azure-storage-services

    cloudEnvironmentSuffix = "core.windows.net"
    host = "#{account_name}.blob.#{cloudEnvironmentSuffix}"
    resourcePath = "/"
    query = %{comp: "list"} |> URI.encode_query()
    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()
    base_uri = "#{uri.scheme}://#{uri.host}:#{uri.port}"
    request_path = "#{uri.path}?#{uri.query}"

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => "2015-04-05"
    }

    hdr = fn headers, name -> "#{name}:" <> headers[name] end

    signature =
      SignedData.new()
      |> Map.put(:verb, "GET")
      |> Map.put(
        :canonicalizedHeaders,
        hdr.(headers, "x-ms-date") <> "\n" <> hdr.(headers, "x-ms-version")
      )
      |> Map.put(
        :canonicalizedResource,
        "/#{account_name}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> SignedData.sign(account_key)

    client =
      BlobClient.new(
        base_uri,
        headers |> Map.put("Authorization", "SharedKey #{account_name}:#{signature}")
      )

    %{status: 200, body: bodyXml} = client |> BlobClient.get(request_path)

    {:ok,
     bodyXml
     |> xmap(
       containers: [
         ~x"/EnumerationResults/Containers/Container"l,
         name: ~x"./Name/text()"s,
         properties: [
           ~x"./Properties",
           lastModified: ~x"./Last-Modified/text()"s,
           eTag: ~x"./Etag/text()"s,
           leaseStatus: ~x"./LeaseStatus/text()"s,
           leaseState: ~x"./LeaseState/text()"s
         ]
       ]
     )}
  end

  def get_container_properties(account_name, account_key, container_name) do
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-container-properties

    cloudEnvironmentSuffix = "core.windows.net"
    host = "#{account_name}.blob.#{cloudEnvironmentSuffix}"
    resourcePath = "/#{container_name}"
    query = %{restype: "container"} |> URI.encode_query()
    uri = "https://#{host}#{resourcePath}?#{query}" |> URI.parse()
    base_uri = "#{uri.scheme}://#{uri.host}:#{uri.port}"
    request_path = "#{uri.path}?#{uri.query}"

    headers = %{
      "x-ms-date" => DateTimeUtils.utc_now(),
      "x-ms-version" => "2015-04-05"
    }

    hdr = fn headers, name -> "#{name}:" <> headers[name] end

    signature =
      SignedData.new()
      |> Map.put(:verb, "GET")
      |> Map.put(
        :canonicalizedHeaders,
        hdr.(headers, "x-ms-date") <> "\n" <> hdr.(headers, "x-ms-version")
      )
      |> Map.put(
        :canonicalizedResource,
        "/#{account_name}#{uri.path}\n" <>
          (uri.query
           |> URI.decode_query()
           |> Enum.sort_by(& &1)
           |> Enum.map_join("\n", fn {k, v} -> "#{k}:#{v}" end))
      )
      |> SignedData.sign(account_key)

    client =
      BlobClient.new(
        base_uri,
        headers |> Map.put("Authorization", "SharedKey #{account_name}:#{signature}")
      )

    %{
      status: 200,
      url: url,
      headers: %{
        "etag" => etag,
        "last-modified" => last_modified,
        "x-ms-lease-state" => lease_state,
        "x-ms-lease-status" => lease_status,
        "x-ms-request-id" => request_id
      }
    } = client |> BlobClient.get(request_path)

    {:ok,
     %{
       url: url,
       etag: etag,
       last_modified: last_modified,
       lease_state: lease_state,
       lease_status: lease_status,
       request_id: request_id
     }}
  end
end
