defmodule Microsoft.Azure.Storage.Models.BlobStorageSignedFields do
  alias Microsoft.Azure.Storage.RequestBuilder
  alias Microsoft.Azure.Storage.AzureStorageContext

  @enforce_keys [
    :verb,
    :contentEncoding,
    :contentLanguage,
    :contentLength,
    :contentMD5,
    :contentType,
    :date,
    :ifModifiedSince,
    :ifMatch,
    :ifNoneMatch,
    :ifUnmodifiedSince,
    :range,
    :canonicalizedHeaders,
    :canonicalizedResource
  ]

  defstruct [
    :verb,
    :contentEncoding,
    :contentLanguage,
    :contentLength,
    :contentMD5,
    :contentType,
    :date,
    :ifModifiedSince,
    :ifMatch,
    :ifNoneMatch,
    :ifUnmodifiedSince,
    :range,
    :canonicalizedHeaders,
    :canonicalizedResource
  ]

  def new(),
    do: %__MODULE__{
      verb: "",
      contentEncoding: "",
      contentLanguage: "",
      contentLength: "",
      contentMD5: "",
      contentType: "",
      date: "",
      ifModifiedSince: "",
      ifMatch: "",
      ifNoneMatch: "",
      ifUnmodifiedSince: "",
      range: "",
      canonicalizedHeaders: "",
      canonicalizedResource: ""
    }

  def stringToSign(fields = %__MODULE__{}) do
    [
      fields.verb,
      fields.contentEncoding,
      fields.contentLanguage,
      fields.contentLength,
      fields.contentMD5,
      fields.contentType,
      fields.date,
      fields.ifModifiedSince,
      fields.ifMatch,
      fields.ifNoneMatch,
      fields.ifUnmodifiedSince,
      fields.range,
      fields.canonicalizedHeaders,
      fields.canonicalizedResource
    ]
    |> Enum.join("\n")
  end

  def sign(data = %__MODULE__{}, accountkey) do
    stringToSign = data |> stringToSign()

    :crypto.hmac(:sha256, accountkey |> Base.decode64!(), stringToSign)
    |> Base.encode64()
  end
end
