defmodule ExMicrosoftAzureStorage do
  alias Microsoft.Azure.Storage.BlobStorage

  def list_containers(),
    do:
      BlobStorage.list_containers(
        "SAMPLE_STORAGE_ACCOUNT_NAME" |> System.get_env(),
        "SAMPLE_STORAGE_ACCOUNT_KEY" |> System.get_env()
      )
end
