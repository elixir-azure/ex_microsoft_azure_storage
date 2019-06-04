
- `tesla\lib\tesla\middleware\core.ex`


```elixir
defmodule Tesla.Middleware.Normalize do
  def normalize(:error) do
    raise %Tesla.Error{message: "unknown adapter error", reason: "unknown error"}
  end
```
