defmodule Stoat.Model do
  defmacro __using__(_opts) do
    quote do
      @before_compile Stoat.Model
    end
  end

  defmacro __before_compile__(env) do
    quote do
      defimpl Jason.Encoder, for: unquote(env.module) do
        @impl Jason.Encoder
        def encode(value, opts) do
          value
          |> Map.from_struct()
          |> Map.reject(fn
            {_k, nil} -> true
            {_k, v} when is_map(v) -> map_size(v) == 0
            {_k, _v} -> false
          end)
          |> Jason.Encode.map(opts)
        end
      end
    end
  end
end
