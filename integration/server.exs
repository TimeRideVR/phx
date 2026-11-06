# A minimal real Phoenix server for the integration tests of the Go client (see integration_test.go).
#
#     PORT=4000 elixir integration/server.exs
#
# PORT              port to listen on (default 4000)
# PHOENIX_VERSION   Hex requirement for Phoenix (default "~> 1.8")
#
# It prints "READY <port>" once it accepts connections. Dependencies are fetched on first start and cached.

Mix.install([
  {:phoenix, System.get_env("PHOENIX_VERSION", "~> 1.8")},
  {:phoenix_pubsub, "~> 2.1"},
  {:bandit, "~> 1.5"},
  {:jason, "~> 1.4"}
])

Logger.configure(level: :warning)

port = String.to_integer(System.get_env("PORT", "4000"))

defmodule TestChannel do
  use Phoenix.Channel

  # Joins to "echo:reject" are refused; every other join is accepted and echoes its params.
  def join("echo:reject", _params, _socket), do: {:error, %{reason: "rejected"}}
  def join("echo:" <> _, params, socket), do: {:ok, %{joined: params}, socket}

  # Replies with the payload it was given.
  def handle_in("ping", payload, socket), do: {:reply, {:ok, payload}, socket}

  # Replies with an error status.
  def handle_in("fail", payload, socket), do: {:reply, {:error, payload}, socket}

  # Never replies, so the client's push times out.
  def handle_in("noreply", _payload, socket), do: {:noreply, socket}

  # Broadcasts the payload to everyone on the topic, the sender included.
  def handle_in("shout", payload, socket) do
    broadcast!(socket, "shout", payload)
    {:noreply, socket}
  end

  # Pushes `count` numbered "seq" messages, then replies.
  def handle_in("sequence", %{"count" => count}, socket) do
    for n <- 1..count, do: push(socket, "seq", %{"n" => n})
    {:reply, {:ok, %{}}, socket}
  end

  # Stops the channel normally: the client gets the reply, then phx_close.
  def handle_in("stop", _payload, socket), do: {:stop, :shutdown, {:ok, %{}}, socket}

  # Crashes the channel process: the client gets phx_error.
  def handle_in("crash", _payload, _socket), do: raise("boom")

  # Makes the server close this client's socket.
  def handle_in("drop_socket", _payload, socket) do
    TestEndpoint.broadcast("test_socket:" <> socket.assigns.client_id, "disconnect", %{})
    {:noreply, socket}
  end
end

defmodule TestSocket do
  use Phoenix.Socket

  channel("echo:*", TestChannel)

  # A connection that sends a token must send the right one.
  def connect(%{"token" => token}, _socket, _connect_info) when token != "secret", do: :error

  def connect(params, socket, _connect_info) do
    {:ok, assign(socket, :client_id, Map.get(params, "id", "anonymous"))}
  end

  def id(socket), do: "test_socket:" <> socket.assigns.client_id
end

defmodule TestEndpoint do
  use Phoenix.Endpoint, otp_app: :phx_test

  socket("/socket", TestSocket, websocket: [timeout: 60_000], longpoll: false)
end

Application.put_env(:phx_test, TestEndpoint,
  adapter: Bandit.PhoenixAdapter,
  http: [ip: {127, 0, 0, 1}, port: port],
  server: true,
  secret_key_base: String.duplicate("a", 64),
  pubsub_server: TestPubSub,
  check_origin: false
)

{:ok, _} =
  Supervisor.start_link([{Phoenix.PubSub, name: TestPubSub}, TestEndpoint],
    strategy: :one_for_one
  )

IO.puts("READY #{port}")
Process.sleep(:infinity)
