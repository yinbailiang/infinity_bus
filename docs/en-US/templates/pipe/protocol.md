# Handshake Payloads (pipe/protocol)

`infinity_bus.templates.pipe.protocol` defines the two Pydantic models used by the
handshake events. Back to the [overview](pipe.md).

---

## `PipeOpenRequest`

```python
class PipeOpenRequest(RequestProtocol):
    pipe_id: str
```

Handshake request payload. It inherits [`RequestProtocol`](../request.md) and adds the
opaque `pipe_id`. `session_id` and `request_id` are injected by the `request` template;
`expect_pipe` optionally filters by `session_id`.

## `PipeLinkedResponse`

```python
class PipeLinkedResponse(ResponseProtocol):
    pass
```

Handshake response payload. It inherits [`ResponseProtocol`](../request.md) and uses the
standard fields:

| Field | Description |
| ---- | ---- |
| `session_id` | Session ID from the request. |
| `request_id` | Request ID from the request. |
| `success` | Whether the handshake succeeded. |
| `error_msg` | Failure reason when `success=False`. |

`open_pipe` only accepts `PipeLinkedResponse`; `success=False` raises
`PipeHandshakeError`.
