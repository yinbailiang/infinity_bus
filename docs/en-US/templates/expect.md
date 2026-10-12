# Expect — Async Event Listener

## Overview

`expect` is a one-shot event listener built on top of EventBus. It allows you to await a
specific event in an async context and obtain the complete `Event` object (including
payload data and metadata). Unlike traditional handler registration, `expect` automatically
manages the listener lifecycle via an async context manager, making it ideal for
**trigger-and-wait** patterns (e.g., waiting for confirmations, async results).

---

## Use Cases

- Waiting for completion notifications of async operations.
- Verifying that specific events are published correctly in tests.
- Implementing the underlying wait logic for request-response patterns (used internally
  by the `request` template).
- Listening for one-shot system signals (e.g., startup complete, shutdown confirmed).
- Scenarios requiring access to event metadata (ID, source chain, timestamps).

---

## Function Signature

```python
@asynccontextmanager
async def expect(
    bus_proxy: EventBus.Proxy,
    event_patterns: Union[str, Regex, List[Union[str, Regex]]],
    filter_func: Optional[Callable[[Event], Union[Awaitable[bool], bool]]] = None,
) -> AsyncGenerator[asyncio.Future[Event], None]
```

| Parameter | Type | Description |
| - | - | - |
| `bus_proxy` | `EventBus.Proxy` | Event bus proxy for accessing the handler registry. |
| `event_patterns` | `str` \| `Regex` \| `List[str \| Regex]` | Event name patterns to listen for. `str` for exact match, `Regex` for pattern match. Accepts a single value or a sequence. |
| `filter_func` | `Optional[Callable]` | Optional filter. Receives an `Event` object and returns a boolean (or awaitable boolean). `True` means the event matches. If omitted, any event matching `event_patterns` will trigger. |

**Yields**: an `asyncio.Future[Event]` — `await` it to get the matching `Event` instance.

---

## Workflow

1. **Register temporary handler**: When entering `async with expect(...) as future:`,
   `expect` registers a one-shot listener with the event bus.
2. **Publish events**: Execute business logic within the context (e.g., publish a request
   event) while the listener is active.
3. **Wait for match**: When the listener receives an event, it checks the filter (if any).
   On match, the complete `Event` object is set on the `future`.
4. **Retrieve result**: `await future` to get the full event. Access the payload via
   `event.data`, or read metadata from `event.id`, `event.sources`, etc.
5. **Auto-cleanup**: On context exit, regardless of match success, the temporary listener
   is unregistered and the `future` is cancelled if not yet completed.

---

## Usage Examples

### Basic: Wait for Any Matching Event

```python
async with expect(bus_proxy, "user.created") as future:
    await create_user(user_data)
    event = await asyncio.wait_for(future, timeout=5.0)
    payload = event.data
    print(f"User created with ID: {payload.user_id}")
```

### Access Event Metadata

```python
async with expect(bus_proxy, "order.shipped") as future:
    await ship_order(order_id)
    event = await asyncio.wait_for(future, timeout=3.0)
    print(f"Event ID: {event.id}")
    print(f"Processing chain: {' -> '.join(event.sources)}")
    print(f"Timestamps: {event.timestamps}")
```

### With Filter: Match Specific Conditions

```python
def is_target_user(event: Event) -> bool:
    return event.data.user_id == "admin-123"

async with expect(bus_proxy, "user.updated", filter_func=is_target_user) as future:
    await update_user("admin-123", new_data)
    event = await asyncio.wait_for(future, timeout=3.0)
    print(f"Updated fields: {event.data.changed_fields}")
```

### Async Filter

```python
async def check_permission(event: Event) -> bool:
    user = await db.get_user(event.data.user_id)
    return user.role == "admin"

async with expect(bus_proxy, "document.accessed", filter_func=check_permission) as future:
    await access_document(doc_id)
    try:
        event = await asyncio.wait_for(future, timeout=5.0)
        print(f"Admin {event.data.user_id} accessed document")
    except asyncio.TimeoutError:
        print("Non-admin access ignored")
```

### Multiple Event Patterns

```python
async with expect(bus_proxy, ["order.paid", "order.cancelled"]) as future:
    await submit_order(order_data)
    event = await asyncio.wait_for(future, timeout=10.0)
    if event.name == "order.paid":
        print("Order payment confirmed")
    else:
        print("Order was cancelled")
```

### Regex Pattern Matching

```python
from infinity_bus import Regex

# Match all events starting with "notify."
async with expect(bus_proxy, Regex(r"notify\..*")) as future:
    await trigger_notifications()
    event = await asyncio.wait_for(future, timeout=2.0)
    print(f"First notification: {event.name}")
```

---

## Error Handling

| Scenario | Behavior |
| - | - |
| Filter raises an exception | Exception is propagated via `future.set_exception()`; `await future` will raise it. |
| Context exits before `future` completes | The `future` is automatically cancelled; awaiting coroutines receive `CancelledError`. |
| Timeout (using `asyncio.wait_for`) | Raises `asyncio.TimeoutError`; resources are auto-cleaned on context exit. |
| Bus is shutting down | Handler registration still works, but events published during shutdown may not be captured (use after bus startup). |

---

## Notes

1. **Must use `async with`**: `expect` is an async context manager and cannot be called standalone.
2. **`future` completes only once**: After a match, subsequent identical events won't affect
   the `future`; the listener is automatically deactivated.
3. **Don't hold `future` outside the context**: After context exit, the `future` may be
   cancelled; awaiting it further will raise errors.
4. **Keep filters lightweight**: Filters run in the event dispatch thread; avoid blocking
   or long-running operations. Use async filters if needed but control execution time.
5. **Relationship with `request`**: `expect` is a lower-level tool; the `request` template
   uses it internally for the request-response pattern. For typical RPC calls, prefer
   `request` directly.

---

## Internals

- Uses `OneShotEventHandler` for single-fire semantics.
- Uses the **async** `temporary_handler` context manager for automatic handler registration/unregistration.
- Filter exceptions are passed directly to the waiter via `future.set_exception`, keeping
  the bus error channel clean (no `__task_error__` event triggered).

---

## Full Example

See `tests/templates/expect_test.py` for complete test cases covering normal matching,
filters, timeouts, multi-pattern, regex, and exception propagation scenarios.
