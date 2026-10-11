# EventBus

## Overview

`EventBus` is the central dispatch hub — responsible for task queuing, concurrency control,
error reporting, and lifecycle management. `Proxy` is the only interface for publishing events.
The system includes two built-in special events: `ShutdownEvent` and `TaskErrorEvent`.

---

## EventBus

```python
class EventBus:
    def __init__(
        self,
        event_registry: EventRegistry,
        handler_registry: EventHandlerRegistry,
        queue: Optional[EventQueue] = None,
        router: Optional[Router] = None,
        max_handler_semaphore: int = 256,
        shutdown: ShutdownConfig = ShutdownConfig(),
        middleware_chain: Optional[MiddlewareChain] = None
    ) -> None

    # Lifecycle
    async def start(self) -> None
    async def stop(self) -> None
    async def __aenter__(self) -> "EventBus"
    async def __aexit__(self, ...) -> Optional[bool]

    # Proxy
    def proxy(self, source: str, raw_event: Optional[Event] = None) -> Proxy

    # Router
    @property
    def router(self) -> Router

    # Observability
    @property
    def is_running(self) -> bool
    @property
    def is_publishing_enabled(self) -> bool
    @property
    def active_task_count(self) -> int
    @property
    def queue_size(self) -> int
```

### Constructor Parameters

| Parameter | Type | Default | Description |
| - | - | - | - |
| `event_registry` | `EventRegistry` | (required) | Event registry instance. |
| `handler_registry` | `EventHandlerRegistry` | (required) | Handler registry instance. |
| `queue` | `Optional[EventQueue]` | `None` | Internal dispatch queue; the bus depends only on the `EventQueue` abstraction. Defaults to `InMemoryEventQueue()` (capacity from its own default config, 1024). See [Event Queue doc](queue.md). |
| `router` | `Optional[Router]` | `None` | Internal router / dispatch table. Defaults to `Router(event_registry)`; exposed as `bus.router`. See [Router doc](router.md). |
| `max_handler_semaphore` | `int` | `256` | Max concurrent handlers (semaphore). |
| `shutdown` | `ShutdownConfig` | `ShutdownConfig()` | Shutdown behavior config. |
| `middleware_chain` | `Optional[MiddlewareChain]` | `None` | Middleware chain for publish hooks. |

Constructing automatically registers `ShutdownEvent` and `TaskErrorEvent` (if not present),
and creates an internal [Router](router.md) (exposed as `bus.router`) for event-to-handler
routing — handlers register their subscription bundles on activation (bus start). Queue capacity and
other settings live on the queue itself — the bus never knows them; inject
`InMemoryEventQueue(InMemoryEventQueueConfig(maxsize=N))` to control capacity.

### Lifecycle

| Method | Description |
| - | - |
| `start()` | Starts the event bus: first activates the handler registry (`registry.activate(bus)` — each handler's subscription bundle registers into `bus.router`), then starts the dispatch loop and initializes middleware. **Failure-safe & retryable**: any failure rolls back (cancel dispatch, deactivate handlers, tear down middleware) to a clean not-started state. Idempotent. |
| `stop()` | Graceful shutdown: publish `__shutdown__` → reject new publishes → drain queue → cancel dispatch → wait for active tasks → deactivate the handler registry (`registry.deactivate()`); handler deactivation failures are logged and summarized. Idempotent. |
| `async with EventBus(...) as bus:` | Context manager, auto start/stop. `stop()` errors on exit won't mask body exceptions. |

### Observability

| Property | Type | Description |
| - | - | - |
| `is_running` | `bool` | Whether the bus is running. See [is_running](#is_running). |
| `is_publishing_enabled` | `bool` | Whether new events are accepted. See [is_publishing_enabled](#is_publishing_enabled). |
| `active_task_count` | `int` | Number of currently active handler tasks. |
| `queue_size` | `int` | Number of events currently queued for dispatch. |

#### `is_running`

Indicates the event bus's running state. Its value changes over the lifecycle as follows:

| Phase | `is_running` | Description |
| - | - | - |
| After construction, before `start()` | `False` | Bus created but not yet started. |
| During `start()` | `False` | Dispatch loop being created, not yet marked as running. |
| After `start()` completes | **`True`** | Dispatch loop started, actively dispatching events. |
| During `stop()` | **`True`** | Draining queue, waiting for active tasks to complete. |
| After `stop()` completes | `False` | All resources released. |

> **Key point**: `is_running` remains **`True` throughout the entire drain and wait period of `stop()`**,
> only becoming `False` after all active tasks complete and middleware teardown finishes.
> This means during shutdown, `is_running=True` but `is_publishing_enabled=False`
> (see below).

```python
bus = EventBus(reg, h_reg)
print(bus.is_running)  # False

await bus.start()
print(bus.is_running)  # True

# During stop(): draining queue + waiting on tasks, is_running remains True
await bus.stop()
print(bus.is_running)  # False
```

#### `is_publishing_enabled`

Indicates whether new events can be published to the bus. Its value changes as follows:

| Phase | `is_publishing_enabled` | Description |
| - | - | - |
| After construction, before `start()` | `False` | Publishing raises `RuntimeError`. |
| After `start()` completes | **`True`** | Normal publishing allowed. |
| After `stop()` publishes `__shutdown__` | **`False`** | New events rejected; publishing raises `BusShuttingDown`. |
| After `stop()` completes | `False` | — |

> **Key point**: `is_publishing_enabled` is cleared **immediately when `stop()` begins**
> (right after the `__shutdown__` event is published), before queue draining and active
> task waiting. This ensures no new events enter the queue during shutdown, while
> already-enqueued events are still dispatched normally.

##### Difference from `is_running`

| Scenario | `is_running` | `is_publishing_enabled` |
| - | - | - |
| Normal operation | `True` | `True` |
| Shutting down (draining queue, waiting tasks) | `True` | **`False`** |
| Not started / Stopped | `False` | `False` |

These two properties have **state separation during shutdown**: `is_running` stays `True`
to ensure the queue and tasks are fully processed, while `is_publishing_enabled` becomes
`False` early to block new events from entering.

##### Usage Scenarios

```python
# Scenario 1: Health check endpoint
@app.get("/health")
async def health():
    return {
        "status": "ok" if bus.is_running else "degraded",
        "can_publish": bus.is_publishing_enabled,
        "queue_depth": bus.queue_size,
        "active_handlers": bus.active_task_count,
    }

# Scenario 2: Wait for bus to fully stop during graceful shutdown
async def graceful_shutdown():
    signal.alarm("shutdown")
    await bus.stop()
    # At this point is_running == False, safe to exit the process
    assert not bus.is_running

# Scenario 3: Pre-publish check (usually not needed; publish raises appropriate exceptions)
async def safe_publish(bus, name, data):
    if not bus.is_publishing_enabled:
        logger.warning("Bus no longer accepting new events, skipping publish")
        return
    await bus.proxy("my_service").publish(name, data)
```

##### State Transition Sequence Diagram

```text
start()                                   stop()
  │                                         │
  │  dispatch_task created                   │  1. publish __shutdown__
  │  _running.set()           ◄── running ───►  _running.clear()
  │  _enable_publish.set()    ◄── publish ───►  _enable_publish.clear()
  │                                         │
  │  mw_chain.setup()                       │  2. drain queue (queue.join())
  │                                         │  3. cancel dispatch_task
  ▼                                         ▼  4. wait active tasks
is_running=True                             │  5. mw_chain.teardown()
is_publishing_enabled=True                  ▼
                                          is_running=False
                                          is_publishing_enabled=False
```

---

## EventBus.Proxy

The proxy is the **only** interface for publishing events. It auto-records event source and chain.

```python
class Proxy:
    async def publish(
        self,
        name: str,
        data: Optional[Union[Dict[str, Any], BaseModel]] = None
    ) -> None

    @property
    def handlers_registry(self) -> EventHandlerRegistry
    @property
    def events_registry(self) -> EventRegistry
    @property
    def middleware(self) -> MiddlewareChain
```

| Member | Description |
| - | - |
| `publish(name, data=None)` | Publish an event. `data` can be a dict or pydantic model. Raises `RuntimeError` if not started, `BusShuttingDown` if stopping, `ValueError` for unknown events, `TypeError` for payload mismatch. |
| `handlers_registry` | Read-only access to handler registry. |
| `events_registry` | Read-only access to event registry. |
| `middleware` | Access middleware chain for runtime modification. |

### Usage

```python
# Basic publish
await bus.proxy("my_service").publish("user.login", {"user_id": "42"})

# Chained publish (event forwarding)
class LoginHandler(EventHandler):
    async def handle(self, payload, bus_proxy, raw_event):
        await bus_proxy.publish("user.profile_loaded", profile_data)
```

---

## ShutdownConfig

Controls graceful shutdown timeout behavior.

```python
class ShutdownConfig(BaseModel):
    queue_timeout_min: float = 1.0
    queue_timeout_max: float = 15.0
    tasks_timeout: float = 15.0
    avg_wait_time: float = 0.05
```

| Field | Type | Default | Description |
| - | - | - | - |
| `queue_timeout_min` | `float` | `1.0` | Minimum queue drain wait (seconds). |
| `queue_timeout_max` | `float` | `15.0` | Maximum queue drain wait (seconds). |
| `tasks_timeout` | `float` | `15.0` | Active handler task completion wait (seconds). |
| `avg_wait_time` | `float` | `0.05` | Estimated per-event processing time (seconds), used for dynamic queue drain timeout calculation. |

Actual queue drain timeout = `max(queue_timeout_min, min(queue_timeout_max, queue_size × avg_wait_time))`.

---

## Exceptions

| Exception | When |
| - | - |
| `BusShuttingDown` | Publish attempted while bus is stopping. Inherits from `Exception`. Callers should catch and perform cleanup. |
| `RuntimeError` | Publish attempted before bus has started. |
| `ValueError` | Unknown event name, or payload mismatch (required vs. provided). |
| `TypeError` | Payload type doesn't match the event declaration. |

---

## Built-in Events

### ShutdownEvent

```python
class ShutdownEvent(EventDeclaration):
    name = 'infinity_bus.__shutdown__'
    # payload_type = None (no payload)
```

Published automatically when `stop()` is called, before queue draining begins.
Handlers can subscribe to this event to perform cleanup before shutdown.

### TaskErrorEvent

```python
class TaskErrorPayload(BaseModel):
    error_event: Event
    handler_id: Optional[str]
    handler_name: str
    error_type: str
    error_message: str

class TaskErrorEvent(EventDeclaration):
    name = 'infinity_bus.__task_error__'
    payload_type = TaskErrorPayload
```

Published when a handler raises an exception. Source is always `"EventBusErrorReporter"`.
Use this for centralized error monitoring.

```python
class ErrorMonitor(EventHandler):
    def __init__(self):
        super().__init__(["infinity_bus.__task_error__"])

    async def handle(self, payload, bus_proxy, raw_event):
        if isinstance(payload, TaskErrorPayload):
            logger.error(
                "Handler %s failed on %s: %s",
                payload.handler_name,
                payload.error_event.name,
                payload.error_message,
            )
```

---

## Usage Examples

### Basic Assembly & Startup

```python
from infinity_bus import EventBus, EventRegistry, EventHandlerRegistry

# 1. Declare events
class MyPayload(BaseModel):
    message: str

class MyEvent(EventDeclaration):
    name = "my.event"
    payload_type = MyPayload

# 2. Register events
reg = EventRegistry()
reg.register(MyEvent)

# 3. Implement handler
class MyHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["my.event"])

    async def handle(self, payload, bus_proxy, raw_event):
        print(f"Received: {payload.message}")

# 4. Register handler
h_reg = EventHandlerRegistry()
h_reg.register(MyHandler())

# 5. Start bus and publish
async with EventBus(reg, h_reg) as bus:
    await bus.proxy("cli").publish("my.event", {"message": "Hello"})
    await asyncio.sleep(0.1)  # Wait for handler output
```

### Chained Publishing

Handlers can publish new events via `bus_proxy.publish` to form processing chains;
the bus automatically tracks sources:

```python
class OrderHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=["order.created"])

    async def handle(self, payload, bus_proxy, raw_event):
        # Process order...
        # Chain-publish a notification event
        await bus_proxy.publish("notification.send", {
            "type": "order_confirmed",
            "order_id": payload.order_id
        })
```
