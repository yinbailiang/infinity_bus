# Router / Subscriptions

## Overview

`Router` is the **single source of truth** for routing: it owns a route table that contains
**only activated subscriptions** (mirroring message brokers, where bindings live only while a
consumer is connected), and `match()` pre-computes a dispatch table routing event types to
registered handlers. `Subscriptions` is the managed subscription container a handler holds
(instead of a bare list), supporting runtime `add/remove/replace` for dynamic self-subscription.

Users usually do not build a `Router` directly — `EventBus` creates one internally and exposes it
as `bus.router`; `EventHandler.on_activate` registers the handler's subscription bundle by default
(activation is routing).

---

## Architecture

```text
EventHandler ──Subscriptions bundle──┐
                                     ├──> Router.route_table ──> {event_name: [handler_id, ...]}
        on_activate(bus) registers ───┘            ↑
                                 event-registry version + route version auto-rebuild
```

`Router` replaces the old `Matcher` (which passively scanned `handler.subscriptions`):
subscription data moved from "bare list on the handler + scan" to "a bundle the handler holds,
registered into the Router on activation".

---

## Subscriptions

The managed subscription container held by a handler:

```python
class Subscriptions:
    def __init__(self, patterns: Iterable[str | Regex] | None = None) -> None
    def add(self, pattern: str | Regex) -> None              # append (dedup)
    def remove(self, pattern: str | Regex) -> bool           # remove
    def replace(self, patterns: Iterable[str | Regex]) -> None  # full replace (state machine)
    @property
    def patterns(self) -> tuple[str | Regex, ...]           # read-only snapshot
    @property
    def is_bound(self) -> bool                              # registered on a Router (active)
    def __iter__ / __len__ / __contains__ / __eq__(list|tuple) / __bool__
```

**Changes auto-sync while active**: on registration (`Router.subscribe`) the bundle binds to that
Router; any subsequent `add/remove/replace` bumps the Router version so the dispatch table rebuilds
lazily. **Changes while inactive** only update the declaration and take effect on the next
`subscribe` (activation).

---

## Router

```python
class Router:
    def __init__(self, event_registry: EventRegistry) -> None

    def subscribe(self, handler_id: str, subs: Subscriptions) -> None  # register (idempotent)
    def unsubscribe(self, handler_id: str) -> bool                     # revoke
    def clear(self) -> None                                            # revoke all

    def match(self, event_name: str) -> list[str]                      # matched handler ids
    @property
    def routes(self) -> tuple[tuple[str, Subscriptions], ...]          # active routes snapshot
    @property
    def dispatch_table(self) -> dict[str, list[str]]                   # read-only dispatch view
    version: int                                                       # route version
```

### Dispatch table

- **Exact `str` subscriptions** → reverse index, O(1) lookup
- **`Regex` subscriptions** → separate scan list, `fullmatch` only
- Per event, matches follow subscription order and are deduped by handler; only handler ids are
  stored — the bus resolves instances from the handler registry.

### Version-aware caching

Any change in the event-registry version or the Router version
(subscribe/unsubscribe/clear/bundle mutation) triggers a lazy rebuild on the next
`match()` / `dispatch_table` access.

```text
match() call → check version (O(1) int compare) → stale? → _rebuild()
                                                  ↓ fresh
                                           lookup table (O(1) dict)
```

---

## Lifecycle: Activation Is Routing

The `EventHandler` base class hooks routing registration into bus-lifecycle hooks
(overriders must call `super()`):

```python
class EventHandler(ABC):
    async def on_activate(self, bus: EventBus) -> None:
        if self.handler_id is not None and self.subscriptions:
            bus.router.subscribe(self.handler_id, self.subscriptions)  # activation is routing

    async def on_deactivate(self, bus: EventBus) -> None:
        if self.handler_id is not None:
            bus.router.unsubscribe(self.handler_id)                    # offline revokes
```

Triggered on registry `activate` (bus start) for every registered handler, and on `register`
into an already-bound registry. **The route table contains only activated subscriptions** — after
`deactivate` (bus stop) or handler unregister, the Router has no entry for it
(the broker forgets disconnected consumers).

---

## Dynamic Self-subscription

Changing your own subscriptions at runtime means mutating your bundle
(auto-synced with the live routes while active):

```python
class GameStateHandler(EventHandler):
    def __init__(self):
        super().__init__(subscriptions=['game.start'])

    async def handle(self, payload, bus_proxy, raw_event) -> None:
        if raw_event.name == 'game.start':
            # state machine: only care about the next stage
            self.subscriptions.replace(['game.tick', 'game.over'])

    # or incrementally at runtime:
    #   self.subscriptions.add('game.over')
    #   self.subscriptions.remove('game.start')
```

Changing while inactive only updates the declaration (applied on next activation), without errors.

---

## Relationship with EventBus

`EventBus` creates a `Router` internally (injectable via `router=`), and the dispatch loop takes
matched ids from it, then resolves handler instances from the handler registry:

```python
class EventBus:
    def __init__(self, event_registry, handler_registry, router=None, ...):
        self._router = router or Router(event_registry)

    @property
    def router(self) -> Router: ...          # public access (subscription mgmt / debugging)

    async def _dispatch_loop(self):
        ...
        for handler_id in self._router.match(event.name):
            handler = self._handlers.get(handler_id)
            if handler is not None:
                ...
```

Users normally do not touch the Router; for runtime dynamic subscriptions they use the handler's
own subscription bundle.

---

## Why Pre-compute?

Without pre-computation every dispatch would iterate over all active subscriptions and run regex
matching per event. With pre-computation:

- Exact `str` subscriptions → O(1) reverse-index lookup
- `Regex` subscriptions → scan list only
- Version-aware caching → rebuild only when the event registry or routes change, not per dispatch
