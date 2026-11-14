undefined
## Non-blocking worker queue admission
Durable workers now use `TrySubmit` when supported; it bounds each hot key to half the global slots and fails fast with delayed broker redelivery, rather than blocking the fetcher. Legacy `Submit` remains available for existing callers. This fixes admission starvation, **not** strict event ordering on retry.
