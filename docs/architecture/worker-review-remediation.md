undefined
## Additional implemented improvements
- The PostgreSQL lease acquisition UPSERT now skips conflict updates on live leases, avoiding unnecessary WAL, locks and dead tuples when another worker owns a shard.
- The outbox publisher reports claimed-batch size through an optional capability and drains up to 16 full batches per wake-up, including at startup. The existing publisher port stays compatible.

These improve performance; they do not establish per-key outbox ordering or bounded retry semantics.
