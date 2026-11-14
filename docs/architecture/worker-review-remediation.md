undefined
## Expired shard recovery
Live workers now optionally recover expired leases in batches of 256 even when at the fixed nominal replica target. PostgreSQL claims via `FOR UPDATE SKIP LOCKED`, increments fencing tokens and returns the recovered leases. The existing active-lease `FOR SHARE` fencing remains authoritative. Recovered leases are not shed just because they exceed the static target. This removes the dead-worker stranding mode but does not provide fair rebalancing when failed replicas rejoin; dynamic membership is still needed for that.
