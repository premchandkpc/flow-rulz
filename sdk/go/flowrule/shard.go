package flowrulz

// computeShard computes a deterministic shard from tenant and partition key
func computeShard(tenantID, partitionKey string, numShards uint32) uint32 {
	key := tenantID + ":" + partitionKey
	h := fnv32a(key)
	return h % numShards
}

func fnv32a(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
