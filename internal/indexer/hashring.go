package indexer

import "hash/fnv"

// JumpHash implements Google's Jump Consistent Hash algorithm.
// O(ln n) time, O(1) space, perfectly uniform distribution.
// Reference: https://arxiv.org/abs/1406.2294
func JumpHash(key uint64, numBuckets int) int32 {
	var b, j int64 = -1, 0
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int32(b)
}

// ShardForRef routes a reference_id to a shard via consistent hash
// on the key prefix "ref:<reference_id>".
func ShardForRef(referenceID string, numShards uint32) uint32 {
	h := fnv.New64a()
	h.Write([]byte("ref:"))
	h.Write([]byte(referenceID))
	return uint32(JumpHash(h.Sum64(), int(numShards)))
}
