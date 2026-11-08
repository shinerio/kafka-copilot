package kafka

// This file implements the partition-lookup hint used by the explicit
// "locate partition by key" switch on the scan page.
//
// The formula mirrors the default Kafka / franz-go partitioner for records
// WITH A NON-NIL KEY:
//
//     partition = int(murmur2(key) & 0x7fffffff) % partitionCount
//
// franz-go uses KafkaHasher(murmur2) both in StickyKeyPartitioner and in
// UniformBytesPartitioner (for keyed records), and the Java producer's
// DefaultPartitioner uses the same murmur2 + 0x7fffffff mask. kafka-pilot's
// own producer does not override the partitioner, so keys written by this
// tool always land in the partition computed here.
//
// IMPORTANT: this is a HINT, never a guarantee. The same key can live in a
// different partition when:
//   - the topic's partition count changed at any point in its history
//     (partition expansion, delete + recreate);
//   - a producer used a custom partitioner, an explicit partition, or a
//     different hash (Sarama FNV-1a, librdkafka CRC32, ...);
//   - the key bytes differ (encoding, case, whitespace, binary vs string).
// Therefore narrowing is opt-in only; the default scan still covers every
// partition. See FR-05 in the design doc for the full contract.

// Murmur2 returns the Kafka-flavoured MurmurHash2 of b (seed 0x9747b28c,
// little-endian 4-byte chunks, 32-bit unsigned wrap-around). This is a
// verbatim reimplementation of franz-go's internal murmur2 / Kafka's
// org.apache.kafka.common.utils.Utils.murmur2.
func Murmur2(b []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	h := seed ^ uint32(len(b))
	for len(b) >= 4 {
		k := uint32(b[3])<<24 + uint32(b[2])<<16 + uint32(b[1])<<8 + uint32(b[0])
		b = b[4:]
		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k
	}
	switch len(b) {
	case 3:
		h ^= uint32(b[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(b[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(b[0])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// KeyPartition returns the partition a standard Kafka producer would choose
// for key, given partitionCount partitions. Returns 0 for a non-positive
// partition count; callers must validate the metadata first.
func KeyPartition(key []byte, partitionCount int) int {
	if partitionCount <= 0 {
		return 0
	}
	return int(Murmur2(key)&0x7fffffff) % partitionCount
}
