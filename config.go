package vfs

import "time"

type (
	VfsConfig struct {
		IndexDir           string   `json:"index_dir"`
		DataDir            string   `json:"data_dir"`
		BaseName           string   `json:"base_name"`
		CacheSize          int      `json:"cache_size"`
		Sync               bool     `json:"sync"`
		ShardDurationDays  float64  `json:"shard_duration_days"`
		ShardRetentionDays float64  `json:"shard_retention_days"`
		RecoveryEnabled    bool     `json:"recovery_enabled"`
		ReferenceTables    []string `json:"reference_tables"`
		StoreKeyInData     bool     `json:"store_key_in_data"`
		ReferenceLimit     uint64   `json:"reference_limit"` // 0 for no limit, otherwise discards references beyond the specified limit
	}
)

func (cfg *VfsConfig) calcShard(when time.Time) uint64 {
	// convert time to an integral
	divisor := uint64(24 * 60 * 60 * 1000 * cfg.ShardDurationDays)
	if divisor < 1 {
		divisor = 1
	}
	shard := uint64(when.UnixMilli())
	shard = shard / divisor

	// multiply by 10 to leave some numeric space between shards
	//
	// for example, it may be desired to compact old shards, and having space to insert
	// another allows transactions to be moved one by one, while the rest of the system
	// continues to operate
	//
	// it also leaves space for migration to a new format
	shard *= 10
	return shard
}

func (cfg *VfsConfig) timeFromShard(shard uint64) time.Time {
	ms := int64(shard/10) * int64(24*60*60*1000*cfg.ShardDurationDays)
	return time.Unix(int64(ms/1000), (ms%1000)*1000*1000)
}
