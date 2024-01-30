package vfs

type (
	VfsConfig struct {
		IndexDir           string  `json:"index_dir"`
		DataDir            string  `json:"data_dir"`
		BaseName           string  `json:"base_name"`
		CacheSize          int     `json:"cache_size"`
		Sync               bool    `json:"sync"`
		SyncTask           bool    `json:"sync_task"`
		ShardDurationDays  float64 `json:"shard_duration_days"`
		ShardRetentionDays float64 `json:"shard_retention_days"`
		RecoveryEnabled    bool    `json:"recovery_enabled"`
	}
)
