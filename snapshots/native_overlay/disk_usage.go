package native_overlay

import (
	"github.com/containerd/containerd/fs"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/containerd/snapshots"
	"context"
)

// Get disk usage for the snapshot
func (o *snapshotter) DiskUsage(s storage.Snapshot, info snapshots.Info) (fs.Usage , error) {
	if !o.isPreparedForImage(info) {
		return fs.DiskUsage(o.upperPath(s.ID))
	}

	if len(s.ParentIDs) == 0 {
		return fs.DiskUsage(o.committedPath(s.ID))
	}

	currentRootDir := o.committedPath(s.ID)
	parentRootDir := o.committedPath(s.ParentIDs[0])

	return fs.DiffUsage(context.Background(), parentRootDir, currentRootDir)
}