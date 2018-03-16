package rbd

import (
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/platforms"
	"os"
	"fmt"
	"path/filepath"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/fs"
	"context"

	"github.com/containerd/containerd/mount"
)

func init()  {
	plugin.Register(
		&plugin.Registration{
			Type: plugin.SnapshotPlugin,
			ID: "rbd",
			InitFn:func(ic *plugin.InitContext)(interface{}, error) {
				ic.Meta.Platforms = append(ic.Meta.Platforms, platforms.DefaultSpec())
				ic.Meta.Exports["root"] = ic.Root
				return NewSnapshotter(ic.Root)
			},
		},
	)
}

type snapshotter struct {
	root string
	ms   *storage.MetaStore
}

// NewSnapshotter returns a Snapshotter which uses rbd. The rbd
// diffs are stored under the provided root. A metadata file is stored under
// the root.
func NewSnapshotter(root string) (snapshots.Snapshotter, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}

	supportsDType, err := fs.SupportsDType(root)
	if err != nil {
		return nil, err
	}
	if !supportsDType {
		return nil, fmt.Errorf("%s does not support d_type. If the backing filesystem is xfs, please reformat with ftype=1 to enable d_type support", root)
	}
	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	return &snapshotter{
		root: root,
		ms:   ms,
	}, nil
}

func (o *snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	return snapshots.Info{},nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	return snapshots.Info{},nil
}

func (o *snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	return snapshots.Usage{},nil
}

func (o *snapshotter)  Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	return []mount.Mount{},nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return []mount.Mount{},nil
}

func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return []mount.Mount{},nil
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	return nil
}

func (o *snapshotter) Remove(ctx context.Context, key string) error {
	return nil
}

func (o *snapshotter) Walk(ctx context.Context, fn func(context.Context, snapshots.Info) error) error {
	return nil
}

func (o *snapshotter) Close() error {
	return nil
}