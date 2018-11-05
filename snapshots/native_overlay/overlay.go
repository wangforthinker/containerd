// +build linux

package native_overlay

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	"github.com/containerd/containerd/fs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/platforms"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.SnapshotPlugin,
		ID:   "overlay1fs",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Platforms = append(ic.Meta.Platforms, platforms.DefaultSpec())
			ic.Meta.Exports["root"] = ic.Root
			return NewSnapshotter(ic.Root)
		},
	})
}

type snapshotter struct {
	root string
	ms   *storage.MetaStore
}

// NewSnapshotter returns a Snapshotter which uses overlay1fs. The overlay1fs
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

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Info{}, err
	}
	defer t.Rollback()
	_, info, _, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return snapshots.Info{}, err
	}

	info, err = storage.UpdateInfo(ctx, info, fieldpaths...)
	if err != nil {
		t.Rollback()
		return snapshots.Info{}, err
	}

	if err := t.Commit(); err != nil {
		return snapshots.Info{}, err
	}

	return info, nil
}

// Usage returns the resources taken by the snapshot identified by key.
//
// For active snapshots, this will scan the usage of the overlay "diff" (aka
// "upper") directory and may take some time.
//
// For committed snapshots, the value is returned from the metadata database.
func (o *snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	var(
		rollback = true
		s = storage.Snapshot{}
	)

	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return snapshots.Usage{}, err
	}

	defer func() {
		if rollback {
			t.Rollback()
		}
	}()

	_, info, usage, err := storage.GetInfo(ctx, key)
	if err != nil {
		return snapshots.Usage{}, err
	}

	// only active snapshot could call storage.GetSnapshot
	if info.Kind == snapshots.KindActive {
		s, err = storage.GetSnapshot(ctx, key)
		if err != nil {
			return snapshots.Usage{}, err
		}
	}

	t.Rollback() // transaction no longer needed at this point.
	rollback = false

	if err != nil {
		return snapshots.Usage{}, err
	}

	if info.Kind == snapshots.KindActive {
		du, err := o.DiskUsage(s, info)
		if err != nil {
			// TODO(stevvooe): Consider not reporting an error in this case.
			return snapshots.Usage{}, err
		}

		usage = snapshots.Usage(du)
	}

	return usage, nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (o *snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	var(
		rollback = true
	)

	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return nil, err
	}

	defer func() {
		if rollback {
			t.Rollback()
		}
	}()

	s, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get active mount")
	}

	_,info,_,err := storage.GetInfo(ctx, key)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get active mount")
	}

	rollback = false
	t.Rollback()

	return o.mounts(s, info), nil
}

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	var(
		rollback = true
	)

	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}

	defer func() {
		if rollback {
			t.Rollback()
		}
	}()

	s, err := storage.GetSnapshot(ctx, key)
	if err != nil {
		return err
	}

	// grab the existing id and info
	_, info, _ ,err := storage.GetInfo(ctx, key)
	if err != nil {
		return err
	}

	rollback = false
	t.Rollback()

	// TODO: if committed failed, related directory should be rollback?
	if ! o.isPreparedForImage(info) {
		logrus.WithField("driver","overlay1fs").Debugf("snapshot %s build committed rootfs", s.ID)
		//build committed root fs
		err = o.buildCommittedRootfs(s, info)
		if err != nil {
			return err
		}
	}

	ctx, t, err = o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	usage, err := o.DiskUsage(s, info)
	if err != nil {
		return err
	}

	logrus.WithField("overlay1fs-snapshot-name", name).WithField("overlay1fs-snapshot-id", s.ID).Debugf("disk usage size:%d, inode:%s", usage.Size, usage.Inodes)

	if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
		return errors.Wrap(err, "failed to commit snapshot")
	}
	return t.Commit()
}

// Remove abandons the transaction identified by key. All resources
// associated with the key will be removed.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && t != nil {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	id, _, err := storage.Remove(ctx, key)
	if err != nil {
		return errors.Wrap(err, "failed to remove")
	}

	path := filepath.Join(o.root, "snapshots", id)
	renamed := filepath.Join(o.root, "snapshots", "rm-"+id)
	if err := os.Rename(path, renamed); err != nil {
		return errors.Wrap(err, "failed to rename")
	}

	err = t.Commit()
	t = nil
	if err != nil {
		if err1 := os.Rename(renamed, path); err1 != nil {
			// May cause inconsistent data on disk
			log.G(ctx).WithError(err1).WithField("path", renamed).Errorf("failed to rename after failed commit")
		}
		return errors.Wrap(err, "failed to commit")
	}
	if err := os.RemoveAll(renamed); err != nil {
		// Must be cleaned up, any "rm-*" could be removed if no active transactions
		log.G(ctx).WithError(err).WithField("path", renamed).Warnf("failed to remove root filesystem")
	}

	return nil
}

// Walk the committed snapshots.
func (o *snapshotter) Walk(ctx context.Context, fn func(context.Context, snapshots.Info) error) error {
	ctx, t, err := o.ms.TransactionContext(ctx, false)
	if err != nil {
		return err
	}
	defer t.Rollback()
	return storage.WalkInfo(ctx, fn)
}

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) ([]mount.Mount, error) {
	var (
		path        string
		snapshotDir = filepath.Join(o.root, "snapshots")
	)

	td, err := ioutil.TempDir(snapshotDir, "new-")
	if err != nil {
		return nil, errors.Wrap(err, "failed to create temp dir")
	}
	defer func() {
		if err != nil {
			if td != "" {
				if err1 := os.RemoveAll(td); err1 != nil {
					err = errors.Wrapf(err, "remove failed: %v", err1)
				}
			}
			if path != "" {
				if err1 := os.RemoveAll(path); err1 != nil {
					err = errors.Wrapf(err, "failed to remove path: %v", err1)
				}
			}
		}
	}()

	fs := filepath.Join(td, "fs")
	if err = os.MkdirAll(fs, 0755); err != nil {
		return nil, err
	}

	if kind == snapshots.KindActive {
		if err = os.MkdirAll(filepath.Join(td, "work"), 0711); err != nil {
			return nil, err
		}
	}

	ctx, t, err := o.ms.TransactionContext(ctx, true)
	if err != nil {
		return nil, err
	}
	rollback := true
	defer func() {
		if rollback {
			if rerr := t.Rollback(); rerr != nil {
				log.G(ctx).WithError(rerr).Warn("failed to rollback transaction")
			}
		}
	}()

	s, err := storage.CreateSnapshot(ctx, kind, key, parent, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create snapshot")
	}

	if len(s.ParentIDs) > 0 {
		st, err := os.Stat(o.committedPath(s.ParentIDs[0]))
		if err != nil {
			return nil, errors.Wrap(err, "failed to stat parent")
		}

		stat := st.Sys().(*syscall.Stat_t)
		if err := os.Lchown(fs, int(stat.Uid), int(stat.Gid)); err != nil {
			return nil, errors.Wrap(err, "failed to chown")
		}
	}

	path = filepath.Join(snapshotDir, s.ID)
	if err = os.Rename(td, path); err != nil {
		return nil, errors.Wrap(err, "failed to rename")
	}
	td = ""

	_,info,_,err := storage.GetInfo(ctx, key)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("snapshot %s labels:%v", s.ID, info.Labels)

	if o.isPreparedForImage(info) {
		logrus.Debugf("snapshotId %s is prepared for image", s.ID)
		err = o.buildRootDir(s)
		if err != nil {
			return nil, err
		}
	}

	rollback = false
	if err = t.Commit(); err != nil {
		return nil, errors.Wrap(err, "commit failed")
	}

	return o.mounts(s, info), nil
}

func (o *snapshotter) buildRootDir(s storage.Snapshot) error {
	var(
		err error = nil
	)

	logrus.Debugf("snapshot %s build root dir, parentID: %v", s.ID, s.ParentIDs)

	if len(s.ParentIDs) == 0 {
		return os.Rename(o.upperPath(s.ID), o.committedPath(s.ID))
	}

	tmpRootDir, err := ioutil.TempDir(o.rootPath(s.ID), "tmpRoot")
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			os.RemoveAll(tmpRootDir)
		}
	}()

	err = os.Chmod(tmpRootDir, 0755)
	if err != nil {
		return err
	}

	st, err := os.Stat(o.committedPath(s.ParentIDs[0]))
	if err != nil {
		return errors.Wrap(err, "failed to stat parent")
	}

	stat := st.Sys().(*syscall.Stat_t)
	if err := os.Lchown(tmpRootDir, int(stat.Uid), int(stat.Gid)); err != nil {
		return errors.Wrap(err, "failed to chown")
	}

	err = copyDir(o.committedPath(s.ParentIDs[0]), tmpRootDir, copyHardlink)
	if err != nil {
		return err
	}

	return os.Rename(tmpRootDir, o.committedPath(s.ID))
}

//if snapshot is not image snapshot and is committed, generate a root dir to save it's rootfs
func (o *snapshotter) buildCommittedRootfs(s storage.Snapshot, info snapshots.Info) error {
	var(
		err error = nil
	)

	tmpRootDir, err := ioutil.TempDir(o.rootPath(s.ID), "tmpRoot")
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			os.RemoveAll(tmpRootDir)
		}
	}()

	err = os.Chmod(tmpRootDir, 0755)
	if err != nil {
		return err
	}

	if len(s.ParentIDs) > 0 {
		st, err := os.Stat(o.committedPath(s.ParentIDs[0]))
		if err != nil {
			return errors.Wrap(err, "failed to stat parent")
		}

		stat := st.Sys().(*syscall.Stat_t)
		if err := os.Lchown(tmpRootDir, int(stat.Uid), int(stat.Gid)); err != nil {
			return errors.Wrap(err, "failed to chown")
		}
	}

	tmpMountDir, err := ioutil.TempDir(o.rootPath(s.ID), "tmpMount")
	if err != nil {
		return err
	}

	defer func() {
		os.RemoveAll(tmpMountDir)
	}()

	mounts := o.mounts(s, info)
	err = mount.All(mounts, tmpMountDir)
	if err != nil {
		return err
	}

	defer func() {
		if uerr := mount.Unmount(tmpMountDir, 0); uerr != nil {
			if err == nil {
				err = uerr
			}
		}
	}()

	err = copyDir(tmpMountDir, tmpRootDir, 0)
	if err != nil {
		return err
	}

	err = os.Rename(tmpRootDir, o.committedPath(s.ID))

	return err
}

func (o *snapshotter) mounts(s storage.Snapshot, info snapshots.Info) []mount.Mount {
	if o.isPreparedForImage(info) {
		roFlag := "rw"
		if s.Kind == snapshots.KindView {
			roFlag = "ro"
		}

		return [] mount.Mount {
			{
				Source: o.committedPath(s.ID),
				Type:   "bind",
				Options: []string{
					roFlag,
					"rbind",
				},
			},
		}
	}

	if len(s.ParentIDs) == 0 {
		// if we only have one layer/no parents then just return a bind mount as overlay
		// will not work
		roFlag := "rw"
		if s.Kind == snapshots.KindView {
			roFlag = "ro"
		}

		return []mount.Mount{
			{
				Source: o.upperPath(s.ID),
				Type:   "bind",
				Options: []string{
					roFlag,
					"rbind",
				},
			},
		}
	}
	var options []string

	if s.Kind == snapshots.KindActive {
		options = append(options,
			fmt.Sprintf("workdir=%s", o.workPath(s.ID)),
			fmt.Sprintf("upperdir=%s", o.upperPath(s.ID)),
		)
	} else {
		return []mount.Mount{
			{
				Source: o.committedPath(s.ParentIDs[0]),
				Type:   "bind",
				Options: []string{
					"ro",
					"rbind",
				},
			},
		}
	}

	options = append(options, fmt.Sprintf("lowerdir=%s", o.committedPath(s.ParentIDs[0])))
	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}

}

func (o *snapshotter) upperPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *snapshotter) workPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "work")
}

func (o *snapshotter) rootPath(id string) string {
	return filepath.Join(o.root, "snapshots", id)
}

func (o *snapshotter) committedPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "root")
}

func (o *snapshotter) isPreparedForImage(info snapshots.Info) bool {
	return info.Labels[snapshots.TypeLabelKey] == snapshots.ImageType
}

// Close closes the snapshotter
func (o *snapshotter) Close() error {
	return o.ms.Close()
}