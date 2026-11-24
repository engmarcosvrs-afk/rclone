package vfs

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVFSSnapshot checks the directory cache is loaded from the
// snapshot and that a directory is re-read before it is changed.
func TestVFSSnapshot(t *testing.T) {
	ctx := context.Background()
	opt := vfscommon.Opt
	opt.Snapshot = filepath.Join(t.TempDir(), "snapshot.json.gz")
	opt.DirCacheTime = fs.Duration(time.Hour)
	opt.PollInterval = 0
	r, vfs := newTestVFSOpt(t, &opt)

	file1 := r.WriteObject(ctx, "dir/file1", "file1 contents", t1)
	file2 := r.WriteObject(ctx, "dir/file2", "file2 contents", t2)
	r.CheckRemoteItems(t, file1, file2)

	// Read the whole tree which saves the snapshot
	vfs.refresh()
	require.FileExists(t, opt.Snapshot)

	// Remove file2 without the VFS knowing
	obj, err := r.Fremote.NewObject(ctx, "dir/file2")
	require.NoError(t, err)
	require.NoError(t, obj.Remove(ctx))

	// Different options so the first VFS isn't reused
	opt2 := opt
	opt2.NoSeek = !opt.NoSeek
	vfs2 := New(ctx, r.Fremote, &opt2)
	t.Cleanup(func() {
		cleanupVFS(t, vfs2)
	})

	// file2 is still listed as it came from the snapshot
	node, err := vfs2.Stat("dir/file2")
	require.NoError(t, err)
	assert.Equal(t, int64(len("file2 contents")), node.Size())

	// file1 is read from the remote through the snapshot entry
	data, err := vfs2.ReadFile("dir/file1")
	require.NoError(t, err)
	assert.Equal(t, "file1 contents", string(data))

	// Changing the directory re-reads it from the remote first
	dir, err := vfs2.Stat("dir")
	require.NoError(t, err)
	_, err = dir.(*Dir).Create("file3", os.O_CREATE|os.O_WRONLY)
	require.NoError(t, err)
	_, err = vfs2.Stat("dir/file2")
	assert.Equal(t, ENOENT, err)
	node, err = vfs2.Stat("dir/file1")
	require.NoError(t, err)
	assert.Equal(t, int64(len("file1 contents")), node.Size())
}

// TestVFSSnapshotSubdir checks a VFS rooted in a subdirectory uses
// the part of the snapshot under it and keeps the rest when saving.
func TestVFSSnapshotSubdir(t *testing.T) {
	ctx := context.Background()
	opt := vfscommon.Opt
	opt.Snapshot = filepath.Join(t.TempDir(), "snapshot.json.gz")
	opt.DirCacheTime = fs.Duration(time.Hour)
	opt.PollInterval = 0
	r, vfs := newTestVFSOpt(t, &opt)

	file1 := r.WriteObject(ctx, "dir/sub/file1", "file1 contents", t1)
	file2 := r.WriteObject(ctx, "other/file2", "file2 contents", t2)
	r.CheckRemoteItems(t, file1, file2)

	// Save the snapshot of the whole remote
	vfs.refresh()
	require.FileExists(t, opt.Snapshot)

	// Remove file1 without the VFS knowing
	obj, err := r.Fremote.NewObject(ctx, "dir/sub/file1")
	require.NoError(t, err)
	require.NoError(t, obj.Remove(ctx))

	// A VFS rooted at dir uses the part of the snapshot under dir
	fsub, err := fs.NewFs(ctx, fspath.JoinRootPath(fs.ConfigStringFull(r.Fremote), "dir"))
	require.NoError(t, err)
	vfsSub := New(ctx, fsub, &opt)
	t.Cleanup(func() {
		cleanupVFS(t, vfsSub)
	})
	node, err := vfsSub.Stat("sub/file1")
	require.NoError(t, err)
	assert.Equal(t, int64(len("file1 contents")), node.Size())

	// Saving from the subdirectory keeps the listings outside it
	vfsSub.saveSnapshot()
	dt, listed, err := readSnapshot(opt.Snapshot, r.Fremote)
	require.NoError(t, err)
	assert.Contains(t, listed, "other")
	assert.Contains(t, listed, "dir/sub")
	assert.Len(t, dt["other"], 1)
}

// TestVFSSnapshotWrongRemote checks a snapshot for another remote is ignored
func TestVFSSnapshotWrongRemote(t *testing.T) {
	name := filepath.Join(t.TempDir(), "snapshot.json.gz")
	out, err := os.Create(name)
	require.NoError(t, err)
	gz := gzip.NewWriter(out)
	require.NoError(t, json.NewEncoder(gz).Encode(snapshotHeader{Version: snapshotVersion, Remote: "other-remote"}))
	require.NoError(t, gz.Close())
	require.NoError(t, out.Close())

	r := fstest.NewRun(t)
	_, _, err = readSnapshot(name, r.Fremote)
	assert.ErrorContains(t, err, "is for remote")
}
