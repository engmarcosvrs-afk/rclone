package vfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
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

// TestVFSSnapshotWrongFs checks a snapshot for another remote is ignored
func TestVFSSnapshotWrongFs(t *testing.T) {
	opt := vfscommon.Opt
	opt.Snapshot = filepath.Join(t.TempDir(), "snapshot.json.gz")
	opt.PollInterval = 0

	r, vfs := newTestVFSOpt(t, &opt)
	r.WriteObject(context.Background(), "file1", "file1 contents", t1)
	vfs.refresh()
	require.FileExists(t, opt.Snapshot)

	_, _, err := readSnapshot(opt.Snapshot, r.Flocal)
	assert.ErrorContains(t, err, "is for")
}
