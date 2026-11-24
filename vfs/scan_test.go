package vfs

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVFSScan checks a scan reads every directory and that directories
// from the snapshot are only read again when they need verifying.
func TestVFSScan(t *testing.T) {
	ctx := context.Background()
	opt := vfscommon.Opt
	opt.Snapshot = filepath.Join(t.TempDir(), "snapshot.json.gz")
	opt.DirCacheTime = fs.Duration(time.Hour)
	opt.PollInterval = 0
	r, vfs := newTestVFSOpt(t, &opt)

	file1 := r.WriteObject(ctx, "dir/a/file1", "file1 contents", t1)
	file2 := r.WriteObject(ctx, "dir/b/file2", "file2 contents!", t2)
	file3 := r.WriteObject(ctx, "file3", "file3", t3)
	r.CheckRemoteItems(t, file1, file2, file3)
	wantBytes := file1.Size + file2.Size + file3.Size

	// A scan reads root, dir, dir/a and dir/b
	require.NoError(t, vfs.scan(ctx))
	st := rcScanParams(vfs)
	assert.Equal(t, int64(4), st["done"])
	assert.Equal(t, int64(4), st["listed"])
	assert.Equal(t, int64(0), st["queued"])
	assert.Equal(t, int64(3), st["files"])
	assert.Equal(t, wantBytes, st["bytes"])
	assert.Equal(t, true, st["complete"])
	assert.Equal(t, false, st["running"])
	require.FileExists(t, opt.Snapshot)

	// A second scan has nothing to read
	require.NoError(t, vfs.scan(ctx))
	assert.Equal(t, int64(0), rcScanParams(vfs)["listed"])

	// Record a change notify position so the snapshot can be trusted
	vfs.setChangeState("position-1")
	vfs.saveSnapshot()

	// A VFS which can resume from the position doesn't need to read anything
	opt2 := opt
	opt2.Snapshot = ""
	opt2.NoSeek = !opt.NoSeek
	vfs2 := New(ctx, r.Fremote, &opt2)
	t.Cleanup(func() {
		cleanupVFS(t, vfs2)
	})
	vfs2.Opt.Snapshot = opt.Snapshot
	assert.Equal(t, "position-1", vfs2.loadSnapshot(true))
	require.NoError(t, vfs2.scan(ctx))
	assert.Equal(t, int64(4), rcScanParams(vfs2)["done"])
	assert.Equal(t, int64(0), rcScanParams(vfs2)["listed"])

	// If the changes can't be found the snapshot is verified again
	vfs2.changesLost()
	require.NoError(t, vfs2.scan(ctx))
	assert.Equal(t, int64(4), rcScanParams(vfs2)["listed"])

	// Without a position to resume from the snapshot is verified too
	opt3 := opt2
	opt3.CaseInsensitive = !opt2.CaseInsensitive
	vfs3 := New(ctx, r.Fremote, &opt3)
	t.Cleanup(func() {
		cleanupVFS(t, vfs3)
	})
	vfs3.Opt.Snapshot = opt.Snapshot
	assert.Equal(t, "", vfs3.loadSnapshot(false))
	require.NoError(t, vfs3.scan(ctx))
	assert.Equal(t, int64(4), rcScanParams(vfs3)["listed"])
}
