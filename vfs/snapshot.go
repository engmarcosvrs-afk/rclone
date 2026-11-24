package vfs

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/rc"
)

// snapshotVersion is the version of the snapshot file format
const snapshotVersion = 3

// snapshotHeader is the first record in a snapshot file.
//
// One snapshot file holds listings for a whole remote with paths from
// the root of the remote, so VFSes rooted at different directories of
// the same remote can share it.
type snapshotHeader struct {
	Version int    `json:"v"`
	Remote  string `json:"remote"`
}

// snapshotEntry is a file or directory in a snapshot listing.
//
// Size is the size of the directory and its contents for a directory
// or -1 if that isn't known.
type snapshotEntry struct {
	Name    string            `json:"n"`
	Dir     bool              `json:"d,omitempty"`
	Size    int64             `json:"s,omitempty"`
	ModTime time.Time         `json:"t"`
	Hashes  map[string]string `json:"h,omitempty"`
}

// snapshotListing is the listing of one directory in a snapshot file.
//
// State is the change notify position the listing is up to date with
// and StateTime when that position was reached, or "" if the listing
// must be read again before it can be trusted.
type snapshotListing struct {
	Dir       string          `json:"p"`
	Entries   []snapshotEntry `json:"e"`
	State     string          `json:"s,omitempty"`
	StateTime int64           `json:"st,omitempty"`
}

// snapshotPrefix returns the path of the root of f from the root of its remote
func snapshotPrefix(f fs.Fs) string {
	return strings.Trim(f.Root(), "/")
}

// relativeTo returns dir relative to prefix and whether dir is prefix
// or inside it.
func relativeTo(prefix, dir string) (string, bool) {
	switch {
	case prefix == "":
		return dir, true
	case dir == prefix:
		return "", true
	case strings.HasPrefix(dir, prefix+"/"):
		return dir[len(prefix)+1:], true
	}
	return "", false
}

// forEachListing calls fn for each listing in the snapshot file for
// the remote named remote.
func forEachListing(name, remote string, fn func(*snapshotListing)) error {
	in, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() {
		_ = in.Close()
	}()
	gz, err := gzip.NewReader(in)
	if err != nil {
		return fmt.Errorf("snapshot %q: %w", name, err)
	}
	dec := json.NewDecoder(gz)
	var header snapshotHeader
	if err = dec.Decode(&header); err != nil {
		return fmt.Errorf("snapshot %q: bad header: %w", name, err)
	}
	if header.Version != snapshotVersion {
		return fmt.Errorf("snapshot %q: unsupported version %d", name, header.Version)
	}
	if header.Remote != remote {
		return fmt.Errorf("snapshot %q is for remote %q not %q", name, header.Remote, remote)
	}
	for {
		var listing snapshotListing
		err = dec.Decode(&listing)
		if err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("snapshot %q: %w", name, err)
		}
		fn(&listing)
	}
}

// snapObject is an fs.Object read from a snapshot file.
//
// It answers metadata queries from the snapshot and looks up the real
// object on the remote the first time it is needed for anything else.
type snapObject struct {
	f       fs.Fs
	remote  string
	size    int64
	modTime time.Time
	hashes  map[hash.Type]string

	mu   sync.Mutex
	real fs.Object
}

// Check the interfaces are satisfied
var _ fs.Object = (*snapObject)(nil)

// String returns a description of the Object
func (o *snapObject) String() string {
	return o.remote
}

// Remote returns the remote path
func (o *snapObject) Remote() string {
	return o.remote
}

// Fs returns the parent Fs
func (o *snapObject) Fs() fs.Info {
	return o.f
}

// Storable says whether this object can be stored
func (o *snapObject) Storable() bool {
	return true
}

// resolved returns the real object if it has been looked up already
func (o *snapObject) resolved() fs.Object {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.real
}

// resolve looks up the real object on the remote
func (o *snapObject) resolve(ctx context.Context) (fs.Object, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.real == nil {
		obj, err := o.f.NewObject(ctx, o.remote)
		if err != nil {
			return nil, err
		}
		o.real = obj
	}
	return o.real, nil
}

// ModTime returns the modification time of the object
func (o *snapObject) ModTime(ctx context.Context) time.Time {
	if r := o.resolved(); r != nil {
		return r.ModTime(ctx)
	}
	return o.modTime
}

// Size returns the size of the object in bytes
func (o *snapObject) Size() int64 {
	if r := o.resolved(); r != nil {
		return r.Size()
	}
	return o.size
}

// Hash returns the requested hash of the object
func (o *snapObject) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if r := o.resolved(); r != nil {
		return r.Hash(ctx, ty)
	}
	if h, ok := o.hashes[ty]; ok {
		return h, nil
	}
	r, err := o.resolve(ctx)
	if err != nil {
		return "", err
	}
	return r.Hash(ctx, ty)
}

// SetModTime sets the modification time of the object
func (o *snapObject) SetModTime(ctx context.Context, t time.Time) error {
	r, err := o.resolve(ctx)
	if err != nil {
		return err
	}
	return r.SetModTime(ctx, t)
}

// Open opens the object for read
func (o *snapObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	r, err := o.resolve(ctx)
	if err != nil {
		return nil, err
	}
	return r.Open(ctx, options...)
}

// Update replaces the contents of the object
func (o *snapObject) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	r, err := o.resolve(ctx)
	if err != nil {
		return err
	}
	return r.Update(ctx, in, src, options...)
}

// Remove removes the object
func (o *snapObject) Remove(ctx context.Context) error {
	r, err := o.resolve(ctx)
	if err != nil {
		return err
	}
	return r.Remove(ctx)
}

// unSnapshot re-reads the directory from the remote if its listing
// came from a snapshot, so that changes are made against the real
// state of the remote rather than a possibly stale snapshot.
func (d *Dir) unSnapshot() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.snapshot {
		return nil
	}
	fs.Debugf(d.path, "Re-reading directory loaded from snapshot before changing it")
	d.read = time.Time{}
	return d._readDir()
}

// loadSnapshot fills the directory cache from the --vfs-snapshot file.
//
// It must be called before the VFS is in use. Directories which were
// not listed when the snapshot was saved are left to be read on demand.
//
// If resumable is set the change notify position to resume from is
// returned so the changes made since the snapshot can be applied, and
// listings which are up to date with it don't need a scan.
func (vfs *VFS) loadSnapshot(resumable bool) (state string) {
	start := time.Now()
	dt, listed, err := readSnapshot(vfs.Opt.Snapshot, vfs.f)
	if errors.Is(err, os.ErrNotExist) {
		fs.Infof(vfs.f, "No VFS snapshot found at %q", vfs.Opt.Snapshot)
		return ""
	} else if err != nil {
		fs.Errorf(vfs.f, "Ignoring VFS snapshot: %v", err)
		return ""
	}
	root := vfs.root
	root.mu.Lock()
	err = root._readDirFromDirTree(dt, start)
	root.mu.Unlock()
	if err != nil {
		fs.Errorf(vfs.f, "Failed to load VFS snapshot: %v", err)
		return ""
	}
	// Resume from the oldest position so no change to any listing is missed
	var stateTime int64
	if resumable {
		for _, l := range listed {
			if l.State != "" && (stateTime == 0 || l.StateTime < stateTime) {
				state, stateTime = l.State, l.StateTime
			}
		}
	}
	vfs.cnMu.Lock()
	vfs.cnState, vfs.cnStateTime = state, stateTime
	vfs.cnMu.Unlock()
	dirs, unverified := 0, 0
	root.walk(func(d *Dir) {
		if l, ok := listed[d.path]; ok {
			d.read = start
			d.snapshot = true
			d.unverified = state == "" || l.State == ""
			if d.unverified {
				unverified++
			}
			dirs++
		} else {
			// Not listed in the snapshot so read it from the remote when needed
			d.read = time.Time{}
		}
	})
	fs.Infof(vfs.f, "Loaded VFS snapshot of %d directories (%d to be verified) in %v", dirs, unverified, time.Since(start))
	return state
}

// setChangeState records the change notify position the directory
// cache is up to date with.
func (vfs *VFS) setChangeState(state string) {
	vfs.cnMu.Lock()
	vfs.cnState, vfs.cnStateTime = state, time.Now().UnixNano()
	vfs.cnMu.Unlock()
}

// changesLost marks the directories loaded from the snapshot as
// needing to be read again because the changes since it was saved
// can't be found out.
func (vfs *VFS) changesLost() {
	fs.Logf(vfs.f, "Changes since the VFS snapshot can't be found so it will be verified by the next scan")
	vfs.root.walk(func(d *Dir) {
		if d.snapshot {
			d.unverified = true
		}
	})
}

// readSnapshot reads the listings under the root of f from the
// snapshot file returning the directory tree and the set of
// directories which were listed in it, both relative to the root of f.
func readSnapshot(name string, f fs.Fs) (dt dirtree.DirTree, listed map[string]snapshotListing, err error) {
	prefix := snapshotPrefix(f)
	dt = dirtree.New()
	listed = make(map[string]snapshotListing)
	err = forEachListing(name, f.Name(), func(listing *snapshotListing) {
		dir, ok := relativeTo(prefix, listing.Dir)
		if !ok {
			return
		}
		listed[dir] = snapshotListing{State: listing.State, StateTime: listing.StateTime}
		entries := make(fs.DirEntries, 0, len(listing.Entries))
		for _, e := range listing.Entries {
			remote := path.Join(dir, e.Name)
			if e.Dir {
				entries = append(entries, fs.NewDir(remote, e.ModTime).SetSize(e.Size))
				continue
			}
			o := &snapObject{
				f:       f,
				remote:  remote,
				size:    e.Size,
				modTime: e.ModTime,
			}
			for typeName, value := range e.Hashes {
				var ty hash.Type
				if ty.Set(typeName) == nil {
					if o.hashes == nil {
						o.hashes = make(map[hash.Type]string, len(e.Hashes))
					}
					o.hashes[ty] = value
				}
			}
			entries = append(entries, o)
		}
		dt[dir] = entries
	})
	if err != nil {
		return nil, nil, err
	}
	return dt, listed, nil
}

// saveSnapshot writes the directories which have been listed to the
// --vfs-snapshot file if it is set.
func (vfs *VFS) saveSnapshot() {
	if vfs.Opt.Snapshot == "" || vfs.root == nil {
		return
	}
	vfs.snapshotMu.Lock()
	defer vfs.snapshotMu.Unlock()
	start := time.Now()
	dirs, err := vfs.writeSnapshot(vfs.Opt.Snapshot)
	if err != nil {
		fs.Errorf(vfs.f, "Failed to save VFS snapshot: %v", err)
		return
	}
	fs.Infof(vfs.f, "Saved VFS snapshot of %d directories in %v", dirs, time.Since(start))
}

// writeSnapshot writes the snapshot to a temporary file and renames
// it into place so a failed save doesn't destroy the previous one.
func (vfs *VFS) writeSnapshot(name string) (dirs int, err error) {
	tmp, err := os.CreateTemp(filepath.Dir(name), filepath.Base(name)+".*.tmp")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	gz := gzip.NewWriter(tmp)
	enc := json.NewEncoder(gz)
	remote := vfs.f.Name()
	prefix := snapshotPrefix(vfs.f)
	if err = enc.Encode(snapshotHeader{Version: snapshotVersion, Remote: remote}); err != nil {
		return 0, err
	}
	// Keep the listings outside this VFS which other VFSes on the same
	// remote may have saved. Those inside it are replaced below.
	keepErr := forEachListing(name, remote, func(listing *snapshotListing) {
		if _, inside := relativeTo(prefix, listing.Dir); !inside && err == nil {
			err = enc.Encode(listing)
		}
	})
	if err != nil {
		return 0, err
	}
	if keepErr != nil && !errors.Is(keepErr, os.ErrNotExist) {
		fs.Infof(vfs.f, "Replacing VFS snapshot: %v", keepErr)
	}
	// Only record hashes which don't need reading the data to compute
	var hashTypes []hash.Type
	if !vfs.f.Features().SlowHash {
		hashTypes = vfs.f.Hashes().Array()
	}
	vfs.cnMu.Lock()
	state, stateTime := vfs.cnState, vfs.cnStateTime
	vfs.cnMu.Unlock()
	vfs.root.walk(func(d *Dir) {
		if err != nil || d.read.IsZero() {
			return
		}
		listing := snapshotListing{Dir: path.Join(prefix, d.path), Entries: make([]snapshotEntry, 0, len(d.items))}
		if !d.unverified {
			listing.State, listing.StateTime = state, stateTime
		}
		for _, node := range d.items {
			switch x := node.(type) {
			case *Dir:
				listing.Entries = append(listing.Entries, snapshotEntry{
					Name:    path.Base(x.path),
					Dir:     true,
					Size:    x.entrySize(),
					ModTime: x.ModTime(),
				})
			case *File:
				o := x.getObject()
				if o == nil {
					// still being written so not on the remote yet
					continue
				}
				e := snapshotEntry{
					Name:    path.Base(o.Remote()),
					Size:    o.Size(),
					ModTime: o.ModTime(vfs.ctx),
				}
				for _, ty := range hashTypes {
					if h, hashErr := o.Hash(vfs.ctx, ty); hashErr == nil && h != "" {
						if e.Hashes == nil {
							e.Hashes = make(map[string]string, len(hashTypes))
						}
						e.Hashes[ty.String()] = h
					}
				}
				listing.Entries = append(listing.Entries, e)
			}
		}
		err = enc.Encode(listing)
		dirs++
	})
	if err != nil {
		return 0, err
	}
	if err = gz.Close(); err != nil {
		return 0, err
	}
	if err = tmp.Close(); err != nil {
		return 0, err
	}
	if err = os.Rename(tmpName, name); err != nil {
		return 0, err
	}
	return dirs, nil
}

func init() {
	rc.Add(rc.Call{
		Path:  "vfs/snapshot",
		Fn:    rcSnapshot,
		Title: "Save the directory cache to the --vfs-snapshot file.",
		Help: `
This writes the directories which have been listed to the file given
by --vfs-snapshot so they can be loaded on the next start without
listing the remote.

    rclone rc vfs/snapshot
` + getVFSHelp,
	})
}

func rcSnapshot(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	vfs, err := getVFS(in)
	if err != nil {
		return nil, err
	}
	if vfs.Opt.Snapshot == "" {
		return nil, errors.New("--vfs-snapshot is not set")
	}
	vfs.snapshotMu.Lock()
	dirs, err := vfs.writeSnapshot(vfs.Opt.Snapshot)
	vfs.snapshotMu.Unlock()
	if err != nil {
		return nil, err
	}
	return rc.Params{"dirs": dirs}, nil
}
