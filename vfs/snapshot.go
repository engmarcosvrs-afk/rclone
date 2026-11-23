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
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/rc"
)

// snapshotVersion is the version of the snapshot file format
const snapshotVersion = 1

// snapshotHeader is the first record in a snapshot file
type snapshotHeader struct {
	Version int    `json:"v"`
	Fs      string `json:"fs"`
}

// snapshotEntry is a file or directory in a snapshot listing
type snapshotEntry struct {
	Name    string            `json:"n"`
	Dir     bool              `json:"d,omitempty"`
	Size    int64             `json:"s,omitempty"`
	ModTime time.Time         `json:"t"`
	Hashes  map[string]string `json:"h,omitempty"`
}

// snapshotListing is the listing of one directory in a snapshot file
type snapshotListing struct {
	Dir     string          `json:"p"`
	Entries []snapshotEntry `json:"e"`
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
func (vfs *VFS) loadSnapshot() {
	start := time.Now()
	dt, listed, err := readSnapshot(vfs.Opt.Snapshot, vfs.f)
	if errors.Is(err, os.ErrNotExist) {
		fs.Infof(vfs.f, "No VFS snapshot found at %q", vfs.Opt.Snapshot)
		return
	} else if err != nil {
		fs.Errorf(vfs.f, "Ignoring VFS snapshot: %v", err)
		return
	}
	root := vfs.root
	root.mu.Lock()
	err = root._readDirFromDirTree(dt, start)
	root.mu.Unlock()
	if err != nil {
		fs.Errorf(vfs.f, "Failed to load VFS snapshot: %v", err)
		return
	}
	dirs := 0
	root.walk(func(d *Dir) {
		if _, ok := listed[d.path]; ok {
			d.read = start
			d.snapshot = true
			dirs++
		} else {
			// Not listed in the snapshot so read it from the remote when needed
			d.read = time.Time{}
		}
	})
	fs.Infof(vfs.f, "Loaded VFS snapshot of %d directories in %v", dirs, time.Since(start))
}

// readSnapshot reads the snapshot file returning the directory tree
// and the set of directories which were listed in it.
func readSnapshot(name string, f fs.Fs) (dt dirtree.DirTree, listed map[string]struct{}, err error) {
	in, err := os.Open(name)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = in.Close()
	}()
	gz, err := gzip.NewReader(in)
	if err != nil {
		return nil, nil, fmt.Errorf("snapshot %q: %w", name, err)
	}
	dec := json.NewDecoder(gz)
	var header snapshotHeader
	if err = dec.Decode(&header); err != nil {
		return nil, nil, fmt.Errorf("snapshot %q: bad header: %w", name, err)
	}
	if header.Version != snapshotVersion {
		return nil, nil, fmt.Errorf("snapshot %q: unsupported version %d", name, header.Version)
	}
	if header.Fs != fs.ConfigString(f) {
		return nil, nil, fmt.Errorf("snapshot %q is for %q not %q", name, header.Fs, fs.ConfigString(f))
	}
	dt = dirtree.New()
	listed = make(map[string]struct{})
	for {
		var listing snapshotListing
		err = dec.Decode(&listing)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, fmt.Errorf("snapshot %q: %w", name, err)
		}
		listed[listing.Dir] = struct{}{}
		entries := make(fs.DirEntries, 0, len(listing.Entries))
		for _, e := range listing.Entries {
			remote := path.Join(listing.Dir, e.Name)
			if e.Dir {
				entries = append(entries, fs.NewDir(remote, e.ModTime))
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
		dt[listing.Dir] = entries
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
	if err = enc.Encode(snapshotHeader{Version: snapshotVersion, Fs: fs.ConfigString(vfs.f)}); err != nil {
		return 0, err
	}
	// Only record hashes which don't need reading the data to compute
	var hashTypes []hash.Type
	if !vfs.f.Features().SlowHash {
		hashTypes = vfs.f.Hashes().Array()
	}
	vfs.root.walk(func(d *Dir) {
		if err != nil || d.read.IsZero() {
			return
		}
		listing := snapshotListing{Dir: d.path, Entries: make([]snapshotEntry, 0, len(d.items))}
		for _, node := range d.items {
			switch x := node.(type) {
			case *Dir:
				listing.Entries = append(listing.Entries, snapshotEntry{
					Name:    path.Base(x.path),
					Dir:     true,
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
