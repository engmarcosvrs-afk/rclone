package vfs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
)

// scanSaveInterval is how often a running scan saves the snapshot so
// an interrupted scan carries on from where it got to next time.
const scanSaveInterval = 2 * time.Minute

// scanStats counts the progress of a scan
type scanStats struct {
	running  atomic.Bool
	complete atomic.Bool  // set if the last scan finished every directory
	started  atomic.Int64 // unix nanoseconds
	finished atomic.Int64 // unix nanoseconds or 0 if running
	done     atomic.Int64 // directories scanned
	listed   atomic.Int64 // directories read from the remote
	queued   atomic.Int64 // directories found but not scanned yet
	files    atomic.Int64 // files in the scanned directories
	bytes    atomic.Int64 // size of the files in the scanned directories
	total    atomic.Int64 // expected size of all the files or -1 if unknown
	errors   atomic.Int64 // directories which couldn't be read
}

// entrySize returns the size of the directory and its contents as
// reported by the remote or -1 if unknown.
func (d *Dir) entrySize() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.entry == nil {
		return -1
	}
	return d.entry.Size()
}

// scan visits every directory under the root, reading from the remote
// those which have never been read, have expired or came from the
// snapshot without a change notify position to keep them up to date.
//
// Directories become usable as soon as they are read, rather than at
// the end like readDirTree, and the snapshot is saved as it goes.
func (vfs *VFS) scan(ctx context.Context) error {
	if !vfs.scanMu.TryLock() {
		return errors.New("a scan is already running")
	}
	defer vfs.scanMu.Unlock()

	st := &vfs.scanStats
	for _, counter := range []*atomic.Int64{&st.done, &st.listed, &st.files, &st.bytes, &st.errors, &st.finished} {
		counter.Store(0)
	}
	st.total.Store(-1)
	st.queued.Store(1)
	st.complete.Store(false)
	st.started.Store(time.Now().UnixNano())
	st.running.Store(true)
	defer func() {
		st.finished.Store(time.Now().UnixNano())
		st.running.Store(false)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		ticker := time.NewTicker(scanSaveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				vfs.saveSnapshot()
			}
		}
	}()

	workers := fs.GetConfig(ctx).Checkers
	if workers < 1 {
		workers = 1
	}
	level := []*Dir{vfs.root}
	for len(level) > 0 {
		var (
			mu   sync.Mutex
			next []*Dir
			wg   sync.WaitGroup
		)
		sem := make(chan struct{}, workers)
		for _, d := range level {
			if ctx.Err() != nil {
				break
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(d *Dir) {
				defer func() {
					<-sem
					wg.Done()
				}()
				children := vfs.scanDir(d)
				mu.Lock()
				next = append(next, children...)
				mu.Unlock()
			}(d)
		}
		wg.Wait()
		if err := ctx.Err(); err != nil {
			vfs.saveSnapshot()
			return err
		}
		level = next
	}
	st.complete.Store(st.errors.Load() == 0)
	vfs.saveSnapshot()
	fs.Infof(vfs.f, "VFS scan finished: %d directories, %d read from the remote, %d errors", st.done.Load(), st.listed.Load(), st.errors.Load())
	return nil
}

// scanDir reads d from the remote if it needs it and returns its subdirectories
func (vfs *VFS) scanDir(d *Dir) (children []*Dir) {
	st := &vfs.scanStats
	var files []*File

	d.mu.Lock()
	var err error
	listed := d.read.IsZero() || d.unverified
	if listed {
		d.read = time.Time{}
		err = d._readDir()
	}
	for _, node := range d.items {
		switch x := node.(type) {
		case *Dir:
			children = append(children, x)
		case *File:
			files = append(files, x)
		}
	}
	isRoot := d.parent == nil
	d.mu.Unlock()

	if err != nil {
		st.errors.Add(1)
		fs.Errorf(d, "Scan failed to read directory: %v", err)
	}
	if listed {
		st.listed.Add(1)
	}
	var bytes int64
	for _, f := range files {
		if size := f.Size(); size > 0 {
			bytes += size
		}
	}
	st.files.Add(int64(len(files)))
	st.bytes.Add(bytes)
	if isRoot {
		// The remote may know the size of each directory with its contents
		total := bytes
		for _, child := range children {
			size := child.entrySize()
			if size < 0 {
				total = -1
				break
			}
			total += size
		}
		st.total.Store(total)
	}
	st.queued.Add(int64(len(children)) - 1)
	st.done.Add(1)
	return children
}

func init() {
	rc.Add(rc.Call{
		Path:  "vfs/scan",
		Fn:    rcScan,
		Title: "Read every directory into the directory cache, a directory at a time.",
		Help: `
This reads every directory which isn't in the directory cache, has
expired or came from the --vfs-snapshot without a way of finding out
the changes made since. Each directory can be used as soon as it has
been read and the snapshot is saved as the scan goes so an interrupted
scan carries on from there next time.

Use it with _async=true and follow it with vfs/scan-status.

    rclone rc vfs/scan _async=true
` + getVFSHelp,
	})
	rc.Add(rc.Call{
		Path:  "vfs/scan-status",
		Fn:    rcScanStatus,
		Title: "Show the progress of vfs/scan.",
		Help: `
This returns

- running - true while a scan is running
- complete - true if the last scan finished every directory
- done - directories scanned
- listed - directories read from the remote
- queued - directories found but not scanned yet
- files - files in the scanned directories
- bytes - size of the files in the scanned directories
- total - expected size of all the files or -1 if unknown
- errors - directories which couldn't be read
- started, finished - times of the last scan

    rclone rc vfs/scan-status
` + getVFSHelp,
	})
}

func rcScan(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	vfs, err := getVFS(in)
	if err != nil {
		return nil, err
	}
	if err = vfs.scan(ctx); err != nil {
		return nil, err
	}
	return rcScanParams(vfs), nil
}

func rcScanStatus(ctx context.Context, in rc.Params) (out rc.Params, err error) {
	vfs, err := getVFS(in)
	if err != nil {
		return nil, err
	}
	return rcScanParams(vfs), nil
}

// rcScanParams returns the progress of the scan as rc.Params
func rcScanParams(vfs *VFS) rc.Params {
	st := &vfs.scanStats
	out := rc.Params{
		"running":  st.running.Load(),
		"complete": st.complete.Load(),
		"done":     st.done.Load(),
		"listed":   st.listed.Load(),
		"queued":   st.queued.Load(),
		"files":    st.files.Load(),
		"bytes":    st.bytes.Load(),
		"total":    st.total.Load(),
		"errors":   st.errors.Load(),
	}
	if t := st.started.Load(); t != 0 {
		out["started"] = time.Unix(0, t)
	}
	if t := st.finished.Load(); t != 0 {
		out["finished"] = time.Unix(0, t)
	}
	return out
}
