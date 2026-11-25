package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"syscall"
)

// A previous turn may have linked the final manifest but failed fsync. Byte
// verification alone is not durability: flush again before hot-data release.
func syncCompletedPublishedArchive(ctx context.Context, root *os.Root, names []string) error {
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return err
		}
		if err := errors.Join(f.Sync(), f.Close()); err != nil {
			return err
		}
	}
	return syncArchiveDirectory(root)
}

func archiveSingleLink(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && st.Nlink == 1
}

// Validate EVERY retained prefix before changing any file. Unknown names,
// conflicting data, links and oversized output are not capacity failures.
func validatePublishedArchivePrefixes(ctx context.Context, root *os.Root, names []string, expected map[string][]byte) error {
	for _, name := range names {
		raw, ok := expected[name]
		if !ok {
			return ErrGenerationIntegrity
		}
		info, err := root.Lstat(name)
		if err != nil {
			return err
		}
		if !archiveSingleLink(info) {
			return ErrGenerationIntegrity
		}
		prefix, err := readArchiveFile(ctx, root, name, int64(len(raw)))
		if err != nil {
			return err
		}
		if !bytes.HasPrefix(raw, prefix) {
			return ErrGenerationChanged
		}
	}
	return ctx.Err()
}

// Append only the missing suffix of an exact revalidated derived copy. Never
// truncate, remove, replace or append to a linked/unknown financial artifact.
func resumePublishedArchiveFile(ctx context.Context, root *os.Root, name string, raw []byte, writeFile func(context.Context, *os.Root, string, []byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	before, err := root.Lstat(name)
	if os.IsNotExist(err) {
		return writeFile(ctx, root, name, raw)
	}
	if err != nil {
		return err
	}
	if !before.Mode().IsRegular() || before.Mode().Perm()&0077 != 0 || !archiveSingleLink(before) || before.Size() > int64(len(raw)) {
		return ErrGenerationIntegrity
	}
	f, err := root.OpenFile(name, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(before, after) || after.Size() != before.Size() || !archiveSingleLink(after) {
		return ErrGenerationChanged
	}
	prefix := make([]byte, after.Size())
	if _, err := io.ReadFull(f, prefix); err != nil {
		return err
	}
	if !bytes.HasPrefix(raw, prefix) {
		return ErrGenerationChanged
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := io.Copy(f, bytes.NewReader(raw[len(prefix):])); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return ctx.Err()
}
