package fs

import (
	"context"
	"io/fs"
	"os"
)

type contextFSKey struct{}

func WithContext(ctx context.Context, fsys fs.FS) context.Context {
	return context.WithValue(ctx, contextFSKey{}, fsys)
}

func FromContext(ctx context.Context) fs.FS {
	fsys, ok := ctx.Value(contextFSKey{}).(fs.FS)
	if ok && fsys != nil {
		return fsys
	}
	return nil
}

func ReadFile(ctx context.Context, path string) ([]byte, error) {
	if fsys := FromContext(ctx); fsys != nil {
		return fs.ReadFile(fsys, path)
	}
	return os.ReadFile(path)
}

func ReadDir(ctx context.Context, path string) ([]fs.DirEntry, error) {
	if fsys := FromContext(ctx); fsys != nil {
		return fs.ReadDir(fsys, path)
	}
	return os.ReadDir(path)
}
