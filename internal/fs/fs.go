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
	return os.DirFS(".")
}

func ReadFile(ctx context.Context, path string) ([]byte, error) {
	fsys := FromContext(ctx)
	return fs.ReadFile(fsys, path)
}

func ReadDir(ctx context.Context, path string) ([]fs.DirEntry, error) {
	fsys := FromContext(ctx)
	return fs.ReadDir(fsys, path)
}
