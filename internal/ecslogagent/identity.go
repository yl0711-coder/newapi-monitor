package ecslogagent

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type diskIdentity struct {
	Version                         int
	Binding, PrivateKey, LocalToken string
}

func privateDirectory(path string) error {
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("agent directory must be private and not a symlink")
	}
	return nil
}

func identityBinding(c Config, meta Metadata) string {
	b, _ := json.Marshal(struct {
		Config   Config
		Metadata Metadata
	}{c, meta})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func openIdentity(c Config, meta Metadata) (diskIdentity, string, *os.File, error) {
	if err := privateDirectory(c.StateRoot); err != nil {
		return diskIdentity{}, "", nil, err
	}
	dir := filepath.Join(c.StateRoot, sourceNode(meta, c.Container)+"-"+c.Kind)
	if err := privateDirectory(dir); err != nil {
		return diskIdentity{}, "", nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "agent.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return diskIdentity{}, "", nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return diskIdentity{}, "", nil, errors.New("another agent owns this source")
	}
	identity, err := loadOrCreateIdentity(filepath.Join(dir, "identity.json"), identityBinding(c, meta))
	if err != nil {
		_ = lock.Close()
		return diskIdentity{}, "", nil, err
	}
	return identity, dir, lock, nil
}

func loadOrCreateIdentity(path, binding string) (diskIdentity, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(filepath.Dir(path))
		if readErr != nil {
			return diskIdentity{}, readErr
		}
		for _, entry := range entries {
			if entry.Name() != "agent.lock" {
				return diskIdentity{}, errors.New("identity missing beside existing state; recovery required")
			}
		}
		_, key, keyErr := ed25519.GenerateKey(rand.Reader)
		if keyErr != nil {
			return diskIdentity{}, keyErr
		}
		token := make([]byte, 32)
		if _, err := rand.Read(token); err != nil {
			return diskIdentity{}, err
		}
		identity := diskIdentity{1, binding, base64.StdEncoding.EncodeToString(key), hex.EncodeToString(token)}
		b, _ := json.Marshal(identity)
		return identity, writeIdentity(path, b)
	}
	if err != nil {
		return diskIdentity{}, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return diskIdentity{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
		return diskIdentity{}, errors.New("identity file is not a private bounded regular file")
	}
	b, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return diskIdentity{}, err
	}
	var identity diskIdentity
	if len(b) > 4096 || json.Unmarshal(b, &identity) != nil || identity.Version != 1 || identity.Binding != binding || len(identity.LocalToken) != 64 {
		return diskIdentity{}, errors.New("identity/config binding mismatch; preserve state for review")
	}
	key, err := base64.StdEncoding.DecodeString(identity.PrivateKey)
	if err != nil || len(key) != ed25519.PrivateKeySize || !ed25519.PrivateKey(key).Equal(ed25519.NewKeyFromSeed(key[:ed25519.SeedSize])) {
		return diskIdentity{}, errors.New("invalid persisted signing key")
	}
	return identity, nil
}

func writeIdentity(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".identity-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.Write(b); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
