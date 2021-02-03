package etcdstorage_test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/go-tk/versionedkv"
	. "github.com/go-tk/versionedkv-etcd/etcdstorage"
	"go.etcd.io/etcd/clientv3"
)

func TestEtcdStorage(t *testing.T) {
	versionedkv.DoTestStorage(t, func() (versionedkv.Storage, error) {
		return makeStorage()
	})
}

type storage struct {
	versionedkv.Storage

	c *clientv3.Client
}

func makeStorage() (storage, error) {
	ep := os.Getenv("ETCD_ENDPOINT")
	c, err := clientv3.New(clientv3.Config{Endpoints: []string{ep}})
	if err != nil {
		return storage{}, err
	}
	s := New(c, Options{
		Prefix: fmt.Sprintf("versionedkv-%d/", rand.Int63()),
	})
	return storage{
		Storage: s,

		c: c,
	}, nil
}

func (s storage) Close() error {
	err := s.Storage.Close()
	s.c.Close()
	return err
}
