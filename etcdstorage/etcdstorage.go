package etcdstorage

import (
	"context"
	"sync/atomic"

	"github.com/go-tk/versionedkv"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/etcdserverpb"
)

// Options represents options for etcd storages.
type Options struct {
	Prefix string
}

func (o *Options) sanitize() {
	if o.Prefix == "" {
		o.Prefix = "versionedkv/"
	}
}

// New creates a new etcd storage with the given options.
func New(client *clientv3.Client, options Options) versionedkv.Storage {
	var es etcdStorage
	es.client = client
	es.options = options
	es.options.sanitize()
	es.closure = make(chan struct{})
	return &es
}

type etcdStorage struct {
	client    *clientv3.Client
	options   Options
	isClosed1 int32
	closure   chan struct{}
}

func (es *etcdStorage) GetValue(ctx context.Context, key string) (string, versionedkv.Version, error) {
	if es.isClosed() {
		return "", nil, versionedkv.ErrStorageClosed
	}
	fullKey := es.fullKey(key)
	getResp, err := es.client.Get(ctx, fullKey)
	if err != err {
		return "", nil, err
	}
	if len(getResp.Kvs) == 0 {
		return "", nil, nil
	}
	kv := getResp.Kvs[0]
	return string(kv.Value), kv.ModRevision, nil
}

func (es *etcdStorage) WaitForValue(ctx context.Context, key string, version versionedkv.Version) (string, versionedkv.Version, error) {
	if es.isClosed() {
		return "", nil, versionedkv.ErrStorageClosed
	}
	fullKey := es.fullKey(key)
	var modRevision int64
	if version == nil {
		getResp, err := es.client.Get(ctx, fullKey)
		if err != nil {
			return "", nil, err
		}
		if len(getResp.Kvs) >= 1 {
			kv := getResp.Kvs[0]
			return string(kv.Value), kv.ModRevision, nil
		}
		modRevision = getResp.Header.Revision
	} else {
		modRevision = version.(int64)
		txn := es.client.Txn(ctx).If(
			clientv3.Compare(clientv3.CreateRevision(fullKey), ">", 0),
			clientv3.Compare(clientv3.ModRevision(fullKey), "!=", modRevision),
		).Then(clientv3.OpGet(fullKey))
		txnResp, err := txn.Commit()
		if err != nil {
			return "", nil, err
		}
		if txnResp.Succeeded {
			getResp := txnResp.Responses[0].Response.(*etcdserverpb.ResponseOp_ResponseRange).ResponseRange
			kv := getResp.Kvs[0]
			return string(kv.Value), kv.ModRevision, nil
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	select {
	case watchResp, ok := <-es.client.Watch(ctx, fullKey, clientv3.WithRev(modRevision+1)):
		if !ok {
			cancel = nil
			return "", nil, ctx.Err()
		}
		if err := watchResp.Err(); err != nil {
			return "", nil, err
		}
		for i := len(watchResp.Events) - 1; ; i-- {
			kv := watchResp.Events[i].Kv
			if version == nil {
				if kv.Version >= 1 {
					return string(kv.Value), kv.ModRevision, nil
				}
			} else {
				if kv.Version == 0 {
					return "", nil, nil
				}
				return string(kv.Value), kv.ModRevision, nil
			}
		}
	case <-es.closure:
		return "", nil, versionedkv.ErrStorageClosed
	}
}

func (es *etcdStorage) CreateValue(ctx context.Context, key, value string) (versionedkv.Version, error) {
	if es.isClosed() {
		return nil, versionedkv.ErrStorageClosed
	}
	fullKey := es.fullKey(key)
	txn := es.client.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(fullKey), "=", 0)).Then(clientv3.OpPut(fullKey, value))
	txnResp, err := txn.Commit()
	if err != nil {
		return nil, err
	}
	if !txnResp.Succeeded {
		return nil, nil
	}
	putResp := txnResp.Responses[0].Response.(*etcdserverpb.ResponseOp_ResponsePut).ResponsePut
	return putResp.Header.Revision, nil
}

func (es *etcdStorage) UpdateValue(ctx context.Context, key, value string, version versionedkv.Version) (versionedkv.Version, error) {
	if es.isClosed() {
		return nil, versionedkv.ErrStorageClosed
	}
	fullKey := es.fullKey(key)
	var cmp clientv3.Cmp
	if version == nil {
		cmp = clientv3.Compare(clientv3.CreateRevision(fullKey), ">", 0)
	} else {
		modRevision := version.(int64)
		cmp = clientv3.Compare(clientv3.ModRevision(fullKey), "=", modRevision)
	}
	txn := es.client.Txn(ctx).If(cmp).Then(clientv3.OpPut(fullKey, value))
	txnResp, err := txn.Commit()
	if err != nil {
		return nil, err
	}
	if !txnResp.Succeeded {
		return nil, nil
	}
	putResp := txnResp.Responses[0].Response.(*etcdserverpb.ResponseOp_ResponsePut).ResponsePut
	return putResp.Header.Revision, nil
}

func (es *etcdStorage) CreateOrUpdateValue(ctx context.Context, key, value string, version versionedkv.Version) (versionedkv.Version, error) {
	if es.isClosed() {
		return nil, versionedkv.ErrStorageClosed
	}
	fullKey := es.fullKey(key)
	if version == nil {
		putResp, err := es.client.Put(ctx, fullKey, value)
		if err != nil {
			return nil, err
		}
		return putResp.Header.Revision, nil
	}
	modRevision := version.(int64)
	txn := es.client.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(fullKey), ">", 0),
		clientv3.Compare(clientv3.ModRevision(fullKey), "!=", modRevision),
	).Else(clientv3.OpPut(fullKey, value))
	txnResp, err := txn.Commit()
	if err != nil {
		return nil, err
	}
	if txnResp.Succeeded {
		return nil, nil
	}
	putResp := txnResp.Responses[0].Response.(*etcdserverpb.ResponseOp_ResponsePut).ResponsePut
	return putResp.Header.Revision, nil
}

func (es *etcdStorage) DeleteValue(ctx context.Context, key string, version versionedkv.Version) (bool, error) {
	if es.isClosed() {
		return false, versionedkv.ErrStorageClosed
	}
	fullKey := es.fullKey(key)
	if version == nil {
		deleteResp, err := es.client.Delete(ctx, fullKey)
		if err != nil {
			return false, err
		}
		ok := deleteResp.Deleted >= 1
		return ok, nil
	}
	modRevision := version.(int64)
	txn := es.client.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision(fullKey), "=", modRevision)).Then(clientv3.OpDelete(fullKey))
	txnResp, err := txn.Commit()
	if err != nil {
		return false, err
	}
	return txnResp.Succeeded, nil
}

func (es *etcdStorage) Close() error {
	if atomic.SwapInt32(&es.isClosed1, 1) != 0 {
		return versionedkv.ErrStorageClosed
	}
	close(es.closure)
	return nil
}

func (es *etcdStorage) Inspect(ctx context.Context) (versionedkv.StorageDetails, error) {
	if es.isClosed() {
		return versionedkv.StorageDetails{IsClosed: true}, nil
	}
	getResp, err := es.client.Get(ctx, es.options.Prefix, clientv3.WithPrefix())
	if err != nil {
		return versionedkv.StorageDetails{}, err
	}
	var valueDetails map[string]versionedkv.ValueDetails
	for _, kv := range getResp.Kvs {
		if valueDetails == nil {
			valueDetails = make(map[string]versionedkv.ValueDetails)
		}
		key := string(kv.Key[len(es.options.Prefix):])
		valueDetails[key] = versionedkv.ValueDetails{
			V:       string(kv.Value),
			Version: kv.ModRevision,
		}
	}
	return versionedkv.StorageDetails{
		Values: valueDetails,
	}, nil
}

func (es *etcdStorage) fullKey(key string) string {
	return es.options.Prefix + key
}

func (es *etcdStorage) isClosed() bool {
	return atomic.LoadInt32(&es.isClosed1) != 0
}
