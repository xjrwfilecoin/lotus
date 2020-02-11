package sealing

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/lotus/node/config"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	badger "github.com/ipfs/go-ds-badger2"
	"go.etcd.io/etcd/client"
)

func TestEtcd(t *testing.T) {
	cfg := client.Config{
		Endpoints: []string{"http://172.16.8.40:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	kapi := client.NewKeysAPI(c)
	// set "/foo" key with "bar" value
	t.Log("Setting '/foo' key with 'bar' value")
	resp, err := kapi.Set(context.Background(), "/foo", "bar", nil)
	if err != nil {
		t.Error(err)
	} else {
		// print common key info
		t.Logf("Set is done. Metadata is %q\n", resp)
	}
	// get "/foo" key's value
	t.Log("Getting '/foo' key value")
	resp, err = kapi.Get(context.Background(), "/foo", nil)
	if err != nil {
		t.Fatal(err)
	} else {
		// print common key info
		t.Logf("Get is done. Metadata is %q\n", resp)
		// print value
		t.Logf("%q key has %q value\n", resp.Node.Key, resp.Node.Value)
	}

}
func TestSimpleRPC(t *testing.T) {
	servCfg := &config.CfgSealAgent{[]string{"172.16.8.40:2379", "172.16.8.37:2379", "172.16.8.39:2379"}, "127.0.0.1", 40000}
	NewAgentService(nil, servCfg)

	timer := time.NewTimer(5 * time.Second)
	<-timer.C
	clientCfg := &config.CfgSealAgent{[]string{"172.16.8.40:2379", "172.16.8.37:2379", "172.16.8.39:2379"}, "127.0.0.1", 40001}
	opts := badger.DefaultOptions
	opts.Truncate = true

	ds, err := badger.NewDatastore("/tmp", &opts)
	/*if fsr.dsErr == nil {
		fsr.ds = datastore.NewLogDatastore(fsr.ds, "fsrepo")
	}*/
	dsw := namespace.Wrap(ds, datastore.NewKey("test"))
	client := NewSealAgent(nil, clientCfg, dsw)

	ret, err := client.AcquireSectorId()
	if err == nil {
		t.Logf("Get is done. Metadata is %v\n", ret)
	} else {
		t.Errorf("error occurs:%v\n", err)
	}

}
