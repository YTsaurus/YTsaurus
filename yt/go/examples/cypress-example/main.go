package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yt/ytrpc"
)

var (
	flagProxy  = flag.String("proxy", "", "cluster address")
	flagUseRPC = flag.Bool("use-rpc", false, "use RPC proxy")
	flagUseTLS = flag.Bool("use-tls", false, "use TLS")
)

func Example() error {
	flag.Parse()

	var yc yt.Client
	var err error
	if *flagUseRPC {
		yc, err = ytrpc.NewClient(&yt.Config{
			Proxy:             *flagProxy,
			ReadTokenFromFile: true,
			UseTLS:            *flagUseTLS,
		})
	} else {
		yc, err = ythttp.NewClient(&yt.Config{
			Proxy:             *flagProxy,
			ReadTokenFromFile: true,
			UseTLS:            *flagUseTLS,
		})
	}

	if err != nil {
		return err
	}

	ctx := context.Background()

	var attrs struct {
		Type         yt.NodeType `yson:"type"`
		CreationTime yson.Time   `yson:"creation_time"`
		Account      string      `yson:"account"`
	}

	if err = yc.GetNode(ctx, ypath.Path("//@"), &attrs, nil); err != nil {
		return err
	}

	fmt.Printf("cluster was created at %v\n", time.Time(attrs.CreationTime))
	return nil
}

func main() {
	if err := Example(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}
