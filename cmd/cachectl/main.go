// Command cachectl is a small command-line client for the cache.
//
//	cachectl [-seeds host:port,...] set <key> <value> [-ttl 30s]
//	cachectl get <key> [-quorum]
//	cachectl del <key>
//	cachectl owners <key>
//
// Seeds default to $SHC_SEEDS or 127.0.0.1:7001.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Kaushlendra-Kumar/self-healing-distributed-cache/pkg/client"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: cachectl [-seeds host:port,...] [-timeout 3s] <command> [args]

commands:
  set <key> <value> [-ttl 30s]   write a value
  get <key> [-quorum]            read a value (quorum: newest of a majority of replicas)
  del <key>                      delete a value
  owners <key>                   show the replica set for a key`)
	os.Exit(2)
}

func main() {
	seedsDefault := os.Getenv("SHC_SEEDS")
	if seedsDefault == "" {
		seedsDefault = "127.0.0.1:7001"
	}
	seeds := flag.String("seeds", seedsDefault, "comma-separated node addresses")
	timeout := flag.Duration("timeout", 3*time.Second, "per-attempt timeout")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		usage()
	}
	cmd, key := args[0], args[1]
	sub := flag.NewFlagSet(cmd, flag.ExitOnError)
	ttl := sub.Duration("ttl", 0, "time to live (0 = none)")
	quorum := sub.Bool("quorum", false, "quorum read")

	var value string
	rest := args[2:]
	if cmd == "set" {
		if len(rest) < 1 {
			usage()
		}
		value, rest = rest[0], rest[1:]
	}
	_ = sub.Parse(rest)

	ctx, cancel := context.WithTimeout(context.Background(), 4**timeout)
	defer cancel()
	c, err := client.New(ctx, client.Config{Seeds: strings.Split(*seeds, ","), Timeout: *timeout})
	if err != nil {
		fail(err)
	}
	defer c.Close()

	switch cmd {
	case "set":
		v, err := c.Set(ctx, key, []byte(value), *ttl)
		if err != nil {
			fail(err)
		}
		fmt.Printf("OK version=%d/%s\n", v.T, v.Node)
	case "get":
		get := c.Get
		if *quorum {
			get = c.GetQuorum
		}
		r, err := get(ctx, key)
		if err != nil {
			fail(err)
		}
		if !r.Found {
			fmt.Println("(not found)")
			os.Exit(1)
		}
		fmt.Println(string(r.Value))
		ttlInfo := "none"
		if r.TTL > 0 {
			ttlInfo = r.TTL.String()
		}
		fmt.Fprintf(os.Stderr, "version=%d/%s ttl=%s\n", r.Version.T, r.Version.Node, ttlInfo)
	case "del":
		if err := c.Delete(ctx, key); err != nil {
			fail(err)
		}
		fmt.Println("OK")
	case "owners":
		fmt.Println(strings.Join(c.Owners(key), " "))
	default:
		usage()
	}
}

func fail(err error) {
	var re *client.Error
	if errors.As(err, &re) {
		fmt.Fprintf(os.Stderr, "error [%s]: %s\n", re.Code, re.Msg)
	} else {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	os.Exit(1)
}
