package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	redshiftdatasetannotator "github.com/mashiike/redshift-data-set-annotator"
)

var version = "current"

func init() {
	redshiftdatasetannotator.Version = version
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer cancel()
	if err := redshiftdatasetannotator.RunCLI(ctx, os.Args[1:]); err != nil {
		log.Fatalf("[error] %v", err)
	}
}
