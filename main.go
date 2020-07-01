package main

import (
	"flag"
	"fmt"
	"github.com/mkabilov/pg2ch/pkg/pg2ch_init"
	"log"
	"os"
	"runtime"

	"github.com/mkabilov/pg2ch/pkg/config"
	"github.com/mkabilov/pg2ch/pkg/replicator"
)

var (
	configFile = flag.String("config", "config.yaml", "path to the config file")
	//generateChDDL = flag.Bool("generate-ch-ddl", false, "generates clickhouse's tables ddl")
	initConf = flag.String("init", "", "path to your init config file")
	dryRun   = flag.Bool("dry-run", false, "init only print to stdout")
	quite    = flag.Bool("q", false, "do not print statements")
	Version  = "synyi-1.2.2"

	GoVersion = runtime.Version()
)

func buildInfo() string {
	return fmt.Sprintf("Postgresql to Clickhouse replicator %s  go version %s", Version, GoVersion)
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n", buildInfo())
		fmt.Fprintf(os.Stderr, "\nUsage:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *configFile == "" {
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	if initConf != nil && *initConf != "" {
		defer func() {
			err := recover()
			if err != nil {
				log.Println(err)
			}
		}()
		pg2ch_init.Pg2chInit(*initConf, *dryRun, *quite)
	} else {
		cfg, err := config.New(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not load config: %v\n", err)
			os.Exit(1)
		}
		repl := replicator.New(*cfg)
		if err := repl.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "could not start: %v\n", err)
			os.Exit(1)
		}
	}
}
