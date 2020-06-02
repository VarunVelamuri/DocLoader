package main

import (
	"./docgen"
	"flag"
	"fmt"
	"os"
)

var options struct {
	bucket    string // bucket to connect
	username  string
	password  string
	kvaddress string
	numDocs   int
}

func argParse() {

	flag.StringVar(&options.bucket, "bucket", "default",
		"buckets to connect")
	flag.StringVar(&options.username, "username", "Administrator",
		"Cluster username")
	flag.StringVar(&options.password, "password", "asdasd",
		"Cluster password")
	flag.StringVar(&options.kvaddress, "kvaddress", "127.0.0.1:8091",
		"KV address")
	flag.IntVar(&options.numDocs, "numDocs", 1000000,
		"Number of documents that can be populated")
	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] <addr> \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	argParse()
	fmt.Printf("Username: %v, password: %v, numDocs: %v\n", options.username, options.password, options.numDocs)

	docgen.Init(options.username, options.password, options.kvaddress)
	docgen.PopulateDocs(options.bucket, (int64)(options.numDocs), 0)
}
