package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	exitCode := flag.Int("exitcode", 0, "exitCode")
	flag.Parse()
	args := flag.Args()
	f, err := os.OpenFile(args[0], os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}
	defer f.Close()
	for i := 0; i < 10; i++ {
		fmt.Fprintf(f, "this is test%d\n", i)
	}
	os.Exit(*exitCode)
}
