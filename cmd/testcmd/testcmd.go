package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	count := flag.Int("count", 10, "count")
	exitCode := flag.Int("exitcode", 0, "exitCode")
	input := flag.String("input", "", "input")
	flag.Parse()
	args := flag.Args()
	readtext := ""
	if input != nil && len(*input) > 0 {
		f, err := os.Open(*input)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(2)
			return
		}
		b, err := io.ReadAll(f)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			os.Exit(2)
			return
		}
		readtext = string(b)
	}
	f, err := os.OpenFile(args[0], os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprint(os.Stderr, err)
	}
	defer f.Close()
	fmt.Fprintf(f, "read %d length string\n", len(readtext))
	for i := 0; i < *count; i++ {
		fmt.Fprintf(f, "this is test%d\n", i)
	}
	os.Exit(*exitCode)
}
