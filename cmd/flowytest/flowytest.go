package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/bioflowy/flowy-exec/workflow"
	"github.com/sirupsen/logrus"
)

func main() {
	timeout := flag.Int("timeout", 3, "timeout in second")
	results := flag.String("results", "results.json", "results JSON File path")
	formatter := &logrus.JSONFormatter{}
	formatter.DisableTimestamp = true
	logrus.SetFormatter(formatter)
	flag.Parse()
	args := flag.Args()
	f, err := os.OpenFile(args[0], os.O_RDONLY, 0)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
		return
	}
	wf, err := workflow.LoadWorkflow(f)
	if err != nil {
		log.Fatal(err)
		return
	}
	go func() {
		time.Sleep(time.Duration(*timeout) * time.Second)
		log.Fatal("timeout has occurred")
	}()
	status_ch := make(chan workflow.Event, 10)
	wr := wf.Execute(status_ch)
	wr.Start = nil
	wr.End = nil
	for _, r := range wr.Results {
		r.Start = nil
		r.End = nil
	}
	b, err := json.Marshal(wr)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	rf, err := os.OpenFile(*results, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
	defer rf.Close()
	rf.Write(b)

}
