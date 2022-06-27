package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/bioflowy/flowy-exec/workflow"
	"github.com/sirupsen/logrus"
)

func main() {
	formatter := &logrus.JSONFormatter{}
	formatter.DisableTimestamp = true
	logrus.SetFormatter(formatter)
	results := flag.String("results", "results.json", "results JSON File path")
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
	status_ch := make(chan workflow.Event, 10)
	wr := wf.Execute(status_ch)
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
