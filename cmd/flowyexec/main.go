package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/bioflowy/flowy-exec/job"
)

func waitForExit(wf *job.Workflow, status_ch chan job.Event) {
	for {
		ev := <-status_ch
		json_str, err := json.Marshal(ev)
		if err == nil {
			fmt.Fprintln(os.Stderr, string(json_str))
		}
		if wf.GetStatus().IsFinished() {
			return
		}
	}
}
func main() {
	f, err := os.OpenFile("../../testdata/s3_upload.json", os.O_RDONLY, 0)
	if err != nil {
		log.Fatal(err)
		return
	}
	Workflow, err := job.LoadWorkflow(f)
	if err != nil {
		log.Fatal(err)
		return
	}
	status_ch := make(chan job.Event)
	go Workflow.ExecuteWorkflow(status_ch)
	waitForExit(Workflow, status_ch)
}
