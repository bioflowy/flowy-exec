package main

import (
	"log"
	"os"

	"github.com/bioflowy/flowy-exec/job"
)

func waitForExit(status_ch chan job.Event) {
	for {
		ev := <-status_ch
		switch ev.GetEventType() {
		case job.WorkflowEvents:
			we := ev.(*job.WorkflowEvent)
			if we.Status == job.Successed {
				println("finished")
				return
			}
		}
	}
}
func main() {
	f, err := os.OpenFile("../../testdata/s3_download.json", os.O_RDONLY, 0)
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
	waitForExit(status_ch)
}
