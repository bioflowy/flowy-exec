package workflow

import (
	"io"

	"github.com/sirupsen/logrus"
)

type PipeHandler struct {
	output Output
	inputs []Input
	Status JobStatus
}

func (p *PipeHandler) addInput(input Input) {
	p.inputs = append(p.inputs, input)
}

func CreateHandlers(jobs []Job) []*PipeHandler {
	handlers := make([]*PipeHandler, 0)
	m := make(map[string]*PipeHandler)
	for _, job := range jobs {
		outputs := job.GetOutputs()
		for _, output := range outputs {
			handler := &PipeHandler{
				output: output,
			}
			m[output.Key()] = handler
			handlers = append(handlers, handler)
		}
		for _, input := range job.GetInputs() {
			h := m[input.Key()]
			h.addInput(input)
		}
	}
	return handlers
}
func (p *PipeHandler) AbortAll() {
	p.output.Abort()
	p.AbortWriters()
}
func (p *PipeHandler) AbortWriters() {
	for _, input := range p.inputs {
		if input != nil {
			input.Abort()
		}
	}
}
func (p *PipeHandler) checkWriters(writers []io.WriteCloser) bool {
	if len(writers) == 0 {
		return false
	}
	closedall := true
	for _, writer := range writers {
		if writer != nil {
			closedall = false
		}
	}
	if closedall {
		p.AbortAll()
	}
	return closedall
}
func (p *PipeHandler) Init() {
}

func (p *PipeHandler) Handle() {
	writers := make([]io.WriteCloser, len(p.inputs))
	reader, err := p.output.GetReader()
	if err != nil {
		logrus.WithError(err).Warn("Cannot get reader")
		p.AbortAll()
		return
	}
	defer reader.Close()
	for idx, input := range p.inputs {
		writer, err := input.GetWriter()
		if err == nil {
			writers[idx] = writer
			defer writer.Close()
		} else {
			logrus.WithError(err).Warn("Cannot get writer")
		}
	}
	if p.checkWriters(writers) {
		return
	}
	p.Status = Running
	buf := make([]byte, 32*1024)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			for idx, writer := range writers {
				if writer != nil {
					writed, err := writer.Write(buf[:n])
					if err == nil && n != writed {
						if n != writed {
							err = io.ErrShortWrite
						}

					}
					if err != nil {
						writers[idx].Close()
						writers[idx] = nil
						if p.checkWriters(writers) {
							return
						}
					}
				}
			}
		}
		if err != nil {
			if err != io.EOF {
				logrus.WithError(err).Warn("error while reading or writing")
				p.AbortAll()
			}
			logrus.Info("reading is finished")
			break
		}
	}
}
func (p *PipeHandler) Finished() {
	if p.output.IsFailed() {
		// If an job fails,next step job status must be aborted even if the job is successed.
		for _, input := range p.inputs {
			input.Abort()
		}
	}
	p.output.Clear()
	for _, input := range p.inputs {
		input.Clear()
	}
}
