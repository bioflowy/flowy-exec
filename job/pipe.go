package job

import (
	"io"
	"os"
	"syscall"
)

type PipeInfo struct {
	Name string
	Path string
}
type PipeEvent struct {
	cause   error
	handler *PipeHandler
	message string
}

func (p *PipeInfo) CreateReader() (io.ReadCloser, error) {
	return os.OpenFile(p.Path, os.O_RDONLY, 0)
}
func (p *PipeInfo) CreateWriter() (io.WriteCloser, error) {
	return os.OpenFile(p.Path, os.O_WRONLY, 0)
}
func (p *PipeInfo) Init() error {
	syscall.Mkfifo(p.Path, 0600)
	return nil
}

type ReaderCreator interface {
	Init() error
	CreateReader() (io.ReadCloser, error)
}
type WriterCreator interface {
	Init() error
	CreateWriter() (io.WriteCloser, error)
}
type PipeHandler struct {
	Input   ReaderCreator
	Outputs []WriterCreator
}

func (p *PipeHandler) Init() error {
	err := p.Input.Init()
	if err != nil {
		return nil
	}
	for _, output := range p.Outputs {
		err := output.Init()
		if err != nil {
			return nil
		}
	}
	return nil
}

func CreateHandlers(jobs []*Job) []*PipeHandler {
	m := make(map[string]*PipeHandler)
	for _, job := range jobs {
		for _, output := range job.Outputs {
			switch output.Type {
			case Pipe:
				m[output.Name] = &PipeHandler{
					Input: &PipeInfo{
						Name: output.Name,
						Path: output.Path,
					},
				}
			case ObjectStrage:
				m[output.Name] = &PipeHandler{
					Input: &ObjectStoreReaderCreator{
						Bucket: output.Bucket,
						Key:    output.Key,
						Path:   output.Path,
					},
				}
			}
		}
		for _, input := range job.Inputs {
			h := m[input.Name]
			var out WriterCreator
			switch input.Type {
			case Pipe:
				out = &PipeInfo{
					Name: input.Name,
					Path: input.Path,
				}
			case ObjectStrage:
				out = &ObjectStoreWriterCreator{
					Bucket: input.Bucket,
					Key:    input.Key,
				}
			}

			h.Outputs = append(m[input.Name].Outputs, out)
		}
	}
	rlst := make([]*PipeHandler, 0, len(m))
	for _, v := range m {
		rlst = append(rlst, v)
	}
	return rlst
}

func (p *PipeHandler) Handle(ch chan Event) {
	inf, err := p.Input.CreateReader()
	if err != nil {
		ch <- &PipeEvent{
			handler: p,
			cause:   err,
			message: err.Error(),
		}
		return
	}
	defer inf.Close()
	ws := make([]io.WriteCloser, len(p.Outputs))
	for i, output := range p.Outputs {
		wrf, err := output.CreateWriter()
		if err != nil {
			ch <- &PipeEvent{
				handler: p,
				cause:   err,
				message: err.Error(),
			}
			return
		}
		defer wrf.Close()
		ws[i] = wrf
	}
	ws2 := make([]io.Writer, len(ws))
	for i, w := range ws {
		ws2[i] = w
	}
	w := io.MultiWriter(ws2...)
	io.Copy(w, inf)
	for _, w := range ws {
		w.Close()
	}
	ch <- &PipeEvent{
		handler: p,
		cause:   nil,
		message: "finished",
	}
}
