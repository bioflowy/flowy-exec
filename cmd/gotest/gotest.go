package main

import "time"

type Test struct {
	name string
}

func test1(ch chan *Test){
	ch <- &Test{
		name: "this is test",
	}
}
func main()  {
	ch := make(chan *Test,10)
	go test1(ch)
	time.Sleep(1 * time.Second)
	t := <- ch
	println(t.name)
}