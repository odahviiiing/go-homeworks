package main

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	thNum = 6
)

type MultiHashIndexedResult struct {
	index  int
	result string
}

var inputDataCount uint32 = 0
var combineResultsCount uint32 = 0
var combineResultsSlice []string
var cancelChannel = make(chan interface{})

func SingleHash(in, out chan interface{}) {

	fmt.Println("in SingleHash")
	for currVal := range in {
		data := fmt.Sprintf("%v", currVal)
		atomic.AddUint32(&inputDataCount, 1)
		//inputDataCount++

		//crcChan := make(chan string)
		//crcMd5Chan := make(chan string)

		md5Res := DataSignerMd5(data)

		//go func(out chan<- string, data string) {
		//	out <- DataSignerCrc32(data)
		//}(crcChan, data)
		//
		//go func(out chan<- string, data string) {
		//	out <- DataSignerCrc32(data)
		//}(crcMd5Chan, md5Res)
		crc := DataSignerCrc32(data)
		crcMd5 := DataSignerCrc32(md5Res)

		// return
		result := crc + "~" + crcMd5
		out <- result
		runtime.Gosched()
	}
}

func multiHashWorker(index int, data string, out chan<- MultiHashIndexedResult) {
	out <- MultiHashIndexedResult{
		index:  index,
		result: DataSignerCrc32(strconv.Itoa(index) + data),
	}
	runtime.Gosched()
}

func MultiHash(in, out chan interface{}) {
	fmt.Println("in MultiHash")
	for currVal := range in {
		data := fmt.Sprintf("%v", currVal)
		result := ""
		//resChan := make(chan MultiHashIndexedResult, thNum)

		for i := 0; i < thNum; i++ {
			result += DataSignerCrc32(strconv.Itoa(i) + data)
			//go multiHashWorker(i, data, resChan)
		}

		//var results [thNum]string
		//for i := 0; i < thNum; i++ {
		//	res := <-resChan
		//	results[res.index] = res.result
		//}
		//close(resChan)
		//
		//for _, value := range results {
		//	result += value
		//}

		out <- result
		runtime.Gosched()
	}
}

func CombineResults(in, out chan interface{}) {
	fmt.Println("in CombineResults")
	for currVal := range in {
		data, ok := currVal.(string)
		if !ok {
			panic("cant convert result data to string")
		}

		combineResultsSlice = append(combineResultsSlice, data)
		atomic.AddUint32(&combineResultsCount, 1)

		loadedInputDataCount := atomic.LoadUint32(&inputDataCount)
		if uint32(len(combineResultsSlice)) < loadedInputDataCount {
			runtime.Gosched()
			continue
		}
		//if combineResultsCount == 3 {
		sort.Strings(combineResultsSlice)

		var result interface{} = strings.Join(combineResultsSlice, "_")
		fmt.Printf("combine result: %s\n", strings.Join(combineResultsSlice, "_"))
		out <- result
		//close(out)
		cancelChannel <- struct{}{}
		//}

		runtime.Gosched()
	}
}

func ExecutePipeline(jobs ...job) {

	//fmt.Println("in ExecutePipeline")
	jobsLength := len(jobs)
	channelsLength := jobsLength + 1

	var channels []chan interface{}
	for i := 0; i < channelsLength; i++ {
		channels = append(channels, make(chan interface{}))
	}
	//fmt.Println(len(channels))

	//fmt.Println("in ExecutePipeline jobs length: ", jobsLength)
	for i := 0; i < jobsLength; i++ {
		//fmt.Println("in ExecutePipeline loop jobs")
		//fmt.Printf("job[%d] in(i): %d\tout(i+1): %d\n", i, i, i+1)
		go jobs[i](channels[i], channels[i+1])
		runtime.Gosched()
	}

	//fmt.Println("in ExecutePipeline after loop")

	//LOOP:
	//	for {
	//		select {
	//		case <-cancelChannel:
	//			break LOOP
	//		default:
	//			runtime.Gosched()
	//			continue
	//		}
	//	}
	//	fmt.Scanln()
}

//func main() {
//	jab := []job{
//		job(func(in, out chan interface{}) {
//			fmt.Println("first")
//			inputData := []int{0, 1, 1, 2, 3, 5, 8}
//			for _, v := range inputData {
//				out <- v
//			}
//		}),
//		job(SingleHash),
//		job(MultiHash),
//		job(CombineResults),
//		job(func(in, out chan interface{}) {
//			dataRaw := <-in
//			fmt.Printf("data returned %v: ", dataRaw)
//			cancelChannel <- struct{}{}
//		}),
//	}
//	ExecutePipeline(jab...)
//
//	var ok = true
//	var recieved uint32
//	freeFlowJobs := []job{
//		job(func(in, out chan interface{}) {
//
//			fmt.Println("first")
//			out <- 1
//			fmt.Println("first data send")
//			time.Sleep(10 * time.Millisecond)
//			fmt.Println("first after timeout")
//			currRecieved := atomic.LoadUint32(&recieved)
//			fmt.Printf("value: %d\n", currRecieved)
//			// в чем тут суть
//			// если вы накапливаете значения, то пока вся функция не отрабоатет - дальше они не пойдут
//			// тут я проверяю, что счетчик увеличился в следующей функции
//			// это значит что туда дошло значение прежде чем текущая функция отработала
//			if currRecieved == 0 {
//				ok = false
//			}
//		}),
//		job(func(in, out chan interface{}) {
//			fmt.Println("second")
//			for _ = range in {
//				fmt.Println("second data recieved")
//				atomic.AddUint32(&recieved, 1)
//				cancelChannel <- struct{}{}
//			}
//		}),
//	}
//
//	ExecutePipeline(freeFlowJobs...)
//	if !ok || recieved == 0 {
//		fmt.Printf("no value free flow - dont collect them")
//	}
//	fmt.Println("test ok")
//	fmt.Scanln()
//}
