package main

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

const (
	thNum = 6
)

type MultiHashIndexedResult struct {
	index  int
	result string
}

var inputDataCount = 0
var combineResultsCount = 0
var combineResultsSlice []string

func SingleHash(in, out chan interface{}) {
	for currVal := range in {
		data := fmt.Sprintf("%v", currVal)
		inputDataCount++

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
	}
}

func CombineResults(in, out chan interface{}) {
	for currVal := range in {
		dataIn := fmt.Sprintf("%v", currVal)

		combineResultsSlice = append(combineResultsSlice, dataIn)
		//if len(combineResultsSlice) < inputDataCount {
		//	continue
		//}

		sort.Strings(combineResultsSlice)
		resutl := strings.Join(combineResultsSlice, "_")
		out <- resutl
	}
}

func ExecutePipeline(jobs ...job) {
	runtime.GOMAXPROCS(0)

	jobsLength := len(jobs)
	channelsLength := jobsLength + 1

	var channels []chan interface{}
	for i := 0; i < channelsLength; i++ {
		channels = append(channels, make(chan interface{}))
	}

	for i := 0; i < jobsLength; i++ {
		go jobs[i](channels[i], channels[i+1])
	}
}

//func main() {
//	jab := []job{
//		job(func(in, out chan interface{}) {
//			fmt.Println("first")
//			inputData := []int{0, 1}
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
//		}),
//	}
//	ExecutePipeline(jab...)
//	fmt.Scanln()
//}
