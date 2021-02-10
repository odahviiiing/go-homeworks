package main

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	thNum = 6
	channelOverHeadCount = 1
)

type MultiHashIndexedResult struct {
	index  int
	result string
}

var inputDataCount uint32 = 0
var outputDataCount uint32 = 0
var combineResultsSlice []string
var controlChannel = make(chan interface{})
var mu = &sync.Mutex{}
var jobsCount int

var secondControlJob = func(in, out chan interface{}) {
	for input := range in {
		atomic.AddUint32(&inputDataCount, 1)
		out <- input
		atomic.AddUint32(&outputDataCount, 1)
		runtime.Gosched()
	}
}

func isNeedCloseControlChannel() bool {
	isMaxDataLenReached := atomic.LoadUint32(&outputDataCount) == MaxInputDataLen
	isInCntAndOutCntEquals := atomic.LoadUint32(&outputDataCount) == atomic.LoadUint32(&inputDataCount)
	isNonZeroCounts := atomic.LoadUint32(&outputDataCount) != 0 &&  atomic.LoadUint32(&inputDataCount) != 0
	return isNonZeroCounts && (isMaxDataLenReached || isInCntAndOutCntEquals)
}

var preLastControlJob = func(in, out chan interface{}) {
	for input := range in {
		out <- input

		if isNeedCloseControlChannel() {
			controlChannel <- struct{}{}
		}
		runtime.Gosched()
	}
}
func calculateSingleHash(in, out, md5Chan chan interface{}) {
	for currVal := range in {
		data := fmt.Sprintf("%v", currVal)

		crcChan, crcMd5Chan := make(chan string), make(chan string)

		go func(out chan<- string, in chan interface{}) {
			out <- DataSignerCrc32(fmt.Sprintf("%v", <-in))
			runtime.Gosched()
		}(crcMd5Chan, md5Chan)

		go func(out chan<- string, data string) {
			out <- DataSignerCrc32(data)
			runtime.Gosched()
		}(crcChan, data)

		result := <-crcChan + "~" + <-crcMd5Chan
		out <- result
		runtime.Gosched()
	}
}

func SingleHash(in, out chan interface{}) {
	input, output, md5Chan := make(chan interface{}), make(chan interface{}), make(chan interface{})
	for {
		select {
		case valueIn := <-in:
			go calculateSingleHash(input, output, md5Chan)
			input <- valueIn
			md5Chan <- DataSignerMd5(fmt.Sprintf("%v", valueIn))
			runtime.Gosched()
		case valueOut := <-output:
			out <- valueOut
			runtime.Gosched()
		}
	}
}

func multiHashWorker(index int, data string, out chan<- MultiHashIndexedResult) {
	out <- MultiHashIndexedResult{
		index:  index,
		result: DataSignerCrc32(strconv.Itoa(index) + data),
	}
	runtime.Gosched()
}

func calculateMultiHash(in, out chan interface{}) {
	for currVal := range in {
		data := fmt.Sprintf("%v", currVal)
		result := ""
		resChan := make(chan MultiHashIndexedResult, thNum)

		for i := 0; i < thNum; i++ {
			go multiHashWorker(i, data, resChan)
		}

		var results [thNum]string
		for i := 0; i < thNum; i++ {
			res := <-resChan
			results[res.index] = res.result
		}
		close(resChan)
		for _, value := range results {
			result += value
		}

		out <- result
		runtime.Gosched()
	}
}

func MultiHash(in, out chan interface{}) {
	input, output := make(chan interface{}), make(chan interface{})
	for {
		select {
		case valueIn := <-in:
			go calculateMultiHash(input, output)
			input <- valueIn
			runtime.Gosched()
		case valueOut := <-output:
			out <- valueOut
			runtime.Gosched()
		}
	}
}

func CombineResults(in, out chan interface{}) {
	for currVal := range in {
		data := fmt.Sprintf("%v", currVal)

		combineResultsSlice = append(combineResultsSlice, data)
		inCnt := atomic.LoadUint32(&inputDataCount)

		if uint32(len(combineResultsSlice)) < inCnt {
			runtime.Gosched()
			continue
		}

		sort.Strings(combineResultsSlice)
		var result interface{} = strings.Join(combineResultsSlice, "_")
		out <- result

		runtime.Gosched()
	}
}

func injectControlJobs(jobs []job) []job {
	var jobsWithControl []job
	jobsWithControl = append(jobsWithControl, jobs[0], secondControlJob)
	jobsWithControl = append(jobsWithControl, jobs[1:len(jobs)-1]...)
	jobsWithControl = append(jobsWithControl, preLastControlJob, jobs[len(jobs)-1])
	return jobsWithControl
}

func closeChannels(channels []chan interface{}) {
	for _, channel := range channels {
		close(channel)
	}
}

func resetCounters() {
	mu.Lock()
	inputDataCount = 0
	outputDataCount = 0
	mu.Unlock()
}

func launchJob(prevChan chan interface{}, jobs []job, launchCounter int) {
	if launchCounter == len(jobs) {
		return
	}
	nextChan := make(chan interface{})
	go jobs[launchCounter](prevChan, nextChan)
	launchCounter++
	launchJob(nextChan, jobs, launchCounter)
}

func pathThrough(in chan interface{}, out chan interface{}) (chan interface{}, chan interface{}) {
	nextOut := make(chan interface{})
	go func() {
		for data := range in {
			out <- data
		}
		close(out)
	}()
	return out, nextOut
}

func ExecutePipeline(jobs ...job) {
	//jobs = injectControlJobs(jobs)
	jobsCount = len(jobs)
	channelsCount := jobsCount + channelOverHeadCount

	var channels []chan interface{}
	for i := 0; i < channelsCount; i++ {
		channels = append(channels, make(chan interface{}, MaxInputDataLen))
	}
	//in := make(chan interface{})
	//launchCounter := 0
	//launchJob(in, jobs, launchCounter)
	in := make(chan interface{})
	for i := 0; i < jobsCount; i++ {
		fmt.Println("i: ", i)
		go jobs[i](pathThrough(in), )
		fmt.Println("index after: ", i)
		//close(channels[i])
		//runtime.Gosched()
	}

	//LOOP:
	//	for {
	//		select {
	//		case <-controlChannel:
	//			resetCounters()
	//			closeChannels(channels)
	//			break LOOP
	//		default:
	//			runtime.Gosched()
	//			continue
	//		}
	//	}
}
