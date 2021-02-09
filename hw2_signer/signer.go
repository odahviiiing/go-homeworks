package main

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	thNum = 6
)

type MultiHashIndexedResult struct {
	index  int
	result string
}

var inputDataCount uint32 = 0
var outputDataCount uint32 = 0
var combineResultsCount uint32 = 0
var combineResultsSlice []string
var controlChannel = make(chan interface{})
var controlJobsCount = 2
var channelOverHeadCount = 1
var mu = &sync.Mutex{}
var globalStart = time.Now()

var secondControlJob = func(in, out chan interface{}) {
	for input := range in {
		//fmt.Println("data recieved in firstControlJob: ", input)
		atomic.AddUint32(&inputDataCount, 1)
		//fmt.Println("in cnt from secondControlJob: ", atomic.LoadUint32(&inputDataCount))

		out <- input
		end := time.Since(globalStart)
		fmt.Println("second control done. time: ", end)
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
		//fmt.Println("data recieved in preLastControlJob")
		//fmt.Println("in count: ", atomic.LoadUint32(&inputDataCount))
		//fmt.Println("out count :", atomic.LoadUint32(&outputDataCount))

		//atomic.AddUint32(&outputDataCount, 1)
		out <- input

		if isNeedCloseControlChannel() {
			controlChannel <- struct{}{}
		}
		runtime.Gosched()
	}
}

func SingleHash(in, out chan interface{}) {
	for currVal := range in {
		start := time.Now()
		data := fmt.Sprintf("%v", currVal)
		//inputDataCount++

		crcChan := make(chan string)
		crcMd5Chan := make(chan string)
		md5Chan := make(chan string)

		//md5Res := DataSignerMd5(data)

		go func(out chan<- string) {
			out <- DataSignerMd5(data)
			runtime.Gosched()
		}(md5Chan)

		go func(out chan<- string, in <-chan string) {
			out <- DataSignerCrc32(<-in)
			runtime.Gosched()
		}(crcMd5Chan, md5Chan)

		go func(out chan<- string, data string) {
			out <- DataSignerCrc32(data)
			runtime.Gosched()
		}(crcChan, data)


		//crc := DataSignerCrc32(data)
		//crcMd5 := DataSignerCrc32(md5Res)

		// return
		result := <-crcChan + "~" + <-crcMd5Chan

		end := time.Since(start)
		fmt.Println("SingleHash done. time: ", end)
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
	for currVal := range in {
		start := time.Now()
		data := fmt.Sprintf("%v", currVal)
		result := ""
		resChan := make(chan MultiHashIndexedResult, thNum)

		for i := 0; i < thNum; i++ {
			//result += DataSignerCrc32(strconv.Itoa(i) + data)
			go multiHashWorker(i, data, resChan)
		}

		var results [thNum]string
		for i := 0; i < thNum; i++ {
			res := <-resChan
			results[res.index] = res.result
		}
		close(resChan)
		//
		for _, value := range results {
			result += value
		}


		out <- result
		end := time.Since(start)
		fmt.Println("MultiHash done. time: ", end)
		runtime.Gosched()
	}
}

func CombineResults(in, out chan interface{}) {
	for currVal := range in {
		data, ok := currVal.(string)
		if !ok {
			panic("cant convert result data to string")
		}

		combineResultsSlice = append(combineResultsSlice, data)
		//atomic.AddUint32(&combineResultsCount, 1)
		inCnt := atomic.LoadUint32(&inputDataCount)
		//fmt.Println("comb cnt: ", len(combineResultsSlice))
		//fmt.Println("in cnt: ", inCnt)
		if uint32(len(combineResultsSlice)) < inCnt {
			runtime.Gosched()
			continue
		}
		//if combineResultsCount == 3 {
		//wg := &sync.WaitGroup{}
		//wg.Add(1)
		//go func() {
		//	sort.Strings(combineResultsSlice)
		//	wg.Done()
		//}()
		//runtime.Gosched()
		//wg.Wait()
		sort.Strings(combineResultsSlice)
		var result interface{} = strings.Join(combineResultsSlice, "_")
		fmt.Printf("combine result: %s\n", strings.Join(combineResultsSlice, "_"))
		out <- result
		//close(out)
		//}

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
	globalStart = time.Now()
	mu.Unlock()
}

func ExecutePipeline(jobs ...job) {
	runtime.GOMAXPROCS(0)
	//fmt.Println("in ExecutePipeline")
	jobs = injectControlJobs(jobs)
	jobsCount := len(jobs)
	channelsCount := jobsCount + channelOverHeadCount

	var channels []chan interface{}
	for i := 0; i < channelsCount; i++ {
		channels = append(channels, make(chan interface{}))
	}
	//fmt.Println(len(channels))

	//fmt.Println("in ExecutePipeline jobs length: ", jobsCount)
	for i := 0; i < jobsCount; i++ {
		//fmt.Println("in ExecutePipeline loop jobs")
		//fmt.Printf("job[%d] in(i): %d\tout(i+1): %d\n", i, i, i+1)
		go jobs[i](channels[i], channels[i+1])
		runtime.Gosched()
	}

	//fmt.Println("in ExecutePipeline after loop")

	LOOP:
		for {
			select {
			case <-controlChannel:
				fmt.Println("close channels")
				resetCounters()
				closeChannels(channels)
				break LOOP
			default:
				runtime.Gosched()
				continue
			}
		}
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
