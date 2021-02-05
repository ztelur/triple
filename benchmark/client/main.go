/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/config"
	"github.com/dubbogo/triple/benchmark/protobuf"
	"github.com/dubbogo/triple/benchmark/stats"
	"github.com/dubbogo/triple/internal/syscall"
	"github.com/dubbogo/triple/pkg/triple"
	"github.com/apache/dubbo-go/common/logger"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

const (
	userName        = "username"
	password        = "password"
	loopbackAddress = "127.0.0.1"
	referenceTestPath             = "com.test.Path"
	referenceTestPathDistinct     = "com.test.Path1"
	testInterfaceName             = "testService"
	testProtocol                  = "testprotocol"
	testSuiteMethodExpectedString = "interface {}"
)


var (
	port      = flag.String("port", "50051", "Localhost port to connect to.")
	numRPC    = flag.Int("r", 1, "The number of concurrent RPCs on each connection.")
	numConn   = flag.Int("c", 1, "The number of parallel connections.")
	warmupDur = flag.Int("w", 10, "Warm-up duration in seconds")
	duration  = flag.Int("d", 60, "Benchmark duration in seconds")
	rqSize    = flag.Int("req", 1, "Request message size in bytes.")
	rspSize   = flag.Int("resp", 1, "Response message size in bytes.")
	rpcType   = flag.String("rpc_type", "unary",
		`Configure different client rpc type. Valid options are:
		   unary;
		   streaming.`)
	testName = flag.String("test_name", "", "Name of the test used for creating profiles.")
	wg       sync.WaitGroup
	hopts    = stats.HistogramOptions{
		NumBuckets:   2495,
		GrowthFactor: .01,
	}
	mu    sync.Mutex
	hists []*stats.Histogram

)

var grpcGreeterImpl = new(GrpcGreeterImpl)

func init() {
	config.SetConsumerService(grpcGreeterImpl)
}


func main()  {

	methods := []string{"SayHello"}
	params := url.Values{}
	params.Set("bean.name", "GrpcGreeterImpl")
	url := common.NewURLWithOptions(common.WithPath("GrpcGreeterImpl"),
		common.WithUsername(userName),
		common.WithPassword(password),
		common.WithProtocol("dubbo3"),
		common.WithIp(loopbackAddress),
		common.WithPort(*port),
		common.WithMethods(methods),
		common.WithParams(params),
		common.WithParamsValue("key2", "value2"))


	connectCtx, connectCancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer connectCancel()


	req := &protobuf.HelloRequest{Name: "zzz"}

	in := make([]reflect.Value, 0, 16)
	in = append(in, reflect.ValueOf(connectCtx))
	in = append(in, reflect.ValueOf(req))


	ctl := buildClients(url, connectCtx)

	warmDeadline := time.Now().Add(time.Duration(*warmupDur) * time.Second)
	endDeadline := warmDeadline.Add(time.Duration(*duration) * time.Second)
	cf, err := os.Create("/tmp/" + *testName + ".cpu")
	if err != nil {
		logger.Error("Error creating file: %v", err)
	}
	defer cf.Close()
	pprof.StartCPUProfile(cf)
	cpuBeg := syscall.GetCPUTime()

	for _, ct := range ctl {
		runWithClient(ct, "SayHello",  in, warmDeadline, endDeadline)
	}

	wg.Wait()
	cpu := time.Duration(syscall.GetCPUTime() - cpuBeg)
	pprof.StopCPUProfile()
	mf, err := os.Create("/tmp/" + *testName + ".mem")
	if err != nil {
		logger.Error("Error creating file: %v", err)
	}
	defer mf.Close()
	runtime.GC() // materialize all statistics
	if err := pprof.WriteHeapProfile(mf); err != nil {
		logger.Error("Error writing memory profile: %v", err)
	}
	hist := stats.NewHistogram(hopts)
	for _, h := range hists {
		hist.Merge(h)
	}
	parseHist(hist)
	fmt.Println("Client CPU utilization:", cpu)
	fmt.Println("Client CPU profile:", cf.Name())
	fmt.Println("Client Mem Profile:", mf.Name())

}

func runWithClient(ct *triple.TripleClient, methodName string,  param []reflect.Value, warmDeadline, endDeadline time.Time) {
	for i := 0; i < *numRPC; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			hist := stats.NewHistogram(hopts)
			for {
				start := time.Now()
				if start.After(endDeadline) {
					mu.Lock()
					hists = append(hists, hist)
					mu.Unlock()
					return
				}
				method := ct.Invoker.MethodByName(methodName)
				res := method.Call(param)
				// check err
				if !res[1].IsNil() {
					logger.Error("call failed: %v", res[1])
				}
				elapsed := time.Since(start)
				if start.After(warmDeadline) {
					hist.Add(elapsed.Nanoseconds())
				}
			}
		}()
	}
}
func buildClients(url *common.URL, ctx context.Context) []*triple.TripleClient {
	ccs := make([]*triple.TripleClient, *numConn)

	for i := range ccs {
		client, err := triple.NewTripleClient(url)
		if err != nil {
			logger.Error("client init failed: %s", i)
		}
		ccs[i] = client
	}
	return ccs
}

func parseHist(hist *stats.Histogram) {
	fmt.Println("qps:", float64(hist.Count)/float64(*duration))
	fmt.Printf("Latency: (50/90/99 %%ile): %v/%v/%v\n",
		time.Duration(median(.5, hist)),
		time.Duration(median(.9, hist)),
		time.Duration(median(.99, hist)))
}

func median(percentile float64, h *stats.Histogram) int64 {
	need := int64(float64(h.Count) * percentile)
	have := int64(0)
	for _, bucket := range h.Buckets {
		count := bucket.Count
		if have+count >= need {
			percent := float64(need-have) / float64(count)
			return int64((1.0-percent)*bucket.LowBound + percent*bucket.LowBound*(1.0+hopts.GrowthFactor))
		}
		have += bucket.Count
	}
	panic("should have found a bound")
}
