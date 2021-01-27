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
	"github.com/dubbogo/triple/internal/syscall"
	"github.com/dubbogo/triple/pkg/triple"
	"github.com/apache/dubbo-go/common/logger"
	"time"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
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


type testService struct {
}

func (s *testService) Method1(ctx context.Context, args testService, rsp *struct{}) error {
	return nil
}
func (s *testService) Method2(ctx context.Context, args []interface{}) (testService, error) {
	return testService{}, nil
}
func (s *testService) Method3(ctx context.Context, args []interface{}, rsp *struct{}) {
}
func (s *testService) Method4(ctx context.Context, args []interface{}, rsp *struct{}) *testService {
	return nil
}
func (s *testService) Reference() string {
	return referenceTestPath
}

var (
	port     = flag.String("port", "50051", "Localhost port to listen on.")
	testName = flag.String("test_name", "", "Name of the test used for creating profiles.")

)

func main()  {


	flag.Parse()
	if *testName == "" {
		logger.Error("test name not set")
	}

	cf, err := os.Create("/tmp/" + *testName + ".cpu")
	if err != nil {
		logger.Error("Failed to create file: %v", err)
	}
	defer cf.Close()
	pprof.StartCPUProfile(cf)
	cpuBeg := syscall.GetCPUTime()

	methods := []string{"Methodone,methodtwo"}
	params := url.Values{}
	params.Set("key", "value")
	url := common.NewURLWithOptions(common.WithPath("com.test.Service"),
		common.WithUsername(userName),
		common.WithPassword(password),
		common.WithProtocol("testprotocol"),
		common.WithIp(loopbackAddress),
		common.WithPort(*port),
		common.WithMethods(methods),
		common.WithParams(params),
		common.WithParamsValue("key2", "value2"))
	service := &testService{}
	tripleServer := triple.NewTripleServer(url, service)
	tripleServer.Start()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	cpu := time.Duration(syscall.GetCPUTime() - cpuBeg)

	tripleServer.Stop()

	pprof.StopCPUProfile()
	mf, err := os.Create("/tmp/" + *testName + ".mem")
	if err != nil {
		logger.Error("Failed to create file: %v", err)
	}
	defer mf.Close()
	runtime.GC() // materialize all statistics
	if err := pprof.WriteHeapProfile(mf); err != nil {
		logger.Error("Failed to write memory profile: %v", err)
	}
	fmt.Println("Server CPU utilization:", cpu)
	fmt.Println("Server CPU profile:", cf.Name())
	fmt.Println("Server Mem Profile:", mf.Name())
	
}