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
	"errors"
	"flag"
	"fmt"
	"github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/dubbogo/triple/benchmark/protobuf"
	"github.com/dubbogo/triple/internal/syscall"
	"github.com/dubbogo/triple/pkg/triple"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
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
	port     = flag.String("port", "50051", "Localhost port to listen on.")
	testName = flag.String("test_name", "", "Name of the test used for creating profiles.")

)

type GreeterProvider struct {
	*protobuf.GreeterProviderBase
}

func NewGreeterProvider() *GreeterProvider {
	return &GreeterProvider{
		GreeterProviderBase: &protobuf.GreeterProviderBase{},
	}
}

func (g *GreeterProvider) SayHello(ctx context.Context, req *protobuf.HelloRequest) (reply *protobuf.HelloReply, err error) {
	fmt.Printf("req: %v", req)
	fmt.Println(ctx.Value("tri-req-id"))
	return nil, errors.New("rpc call error")
	//return &protobuf.HelloReply{Message: "this is message from reply"}, nil
}

func (g *GreeterProvider) Reference() string {
	return "GrpcGreeterImpl"
}


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

	methods := []string{"SayHello"}
	params := url.Values{}
	params.Set("key", "value")
	url := common.NewURLWithOptions(common.WithPath("GrpcGreeterImpl"),
		common.WithUsername(userName),
		common.WithPassword(password),
		common.WithProtocol("dubbo3"),
		common.WithIp(loopbackAddress),
		common.WithPort(*port),
		common.WithMethods(methods),
		common.WithParams(params),
		common.WithParamsValue("key2", "value2"))
	service := &GreeterProvider{}


	

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