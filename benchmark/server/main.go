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
	"flag"
	"fmt"
	"github.com/apache/dubbo-go/common/extension"
	"github.com/apache/dubbo-go/common/logger"
	_ "github.com/apache/dubbo-go/common/proxy/proxy_factory"
	"github.com/apache/dubbo-go/config"
	_ "github.com/apache/dubbo-go/filter/filter_impl"
	_ "github.com/apache/dubbo-go/protocol/dubbo3"
	_ "github.com/apache/dubbo-go/registry/protocol"
	_ "github.com/apache/dubbo-go/registry/zookeeper"
	"github.com/dubbogo/triple/benchmark/server/pkg"
	ts_call "github.com/dubbogo/triple/internal/syscall"
	_ "github.com/dubbogo/triple/pkg/triple"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

const (
	userName                      = "username"
	password                      = "password"
	loopbackAddress               = "127.0.0.1"
	referenceTestPath             = "com.test.Path"
	referenceTestPathDistinct     = "com.test.Path1"
	testInterfaceName             = "testService"
	testProtocol                  = "testprotocol"
	testSuiteMethodExpectedString = "interface {}"
)

var (
	testName = flag.String("test_name", "server", "Name of the test used for creating profiles.")
	survivalTimeout = int(3 * time.Second)
)

func main() {

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
	cpuBeg := ts_call.GetCPUTime()

	config.SetProviderService(pkg.NewGreeterProvider())
	config.Load()


	logger.Info("config.Load")


	extension.AddCustomShutdownCallback(func() {
		logger.Info("con12312312fig.Load")
		time.Sleep(time.Second * 10)
	})

	signals := make(chan os.Signal, 1)
	// It is not possible to block SIGKILL or syscall.SIGSTOP
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		sig := <-signals
		logger.Infof("get signal %s", sig.String())
		switch sig {
		case syscall.SIGHUP:
			// reload()
			logger.Infof("2222get signal %s", sig.String())

			cpu := time.Duration(ts_call.GetCPUTime() - cpuBeg)
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
		default:
			logger.Infof("1111get signal %s", sig.String())

			cpu := time.Duration(ts_call.GetCPUTime() - cpuBeg)
			logger.Infof("1444get signal %s", sig.String())

			pprof.StopCPUProfile()
			logger.Infof("13333get signal %s", sig.String())

			mf, err := os.Create("/tmp/" + *testName + ".mem")
			if err != nil {
				logger.Error("Failed to create file: %v", err)
			}
			logger.Infof("555 signal %s", sig.String())
			defer mf.Close()
			runtime.GC() // materialize all statistics
			if err := pprof.WriteHeapProfile(mf); err != nil {
				logger.Error("Failed to write memory profile: %v", err)
			}
			fmt.Println("Server CPU utilization:", cpu)
			fmt.Println("Server CPU profile:", cf.Name())
			fmt.Println("Server Mem Profile:", mf.Name())

			time.Sleep(time.Second * 5)
			time.AfterFunc(time.Duration(survivalTimeout), func() {
				logger.Warnf("app exit now by force...")
				os.Exit(1)
			})

			// The program exits normally or timeout forcibly exits.
			fmt.Println("provider app exit now...")
			return
		}
	}
}
