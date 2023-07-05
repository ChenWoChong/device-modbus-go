// -*- Mode: Go; indent-tabs-mode: t -*-
//
// Copyright (C) 2018-2023 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

// Package driver is used to execute device-sdk's commands
package driver

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/edgexfoundry/device-sdk-go/v3/pkg/interfaces"
	sdkModel "github.com/edgexfoundry/device-sdk-go/v3/pkg/models"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/errors"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/models"
)

var once sync.Once
var driver *Driver

type Driver struct {
	Logger              logger.LoggingClient
	AsyncCh             chan<- *sdkModel.AsyncValues
	mutex               sync.Mutex
	addressMap          map[string]chan bool
	workingAddressCount map[string]int
	stopped             bool
}

var concurrentCommandLimit = 100

func (d *Driver) DisconnectDevice(deviceName string, protocols map[string]models.ProtocolProperties) error {
	d.Logger.Warn("Driver's DisconnectDevice function didn't implement")
	return nil
}

// lockAddress mark address is unavailable because real device handle one request at a time
func (d *Driver) lockAddress(address string) error {
	if d.stopped {
		return fmt.Errorf("service attempts to stop and unable to handle new request")
	}
	d.mutex.Lock()
	lock, ok := d.addressMap[address]
	if !ok {
		lock = make(chan bool, 1)
		d.addressMap[address] = lock
	}

	// workingAddressCount used to check high-frequency command execution to avoid goroutine block
	count, ok := d.workingAddressCount[address]
	if !ok {
		d.workingAddressCount[address] = 1
	} else if count >= concurrentCommandLimit {
		d.mutex.Unlock()
		errorMessage := fmt.Sprintf("High-frequency command execution. There are %v commands with the same address in the queue", concurrentCommandLimit)
		d.Logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	} else {
		d.workingAddressCount[address] = count + 1
	}

	d.mutex.Unlock()
	lock <- true

	return nil
}

// unlockAddress remove token after command finish
func (d *Driver) unlockAddress(address string) {
	d.mutex.Lock()
	lock := d.addressMap[address]
	d.workingAddressCount[address] = d.workingAddressCount[address] - 1
	d.mutex.Unlock()
	<-lock
}

// lockableAddress return the lockable address according to the protocol
func (d *Driver) lockableAddress(info *ConnectionInfo) string {
	var address string
	if info.Protocol == ProtocolTCP {
		address = fmt.Sprintf("%s:%d", info.Address, info.Port)
	} else {
		address = info.Address
	}
	return address
}

/*
	1. reqs 根据 primary table 分类合并，获取数据
	2. 返回时保持数据原始的格式
*/

func (d *Driver) HandleReadCommands(deviceName string, protocols map[string]models.ProtocolProperties, reqs []sdkModel.CommandRequest) (responses []*sdkModel.CommandValue, err error) {
	connectionInfo, err := createConnectionInfo(protocols)
	if err != nil {
		driver.Logger.Errorf("Fail to create read command connection info. err:%v \n", err)
		return responses, err
	}

	err = d.lockAddress(d.lockableAddress(connectionInfo))
	if err != nil {
		return responses, err
	}
	defer d.unlockAddress(d.lockableAddress(connectionInfo))

	responses = make([]*sdkModel.CommandValue, len(reqs))
	var deviceClient DeviceClient

	// create device client and open connection
	deviceClient, err = NewDeviceClient(connectionInfo)
	if err != nil {
		driver.Logger.Infof("Read command NewDeviceClient failed. err:%v \n", err)
		return responses, err
	}

	err = deviceClient.OpenConnection()
	if err != nil {
		driver.Logger.Infof("Read command OpenConnection failed. err:%v \n", err)
		return responses, err
	}

	defer func() { _ = deviceClient.CloseConnection() }()
	bys, _ := json.Marshal(reqs)

	fmt.Println("\n---------------------------------------------\n")
	fmt.Println("reqs:", string(bys))
	fmt.Println("---------------------------------------------")

	reqMap := make(map[string][]int) // primaryTable --> req-index-id list:
	for i, req := range reqs {

		if _, ok := req.Attributes[PRIMARY_TABLE]; !ok {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("attribute %s not exists", PRIMARY_TABLE), nil)
		}
		primaryTable := fmt.Sprintf("%v", req.Attributes[PRIMARY_TABLE])
		primaryTable = strings.ToUpper(primaryTable)

		if reqMap[primaryTable] == nil {
			reqMap[primaryTable] = make([]int, 0)
		}

		reqMap[primaryTable] = append(reqMap[primaryTable], i)
	}

	sortByStartingAddr := func(reqIDList []int) []int {
		sort.Slice(reqIDList, func(i, j int) bool {

			reqA, reqB := reqs[reqIDList[i]], reqs[reqIDList[j]]
			startingAddrA, _ := getReqStartingAddr(reqA)
			startingAddrB, _ := getReqStartingAddr(reqB)

			return startingAddrA < startingAddrB
		})

		return reqIDList
	}

	for primaryTable, reqIdxList := range reqMap {
		sortedReqIdxList := sortByStartingAddr(reqIdxList)
		fmt.Println(primaryTable, sortedReqIdxList)
		responses, err = handleReadCommandRequests(primaryTable, deviceClient, reqs, responses, sortedReqIdxList)
		if err != nil {
			driver.Logger.Infof("Read commands failed. Cmd:%v err:%v \n", primaryTable, err)
			return responses, err
		}
	}

	// old
	// handle command requests
	// for i, req := range reqs {
	// 	// if req.Attributes["stringRegisterSize"] != 0 {
	// 	// 	req.Attributes["stringRegisterSize"] = 4
	// 	// }
	// 	bys, _ := json.Marshal(req)
	// 	fmt.Println("\nreq: ", i, string(bys))
	// 	res, err := handleReadCommandRequest(deviceClient, req)
	// 	if err != nil {
	// 		driver.Logger.Infof("Read command failed. Cmd:%v err:%v \n", req.DeviceResourceName, err)
	// 		return responses, err
	// 	}

	// 	responses[i] = res
	// }

	return responses, nil
}

func handleReadCommandRequests(primaryTable string, deviceClient DeviceClient, reqs []sdkModel.CommandRequest, responses []*sdkModel.CommandValue, sortedReqIdxList []int) ([]*sdkModel.CommandValue, error) {
	var response []byte
	var result = &sdkModel.CommandValue{}
	var err error

	commandInfo, err := createCommandInfoForReqs(primaryTable, reqs, sortedReqIdxList)
	if err != nil {
		return nil, err
	}

	fmt.Printf("commandInfo : %+v", *commandInfo)

	response, err = deviceClient.GetValue(commandInfo)
	if err != nil {
		return nil, err
	}

	for _, idx := range sortedReqIdxList {
		req := reqs[idx]
		reqCmd, err := createCommandInfo(&req)
		if err != nil {
			return nil, err
		}

		left := reqCmd.StartingAddress - commandInfo.StartingAddress
		right := left + reqCmd.Length

		result, err = TransformDataBytesToResult(&req, response[left:right], reqCmd)

		if err != nil {
			return nil, err
		} else {
			driver.Logger.Infof("Read command finished. Cmd:%v, %v \n", req.DeviceResourceName, result)
		}

		responses[idx] = result
	}

	return responses, nil
}

func handleReadCommandRequest(deviceClient DeviceClient, req sdkModel.CommandRequest) (*sdkModel.CommandValue, error) {
	var response []byte
	var result = &sdkModel.CommandValue{}
	var err error

	commandInfo, err := createCommandInfo(&req)
	if err != nil {
		return nil, err
	}

	response, err = deviceClient.GetValue(commandInfo)
	if err != nil {
		return result, err
	}

	result, err = TransformDataBytesToResult(&req, response, commandInfo)

	if err != nil {
		return result, err
	} else {
		driver.Logger.Infof("Read command finished. Cmd:%v, %v \n", req.DeviceResourceName, result)
	}

	return result, nil
}

func (d *Driver) HandleWriteCommands(deviceName string, protocols map[string]models.ProtocolProperties, reqs []sdkModel.CommandRequest, params []*sdkModel.CommandValue) error {
	connectionInfo, err := createConnectionInfo(protocols)
	if err != nil {
		driver.Logger.Errorf("Fail to create write command connection info. err:%v \n", err)
		return err
	}

	err = d.lockAddress(d.lockableAddress(connectionInfo))
	if err != nil {
		return err
	}
	defer d.unlockAddress(d.lockableAddress(connectionInfo))

	var deviceClient DeviceClient

	// create device client and open connection
	deviceClient, err = NewDeviceClient(connectionInfo)
	if err != nil {
		return err
	}

	err = deviceClient.OpenConnection()
	if err != nil {
		return err
	}

	defer func() { _ = deviceClient.CloseConnection() }()

	// handle command requests
	for i, req := range reqs {
		err = handleWriteCommandRequest(deviceClient, req, params[i])
		if err != nil {
			d.Logger.Error(err.Error())
			break
		}
	}

	return err
}

func handleWriteCommandRequest(deviceClient DeviceClient, req sdkModel.CommandRequest, param *sdkModel.CommandValue) error {
	var err error

	commandInfo, err := createCommandInfo(&req)
	if err != nil {
		return err
	}

	dataBytes, err := TransformCommandValueToDataBytes(commandInfo, param)
	if err != nil {
		return fmt.Errorf("transform command value failed, err: %v", err)
	}

	err = deviceClient.SetValue(commandInfo, dataBytes)
	if err != nil {
		return fmt.Errorf("handle write command request failed, err: %v", err)
	}

	driver.Logger.Infof("Write command finished. Cmd:%v \n", req.DeviceResourceName)
	return nil
}

func (d *Driver) Initialize(sdk interfaces.DeviceServiceSDK) error {
	d.Logger = sdk.LoggingClient()
	d.AsyncCh = sdk.AsyncValuesChannel()
	d.addressMap = make(map[string]chan bool)
	d.workingAddressCount = make(map[string]int)
	return nil
}

func (d *Driver) Start() error {
	return nil
}

func (d *Driver) Stop(force bool) error {
	d.stopped = true
	if !force {
		d.waitAllCommandsToFinish()
	}
	for _, locked := range d.addressMap {
		close(locked)
	}
	return nil
}

// waitAllCommandsToFinish used to check and wait for the unfinished job
func (d *Driver) waitAllCommandsToFinish() {
loop:
	for {
		for _, count := range d.workingAddressCount {
			if count != 0 {
				// wait a moment and check again
				time.Sleep(time.Second * SERVICE_STOP_WAIT_TIME)
				continue loop
			}
		}
		break loop
	}
}

func (d *Driver) AddDevice(deviceName string, protocols map[string]models.ProtocolProperties, adminState models.AdminState) error {
	d.Logger.Debugf("Device %s is added", deviceName)
	return nil
}

func (d *Driver) UpdateDevice(deviceName string, protocols map[string]models.ProtocolProperties, adminState models.AdminState) error {
	d.Logger.Debugf("Device %s is updated", deviceName)
	return nil
}

func (d *Driver) RemoveDevice(deviceName string, protocols map[string]models.ProtocolProperties) error {
	d.Logger.Debugf("Device %s is removed", deviceName)
	return nil
}

func (d *Driver) Discover() error {
	return fmt.Errorf("driver's Discover function isn't implemented")
}

func (d *Driver) ValidateDevice(device models.Device) error {
	_, err := createConnectionInfo(device.Protocols)
	if err != nil {
		return fmt.Errorf("invalid protocol properties, %v", err)
	}
	return nil
}

func NewProtocolDriver() interfaces.ProtocolDriver {
	once.Do(func() {
		driver = new(Driver)
	})
	return driver
}
