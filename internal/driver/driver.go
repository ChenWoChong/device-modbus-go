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
	"golang.org/x/sync/errgroup"
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

	properties := protocols[ProtocolTCP]
	if properties == nil {
		properties = make(models.ProtocolProperties)
	}
	driver.Logger.Debugf("Read command properties: %v \n", properties)

	// 判断是否启用重连
	if enableReconnet, ok := properties[Reconnect_After_Consecutive_Tries]; ok && enableReconnet.(bool) {
		for i := 0; i < Default_Retry_Connect_Count; i++ {
			err = deviceClient.OpenConnection()
			if err == nil {
				break
			} else if i == Default_Retry_Connect_Count-1 {
				return responses, err
			}
		}
	} else {
		err = deviceClient.OpenConnection()
		if err != nil {
			driver.Logger.Infof("Read command OpenConnection failed. err:%v \n", err)
			return responses, err
		}
	}

	defer func() { _ = deviceClient.CloseConnection() }()
	bys, _ := json.Marshal(reqs)

	fmt.Println("\n---------------------------------------------")
	fmt.Println("reqs:", string(bys))
	fmt.Println("---------------------------------------------")

	//  Zero based address
	var isZerobasedAddressing bool
	if isZero, ok := properties[Zero_Based_Addressing]; ok {
		isZerobasedAddressing = isZero.(bool)
	} else {
		isZerobasedAddressing = false
	}

	reqMap := make(map[string][]int) // primaryTable --> req-index-id list:
	for i, req := range reqs {

		// Sort by primaryTable
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

	eg := new(errgroup.Group)

	for primaryTable := range reqMap {

		var quantity float64
		properties := protocols[ProtocolTCP]
		if properties != nil {
			maxPerReq := PrimaryTableMaxMap[primaryTable]
			if value, ok := properties[maxPerReq]; ok {
				quantity = value.(float64)
			}
		}

		reqIdxList := reqMap[primaryTable]
		if isConcurrent, ok := properties[Concurrent_Request]; ok && isConcurrent.(bool) {
			eg.Go(func() error {
				return handleReadCommandRequests(deviceClient, protocols, reqs, responses, reqIdxList, uint16(quantity), isZerobasedAddressing)
			})
		} else {
			if err := handleReadCommandRequests(deviceClient, protocols, reqs, responses, reqIdxList, uint16(quantity), isZerobasedAddressing); err != nil {
				driver.Logger.Infof("Read commands failed. Cmd:%v err:%v \n", primaryTable, err)
				return responses, err
			}
		}
	}

	if err := eg.Wait(); err != nil {
		driver.Logger.Infof("Read commands failed.  err:%v \n", err)
		return responses, err
	}

	return responses, nil
}

func handleReadCommandRequests(deviceClient DeviceClient, protocols map[string]models.ProtocolProperties, reqs []sdkModel.CommandRequest, responses []*sdkModel.CommandValue, reqIdxList []int, quantity uint16, isZerobasedAddressing bool) error {

	var allowSpanGaps bool // allowSpanGaps
	var maxRetryCount int  // maxRetryCount
	properties := protocols[ProtocolTCP]
	if properties != nil {
		if allow, ok := properties[Allow_Span_Gaps]; ok {
			allowSpanGaps = allow.(bool)
		}
		if count, ok := properties[Max_Retry_Count]; ok {
			maxRetryCount = int(count.(float64))
		}
		if maxRetryCount == 0 {
			maxRetryCount = 1
		}
	}

	// order reqIdxList by StartingAddress form min to max .
	fmt.Printf("reqIdxList: %v", reqIdxList)
	sort.Slice(reqIdxList, func(i, j int) bool {
		reqA, reqB := &reqs[reqIdxList[i]], &reqs[reqIdxList[j]]
		cmdA, _ := createCommandInfo(reqA)
		cmdB, _ := createCommandInfo(reqB)
		return cmdA.StartingAddress < cmdB.StartingAddress
	})
	fmt.Printf(",sorted reqIdxList: %v\n", reqIdxList)

	// create commandInfoList By sorted reqIdxList
	commandInfoList := make([]*CommandInfo, len(reqIdxList))
	for i, idx := range reqIdxList {
		cmd, err := createCommandInfo(&reqs[idx])
		if err != nil {
			return err
		}

		// Not zero, StartingAddress add one
		if !isZerobasedAddressing {
			cmd.StartingAddress++
		}

		commandInfoList[i] = cmd
	}

	minIndex, maxIndex := 0, len(commandInfoList)-1

	// getLength: get the length from the min-req-startingAddress to max-req-startingAddress
	getLength := func(minIndex, maxIndex int) (length uint16) {
		return (commandInfoList[maxIndex].StartingAddress - commandInfoList[minIndex].StartingAddress) + commandInfoList[maxIndex].Length
	}

	// getResponeFromCommandInfoList: get Respone of CommandInfoList
	getResponeFromCommandInfoList := func(minIndex, maxIndex int) (err error) {

		length := getLength(minIndex, maxIndex)
		newCommandInfo := &CommandInfo{
			PrimaryTable:    commandInfoList[minIndex].PrimaryTable,
			StartingAddress: commandInfoList[minIndex].StartingAddress,
			// how many register need to read
			Length: length,
		}

		fmt.Printf("CommandInfo : %+v\n", *newCommandInfo)

		// maxRetryCount
		var response []byte
		for i := 0; i < maxRetryCount; i++ {
			response, err = deviceClient.GetValue(newCommandInfo)
			if err == nil {
				break
			} else if i == maxRetryCount-1 {
				return err
			}
		}

		for i := minIndex; i <= maxIndex; i++ {
			req := reqs[reqIdxList[i]]
			commandInfo := commandInfoList[i]
			startingAddress := commandInfo.StartingAddress - newCommandInfo.StartingAddress
			left, right := startingAddress*2, (startingAddress+commandInfo.Length)*2
			if int(left) > len(response) || int(right) > len(response) {
				return err
			}
			sliceResponse := response[left:right]
			driver.Logger.Debug(fmt.Sprintf("Modbus client Request's CommandInfo %v", commandInfo))
			driver.Logger.Debug(fmt.Sprintf("Modbus client Response's results %v", sliceResponse))

			result, err := TransformDataBytesToResult(&req, sliceResponse, commandInfo)
			if err != nil {
				return err
			} else {
				driver.Logger.Infof("Read command finished. Cmd:%v, %v \n", req.DeviceResourceName, result)
			}

			responses[reqIdxList[i]] = result
		}

		return nil
	}

	// If quantity is zero or allowSpanGaps, merge all to one command
	if quantity == 0 && allowSpanGaps {
		return getResponeFromCommandInfoList(minIndex, maxIndex)
	}

	// Check whether per length is greater than quantity.
	for i := range commandInfoList {
		if quantity > 0 && commandInfoList[i].Length > quantity {
			return errors.NewCommonEdgeX(errors.KindCommunicationError, fmt.Sprintf("per length is greater than quantity %d > %d", commandInfoList[i].Length, quantity), nil)
		}
	}

	isSpanGaps := func(left, right int) bool {
		if left < 0 || left > right || right >= len(commandInfoList) {
			return false
		}

		leftCmd, rightCmd := commandInfoList[left], commandInfoList[right]

		// only one command, or allow spanGap
		if left == right || allowSpanGaps {
			return true
		}

		if leftCmd.StartingAddress+leftCmd.Length == rightCmd.StartingAddress {
			return true
		}

		return false
	}

	allowQuantity := func(length, quantity uint16) bool {
		if quantity == 0 {
			return true
		}
		return length <= quantity
	}

	// Else if quantity > 0,  split commands within quantity.
	for right := 0; right < len(commandInfoList); right++ {
		left := right
		curLength := getLength(left, right)

		for isSpanGaps(left, right) && allowQuantity(curLength, quantity) && right+1 < len(commandInfoList) {
			curLength = getLength(left, right+1)
			right++
		}

		// 如果curLength > quantity, 则需要减掉回退一个
		if !allowQuantity(curLength, quantity) || !isSpanGaps(left, right) {
			right--
		}

		if err := getResponeFromCommandInfoList(left, right); err != nil {
			return err
		}
	}

	return nil
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
