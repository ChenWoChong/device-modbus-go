// -*- Mode: Go; indent-tabs-mode: t -*-
//
// Copyright (C) 2018-2021 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	sdkModel "github.com/edgexfoundry/device-sdk-go/v3/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v3/clients/logger"
)

func init() {
	driver = new(Driver)
	driver.Logger = logger.NewMockClient()
}

func TestLockAddressWithAddressCountLimit(t *testing.T) {
	address := "/dev/USB0tty"
	driver.addressMap = make(map[string]chan bool)
	driver.workingAddressCount = make(map[string]int)
	driver.workingAddressCount[address] = concurrentCommandLimit

	err := driver.lockAddress(address)

	if err == nil || !strings.Contains(err.Error(), "High-frequency command execution") {
		t.Errorf("Unexpect result, it should return high-frequency error, %v", err)
	}
}

func TestLockAddressWithAddressCountUnderLimit(t *testing.T) {
	address := "/dev/USB0tty"
	driver.addressMap = make(map[string]chan bool)
	driver.workingAddressCount = make(map[string]int)
	driver.workingAddressCount[address] = concurrentCommandLimit - 1

	err := driver.lockAddress(address)

	if err != nil {
		t.Errorf("Unexpect result, address should be lock successfully, %v", err)
	}
}

func TestCreateCommandInfo(t *testing.T) {
	req := sdkModel.CommandRequest{
		DeviceResourceName: "OperationMode",
		// Attributes is a key/value map to represent the attributes of the Device Resource
		Attributes: make(map[string]interface{}),
		// Type is the data type of the Device Resource
		Type: "Int16",
	}
	expected := &CommandInfo{
		PrimaryTable:    "HOLDING_REGISTERS",
		StartingAddress: 1,
		ValueType:       "Int16",
		// how many register need to read
		Length:     1,
		IsByteSwap: false,
		IsWordSwap: false,
		RawType:    "Int16",
	}
	req.Attributes["primaryTable"] = "HOLDING_REGISTERS"
	req.Attributes["startingAddress"] = 1

	commandInfo, err := createCommandInfo(&req)
	require.NoError(t, err)

	assert.Equal(t, expected, commandInfo)
}

    