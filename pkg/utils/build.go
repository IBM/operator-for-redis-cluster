package utils

import (
	"fmt"
	"time"
)

var BUILDTIME string
var OPERATOR_VERSION string
var REDIS_VERSION string
var REVISION string

// BuildInfos returns builds information
func BuildInfos() {
	fmt.Println("Program started at: " + time.Now().String())
	if BUILDTIME != "" {
		fmt.Println("BUILDTIME=" + BUILDTIME)
	}
	if OPERATOR_VERSION != "" {
		fmt.Println("OPERATOR_VERSION=" + OPERATOR_VERSION)
	}
	if REDIS_VERSION != "" {
		fmt.Println("REDIS_VERSION=" + REDIS_VERSION)
	}
	if REVISION != "" {
		fmt.Println("REVISION=" + REVISION)
	}
}
