package utils

import (
	"fmt"
	"time"
)

var BUILDTIME string
var VERSION string
var REVISION string

// BuildInfos returns builds information
func BuildInfos() {
	fmt.Println("Program started at: " + time.Now().String())
	fmt.Println("BUILDTIME=" + BUILDTIME)
	fmt.Println("VERSION=" + VERSION)
	fmt.Println("REVISION=" + REVISION)
}
