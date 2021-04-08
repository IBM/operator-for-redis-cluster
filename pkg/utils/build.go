package utils

import (
	"fmt"
	"time"
)

//BUILDTIME should be populated by at build time: -ldflags "-w -X github.com/TheWeatherCompany/icm-redis-operator/pkg/utils.BUILDTIME=${DATE}
//with for example DATE=$(shell date +%Y-%m-%d/%H:%M:%S )   (pay attention not to use space!)
var BUILDTIME string

//TAG should be populated by at build time: -ldflags "-w -X github.com/TheWeatherCompany/icm-redis-operator/pkg/utils.TAG=${TAG}
//with for example TAG=$(shell git tag|tail -1)
var TAG string

//COMMIT should be populated by at build time: -ldflags "-w -X github.com/TheWeatherCompany/icm-redis-operator/pkg/utils.COMMIT=${COMMIT}
//with for example COMMIT=$(shell git rev-parse HEAD)
var COMMIT string

// BuildInfos returns builds information
func BuildInfos() {
	fmt.Println("Program started at: " + time.Now().String())
	fmt.Println("BUILDTIME=" + BUILDTIME)
	fmt.Println("TAG=" + TAG)
	fmt.Println("COMMIT=" + COMMIT)
}
