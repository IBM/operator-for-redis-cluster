package main

import (
	"fmt"
	"regexp"
	"strings"

	rapi "github.com/IBM/operator-for-redis-cluster/api/v1alpha1"
	kapiv1 "k8s.io/api/core/v1"
)

var (
	dbKeysRegex       = regexp.MustCompile("^([a-zA-Z0-9]+):keys=([0-9]+)")
	noCommandOverride []string
)

type PodInfo struct {
	name       string
	ip         string
	node       string
	id         string
	zone       string
	slots      string
	role       string
	usedMemory string
	maxMemory  string
	keys       string
	pod        *kapiv1.Pod
}

func NewPodInfo(pod *kapiv1.Pod, node rapi.RedisClusterNode) *PodInfo {
	return &PodInfo{
		name:  pod.Name,
		ip:    pod.Status.PodIP,
		node:  pod.Status.HostIP,
		id:    node.ID,
		slots: strings.Join(node.Slots, " "),
		zone:  node.Zone,
		role:  string(node.Role),
		pod:   pod,
	}
}

func NewPodInfoFromClusterStatus(pod *kapiv1.Pod) *PodInfo {
	return &PodInfo{
		name:  pod.Name,
		ip:    pod.Status.PodIP,
		node:  pod.Status.HostIP,
		id:    "",
		slots: "",
		zone:  "",
		role:  "",
		pod:   pod,
	}
}

func (pi *PodInfo) populateKeyStats(lines []string) {
	for i := len(lines) - 1; i > 0; i-- {
		if strings.Contains(lines[i], "# Keyspace") {
			for j := i + 1; j < len(lines); j++ {
				match := dbKeysRegex.FindStringSubmatch(lines[j])
				if len(match) == 3 {
					if pi.keys != "" {
						pi.keys += ","
					}
					pi.keys += fmt.Sprintf("%s=%s", match[1], match[2])
				}
			}
			break
		}
	}
}

func (pi *PodInfo) populateMemoryStats(lines []string) {
	for _, line := range lines {
		if pi.usedMemory == "" && strings.Contains(line, "used_memory_human:") {
			pi.usedMemory = getInfoValueString(line)
		} else if pi.maxMemory == "" && strings.Contains(line, "maxmemory_human:") {
			pi.maxMemory = getInfoValueString(line)
		} else if pi.role == "" && strings.Contains(line, "role:") {
			pi.role = getInfoValueString(line)
			if pi.role == "master" {
				pi.role = string(rapi.RedisClusterNodeRolePrimary)
			} else if pi.role == "slave" {
				pi.role = string(rapi.RedisClusterNodeRoleReplica)
			}
		}
		if pi.usedMemory != "" && pi.maxMemory != "" && pi.role != "" {
			break
		}
	}
}

func (pi *PodInfo) populateRoleChar() string {
	if pi.role == string(rapi.RedisClusterNodeRolePrimary) {
		return "+ "
	} else if pi.role == string(rapi.RedisClusterNodeRoleReplica) {
		return "| "
	}
	return "? "
}
