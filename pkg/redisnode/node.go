package redisnode

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/IBM/operator-for-redis-cluster/pkg/config"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
	"github.com/golang/glog"
)

const (
	dataFolder = "/redis-data"
)

// Node struct that represent a RedisNodeWrapper
type Node struct {
	config     *Config
	Addr       string
	RedisAdmin redis.AdminInterface
}

// NewNode return an instance of a Node
func NewNode(c *Config, admin redis.AdminInterface) *Node {
	n := &Node{
		config:     c,
		RedisAdmin: admin,
		Addr:       net.JoinHostPort(c.Redis.ServerIP, c.Redis.ServerPort),
	}

	return n
}

// Clear clear possible initialized resources
func (n *Node) Clear() {
	if n.RedisAdmin != nil {
		n.RedisAdmin.Close()
	}

}

// UpdateNodeConfigFile update the redis config file with node information: ip, port
func (n *Node) UpdateNodeConfigFile() error {
	if n.config.Redis.ConfigFileName != config.RedisConfigFileDefault {
		if err := n.addSettingInConfigFile("include " + config.RedisConfigFileDefault); err != nil {
			return err
		}
	}

	if err := n.addSettingInConfigFile("port " + n.config.Redis.ServerPort); err != nil {
		return err
	}

	if err := n.addSettingInConfigFile("cluster-enabled yes"); err != nil {
		return err
	}

	if n.config.Redis.MaxMemory > 0 {
		if err := n.addSettingInConfigFile(fmt.Sprintf("maxmemory %d", n.config.Redis.MaxMemory)); err != nil {
			return err
		}
	} else {
		maxMemory, err := n.getConfig("maxmemory")
		if err != nil {
			return err
		}
		if maxMemory == "" { //set only if the user didn't set it in additional configs
			memLimitBytes, err := getPodMemoryLimit(n.config.Redis.PodMemLimitFilePath)
			if err != nil {
				return err
			}
			// use 70% of pod memory limit as maxmemory
			err = n.addSettingInConfigFile(fmt.Sprintf("maxmemory %d", uint64(float64(memLimitBytes)*0.7)))
			if err != nil {
				return err
			}
		}
	}
	if n.config.Redis.MaxMemoryPolicy != config.RedisMaxMemoryPolicyDefault {
		if err := n.addSettingInConfigFile(fmt.Sprintf("maxmemory-policy %s", n.config.Redis.MaxMemoryPolicy)); err != nil {
			return err
		}
	}

	if err := n.addSettingInConfigFile("bind 0.0.0.0"); err != nil {
		return err
	}

	if err := n.addSettingInConfigFile("cluster-config-file /redis-data/node.conf"); err != nil {
		return err
	}

	if err := n.addSettingInConfigFile("dir /redis-data"); err != nil {
		return err
	}

	if err := n.addSettingInConfigFile("cluster-node-timeout " + strconv.Itoa(n.config.Redis.ClusterNodeTimeout)); err != nil {
		return err
	}
	if n.config.Redis.GetRenameCommandsFile() != "" {

		if err := n.addSettingInConfigFile("include " + n.config.Redis.GetRenameCommandsFile()); err != nil {
			return err
		}
	}

	// Add at the end any configuration file provided as redis-node's arguments.
	for _, file := range n.config.Redis.ConfigFiles {
		if err := n.addSettingInConfigFile("include " + file); err != nil {
			return err
		}
	}

	return nil
}

// addSettingInConfigFile add a line in the redis configuration file
func (n *Node) addSettingInConfigFile(line string) error {
	f, err := os.OpenFile(n.config.Redis.ConfigFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("unable to set '%s' in config file, openfile error %s err:%v", line, n.config.Redis.ConfigFileName, err)
	}

	defer f.Close()

	_, err = f.WriteString(line + "\n")
	if err != nil {
		return fmt.Errorf("unable to set '%s' in config file, err:%v", line, err)
	}
	return nil
}

// getConfig reads a config from redis config file
func (n *Node) getConfig(name string) (string, error) {
	configFiles := append(n.config.Redis.ConfigFiles, n.config.Redis.ConfigFileName)
	for _, configFile := range configFiles {
		f, err := os.OpenFile(configFile, os.O_RDONLY, 0600)
		if err != nil {
			if os.IsNotExist(err) {
				glog.Warningf("Config %s not found", configFile)
				continue
			}
			return "", fmt.Errorf("unable to read a config from file, openfile error %s err:%v", configFile, err)
		}

		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if len(line) == 0 {
				continue
			}
			if string(line[0]) == "#" { //it's a comment
				continue
			}

			strIndex := strings.Index(line, name)
			if strIndex == -1 {
				continue
			}

			return strings.TrimSpace(line[strIndex:]), nil
		}
	}

	return "", nil
}

// InitRedisCluster used to init a redis cluster with the current node
func (n *Node) InitRedisCluster(ctx context.Context, addr string) error {
	glog.Info("InitRedis Cluster... starting")
	err := n.RedisAdmin.InitRedisCluster(ctx, addr)
	glog.Info("InitRedis Cluster... done")

	return err
}

// AttachNodeToCluster used to attach the current node to a redis cluster
func (n *Node) AttachNodeToCluster(ctx context.Context, addr string) error {
	glog.Info("AttachNodeToCluster... starting")

	return n.RedisAdmin.AttachNodeToCluster(ctx, addr)
}

// ForgetNode used to remove a node for a cluster
func (n *Node) ForgetNode(ctx context.Context) error {
	glog.Info("ForgetNode... starting")

	return n.RedisAdmin.ForgetNodeByAddr(ctx, n.Addr)
}

// StartFailover start Failover if needed
func (n *Node) StartFailover(ctx context.Context) error {
	glog.Info("StartFailover... starting")

	return n.RedisAdmin.StartFailover(ctx, n.Addr)
}

// ClearDataFolder completely erase all files in the /data folder
func (n *Node) ClearDataFolder() error {
	return clearFolder(dataFolder)
}

// ClearFolder remover all files and folder in a given folder
func clearFolder(folder string) error {
	glog.Infof("Clearing '%s' folder... ", folder)
	d, err := os.Open(folder)
	if err != nil {
		glog.Infof("Cannot access folder %s: %v", folder, err)
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		glog.Infof("Cannot read files in %s: %v", folder, err)
		return err
	}
	for _, name := range names {
		file := filepath.Join(folder, name)
		glog.V(2).Infof("Removing %s", file)
		err = os.RemoveAll(file)
		if err != nil {
			glog.Errorf("Error while removing %s: %v", file, err)
			return err
		}
	}
	return nil
}

// getPodMemoryLimit return pod's memory limit in bytes
func getPodMemoryLimit(memFilePath string) (uint64, error) {
	f, err := os.Open(memFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	memLimitStr, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}

	if len(memLimitStr) == 0 { //mem limit is not set
		return 0, nil
	}

	return strconv.ParseUint(string(memLimitStr), 10, strconv.IntSize)
}
