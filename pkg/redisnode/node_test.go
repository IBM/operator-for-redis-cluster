package redisnode

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/IBM/operator-for-redis-cluster/pkg/config"
	"github.com/IBM/operator-for-redis-cluster/pkg/redis/fake/admin"
)

func TestUpdateNodeConfigFile(t *testing.T) {
	tt := []struct {
		name              string
		maxMemory         uint64
		podRequestLimit   string
		additionalConfigs []string
		expectedConfig    string
	}{
		{
			name:      "with additional configs",
			maxMemory: 1048576,
			additionalConfigs: []string{
				`requirepass "hello world"
tls-session-caching no
`,
				`repl-timeout 60
replica-priority 100
`,
			},
			expectedConfig: `include /redis-conf/redis.conf
port 1234
cluster-enabled yes
maxmemory 1048576
maxmemory-policy allkeys-lru
bind 0.0.0.0
cluster-config-file /redis-data/node.conf
dir /redis-data
cluster-node-timeout 321`,
		},
		{
			name:            "with max memory not set",
			maxMemory:       0,
			podRequestLimit: "10000",
			additionalConfigs: []string{
				`requirepass "hello world"
tls-session-caching no
`,
				`repl-timeout 60
replica-priority 100
`,
			},
			expectedConfig: `include /redis-conf/redis.conf
port 1234
cluster-enabled yes
maxmemory 7000
maxmemory-policy allkeys-lru
bind 0.0.0.0
cluster-config-file /redis-data/node.conf
dir /redis-data
cluster-node-timeout 321`,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {

			redisConfDir, _ := os.MkdirTemp("", "redisconf")
			redisConfFile, createerr := os.Create(filepath.Join(redisConfDir, "redisconfig.conf"))
			if createerr != nil {
				t.Errorf("Couldn' t create temporary config file: %v", createerr)
			}
			defer os.RemoveAll(redisConfDir)
			redisConfFile.Close()

			podInfoTempDir, _ := os.MkdirTemp("", "pod-info-test")
			memLimitFile, err := os.Create(filepath.Join(podInfoTempDir, "mem-limit"))
			if err != nil {
				t.Errorf("Couldn' t create temporary config file: %v", err)
			}
			defer os.RemoveAll(podInfoTempDir)
			_, err = memLimitFile.Write([]byte(tc.podRequestLimit))
			if err != nil {
				t.Errorf("Couldn't write to temporary config file: %v", err)
			}
			memLimitFile.Close()

			var additionalConfigFileNames []string
			additionalConfDir, _ := os.MkdirTemp("", "additional-redisconf")
			for i, additionalConfigContent := range tc.additionalConfigs {
				configFile, err := os.Create(filepath.Join(additionalConfDir, "additional-redisconf-"+strconv.Itoa(i)+".conf"))
				if err != nil {
					t.Errorf("Couldn' t create temporary config file: %v", createerr)
				}
				additionalConfigFileNames = append(additionalConfigFileNames, configFile.Name())
				_, err = configFile.Write([]byte(additionalConfigContent))
				if err != nil {
					t.Errorf("Couldn't write to temporary config file: %v", err)
				}
				defer os.RemoveAll(redisConfDir)
				configFile.Close()
			}

			a := admin.NewFakeAdmin()
			c := Config{
				Redis: config.Redis{
					ServerPort:          "1234",
					MaxMemory:           tc.maxMemory,
					MaxMemoryPolicy:     "allkeys-lru",
					PodMemLimitFilePath: memLimitFile.Name(),
					ClusterNodeTimeout:  321,
					ConfigFileName:      redisConfFile.Name(),
					ConfigFiles:         additionalConfigFileNames,
				},
			}

			node := NewNode(&c, a)
			defer node.Clear()
			err = node.UpdateNodeConfigFile()
			if err != nil {
				t.Errorf("Unexpected error while updating config file: %v", err)
			}

			includePart := ""
			for _, fileName := range additionalConfigFileNames {
				includePart += "\n" + "include " + fileName
			}
			includePart += "\n"
			// checking file content
			content, _ := os.ReadFile(redisConfFile.Name())
			var expected = tc.expectedConfig + includePart
			if expected != string(content) {
				t.Errorf("Wrong file content, expected '%s', got '%s'", expected, string(content))
			}
		})
	}
}

func TestAdminCommands(t *testing.T) {
	a := admin.NewFakeAdmin()
	ctx := context.Background()
	c := Config{
		Redis: config.Redis{ServerPort: "1234"},
	}

	node := NewNode(&c, a)
	defer node.Clear()

	// all methods below simply call the fake admin, test currently only improves coverage
	err := node.InitRedisCluster(ctx, "1.1.1.1")
	if err != nil {
		t.Errorf("InitRedisCluster failed: %s", err)
	}
	err = node.AttachNodeToCluster(ctx, "1.1.1.1")
	if err != nil {
		t.Errorf("AttachNodeToCluster failed: %s", err)
	}
	err = node.ForgetNode(ctx)
	if err != nil {
		t.Errorf("ForgetNode failed: %s", err)
	}
	err = node.StartFailover(ctx)
	if err != nil {
		t.Errorf("StartFailover failed: %s", err)
	}
}
