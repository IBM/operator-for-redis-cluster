package redis

import "github.com/TheWeatherCompany/icm-redis-operator/pkg/api/redis/v1"

// Manager regroups Function for managing a Redis Cluster
type Manager struct {
	admin *Admin
}

// NewManager builds and returns new Manager instance
func NewManager(admin *Admin) *Manager {
	return &Manager{
		admin: admin,
	}
}

// BuildClusterStatus builds and returns new instance of the RedisClusterState
func (m *Manager) BuildClusterStatus() (*v1.RedisClusterState, error) {
	status := &v1.RedisClusterState{}

	return status, nil
}
