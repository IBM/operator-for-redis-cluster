package admin

import (
	"context"

	"github.com/IBM/operator-for-redis-cluster/pkg/redis"
)

// Connections fake redis connection handler, do nothing
type Connections struct {
	clients map[string]redis.ClientInterface
}

// Close used to close all possible resources instantiate by the Connections
func (cnx *Connections) Close() {
}

// Add connect to the given address and
// register the client connection to the map
func (cnx *Connections) Add(ctx context.Context, addr string) error {
	return nil
}

// Remove disconnect and remove the client connection from the map
func (cnx *Connections) Remove(addr string) {
}

// Get returns a client connection for the given adress,
// connects if the connection is not in the map yet
func (cnx *Connections) Get(ctx context.Context, addr string) (redis.ClientInterface, error) {
	return nil, nil
}

// GetRandom returns a client connection to a random node of the client map
func (cnx *Connections) GetRandom() (redis.ClientInterface, error) {
	return nil, nil
}

// GetDifferentFrom returns random a client connection different from given address
func (cnx *Connections) GetDifferentFrom(addr string) (redis.ClientInterface, error) {
	return nil, nil
}

// GetAll returns a map of all clients per address
func (cnx *Connections) GetAll() map[string]redis.ClientInterface {
	return cnx.clients
}

// GetSelected returns a map of all clients per address
func (cnx *Connections) GetSelected(addrs []string) map[string]redis.ClientInterface {
	return cnx.clients
}

// Reconnect force a reconnection on the given address
// is the address is not part of the map, act like Add
func (cnx *Connections) Reconnect(ctx context.Context, addr string) error {
	return nil
}

// AddAll connect to the given list of addresses and
// register them in the map
// fail silently
func (cnx *Connections) AddAll(ctx context.Context, addrs []string) {
}

// ReplaceAll clear the pool and re-populate it with new connections
// fail silently
func (cnx *Connections) ReplaceAll(ctx context.Context, addrs []string) {
}

// Reset close all connections and clear the connection map
func (cnx *Connections) Reset() {
}

// ValidateResp checks if the redis resp is empty and will attempt to reconnect on connection error.
// In case of error, customize the error, log it and return it.
func (cnx *Connections) ValidateResp(ctx context.Context, resp interface{}, err error, addr, errMessage string) error {
	return nil
}
