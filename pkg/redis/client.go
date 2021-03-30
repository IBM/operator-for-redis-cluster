package redis

import (
	"github.com/mediocregopher/radix/v3"
	"strings"
	"time"
)
// ClientInterface redis client interface
type ClientInterface interface {
	// Close closes the connection.
	Close() error

	// DoCmd calls the given Redis command and retrieves a result.
	DoCmd(rcv interface{}, cmd, key string, args ...interface{}) error

	// NumActiveConnections returns the number of active connections
	NumActiveConnections() int
}

// Client structure representing a client connection to redis
type Client struct {
	commandsMapping map[string]string
	client          radix.Client
}

// NewClient build a client connection and connect to a redis address
func NewClient(addr string, cnxTimeout time.Duration, commandsMapping map[string]string) (*Client, error) {
	var err error
	c := &Client{
		commandsMapping: commandsMapping,
	}
	c.client, err = radix.Dial("tcp", addr, radix.DialConnectTimeout(cnxTimeout))
	return c, err
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.client.Close()
}

// Cmd calls the given Redis command.
func (c *Client) DoCmd(rcv interface{}, cmd, key string, args ...interface{}) error {
	return c.client.Do(radix.FlatCmd(rcv, c.getCommand(cmd), key, args...))
}

// getCommand return the command name after applying rename-command
func (c *Client) getCommand(cmd string) string {
	upperCmd := strings.ToUpper(cmd)
	if renamed, found := c.commandsMapping[upperCmd]; found {
		return renamed
	}
	return upperCmd
}
