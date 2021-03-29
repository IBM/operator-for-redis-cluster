package redis

import (
	"github.com/mediocregopher/radix/v3"
	"strings"
	"time"
)

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
func (c *Client) Cmd(rcv interface{}, cmd string, args ...string) error {
	return c.client.Do(radix.Cmd(rcv, c.getCommand(cmd), args...))
}

// PipeAppend adds the given call to the pipeline queue.
func (c *Client) PipeAppend(rcv interface{}, cmd string, args ...string) {
	// TODO: fix this pipeline call
	_ = c.client.Do(radix.Pipeline(radix.Cmd(rcv, c.getCommand(cmd), args...)))
}

// PipeResp returns the reply for the next request in the pipeline queue.
func (c *Client) PipeResp() error {
	// TODO: fix this pipeline call
	return c.client.Do(radix.Pipeline)
}

// PipeClear clears the contents of the current pipeline queue
func (c *Client) PipeClear() (int, int) {
	// TODO: fix this pipeline call
	return c.client.PipeClear()
}

// ReadResp will read a Resp off of the connection without sending anything
func (c *Client) ReadResp() error {
	// TODO: fix this read response call
	return c.client.Do(radix.Cmd())
}

// getCommand return the command name after applying rename-command
func (c *Client) getCommand(cmd string) string {
	upperCmd := strings.ToUpper(cmd)
	if renamed, found := c.commandsMapping[upperCmd]; found {
		return renamed
	}
	return upperCmd
}
