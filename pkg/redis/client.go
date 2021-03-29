package redis

import (
	"github.com/mediocregopher/radix/v3"
	"github.com/mediocregopher/radix/v3/resp/resp2"
	"strings"
	"time"
)
// ClientInterface redis client interface
type ClientInterface interface {
	// Close closes the connection.
	Close() error

	// Cmd calls the given Redis command.
	Cmd(cmd string, args ...interface{}) error

	// PipeAppend adds the given call to the pipeline queue.
	// Use PipeResp() to read the response.
	PipeAppend(cmd string, args ...interface{})

	// PipeResp returns the reply for the next request in the pipeline queue. Err
	// with ErrPipelineEmpty is returned if the pipeline queue is empty.
	PipeResp() resp2.Any

	// PipeClear clears the contents of the current pipeline queue, both commands
	// queued by PipeAppend which have yet to be sent and responses which have yet
	// to be retrieved through PipeResp. The first returned int will be the number
	// of pending commands dropped, the second will be the number of pending
	// responses dropped
	PipeClear() (int, int)

	// ReadResp will read a Resp off of the connection without sending anything
	// first (useful after you've sent a SUSBSCRIBE command). This will block until
	// a reply is received or the timeout is reached (returning the IOErr). You can
	// use IsTimeout to check if the Resp is due to a Timeout
	//
	// Note: this is a more low-level function, you really shouldn't have to
	// actually use it unless you're writing your own pub/sub code
	ReadResp() resp2.Any
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
	return c.client.Do()
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
