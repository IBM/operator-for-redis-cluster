package redis

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/mediocregopher/radix/v4"
)

// ClientInterface redis client interface
type ClientInterface interface {
	// Close closes the connection.
	Close() error

	// DoCmd calls the given Redis command and retrieves a result.
	DoCmd(ctx context.Context, rcv interface{}, cmd string, args ...string) error

	// DoCmdWithRetries calls the given Redis command and retrieves a result with retires.
	DoCmdWithRetries(ctx context.Context, rcv interface{}, cmd string, args ...string) error

	// PipeAppend adds the given call to the pipeline queue.
	PipeAppend(action radix.Action)

	// PipeReset discards all Actions and resets all internal state.
	PipeReset()

	// DoPipe executes all of the commands in the pipeline.
	DoPipe(ctx context.Context) error
}

// Client structure representing a client connection to redis
type Client struct {
	commandsMapping map[string]string
	client          radix.Client
	pipeline        *radix.Pipeline
}

// NewClient build a client connection and connect to a redis address
func NewClient(ctx context.Context, addr string, cnxTimeout time.Duration, commandsMapping map[string]string) (*Client, error) {
	var err error
	c := &Client{
		commandsMapping: commandsMapping,
		pipeline:        radix.NewPipeline(),
	}
	dialer := &radix.Dialer{
		NetDialer: &net.Dialer{
			Timeout: cnxTimeout,
		},
	}
	c.client, err = dialer.Dial(ctx, "tcp", addr)
	return c, err
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.client.Close()
}

// DoCmd calls the given Redis command.
func (c *Client) DoCmd(ctx context.Context, rcv interface{}, cmd string, args ...string) error {
	return c.client.Do(ctx, radix.Cmd(rcv, c.getCommand(cmd), args...))
}

func (c *Client) delayLinear(i int, cmd string) {
	delay := defaultRetryTimeout * time.Duration(i)
	glog.Warningf("%s attempt %d/%d. Retry in %s", cmd, i, defaultRetryAttempts, delay)
	time.Sleep(delay)
}

func (c *Client) doCmdWithRetries(ctx context.Context, rcv interface{}, cmd string, i int, args ...string) error {
	if err := c.DoCmd(ctx, &rcv, cmd, args...); err != nil {
		if i = i + 1; i >= defaultRetryAttempts {
			return err
		}
		c.delayLinear(i, cmd)
		return c.doCmdWithRetries(ctx, &rcv, cmd, i, args...)
	}
	return nil
}

// DoCmdWithRetries calls the given Redis command.
func (c *Client) DoCmdWithRetries(ctx context.Context, rcv interface{}, cmd string, args ...string) error {
	return c.doCmdWithRetries(ctx, &rcv, cmd, 1, args...)
}

// PipeAppend adds the given call to the pipeline queue.
func (c *Client) PipeAppend(action radix.Action) {
	c.pipeline.Append(action)
}

// PipeReset discards all Actions and resets all internal state.
func (c *Client) PipeReset() {
	c.pipeline.Reset()
}

// DoPipe executes all the commands in the pipeline.
func (c *Client) DoPipe(ctx context.Context) error {
	return c.client.Do(ctx, c.pipeline)
}

// getCommand return the command name after applying rename-command
func (c *Client) getCommand(cmd string) string {
	upperCmd := strings.ToUpper(cmd)
	if renamed, found := c.commandsMapping[upperCmd]; found {
		return renamed
	}
	return upperCmd
}
