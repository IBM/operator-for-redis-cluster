package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
)

// AdminConnectionsInterface interface representing the map of admin connections to redis cluster nodes
type AdminConnectionsInterface interface {
	// Add connect to the given address and
	// register the client connection to the pool
	Add(ctx context.Context, addr string) error
	// Remove disconnect and remove the client connection from the map
	Remove(addr string)
	// Get returns a client connection for the given address,
	// connects if the connection is not in the map yet
	Get(ctx context.Context, addr string) (ClientInterface, error)
	// GetRandom returns a client connection to a random node of the client map
	GetRandom() (ClientInterface, error)
	// GetDifferentFrom returns a random client connection different from given address
	GetDifferentFrom(addr string) (ClientInterface, error)
	// GetAll returns a map of all clients per address
	GetAll() map[string]ClientInterface
	//GetSelected returns a map of clients based on the input addresses
	GetSelected(addrs []string) map[string]ClientInterface
	// Reconnect force a reconnection on the given address
	// if the adress is not part of the map, act like Add
	Reconnect(ctx context.Context, addr string) error
	// AddAll connect to the given list of addresses and
	// register them in the map
	// fail silently
	AddAll(ctx context.Context, addrs []string)
	// ReplaceAll clear the map and re-populate it with new connections
	// fail silently
	ReplaceAll(ctx context.Context, addrs []string)
	// ValidateResp checks if the redis resp is empty and will attempt to reconnect on connection error.
	// In case of error, customize the error, log it and return it.
	ValidateResp(ctx context.Context, resp interface{}, err error, addr, errMessage string) error
	// Reset close all connections and clear the connection map
	Reset()
}

// AdminConnections connection map for redis cluster
// currently the admin connection is not threadSafe since it is only use in the Events thread.
type AdminConnections struct {
	clients           map[string]ClientInterface
	connectionTimeout time.Duration
	commandsMapping   map[string]string
	clientName        string
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewAdminConnections returns and instance of AdminConnectionsInterface
func NewAdminConnections(ctx context.Context, addrs []string, options *AdminOptions) AdminConnectionsInterface {
	cnx := &AdminConnections{
		clients:           make(map[string]ClientInterface),
		connectionTimeout: defaultClientTimeout,
		commandsMapping:   make(map[string]string),
		clientName:        defaultClientName,
	}
	if options != nil {
		if options.ConnectionTimeout != 0 {
			cnx.connectionTimeout = options.ConnectionTimeout
		}
		if _, err := os.Stat(options.RenameCommandsFile); err == nil {
			cnx.commandsMapping = buildCommandReplaceMapping(options.RenameCommandsFile)
		}
		cnx.clientName = options.ClientName
	}
	cnx.AddAll(ctx, addrs)
	return cnx
}

// Close used to close all possible resources instantiated by the Connections
func (cnx *AdminConnections) Close() {
	for _, c := range cnx.clients {
		c.Close()
	}
}

// Add connect to the given address and
// register the client connection to the map
func (cnx *AdminConnections) Add(ctx context.Context, addr string) error {
	_, err := cnx.Update(ctx, addr)
	return err
}

// Remove disconnect and remove the client connection from the map
func (cnx *AdminConnections) Remove(addr string) {
	if c, ok := cnx.clients[addr]; ok {
		c.Close()
		delete(cnx.clients, addr)
	}
}

// Update returns a client connection for the given adress,
// connects if the connection is not in the map yet
func (cnx *AdminConnections) Update(ctx context.Context, addr string) (ClientInterface, error) {
	// if already exist close the current connection
	if c, ok := cnx.clients[addr]; ok {
		c.Close()
	}

	c, err := cnx.connect(ctx, addr)
	if err == nil && c != nil {
		cnx.clients[addr] = c
	} else {
		glog.V(3).Infof("Cannot connect to %s ", addr)
	}
	return c, err
}

// Get returns a client connection for the given adress,
// connects if the connection is not in the map yet
func (cnx *AdminConnections) Get(ctx context.Context, addr string) (ClientInterface, error) {
	if c, ok := cnx.clients[addr]; ok {
		return c, nil
	}
	c, err := cnx.connect(ctx, addr)
	if err == nil && c != nil {
		cnx.clients[addr] = c
	}
	return c, err
}

// GetRandom returns a client connection to a random node of the client map
func (cnx *AdminConnections) GetRandom() (ClientInterface, error) {
	_, c, err := cnx.getRandomKeyClient()
	return c, err
}

// GetDifferentFrom returns random a client connection different from given address
func (cnx *AdminConnections) GetDifferentFrom(addr string) (ClientInterface, error) {
	if len(cnx.clients) == 1 {
		for a, c := range cnx.clients {
			if a != addr {
				return c, nil
			}
			return nil, errors.New(ErrNotFound)
		}
	}

	for {
		a, c, err := cnx.getRandomKeyClient()
		if err != nil {
			return nil, err
		}
		if a != addr {
			return c, nil
		}
	}
}

// GetAll returns a map of all clients per address
func (cnx *AdminConnections) GetAll() map[string]ClientInterface {
	return cnx.clients
}

// GetSelected returns a map of clients based on the input addresses
func (cnx *AdminConnections) GetSelected(addrs []string) map[string]ClientInterface {
	clientsSelected := make(map[string]ClientInterface)
	for _, addr := range addrs {
		if client, ok := cnx.clients[addr]; ok {
			clientsSelected[addr] = client
		}
	}
	return clientsSelected
}

// Reconnect force a reconnection on the given address
// if the address is not part of the map, act like Add
func (cnx *AdminConnections) Reconnect(ctx context.Context, addr string) error {
	glog.Infof("Reconnecting to %s", addr)
	cnx.Remove(addr)
	return cnx.Add(ctx, addr)
}

// AddAll connect the given list of addresses and add them to the connection map
// fail silently
func (cnx *AdminConnections) AddAll(ctx context.Context, addrs []string) {
	for _, addr := range addrs {
		err := cnx.Add(ctx, addr)
		if err != nil {
			glog.V(6).Infof("Can't connect to %s: %v", addr, err)
		}
	}
}

// ReplaceAll clear the pool and re-populate it with new connections
// fail silently
func (cnx *AdminConnections) ReplaceAll(ctx context.Context, addrs []string) {
	cnx.Reset()
	cnx.AddAll(ctx, addrs)
}

// Reset close all connections and clear the connection map
func (cnx *AdminConnections) Reset() {
	for _, c := range cnx.clients {
		c.Close()
	}
	cnx.clients = map[string]ClientInterface{}
}

// ValidateResp checks the redis resp is empty and will attempt to reconnect on connection error.
// In case of error, customize the error, log it and return it.
func (cnx *AdminConnections) ValidateResp(ctx context.Context, resp interface{}, err error, addr, errMessage string) error {
	if resp == nil {
		glog.Errorf("%s: Unable to connect to node %s", errMessage, addr)
		return fmt.Errorf("%s: Unable to connect to node %s", errMessage, addr)
	}
	if err != nil {
		cnx.handleError(ctx, addr, err)
		glog.Errorf("%s: Unexpected error on node %s: %v", errMessage, addr, err)
		return fmt.Errorf("%s: Unexpected error on node %s: %v", errMessage, addr, err)
	}
	return nil
}

// GetRandom returns a client connection to a random node of the client map
func (cnx *AdminConnections) getRandomKeyClient() (string, ClientInterface, error) {
	nbClient := len(cnx.clients)
	if nbClient == 0 {
		return "", nil, errors.New(ErrNotFound)
	}
	randNumber := rand.Intn(nbClient)
	for k, c := range cnx.clients {
		if randNumber == 0 {
			return k, c, nil
		}
		randNumber--
	}

	return "", nil, errors.New(ErrNotFound)
}

// handleError handle a network error, reconnects if necessary, in that case, returns true
func (cnx *AdminConnections) handleError(ctx context.Context, addr string, err error) bool {
	if err == nil {
		return false
	} else if netError, ok := err.(net.Error); ok && netError.Timeout() {
		// timeout, reconnect
		if err = cnx.Reconnect(ctx, addr); err != nil {
			glog.Errorf("Can't reconnect to %s", addr)
		}
		return true
	}
	switch err.(type) {
	case *net.OpError:
		// connection refused, reconnect
		if err = cnx.Reconnect(ctx, addr); err != nil {
			glog.Errorf("Can't reconnect to %s", addr)
		}
		return true
	}
	return false
}

func (cnx *AdminConnections) connect(ctx context.Context, addr string) (ClientInterface, error) {
	c, err := NewClient(ctx, addr, cnx.connectionTimeout, cnx.commandsMapping)
	if err != nil {
		return nil, err
	}
	if cnx.clientName != "" {
		var resp string
		cmdErr := c.DoCmd(ctx, &resp, "CLIENT", "SETNAME", cnx.clientName)
		return c, cnx.ValidateResp(ctx, &resp, cmdErr, addr, "Unable to run command CLIENT SETNAME")
	}

	return c, nil
}

// buildCommandReplaceMapping reads the config file with the command-replace lines and build a mapping of
// bad lines are ignored silently
func buildCommandReplaceMapping(filePath string) map[string]string {
	mapping := make(map[string]string)
	file, err := os.Open(filePath)
	if err != nil {
		glog.Errorf("Cannot open %s: %v", filePath, err)
		return mapping
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		elems := strings.Fields(scanner.Text())
		if len(elems) == 3 && strings.ToLower(elems[0]) == "rename-command" {
			mapping[strings.ToUpper(elems[1])] = elems[2]
		}
	}

	if err = scanner.Err(); err != nil {
		glog.Errorf("Cannot parse %s: %v", filePath, err)
		return mapping
	}
	return mapping
}
