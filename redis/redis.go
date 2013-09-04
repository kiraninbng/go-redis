// Copyright 2013 Alexandre Fiori
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// This is a modified version of gomemcache adapted to redis.
// Original code and license at https://github.com/bradfitz/gomemcache/

// WORK IN PROGRESS
// Package redis provides a client for the redis cache server.
package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("redis: no servers configured or available")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("redis: server error")

	// ErrTimedOut is returned when a Read or Write operation times out
	ErrTimedOut = errors.New("redis: timed out")
)

// DefaultTimeout is the default socket read/write timeout.
const DefaultTimeout = time.Duration(100) * time.Millisecond

// TODO: Make this configurable?
const maxIdleConnsPerAddr = 2

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	if err == ErrServerError {
		return true
	}
	return false // time outs, broken pipes, etc
}

// New returns a redis client using the provided server(s) with equal weight.
// If a server is listed multiple times, it gets a proportional amount of
// weight.
//
// New supports ip:port or /unix/path, and optional *db* and *passwd* arguments.
// Example:
//
//	rc := redis.New("ip:port db=N passwd=foobared")
//	rc := redis.New("/tmp/redis.sock db=N passwd=foobared")
func New(server ...string) *Client {
	c, _ := NewClient(Modulo, server...)
	return c
}

// NewClient returns a redis client using the provided ServerSelector, which
// is either Modulo (default, same as New) or HashRing for consistent hasing.
//
// NewClient supports ip:port or /unix/path, and optional *db* and *passwd* arguments.
// Example:
//
//	rc := redis.NewClient(redis.HashRing, "ip:port1", "ip:port2")
//	rc := redis.NewClient(redis.Modulo, "/tmp/redis.sock db=N passwd=foobared")
func NewClient(selector ServerSelector, server ...string) (*Client, error) {
	if selector == nil {
		selector = Modulo
	}
	for _, srv := range server {
		if si, err := parseServerInfo(srv); err != nil {
			return nil, err
		} else {
			selector.Add(si)
		}
	}
	return &Client{selector: selector}, nil
}

func parseServerInfo(s string) (*ServerInfo, error) {
	var (
		err error
		si  = new(ServerInfo)
	)
	// addr:port db=N passwd=foobar
	items := strings.Split(s, " ")
	if strings.Contains(items[0], "/") {
		si.Addr, err = net.ResolveUnixAddr("unix", items[0])
	} else {
		si.Addr, err = net.ResolveTCPAddr("tcp", items[0])
	}
	if err != nil {
		return nil, fmt.Errorf("Invalid redis server '%s': %s", s, err)
	}
	if len(items) > 1 {
		for _, item := range items[1:] {
			kv := strings.Split(item, "=")
			if len(kv) != 2 {
				return nil,
					fmt.Errorf("Unknown option: %s", item)
			}
			switch kv[0] {
			case "db":
				si.DB = kv[1]
			case "passwd":
				si.Passwd = kv[1]
			}
		}
	}
	return si, nil
}

// Client is a redis client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	selector ServerSelector

	lk       sync.Mutex
	freeconn map[net.Addr][]*conn
}

// conn is a connection to a server.
type conn struct {
	nc  net.Conn
	rw  *bufio.ReadWriter
	srv *ServerInfo
	c   *Client
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.srv.Addr, cn)
}

func (cn *conn) extendDeadline() {
	cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

// condRelease releases this connection if the error pointed to by err
// is nil (not an error) or is only a protocol level error.
// The purpose is to not recycle TCP connections that are bad.
func (cn *conn) condRelease(err *error) {
	if *err == nil || resumableError(*err) {
		cn.release()
	} else {
		cn.nc.Close()
	}
}

func (c *Client) putFreeConn(addr net.Addr, cn *conn) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[net.Addr][]*conn)
	}
	freelist := c.freeconn[addr]
	if len(freelist) >= maxIdleConnsPerAddr {
		cn.nc.Close()
		return
	}
	c.freeconn[addr] = append(freelist, cn)
}

func (c *Client) getFreeConn(srv *ServerInfo) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[srv.Addr]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[srv.Addr] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "redis: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	type connError struct {
		cn  net.Conn
		err error
	}
	ch := make(chan connError)
	go func() {
		nc, err := net.Dial(addr.Network(), addr.String())
		ch <- connError{nc, err}
	}()
	select {
	case ce := <-ch:
		return ce.cn, ce.err
	case <-time.After(c.netTimeout()):
		// Too slow. Fall through.
	}
	// Close the conn if it does end up finally coming in
	go func() {
		ce := <-ch
		if ce.err == nil {
			ce.cn.Close()
		}
	}()
	return nil, &ConnectTimeoutError{addr}
}

func (c *Client) getConn(srv *ServerInfo) (*conn, error) {
	cn, ok := c.getFreeConn(srv)
	if ok {
		cn.extendDeadline()
		return cn, nil
	}
	nc, err := c.dial(srv.Addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:  nc,
		srv: srv,
		rw:  bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:   c,
	}
	cn.extendDeadline()
	if srv.Passwd != "" {
		_, err := c.execute_urp(cn.rw, "AUTH", srv.Passwd)
		if err != nil {
			return nil, err
		}
	}
	if srv.DB != "" {
		_, err := c.execute(cn.rw, "SELECT", srv.DB)
		if err != nil {
			return nil, err
		}
	}
	return cn, nil
}

// execWithKey picks a server based on the key, and executes a command in redis.
func (c *Client) execWithKey(urp bool, cmd, key string, a ...interface{}) (v interface{}, err error) {
	srv := c.selector.Get(key)
	x := []interface{}{cmd, key}
	return c.execWithAddr(urp, srv, append(x, a...)...)
}

// execWithKeys calls execWithKey for each key, returns an array of results.
func (c *Client) execWithKeys(urp bool, cmd string, keys []string) (v interface{}, err error) {
	var r []interface{}
	for _, k := range keys {
		if tmp, e := c.execWithKey(urp, cmd, k); e != nil {
			err = e
			return
		} else {
			if mi, ok := tmp.([]interface{}); ok {
				for _, mx := range mi {
					r = append(r, mx)
				}
			}
		}
	}
	v = r
	return
}

// execOnFirst executes a command on the first listed server.
// execOnFirst is used by commands that are not bound to a key. e.g.: ping, info
func (c *Client) execOnFirst(urp bool, a ...interface{}) (interface{}, error) {
	return c.execWithAddr(urp, c.selector.GetFirst(), a...)
}

// execWithAddr executes a command in a specific redis server.
func (c *Client) execWithAddr(urp bool, srv *ServerInfo, a ...interface{}) (v interface{}, err error) {
	cn, err := c.getConn(srv)
	if err != nil {
		return
	}
	defer cn.condRelease(&err)
	if urp {
		return c.execute_urp(cn.rw, a...)
	} else {
		return c.execute(cn.rw, a...)
	}
	// unreachable, but necessary for backwards compatibility with go1
	return
}

// execute sends a command to redis, then reads and parses the response.
// It uses the old protocol and can be used by simple commands, such as DB.
// Redis protocol <http://redis.io/topics/protocol>
func (c *Client) execute(rw *bufio.ReadWriter, a ...interface{}) (v interface{}, err error) {
	//fmt.Printf("\nSending: %#v\n", a)
	// old redis protocol.
	_, err = fmt.Fprintf(rw, strings.Join(autoconv_args(a), " ")+"\r\n")
	if err != nil {
		return
	}
	if err = rw.Flush(); err != nil {
		return
	}
	return c.parseResponse(rw)
}

// execute sends a command to redis, then reads and parses the response.
// It uses the current protocol and must be used by most commands, such as SET.
// Redis protocol <http://redis.io/topics/protocol>
func (c *Client) execute_urp(rw *bufio.ReadWriter, a ...interface{}) (v interface{}, err error) {
	//fmt.Printf("\nSending: %#v\n", a)
	// unified request protocol
	s := autoconv_args(a)
	_, err = fmt.Fprintf(rw, "*%d\r\n", len(a))
	if err != nil {
		return
	}
	for _, i := range s {
		_, err = fmt.Fprintf(rw, "$%d\r\n%s\r\n", len(i), i)
		if err != nil {
			return
		}
	}
	if err = rw.Flush(); err != nil {
		return
	}
	return c.parseResponse(rw)
}

// parseResponse reads and parses a single response from redis.
func (c *Client) parseResponse(rw *bufio.ReadWriter) (v interface{}, err error) {
	line, e := rw.ReadSlice('\n')
	if err != nil {
		err = e
		return
	}
	//fmt.Printf("line=%#v err=%#v\n", string(line), err)
	if len(line) < 1 {
		err = ErrTimedOut
		return
	}
	reply := byte(line[0])
	lineLen := len(line)
	if len(line) > 2 && bytes.Equal(line[lineLen-2:], []byte("\r\n")) {
		line = line[1 : lineLen-2]
	}
	switch reply {
	case '-': // Error reply
		err = errors.New(string(line))
		return
	case '+': // Status reply
		v = string(line)
		return
	case ':': // Integer reply
		response, e := strconv.Atoi(string(line))
		if e != nil {
			err = e
			return
		}
		v = response
	case '$': // Bulk reply
		valueLen, e := strconv.Atoi(string(line))
		if e != nil {
			err = e
			return
		}
		if valueLen == -1 {
			v = "" // err = ErrCacheMiss
			return
		}
		b := make([]byte, valueLen+2) // 2==crlf, TODO: fix this
		var s byte
		for n := 0; n < cap(b); n++ {
			s, err = rw.ReadByte()
			if err != nil {
				return
			}
			b[n] = s
		}
		if len(b) != cap(b) {
			err = errors.New(
				fmt.Sprintf("Unexpected response: %#v",
					string(line)))
			return
		}
		v = string(b[:valueLen]) // removes proto trailing crlf
		return
	case '*': // Multi-bulk reply
		//fmt.Printf("multibulk line=%#v\n", line)
		nitems, e := strconv.Atoi(string(line))
		if e != nil {
			err = e
			return
		}
		if nitems < 1 {
			v = nil
			return
		}
		resp := make([]interface{}, nitems)
		for n := 0; n < nitems; n++ {
			resp[n], err = c.parseResponse(rw)
			if err != nil {
				return
			}
		}
		//fmt.Printf("multibulk=%#v\n", resp)
		v = resp
		return
	default:
		// TODO: return error and kill the connection
		panic("Unexpected line:" + string(line))
	}

	return
}

// Used by tests.
func errUnexpected(msg interface{}) string {
	return fmt.Sprintf("Unexpected response from redis-server: %#v\n", msg)
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
