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

package redis

import (
	"hash/crc32"
	"net"
	"sync"

	"github.com/stathat/consistent"
)

// ServerInfo stores parsed server information.
type ServerInfo struct {
	Addr   net.Addr // Redis ip:port
	DB     string   // Redis dbid
	Passwd string   // Redis password
}

// ServerSelector is an interface where servers are added and selected by
// a given key, that is used for a redis operation such as Get or Set later.
type ServerSelector interface {
	Add(s *ServerInfo)          // Add new server
	Get(key string) *ServerInfo // Get server for the given key
	GetFirst() *ServerInfo      // For commands such as Ping
	TotalServers() int          // Number of servers added to the selector
}

// Modulo implements the basic server ServerSelector, hashing by key % nservers.
type ModuloSelector struct {
	mu     sync.RWMutex
	server []*ServerInfo
}

// Add implements the ServerSelector interface.
func (m *ModuloSelector) Add(s *ServerInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.server = append(m.server, s)
}

// Get implements the ServerSelector interface.
func (m *ModuloSelector) Get(key string) *ServerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.server) == 0 {
		panic("No servers were added to this ModuloSelector, " +
			"Get failed.")
	}
	return m.server[crc32.ChecksumIEEE([]byte(key))%uint32(len(m.server))]
}

// GetFirst implements the ServerSelector interface.
func (m *ModuloSelector) GetFirst() *ServerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.server) == 0 {
		panic("No servers were added to this ModuloSelector, " +
			"GetFirst failed.")
	}
	return m.server[0]
}

// TotalServers implements the ServerSelector interface.
func (m *ModuloSelector) TotalServers() int {
	return len(m.server)
}

// HashRingSelector implements a server ServerSelector that uses consistent
// hashing for selecting a server based on a given key.
type HashRingSelector struct {
	mu     sync.RWMutex
	ring   *consistent.Consistent
	first  *net.Addr
	server map[string]*ServerInfo
}

// Add implements the ServerSelector interface.
func (h *HashRingSelector) Add(s *ServerInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ring == nil {
		h.ring = consistent.New()
		h.server = make(map[string]*ServerInfo)
		h.first = &s.Addr
	}
	h.server[s.Addr.String()] = s
	h.ring.Add(s.Addr.String())
}

// Get implements the ServerSelector interface.
func (h *HashRingSelector) Get(key string) *ServerInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.ring == nil {
		panic("No servers were added to this HashRingSelector, " +
			"Get failed.")
	}
	addr, err := h.ring.Get(key)
	if err != nil {
		panic("Unexpected error on HashRingSelector: " + err.Error())
	}
	si, ok := h.server[addr]
	if !ok {
		panic("Unexpected error on HashRingSelector: " +
			"server info not found")
	}
	return si
}

// GetFirst implements the ServerSelector interface.
func (h *HashRingSelector) GetFirst() *ServerInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.ring == nil {
		panic("No servers were added to this HashRingSelector, " +
			"Get failed.")
	}
	si, ok := h.server[(*h.first).String()]
	if !ok {
		panic("Unexpected error on HashRingSelector: " +
			"server info not found")
	}
	return si
}

// TotalServers implements the ServerSelector interface.
func (h *HashRingSelector) TotalServers() int {
	return len(h.server)
}

var (
	Modulo   = new(ModuloSelector)
	HashRing = new(HashRingSelector)
)
