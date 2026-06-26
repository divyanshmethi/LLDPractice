package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
)

/*Implement consistent hashing in go
Steps:
1. We need servers to be mapped into an array
2. We need hash function to hash the server name and we would store the hash value in a sorted array
3. We would need a map to store hash to server name mapping
4. We need to take replica as input to create virtual nodes for each server
5. In order to find which server the key belongs to we do a binary search on the sorted array of has values and find first >= hash value of the key. If not found we would return the first server in the sorted array
*/

type ConsistentHashing struct {
	mu                  sync.RWMutex
	ring                []uint64
	hashToServerMapping map[uint64]string
	serverToHashMapping map[string][]uint64
	replicas            int
}

func NewConsistentHashing(replicas int) *ConsistentHashing {
	return &ConsistentHashing{
		replicas:            replicas,
		ring:                make([]uint64, 0),
		hashToServerMapping: make(map[uint64]string),
		serverToHashMapping: make(map[string][]uint64),
	}
}

func (c *ConsistentHashing) hash(key string) uint64 {
	return xxhash.Sum64String(key)
}

func (c *ConsistentHashing) AddServers(serverNames []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, serverName := range serverNames {
		for i := 0; i < c.replicas; i++ {
			generatedHash := c.hash(serverName + "#" + strconv.Itoa(i))
			c.ring = append(c.ring, generatedHash)
			c.hashToServerMapping[generatedHash] = serverName
			c.serverToHashMapping[serverName] = append(c.serverToHashMapping[serverName], generatedHash)
		}
	}
	sort.Slice(c.ring, func(i, j int) bool {
		return c.ring[i] < c.ring[j]
	})
}

func (c *ConsistentHashing) RemoveServer(serverName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	allReplicaHashes := c.serverToHashMapping[serverName]
	for _, replicaHash := range allReplicaHashes {
		delete(c.hashToServerMapping, replicaHash)
		indexOfServerReplica := sort.Search(len(c.ring), func(i int) bool {
			return c.ring[i] >= replicaHash
		})
		if indexOfServerReplica < len(c.ring) && c.ring[indexOfServerReplica] == replicaHash {
			c.ring = append(c.ring[:indexOfServerReplica], c.ring[indexOfServerReplica+1:]...)
		}
	}
	delete(c.serverToHashMapping, serverName)
}

func (c *ConsistentHashing) findServer(key string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.ring) == 0 {
		return ""
	}
	hashOfKey := c.hash(key)
	idx := sort.Search(len(c.ring), func(i int) bool {
		return c.ring[i] >= hashOfKey
	})
	if idx >= len(c.ring) {
		idx = 0
	}
	return c.hashToServerMapping[c.ring[idx]]
}

func (c *ConsistentHashing) PrintRing() {
	for _, v := range c.ring {
		fmt.Printf("%d:%s\n", v, c.hashToServerMapping[v])
	}
}

func main() {
	ch := NewConsistentHashing(100)
	ch.AddServers([]string{"Server1", "Server2", "Server3"})
	ch.PrintRing()
	fmt.Println(ch.findServer("key1"))
	fmt.Println(ch.findServer("key2"))
	fmt.Println(ch.findServer("key3"))
	fmt.Println(ch.findServer("key4"))
	fmt.Println(ch.findServer("key5"))
	ch.RemoveServer("Server1")
	fmt.Println(ch.findServer("key1"))
	fmt.Println(ch.findServer("key2"))
	fmt.Println(ch.findServer("key3"))
	fmt.Println(ch.findServer("key4"))
	fmt.Println(ch.findServer("key5"))
}
