package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"sync"
	"unsafe"

	"github.com/hashicorp/mdns"
	"github.com/hashicorp/memberlist"
)

// ---------------------------------------------------------------------------
// Handle registry
// ---------------------------------------------------------------------------

var (
	mu          sync.Mutex
	nextID      int64 = 1
	registry          = map[int64]*memberlist.Memberlist{}
	delegates         = map[int64]*gossipDelegate{}
	mdnsServers       = map[int64]*mdns.Server{}
)

func register(m *memberlist.Memberlist, d *gossipDelegate) int64 {
	mu.Lock()
	defer mu.Unlock()
	id := nextID
	nextID++
	registry[id] = m
	delegates[id] = d
	return id
}

func lookup(handle C.int64_t) *memberlist.Memberlist {
	mu.Lock()
	defer mu.Unlock()
	return registry[int64(handle)]
}

func lookupDelegate(handle C.int64_t) *gossipDelegate {
	mu.Lock()
	defer mu.Unlock()
	return delegates[int64(handle)]
}

func remove(handle C.int64_t) {
	mu.Lock()
	defer mu.Unlock()
	delete(registry, int64(handle))
	delete(delegates, int64(handle))
	delete(mdnsServers, int64(handle))
}

// ---------------------------------------------------------------------------
// simpleBroadcast — implements memberlist.Broadcast for the queue
// ---------------------------------------------------------------------------

type simpleBroadcast struct{ msg []byte }

func (b *simpleBroadcast) Invalidates(other memberlist.Broadcast) bool { return false }
func (b *simpleBroadcast) Message() []byte                             { return b.msg }
func (b *simpleBroadcast) Finished()                                   {}

// ---------------------------------------------------------------------------
// FreeString
// ---------------------------------------------------------------------------

//export FreeString
func FreeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func main() {}
