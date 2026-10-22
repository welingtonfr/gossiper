package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"encoding/json"
	"time"

	"github.com/hashicorp/memberlist"
)

// MemberlistCreate creates a new memberlist node.
// Returns a positive handle on success, or -1 on error.
//
//export MemberlistCreate
func MemberlistCreate(nameC *C.char, bindAddrC *C.char, bindPort C.int, errOut **C.char) C.int64_t {
	cfg := memberlist.DefaultLANConfig()
	if nameC != nil {
		cfg.Name = C.GoString(nameC)
	}
	if bindAddrC != nil {
		cfg.BindAddr = C.GoString(bindAddrC)
	}
	if bindPort > 0 {
		cfg.BindPort = int(bindPort)
	}

	d := newDelegate()
	cfg.Events = d
	cfg.Delegate = d
	cfg.Logger = nil
	cfg.LogOutput = nil

	m, err := memberlist.Create(cfg)
	if err != nil {
		setErr(errOut, err.Error())
		return -1
	}

	d.ml = m // wire up after creation so NumNodes can call NumMembers
	return C.int64_t(register(m, d))
}

// MemberlistJoin joins an existing cluster.
// addrsJson is a JSON string array, e.g. '["192.168.1.2:7946"]'.
// Returns the number of nodes successfully contacted, or -1 on error.
//
//export MemberlistJoin
func MemberlistJoin(handle C.int64_t, addrsJson *C.char, errOut **C.char) C.int {
	m := lookup(handle)
	if m == nil {
		setErr(errOut, "invalid handle")
		return -1
	}

	var addrs []string
	if err := json.Unmarshal([]byte(C.GoString(addrsJson)), &addrs); err != nil {
		setErr(errOut, "addrsJson must be a JSON string array: "+err.Error())
		return -1
	}

	n, err := m.Join(addrs)
	if err != nil {
		setErr(errOut, err.Error())
		return C.int(n)
	}
	return C.int(n)
}

// MemberlistMembers returns a JSON array of current cluster members.
// Caller must call FreeString on the returned pointer.
//
//export MemberlistMembers
func MemberlistMembers(handle C.int64_t) *C.char {
	m := lookup(handle)
	if m == nil {
		return C.CString("[]")
	}

	type memberInfo struct {
		Name string `json:"name"`
		Addr string `json:"addr"`
		Port int    `json:"port"`
	}

	nodes := m.Members()
	result := make([]memberInfo, 0, len(nodes))
	for _, n := range nodes {
		result = append(result, memberInfo{n.Name, n.Addr.String(), int(n.Port)})
	}

	b, _ := json.Marshal(result)
	return C.CString(string(b))
}

// MemberlistLocalNode returns a JSON object for this node.
// Caller must call FreeString on the returned pointer.
//
//export MemberlistLocalNode
func MemberlistLocalNode(handle C.int64_t) *C.char {
	m := lookup(handle)
	if m == nil {
		return C.CString("{}")
	}

	n := m.LocalNode()
	type nodeInfo struct {
		Name string `json:"name"`
		Addr string `json:"addr"`
		Port int    `json:"port"`
	}
	b, _ := json.Marshal(nodeInfo{n.Name, n.Addr.String(), int(n.Port)})
	return C.CString(string(b))
}

// MemberlistNumMembers returns the number of alive cluster members.
//
//export MemberlistNumMembers
func MemberlistNumMembers(handle C.int64_t) C.int {
	m := lookup(handle)
	if m == nil {
		return 0
	}
	return C.int(m.NumMembers())
}

// MemberlistPollEvents drains pending membership events as a JSON array.
// Caller must call FreeString on the returned pointer.
//
//export MemberlistPollEvents
func MemberlistPollEvents(handle C.int64_t) *C.char {
	d := lookupDelegate(handle)
	if d == nil {
		return C.CString("[]")
	}
	return C.CString(d.drainEvents())
}

// MemberlistLeave gracefully leaves the cluster.
//
//export MemberlistLeave
func MemberlistLeave(handle C.int64_t, timeoutMs C.int, errOut **C.char) C.int {
	m := lookup(handle)
	if m == nil {
		setErr(errOut, "invalid handle")
		return -1
	}

	if err := m.Leave(time.Duration(timeoutMs) * time.Millisecond); err != nil {
		setErr(errOut, err.Error())
		return -1
	}
	return 0
}

// MemberlistDestroy shuts down the node, stops any mDNS server, and frees resources.
//
//export MemberlistDestroy
func MemberlistDestroy(handle C.int64_t) {
	MemberlistStopMDNS(handle)

	m := lookup(handle)
	if m != nil {
		m.Shutdown() //nolint:errcheck
	}
	remove(handle)
}

// MemberlistBroadcast queues a message to be gossiped to every node in the cluster.
//
// data is an opaque byte string; gossiper does not interpret it — use JSON,
// Protobuf, or any encoding your application chooses.
// Returns 0 on success, -1 on error.
//
//export MemberlistBroadcast
func MemberlistBroadcast(handle C.int64_t, dataC *C.char, errOut **C.char) C.int {
	d := lookupDelegate(handle)
	if d == nil {
		setErr(errOut, "invalid handle")
		return -1
	}
	entry := logEntry{
		ID:   newUUID(),
		Data: C.GoString(dataC),
		At:   time.Now().UnixMilli(),
	}
	b, _ := json.Marshal(entry)
	d.mu.Lock()
	d.msgLog = append(d.msgLog, entry)
	d.appendToDisk(entry)
	d.pruneLog()
	d.mu.Unlock()
	d.broadcasts.QueueBroadcast(&simpleBroadcast{b})
	return 0
}

// MemberlistSetReplayWindow configures the in-memory replay log.
// maxEntries: maximum number of messages to keep (0 = keep current setting).
// windowHours: how many hours of history to retain (0 = keep current setting).
// Defaults are 500 entries / 24 hours.
//
//export MemberlistSetReplayWindow
func MemberlistSetReplayWindow(handle C.int64_t, maxEntries C.int, windowHours C.int) {
	d := lookupDelegate(handle)
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if maxEntries > 0 {
		d.maxLog = int(maxEntries)
	}
	if windowHours > 0 {
		d.logWindow = time.Duration(windowHours) * time.Hour
	}
}

// MemberlistSetPersistPath configures a file path for durable message log storage.
// On call, the existing file (if any) is loaded and merged into the in-memory log,
// so a node that restarts automatically recovers its message history before joining.
// Pass an empty string to disable persistence.
//
//export MemberlistSetPersistPath
func MemberlistSetPersistPath(handle C.int64_t, pathC *C.char) {
	d := lookupDelegate(handle)
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.persistPath = C.GoString(pathC)
	d.loadFromDisk()
}

// MemberlistPollMessages drains messages received from other cluster nodes.
// Returns a JSON array of raw message strings.
// Caller must call FreeString on the returned pointer.
//
//export MemberlistPollMessages
func MemberlistPollMessages(handle C.int64_t) *C.char {
	d := lookupDelegate(handle)
	if d == nil {
		return C.CString("[]")
	}
	return C.CString(d.drainMessages())
}
