package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"sort"
	"sync"
	"time"
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
// gossipDelegate — implements memberlist.EventDelegate + memberlist.Delegate
// ---------------------------------------------------------------------------

const (
	EventJoin   = 0
	EventLeave  = 1
	EventUpdate = 2
)

type nodeEvent struct {
	Type int    `json:"type"`
	Name string `json:"name"`
	Addr string `json:"addr"`
	Port int    `json:"port"`
}

// logEntry is the envelope wrapping every broadcast message.
// It travels over gossip so peers can dedup and replay on reconnect.
type logEntry struct {
	ID   string `json:"id"`
	Data string `json:"data"`
	At   int64  `json:"at"` // unix milliseconds
}

type gossipDelegate struct {
	mu          sync.Mutex
	events      []nodeEvent
	received    []string
	broadcasts  *memberlist.TransmitLimitedQueue
	ml          *memberlist.Memberlist
	msgLog      []logEntry
	maxLog      int
	logWindow   time.Duration
	persistPath string
}

func newDelegate() *gossipDelegate {
	d := &gossipDelegate{
		maxLog:    500,
		logWindow: 24 * time.Hour,
	}
	d.broadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			if d.ml == nil {
				return 1
			}
			return d.ml.NumMembers()
		},
		RetransmitMult: 3,
	}
	return d
}

// loadFromDisk reads the JSON-lines persist file and merges entries into msgLog.
// Must be called with d.mu held.
func (d *gossipDelegate) loadFromDisk() {
	if d.persistPath == "" {
		return
	}
	f, err := os.Open(d.persistPath)
	if err != nil {
		return // file doesn't exist yet — fine on first run
	}
	defer f.Close()
	known := make(map[string]bool, len(d.msgLog))
	for _, e := range d.msgLog {
		known[e.ID] = true
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry logEntry
		if err := json.Unmarshal(line, &entry); err != nil || entry.ID == "" {
			continue
		}
		if !known[entry.ID] {
			d.msgLog = append(d.msgLog, entry)
			known[entry.ID] = true
		}
	}
	sort.Slice(d.msgLog, func(i, j int) bool {
		return d.msgLog[i].At < d.msgLog[j].At
	})
}

// appendToDisk appends one entry as a JSON line to the persist file.
// Must be called with d.mu held.
func (d *gossipDelegate) appendToDisk(entry logEntry) {
	if d.persistPath == "" {
		return
	}
	line, err := json.Marshal(entry)
	if err != nil {
		return
	}
	f, err := os.OpenFile(d.persistPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.Write(append(line, '\n')) //nolint:errcheck
}

// rewriteDisk atomically rewrites the persist file with the current msgLog.
// Must be called with d.mu held.
func (d *gossipDelegate) rewriteDisk() {
	if d.persistPath == "" {
		return
	}
	tmp := d.persistPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return
	}
	ok := true
	for _, entry := range d.msgLog {
		line, err := json.Marshal(entry)
		if err != nil {
			ok = false
			break
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			ok = false
			break
		}
	}
	f.Close()
	if ok {
		os.Rename(tmp, d.persistPath) //nolint:errcheck
	} else {
		os.Remove(tmp) //nolint:errcheck
	}
}

// pruneLog removes entries older than logWindow or beyond maxLog cap.
// Rewrites disk only when entries are actually removed.
// Must be called with d.mu held.
func (d *gossipDelegate) pruneLog() {
	cutoff := time.Now().Add(-d.logWindow).UnixMilli()
	start := 0
	for start < len(d.msgLog) && d.msgLog[start].At < cutoff {
		start++
	}
	pruned := start > 0 || len(d.msgLog) > d.maxLog
	d.msgLog = d.msgLog[start:]
	if len(d.msgLog) > d.maxLog {
		d.msgLog = d.msgLog[len(d.msgLog)-d.maxLog:]
	}
	if pruned {
		d.rewriteDisk()
	}
}

// memberlist.EventDelegate

func (d *gossipDelegate) NotifyJoin(n *memberlist.Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, nodeEvent{EventJoin, n.Name, n.Addr.String(), int(n.Port)})
}

func (d *gossipDelegate) NotifyLeave(n *memberlist.Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, nodeEvent{EventLeave, n.Name, n.Addr.String(), int(n.Port)})
}

func (d *gossipDelegate) NotifyUpdate(n *memberlist.Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, nodeEvent{EventUpdate, n.Name, n.Addr.String(), int(n.Port)})
}

func (d *gossipDelegate) NodeMeta(limit int) []byte { return nil }

func (d *gossipDelegate) NotifyMsg(b []byte) {
	if len(b) == 0 {
		return
	}
	var entry logEntry
	if err := json.Unmarshal(b, &entry); err != nil || entry.ID == "" {
		// plain string — not from gossiper broadcast, deliver as-is
		d.mu.Lock()
		d.received = append(d.received, string(b))
		d.mu.Unlock()
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range d.msgLog {
		if e.ID == entry.ID {
			return // already seen — gossip retransmit, discard
		}
	}
	d.msgLog = append(d.msgLog, entry)
	d.received = append(d.received, entry.Data)
	d.appendToDisk(entry)
	d.pruneLog()
}

func (d *gossipDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	return d.broadcasts.GetBroadcasts(overhead, limit)
}

// LocalState is called by memberlist to get a full snapshot of this node's
// message log, which is pushed to peers during joins and periodic syncs.
func (d *gossipDelegate) LocalState(join bool) []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pruneLog()
	if len(d.msgLog) == 0 {
		return nil
	}
	b, _ := json.Marshal(d.msgLog)
	return b
}

// MergeRemoteState is called when a peer's LocalState arrives.
// New entries (not already in our log) are added and delivered to the app,
// giving a PDV that was offline a chance to catch up on missed messages.
func (d *gossipDelegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}
	var incoming []logEntry
	if err := json.Unmarshal(buf, &incoming); err != nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	known := make(map[string]bool, len(d.msgLog))
	for _, e := range d.msgLog {
		known[e.ID] = true
	}
	added := 0
	for _, e := range incoming {
		if !known[e.ID] {
			d.msgLog = append(d.msgLog, e)
			d.received = append(d.received, e.Data)
			known[e.ID] = true
			added++
		}
	}
	sort.Slice(d.msgLog, func(i, j int) bool {
		return d.msgLog[i].At < d.msgLog[j].At
	})
	d.pruneLog()
	if added > 0 {
		d.rewriteDisk()
	}
}

// drain helpers

func (d *gossipDelegate) drainEvents() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.events) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(d.events)
	d.events = d.events[:0]
	return string(b)
}

func (d *gossipDelegate) drainMessages() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.received) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(d.received)
	d.received = d.received[:0]
	return string(b)
}

// ---------------------------------------------------------------------------
// Exported C functions — memberlist core
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Exported C functions — broadcast messaging
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Exported C functions — mDNS discovery
// ---------------------------------------------------------------------------

// MemberlistStartMDNS advertises this node on the local network via mDNS.
//
//export MemberlistStartMDNS
func MemberlistStartMDNS(handle C.int64_t, serviceNameC *C.char, errOut **C.char) C.int {
	m := lookup(handle)
	if m == nil {
		setErr(errOut, "invalid handle")
		return -1
	}

	node := m.LocalNode()
	serviceName := C.GoString(serviceNameC)

	ips, err := localIPs(node.Addr.String())
	if err != nil {
		setErr(errOut, "could not resolve local IPs: "+err.Error())
		return -1
	}

	svc, err := mdns.NewMDNSService(
		node.Name, serviceName, "", "", int(node.Port), ips, []string{"gossiper"},
	)
	if err != nil {
		setErr(errOut, "mdns service init failed: "+err.Error())
		return -1
	}

	srv, err := mdns.NewServer(&mdns.Config{Zone: svc})
	if err != nil {
		setErr(errOut, "mdns server start failed: "+err.Error())
		return -1
	}

	mu.Lock()
	if old, ok := mdnsServers[int64(handle)]; ok {
		old.Shutdown() //nolint:errcheck
	}
	mdnsServers[int64(handle)] = srv
	mu.Unlock()

	return 0
}

// MemberlistStopMDNS stops the mDNS advertisement for this node.
//
//export MemberlistStopMDNS
func MemberlistStopMDNS(handle C.int64_t) {
	mu.Lock()
	srv, ok := mdnsServers[int64(handle)]
	if ok {
		delete(mdnsServers, int64(handle))
	}
	mu.Unlock()

	if srv != nil {
		srv.Shutdown() //nolint:errcheck
	}
}

// MemberlistDiscoverMDNS queries the local network for nodes advertising the
// given serviceName and returns a JSON array of "addr:port" strings.
// Caller must call FreeString on the returned pointer.
//
//export MemberlistDiscoverMDNS
func MemberlistDiscoverMDNS(serviceNameC *C.char, timeoutMs C.int) *C.char {
	serviceName := C.GoString(serviceNameC)
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout < 100*time.Millisecond {
		timeout = 100 * time.Millisecond
	}

	entries := make(chan *mdns.ServiceEntry, 32)

	go func() {
		_ = mdns.Query(&mdns.QueryParam{
			Service: serviceName,
			Timeout: timeout,
			Entries: entries,
		})
		close(entries)
	}()

	var addrs []string
	seen := map[string]bool{}
	for entry := range entries {
		addr := net.JoinHostPort(entry.AddrV4.String(), itoa(entry.Port))
		if !seen[addr] {
			seen[addr] = true
			addrs = append(addrs, addr)
		}
	}

	b, _ := json.Marshal(addrs)
	return C.CString(string(b))
}

// ---------------------------------------------------------------------------
// FreeString
// ---------------------------------------------------------------------------

//export FreeString
func FreeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func main() {}
