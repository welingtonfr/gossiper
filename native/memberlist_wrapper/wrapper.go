package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"encoding/json"
	"net"
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
	mu         sync.Mutex
	nextID     int64 = 1
	registry         = map[int64]*memberlist.Memberlist{}
	delegates        = map[int64]*gossipDelegate{}
	mdnsServers      = map[int64]*mdns.Server{}
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

func remove(handle C.int64_t) {
	mu.Lock()
	defer mu.Unlock()
	delete(registry, int64(handle))
	delete(delegates, int64(handle))
	delete(mdnsServers, int64(handle))
}

// ---------------------------------------------------------------------------
// Event delegate — queues node join/leave/update events for Dart to poll
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

type gossipDelegate struct {
	mu     sync.Mutex
	events []nodeEvent
}

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

func (d *gossipDelegate) drainJSON() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.events) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(d.events)
	d.events = d.events[:0]
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

	d := &gossipDelegate{}
	cfg.Events = d
	cfg.Logger = nil
	cfg.LogOutput = nil

	m, err := memberlist.Create(cfg)
	if err != nil {
		setErr(errOut, err.Error())
		return -1
	}

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

// MemberlistPollEvents drains pending node events as a JSON array.
// Caller must call FreeString on the returned pointer.
//
//export MemberlistPollEvents
func MemberlistPollEvents(handle C.int64_t) *C.char {
	mu.Lock()
	d := delegates[int64(handle)]
	mu.Unlock()
	if d == nil {
		return C.CString("[]")
	}
	return C.CString(d.drainJSON())
}

// MemberlistLeave gracefully leaves the cluster.
// timeoutMs is the leave broadcast timeout in milliseconds.
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
	// Stop mDNS first so we don't advertise a dead node
	MemberlistStopMDNS(handle)

	m := lookup(handle)
	if m != nil {
		m.Shutdown() //nolint:errcheck
	}
	remove(handle)
}

// ---------------------------------------------------------------------------
// Exported C functions — mDNS discovery
// ---------------------------------------------------------------------------

// MemberlistStartMDNS advertises this node on the local network via mDNS.
//
// serviceName should follow the mDNS convention, e.g. "_gossiper._tcp".
// Other nodes can discover it by calling MemberlistDiscoverMDNS with the
// same serviceName.
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

	// Resolve the node's bind address to a usable IP for mDNS
	ips, err := localIPs(node.Addr.String())
	if err != nil {
		setErr(errOut, "could not resolve local IPs: "+err.Error())
		return -1
	}

	svc, err := mdns.NewMDNSService(
		node.Name,   // instance name (unique per node)
		serviceName, // service type, e.g. "_gossiper._tcp"
		"",          // domain — "" defaults to "local."
		"",          // host — "" defaults to hostname
		int(node.Port),
		ips,
		[]string{"gossiper"},
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
	// Shut down any previous mDNS server for this handle
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
// given serviceName via mDNS and returns a JSON array of "addr:port" strings
// suitable for passing to MemberlistJoin.
//
// timeoutMs controls how long to wait for responses (minimum ~100ms recommended).
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
// FreeString — must be called for every *C.char returned by this library
// ---------------------------------------------------------------------------

//export FreeString
func FreeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func setErr(errOut **C.char, msg string) {
	if errOut != nil {
		*errOut = C.CString(msg)
	}
}

// localIPs returns usable non-loopback IPs for the given address.
// If addr is "0.0.0.0" or "::" it enumerates the machine's interfaces.
func localIPs(addr string) ([]net.IP, error) {
	ip := net.ParseIP(addr)
	if ip != nil && !ip.IsUnspecified() {
		return []net.IP{ip}, nil
	}

	// Bind address is wildcard — pick all non-loopback unicast IPs
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var ips []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var candidate net.IP
			switch v := a.(type) {
			case *net.IPNet:
				candidate = v.IP
			case *net.IPAddr:
				candidate = v.IP
			}
			if candidate != nil && candidate.To4() != nil {
				ips = append(ips, candidate)
			}
		}
	}
	if len(ips) == 0 {
		return nil, net.ErrWriteToConnected // no suitable interface
	}
	return ips, nil
}

func itoa(n int) string {
	b := make([]byte, 0, 8)
	if n == 0 {
		return "0"
	}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func main() {}
