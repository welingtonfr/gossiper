package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"encoding/json"
	"net"
	"time"

	"github.com/hashicorp/mdns"
)

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
