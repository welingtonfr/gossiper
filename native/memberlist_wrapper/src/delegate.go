package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

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

// ---------------------------------------------------------------------------
// Disk persistence
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// memberlist.EventDelegate
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// memberlist.Delegate
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Drain helpers (called by CGO exports)
// ---------------------------------------------------------------------------

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
