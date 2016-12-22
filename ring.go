// Package dallimin provides dalli compatible server selector for gomemcache
package dallimin

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	pointsPerServer = 160 // MEMCACHED_POINTS_PER_SERVER_KETAMA
)

var (
	ErrNoServers = errors.New("memcache: no servers configured or available")
)

type node struct {
	label  string
	addr   net.Addr
	weight int
}

type entry struct {
	node  node
	point uint
}

type Option struct {
	CheckAlive bool
	Failover   bool
}

type Ring struct {
	mu     sync.Mutex
	addrs  []net.Addr
	rings  entries
	option Option
}

type entries []entry

func (c entries) Less(i, j int) bool { return c[i].point < c[j].point }
func (c entries) Len() int           { return len(c) }
func (c entries) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func New(servers []string, option Option) (*Ring, error) {
	if len(servers) == 0 {
		return &Ring{option: option}, nil
	}

	return NewWithWeights(mapServer(servers), option)
}

func NewWithWeights(servers map[string]int, option Option) (*Ring, error) {
	if len(servers) == 0 {
		return &Ring{option: option}, nil
	}

	ss, sw := extract(servers)
	return newRingWeights(ss, sw, option)
}

// Each iterates over each server calling the given function
func (h *Ring) Each(f func(net.Addr) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, a := range h.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

// PickServer returns the server address that a given item should be shared onto.
func (h *Ring) PickServer(key string) (net.Addr, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	n := len(h.rings)

	if n == 0 {
		return nil, ErrNoServers
	}

	if n == 1 {
		return h.rings[0].node.addr, nil
	}

	return h.pickServer(key, n)
}

// Servers return available server addresses
func (h *Ring) Servers() []net.Addr {
	return h.addrs
}

func (h *Ring) pickServer(key string, z int) (a net.Addr, err error) {
	x := hash(key)
	c := func(key string, i int) uint {
		return hash(fmt.Sprintf("%d%s", i, key))
	}

	for i := 0; i < 20; i++ {
		n := search(h.rings, x)

		if n >= uint(z) && h.option.Failover {
			x = c(key, i)
			continue
		}

		a := h.rings[n].node.addr

		if !h.option.CheckAlive {
			return a, nil
		}

		if isAlive(a) {
			return a, nil
		}

		if !h.option.Failover {
			break
		}

		x = c(key, i)
	}

	return nil, ErrNoServers
}

func newRingWeights(ss []string, sw []int, option Option) (*Ring, error) {
	if len(sw) == 1 {
		server := ss[0]
		weight := sw[0]

		addr, err := nodeAddr(server)
		if err != nil {
			return nil, err
		}

		h := &Ring{option: option}
		h.addrs = append(h.addrs, addr)
		h.rings = append(h.rings, buildEntry(server, addr, weight, 0))

		return h, nil
	}

	totalWeight := 0
	totalServers := len(ss)

	for i := range sw {
		totalWeight += sw[i]
	}

	var rings entries
	var addrs []net.Addr

	for i, server := range ss {
		weight := sw[i]
		count := entryCount(weight, totalServers, totalWeight)

		addr, err := nodeAddr(server)
		if err != nil {
			return nil, err
		}

		for i := 0; i < count; i++ {
			rings = append(rings, buildEntry(server, addr, weight, i))
		}

		addrs = append(addrs, addr)
	}

	sort.Sort(rings)

	return &Ring{addrs: addrs, rings: rings, option: option}, nil
}

func extract(servers map[string]int) ([]string, []int) {
	var ss []string
	var sw []int

	for server, weight := range servers {
		ss = append(ss, server)
		sw = append(sw, weight)
	}

	return ss, sw
}

func mapServer(ss []string) map[string]int {
	ms := map[string]int{}

	for _, s := range ss {
		w := 1
		q := strings.SplitN(s, ":", 3)

		if len(q) == 3 {
			w, _ = strconv.Atoi(q[2])
			s = strings.Join(q[:2], ":")
		}

		ms[s] = w
	}

	return ms
}

func hash(key string) uint {
	return uint(crc32.ChecksumIEEE([]byte(key)))
}

// Taken from: https://github.com/dgryski/go-ketama/blob/master/ketama.go
func search(ring entries, h uint) uint {
	var maxp = uint(len(ring))
	var lowp = uint(0)
	var highp = maxp

	for {
		midp := (lowp + highp) / 2
		if midp >= maxp {
			if midp == maxp {
				midp = 1
			} else {
				midp = maxp
			}

			return midp - 1
		}

		midval := ring[midp].point

		var midval1 uint

		if midp == 0 {
			midval1 = 0
		} else {
			midval1 = ring[midp-1].point
		}

		if h <= midval && h > midval1 {
			return midp - 1
		}

		if midval < h {
			lowp = midp + 1
		} else {
			highp = midp - 1
		}

		if lowp > highp {
			return 0
		}
	}
}

func buildEntry(label string, addr net.Addr, weight int, index int) entry {
	return entry{
		node:  node{addr: addr, weight: weight},
		point: serverPoint(label, index),
	}
}

func entryCount(weight, totalServers, totalWeight int) int {
	return (totalServers * 160 * weight) / int(math.Floor(float64(totalWeight)))
}

func serverPoint(server string, index int) uint {
	c := sha1.New()
	io.WriteString(c, fmt.Sprintf("%s:%d", server, index))

	hash := hex.EncodeToString(c.Sum(nil))
	hx := "0x" + hash[0:8]

	d, _ := strconv.ParseUint(hx, 0, 64)

	return uint(d)
}

func nodeAddr(node string) (net.Addr, error) {
	if strings.Contains(node, "/") {
		return net.ResolveUnixAddr("unix", node)
	}

	return net.ResolveTCPAddr("tcp", node)
}

func isAlive(addr net.Addr) bool {
	conn, err := net.Dial(addr.Network(), addr.String())
	if err != nil {
		return false
	}
	defer conn.Close()

	_, err = conn.Read([]byte{})
	if err != nil {
		return false
	}

	return true
}
