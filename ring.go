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
)

const (
	pointsPerServer = 160 // MEMCACHED_POINTS_PER_SERVER_KETAMA
)

var (
	ErrNoServers = errors.New("memcache: no servers configured or available")
)

type Node struct {
	Label  string
	Addr   net.Addr
	Weight int
}

type Entry struct {
	Node  Node
	Point uint
}

type Ring struct {
	addrs []net.Addr
	rings entries
}

type entries []Entry

func (c entries) Less(i, j int) bool { return c[i].Point < c[j].Point }
func (c entries) Len() int           { return len(c) }
func (c entries) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func New(servers []string) (*Ring, error) {
	h := &Ring{}

	if len(servers) == 0 {
		return h, nil
	}

	if len(servers) == 1 {
		server := servers[0]

		addr, err := nodeAddr(server)
		if err != nil {
			return nil, err
		}

		h.addrs = append(h.addrs, addr)
		h.rings = append(h.rings, buildEntry(server, addr, 1, 0))

		return h, nil
	}

	totalWeight := len(servers)
	totalServers := len(servers)

	var rings entries
	var addrs []net.Addr

	for _, server := range servers {
		weight := 1
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

	return &Ring{addrs: addrs, rings: rings}, nil
}

func (h *Ring) Each(f func(net.Addr) error) error {
	for _, a := range h.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}

func (h *Ring) PickServer(key string) (net.Addr, error) {
	if len(h.rings) == 0 {
		return nil, ErrNoServers
	}

	if len(h.rings) == 1 {
		return h.rings[0].Node.Addr, nil
	}

	x := hash(key)
	i := search(h.rings, x)

	return h.rings[i].Node.Addr, nil
}

func hash(key string) uint {
	return uint(crc32.ChecksumIEEE([]byte(key)))
}

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

		midval := ring[midp].Point

		var midval1 uint

		if midp == 0 {
			midval1 = 0
		} else {
			midval1 = ring[midp-1].Point
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

func buildEntry(label string, addr net.Addr, weight int, index int) Entry {
	return Entry{
		Node:  Node{Addr: addr, Weight: weight},
		Point: serverPoint(label, index),
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
