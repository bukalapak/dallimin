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
	Weight int
}

type Entry struct {
	Node  Node
	Point uint
}

type Ring struct {
	rings entries
}

type entries []Entry

func (c entries) Less(i, j int) bool { return c[i].Point < c[j].Point }
func (c entries) Len() int           { return len(c) }
func (c entries) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func New(servers []string) *Ring {
	h := &Ring{}

	if len(servers) == 0 {
		return h
	}

	if len(servers) == 1 {
		h.rings = append(h.rings, buildEntry(servers[0], 1, 0))

		return h
	}

	totalWeight := len(servers)
	totalServers := len(servers)

	var rings entries

	for _, server := range servers {
		weight := 1
		count := entryCount(weight, totalServers, totalWeight)

		for i := 0; i < count; i++ {
			rings = append(rings, buildEntry(server, weight, i))
		}
	}

	sort.Sort(rings)

	return &Ring{rings: rings}
}

func (h *Ring) PickServer(key string) (net.Addr, error) {
	if len(h.rings) == 0 {
		return nil, ErrNoServers
	}

	if len(h.rings) == 1 {
		return nodeAddr(h.rings[0].Node.Label)
	}

	x := hash(key)
	i := search(h.rings, x)

	return nodeAddr(h.rings[i].Node.Label)
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

func buildEntry(server string, weight int, index int) Entry {
	return Entry{
		Node:  Node{Label: server, Weight: weight},
		Point: serverPoint(server, index),
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
