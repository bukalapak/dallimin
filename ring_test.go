package dallimin_test

import (
	"encoding/json"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"

	"github.com/bukalapak/dallimin"
	"github.com/stretchr/testify/assert"
)

type Result struct {
	Server string `json:"server"`
	Key    string `json:"key"`
	Weight int    `json:"weight"`
}

type Fixture struct {
	Results []Result
	Servers []string
	Keys    []string
}

func loadFixture(fname string) Fixture {
	file, err := ioutil.ReadFile(fname)
	panicErr(err)

	var fixture Fixture

	err = json.Unmarshal(file, &fixture)
	panicErr(err)

	return fixture
}

func panicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func TestServers(t *testing.T) {
	f := loadFixture("fixtures/keys.json")
	h, err := dallimin.New(f.Servers, dallimin.Option{})
	assert.Nil(t, err)
	assert.Len(t, h.Servers(), 3)

	xs := []string{
		"127.0.0.1:11210",
		"127.0.0.1:11211",
		"127.0.0.1:11212",
	}

	for _, addr := range h.Servers() {
		assert.Contains(t, xs, addr.String())
	}
}

func TestPickServer(t *testing.T) {
	f := loadFixture("fixtures/keys.json")
	h, _ := dallimin.New(f.Servers, dallimin.Option{})

	for _, data := range f.Results {
		addr, err := h.PickServer(data.Key)
		server := strings.Split(data.Server, ":")

		assert.Nil(t, err)
		assert.Equal(t, "127.0.0.1:"+server[1], addr.String())
	}
}

func TestPickServer_inlineWeights(t *testing.T) {
	f := loadFixture("fixtures/keys-with-weights.json")
	h, _ := dallimin.New(f.Servers, dallimin.Option{})

	for _, data := range f.Results {
		addr, err := h.PickServer(data.Key)
		server := strings.Split(data.Server, ":")

		assert.Nil(t, err)
		assert.Equal(t, "127.0.0.1:"+server[1], addr.String())
	}
}

func TestPickServer_withWeights(t *testing.T) {
	f := loadFixture("fixtures/keys-with-weights.json")
	s := map[string]int{}

	for _, server := range f.Servers {
		w := strings.Split(server, ":")[2]
		v := strings.TrimSuffix(server, ":"+w)

		n, _ := strconv.Atoi(w)

		s[v] = n
	}

	h, _ := dallimin.NewWithWeights(s, dallimin.Option{})

	for _, data := range f.Results {
		addr, err := h.PickServer(data.Key)
		server := strings.Split(data.Server, ":")

		assert.Nil(t, err)
		assert.Equal(t, "127.0.0.1:"+server[1], addr.String())
	}
}

func TestPickServer_singleServer(t *testing.T) {
	s := []string{"127.0.0.1:11211"}
	h, _ := dallimin.New(s, dallimin.Option{})

	addr, err := h.PickServer("api:foo")

	assert.Nil(t, err)
	assert.Equal(t, "127.0.0.1:11211", addr.String())
}

func TestPickServer_noServer(t *testing.T) {
	s := []string{}
	h, _ := dallimin.New(s, dallimin.Option{})

	addr, err := h.PickServer("api:foo")

	assert.Equal(t, err, dallimin.ErrNoServers)
	assert.Nil(t, addr)
}

func TestPickServer_whenNoServerAlive(t *testing.T) {
	s := []string{
		"127.0.0.1:12345",
		"127.0.0.1:12346",
	}

	h, _ := dallimin.New(s, dallimin.Option{CheckAlive: true})

	addr, err := h.PickServer("api:foo")
	assert.Equal(t, err, dallimin.ErrNoServers)
	assert.Nil(t, addr)
}

func TestPickServer_whenAtLeastOneAlive(t *testing.T) {
	s := []string{
		"127.0.0.1:12345",
		"127.0.0.1:11211",
	}

	h, _ := dallimin.New(s, dallimin.Option{CheckAlive: true})

	addr, err := h.PickServer("api:foo")
	assert.Nil(t, err)
	assert.Equal(t, addr.String(), "127.0.0.1:11211")
}

func TestPickServer_checkAliveAndfailover(t *testing.T) {
	s := []string{
		"127.0.0.1:12346",
		"127.0.0.1:11210",
	}

	h, _ := dallimin.New(s, dallimin.Option{CheckAlive: true, Failover: true})

	addr, err := h.PickServer("api:foo")
	assert.Nil(t, err)
	assert.Equal(t, addr.String(), "127.0.0.1:11210")
}
