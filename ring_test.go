package dallimin_test

import (
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/subosito/dallimin"
)

type Result struct {
	Server string `json:"server"`
	Key    string `json:"key"`
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

func TestPickServer(t *testing.T) {
	f := loadFixture("fixtures/keys.json")
	h := dallimin.New(f.Servers)

	for _, data := range f.Results {
		addr, err := h.PickServer(data.Key)
		server := strings.Split(data.Server, ":")

		assert.Nil(t, err)
		assert.Equal(t, "127.0.0.1:"+server[1], addr.String())
	}
}

func TestPickServer_singleServer(t *testing.T) {
	s := []string{"127.0.0.1:11211"}
	h := dallimin.New(s)

	addr, err := h.PickServer("api:foo")

	assert.Nil(t, err)
	assert.Equal(t, "127.0.0.1:11211", addr.String())
}

func TestPickServer_noServer(t *testing.T) {
	s := []string{}
	h := dallimin.New(s)

	addr, err := h.PickServer("api:foo")

	assert.Equal(t, dallimin.ErrNoServers, err)
	assert.Nil(t, addr)
}
