# dallimin

[![Build Status](https://travis-ci.org/subosito/dallimin.svg?branch=master)](https://travis-ci.org/subosito/dallimin)
[![GoDoc](https://godoc.org/github.com/subosito/dallimin?status.svg)](https://godoc.org/github.com/subosito/dallimin)

Dalli Ring written in Go.

## Usage

Say, we have ruby program that use [dalli](https://github.com/petergoldstein/dalli) to save our data to memcache:

```ruby
require 'dalli'
require 'json'

options = { namespace: 'api', compress: true, serializer: JSON }
servers = %w(
  cache1.example.com:11210
  cache2.example.com:11211
  cache3.example.com:11212
)

client = Dalli::Client.new(servers, options)

client.set('v1/foo', foo: 'bar')
client.set('say:hello', 'World!')

puts client.get('v1/foo')
puts client.get('say:hello')
```

Then, we can access that within Go by using:

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/subosito/dallimin"
)

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	ss, err := dallimin.New([]string{
		"cache1.example.com:11210",
		"cache2.example.com:11211",
		"cache3.example.com:11212",
	})

	checkErr(err)

	client := memcache.NewFromSelector(ss)

	type Data struct {
		Foo string `json:"foo"`
	}

	it, err := client.Get("api:v1/foo")
	checkErr(err)

	a := &Data{}
	b := bytes.NewReader(it.Value)
	j := json.NewDecoder(b)
	checkErr(err)

	err = j.Decode(a)
	checkErr(err)

	fmt.Printf("%s => %#v\n", it.Key, a) // RETURNS: api:v1/foo => &main.Data{Foo:"bar"}

	it, err = client.Get("api:say:hello")
	checkErr(err)

	fmt.Printf("%s => %s\n", it.Key, string(it.Value)) // RETURNS: api:say:hello => "World!"
}
```

That's it.
