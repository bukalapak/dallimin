require 'dalli'
require 'json'

File.open('keys.json', 'w') do |f|
  data = {
    results: {},
    servers: %w(
      cache1.lvh.me:11210
      cache2.lvh.me:11211
      cache3.lvh.me:11212
    ),
    keys: %w(
      api:foo
      api:foo:bar
      api:bar
      api:bar:foo
      foo:info
      foo:info/bar
      foo:info/baz
    )
  }

  servers = data[:servers].map { |server| Dalli::Server.new(server) }
  ring = Dalli::Ring.new(servers, {})
  data[:results] = data[:keys].map { |key| { key: key, server: ring.server_for_key(key).name } }

  f.write(JSON.pretty_generate(data))
end
