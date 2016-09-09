require 'dalli'
require 'json'

data = {
  results: {},
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

def generate_results(servers, keys)
  servers = servers.map { |server| Dalli::Server.new(server) }
  ring = Dalli::Ring.new(servers, {})

  keys.map do |key|
    server = ring.server_for_key(key)

    hash = {}
    hash[:key] = key
    hash[:server] = server.name
    hash[:weight] = server.weight
    hash
  end
end

File.open('keys.json', 'w') do |f|
  data = data.merge(servers: %w(
    cache1.lvh.me:11210
    cache2.lvh.me:11211
    cache3.lvh.me:11212
  ))

  data[:results] = generate_results(data[:servers], data[:keys])

  f.write(JSON.pretty_generate(data))
end

File.open('keys-with-weights.json', 'w') do |f|
  data = data.merge(servers: %w(
    cache1.lvh.me:11210:20
    cache2.lvh.me:11211:25
    cache3.lvh.me:11212:10
  ))

  data[:results] = generate_results(data[:servers], data[:keys])

  f.write(JSON.pretty_generate(data))
end
