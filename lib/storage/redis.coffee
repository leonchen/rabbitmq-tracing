Promise = require 'bluebird'
redis = require 'redis'

###
stores message id and timestamp in redis sorted list, with the timestamp as the score, and removes the outofrange elements when adding new data depends on the given ttl
for same ids, it will only store the most recent one
###

class RedisStorage
  constructor: (@options) ->
    @ttl = @options.ttl
    @client = redis.createClient(@options.redis || {})

  savePublish: (data, cb) ->
    @save(data, 'publish', cb)

  saveDeliver: (data, cb) ->
    @save(data, 'deliver', cb)

  save: (data, type, cb) ->
    {domain, app, role, event, status, id} = data
    key = @getKey(domain, app, role, event, status, type)
    now = Date.now()
    @client.zadd key, now, id, (err) =>
      return cb(err) if err
      @client.zremrangebyscore key, 0, now-@ttl*1000, cb
    

  getPublished: (params, cb) ->
    @get(params, 'publish', cb)

  getDelivered: (params, cb) ->
    @get(params, 'deliver', cb)

  get: (params, type, cb) ->
    {domain, app, role, event, status, from, to} = params
    key = @getKey(domain, app, role, event, status, type)
    to = to || "+inf"
    @client.zrevrangebyscore key, to, from, "WITHSCORES", (err, resp) =>
      return cb(err) if err
      data = []
      l = resp.length
      i = 0
      until i >= l
        data.push {id: resp[i], t: parseInt(resp[i+1])}
        i+=2
      cb(null, data)


  purgePublish: (params, cb) ->
    @purge(params, 'publish', cb)

  purgeDeliver: (params, cb) ->
    @purge(params, 'deliver', cb)

  purge: (params, type, cb) ->
    {domain, app, role, event, status, from, to, id} = params
    key = @getKey(domain, app, role, event, status, type)
    @client.del key, cb

  getKey: (domain, app, role, event, status, type) ->
    return [@options.keyPrefix, type, domain, app, role, event, status].join ":"

  close: ->
    @client.end()

Promise.promisifyAll(RedisStorage.prototype)
module.exports = RedisStorage
