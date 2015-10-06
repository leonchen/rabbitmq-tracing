Promise = require 'bluebird'

STORAGES =
  persist: require('./mongo')
  recent: require('./redis')

###
options example:
storages:
  persist:
    host: "127.0.0.1"
    port: 27017
    db: "my-traces"
  recent:
    keyPrefix: "trace"
    ttl: 1800 (seconds)  
###

class Storage
  constructor: (@options) ->
    @storages = {}
    for s, conf of @options.storages
      throw "unkown storage #{s}" unless STORAGES[s]
      @storages[s] = new STORAGES[s](conf)


  savePublish: (data) ->
    messages = @getPublishedMessages(data)
    Promise.map Object.keys(@storages), (s) =>
      Promise.map messages, (m) =>
        @storages[s].savePublishAsync(m)

  saveDeliver: (data) ->
    message = @getDeliveredMessage(data)
    Promise.map Object.keys(@storages), (s) =>
      @storages[s].saveDeliverAsync(message)

  getRecentPublish: (params) ->
    return [] unless @storages.recent
    return yield @storages.recent.getPublishedAsync(params)

  getRecentDeliver: (params) ->
    return [] unless @storages.recent
    return yield @storages.recent.getDeliveredAsync(params)


  getLog: (params) ->
    return [] unless @storages.persist
    return yield @storages.persist.getAsync(params)

  getPublishedMessages: (data) ->
    ms = []
    {properties, content} = data
    {headers} = properties
    [mId, message] = @parseMessage(headers, content)

    for routingKey, idx in headers.routing_keys
      [_, domain, app, role, event, status] = routingKey.split(".")
      m = {domain, app, role, event, status}
      m.id = mId
      m.data = message
      m.queue = headers.routed_queues[idx]
      m.exchange = headers.exchange_name
      ms.push m
    
    return ms

  getDeliveredMessage: (data) ->
    {properties, content} = data
    {headers} = properties
    [mId, message] = @parseMessage(headers, content)

    routingKey = headers.routing_keys[0]
    [_, domain, app, role, event, status] = routingKey.split(".")
    m = {domain, app, role, event, status}
    m.id = mId
    m.data = message
    m.exchange = headers.exchange_name
    return m

  parseMessage: (headers, content) ->
    mId = null
    message = content.toString()
    pros = headers.properties
    # this should be the normal usage
    mId = pros.message_id if pros.message_id
    # this is for tests since json parsing may cause huge performance issue
    if pros.content_type == "application/json"
      message = JSON.parse(message)
      mId = message.id unless mId
    return [mId, message]

  # probably for tests only
  purgeRecentPublish: (params) ->
    return unless @storages.recent
    @storages.recent.purgePublishAsync(params)
  purgeRecentDeliver: (params) ->
    return unless @storages.recent
    @storages.recent.purgeDeliverAsync(params)

  purgeLog: (params) ->
    return unless @storages.persist
    yield @storages.persist.purgeAsync(params)

  close: ->
    for n, s of @storages
      s.close()
    @storages = {}


module.exports = Storage
