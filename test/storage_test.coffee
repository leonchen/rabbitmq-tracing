should = require 'should'
{execSync} = require 'child_process'
amqp = require 'amqplib'
merge = require 'merge'
Tracer = require 'amqp-tracer'
Promise = require 'bluebird'

Storage = require '../lib/storage'
config = require '../config'

# ensure trace is on for rabbitmq
execSync "rabbitmqctl trace_on"

testQueue = "my.test.queue"
testExchange = "my.test.exchange"
routingParams =
  domain: "nba"
  app: "testApp"
  role: "server"
  event: "testEvent"
  status: "ready"
routingKey = "my"
for k, v of routingParams
  routingKey += ".#{v}"


publish = (ch, msg) ->
  ch.publish(testExchange, routingKey, new Buffer(JSON.stringify(msg)), {contentType: "application/json", messageId: msg.id})

consume = (ch, cb) ->
  ch.consume testQueue, (msg) ->
    cb(msg)
    setTimeout ->
      ch.ack(msg)
    , 5

describe "storage", ->
  
  describe "recent", ->
    redisConf = config(__dirname+"/test_redis.yaml")
    redisStorage = new Storage(redisConf.storage)

    ch = null
    conn = null
    tracer = null
    ttl = redisConf.storage.storages.recent.ttl

    before ->
      # binding to tracer
      tracer = new Tracer()
      tracer.on 'message.published', (data) ->
        redisStorage.savePublish(data)
      tracer.on 'message.delivered', (data) ->
        redisStorage.saveDeliver(data)
      tracer.on 'error', (err) ->
        console.warn err

      conn = yield amqp.connect()
      ch = yield conn.createChannel()
      yield ch.assertExchange(testExchange, 'topic')
      yield ch.assertQueue(testQueue)
      yield ch.bindQueue(testQueue, testExchange, routingKey)
      yield ch.purgeQueue(testQueue)

      redisStorage.purgeRecentPublish(routingParams)

    after ->
      tracer.close()
      ch.close()
      conn.close()
      redisStorage.purgeRecentPublish(routingParams)
      redisStorage.close()

    it "should load configs", ->
      redisStorage.storages.recent.should.be.ok
      should(redisStorage.storages.persist).not.be.ok
      redisStorage.storages.recent.ttl.should.equal(ttl)

    it "should save published data to recent storage", ->
      @timeout ttl*2000

      msg = {id: "testId", text: "test message"}
      params = merge {}, routingParams
      params.from = Date.now()

      publish(ch, msg)
      # waiting 200ms for consuming/insertion
      yield Promise.delay(200)
      data = yield redisStorage.getRecentPublish(params)
      data.length.should.equal(1)
      data[0].id.should.equal("testId")
      t1 = data[0].t

      publish(ch, msg)
      yield Promise.delay(200)
      data = yield redisStorage.getRecentPublish(params)
      # only one record for one id
      data.length.should.equal(1)
      data[0].id.should.equal("testId")
      data[0].t.should.above(t1)

      msg.id = "testId2"
      publish(ch, msg)
      yield Promise.delay(200)
      data = yield redisStorage.getRecentPublish(params)
      data.length.should.equal(2)
      data[0].id.should.equal("testId2")
      data[1].id.should.equal("testId")
      data[0].t.should.above(data[1].t)

      # expired
      yield Promise.delay(ttl * 1000)
      msg.id = "testId3"
      publish(ch, msg)
      yield Promise.delay(200)
      data = yield redisStorage.getRecentPublish(params)
      data.length.should.equal(1)
      data[0].id.should.equal("testId3")

    it "should save delivered data to recent storage", ->
      @timeout ttl*2000

      count = 0
      params = merge {}, routingParams
      params.from = Date.now()

      consume ch, (msg) ->
        count++
      yield Promise.delay(200)
      count.should.be.equal(4)

      data = yield redisStorage.getRecentDeliver(params)
      # 2 published messages with the same id in the test above
      data.length.should.equal(3)

      yield Promise.delay(ttl * 1000)
      msg = {id: "testId4", text: "test message"}
      publish(ch, msg)
      yield Promise.delay(200)
      count.should.be.equal(5)
      data = yield redisStorage.getRecentDeliver(params)
      data.length.should.equal(1)
      data[0].id.should.equal("testId4")


  describe "persist", ->
    mongoConf = config(__dirname+"/test_mongo.yaml")
    mongoStorage = new Storage(mongoConf.storage)
    ch = null
    conn = null
    tracer = null

    before ->
      tracer = new Tracer()
      tracer.on 'message.published', (data) ->
        mongoStorage.savePublish(data)
      tracer.on 'message.delivered', (data) ->
        mongoStorage.saveDeliver(data)

      conn = yield amqp.connect()
      ch = yield conn.createChannel()
      yield ch.assertExchange(testExchange, 'topic')
      yield ch.assertQueue(testQueue)
      yield ch.bindQueue(testQueue, testExchange, routingKey)
      yield ch.purgeQueue(testQueue)

    after ->
      tracer.close()
      ch.close()
      conn.close()
      mongoStorage.purgeLog(routingParams)
      mongoStorage.close()

    it "should load configs", ->
      mongoStorage.storages.persist.should.be.ok
      should(mongoStorage.storages.recent).not.be.ok

    it "should save data to persist storage", ->
      msg = {id: "testId", text: "test message"}
      params = merge {}, routingParams

      publish(ch, msg)
      yield Promise.delay(100)
      data = yield mongoStorage.getLog(params)
      data.length.should.equal(1)
      m = data[0]
      m.id.should.equal("testId")
      m.event.should.equal("testEvent")
      m.status.should.equal("ready")
      m.message.text.should.equal("test message")
      m.publishedAt.should.be.ok
      should(m.deliveredAt).not.be.ok

      count = 0
      consume ch, (msg) ->
        count++
      yield Promise.delay(200)
      count.should.be.equal(1)
      data = yield mongoStorage.getLog(params)
      data.length.should.equal(1)
      m = data[0]
      m.publishedAt.should.be.ok
      m.deliveredAt.should.be.ok
      m.deliveredAt.should.above(m.publishedAt)

    it "should updates the timestamp for publish", ->
      # stop consuming
      ch.close()
      ch = yield conn.createChannel()
      msg = {id: "testId", text: "test message"}
      params = merge {}, routingParams
      publish(ch, msg)
      yield Promise.delay(100)
      data = yield mongoStorage.getLog(params)
      data.length.should.equal(1)
      m = data[0]
      m.id.should.equal("testId")
      m.publishedAt.should.be.ok
      should(m.deliveredAt).not.be.ok

  describe "multi", ->
    multiConf = config(__dirname+"/test.yaml")
    multiStorage = new Storage(multiConf.storage)

    ch = null
    conn = null
    tracer = null

    before ->
      tracer = new Tracer()
      tracer.on 'message.published', (data) ->
        multiStorage.savePublish(data)
      tracer.on 'message.delivered', (data) ->
        multiStorage.saveDeliver(data)

      conn = yield amqp.connect()
      ch = yield conn.createChannel()
      yield ch.assertExchange(testExchange, 'topic')
      yield ch.assertQueue(testQueue)
      yield ch.bindQueue(testQueue, testExchange, routingKey)
      yield ch.purgeQueue(testQueue)

    after ->
      tracer.close()
      ch.close()
      conn.close()
      multiStorage.purgeLog(routingParams)
      multiStorage.close()

    it "should load configs", ->
      multiStorage.storages.persist.should.be.ok
      multiStorage.storages.recent.should.be.ok

    it "should save data to persist storage", ->
      msg = {id: "testId", text: "test message"}
      params = merge {}, routingParams
      params.from = Date.now()

      publish(ch, msg)
      yield Promise.delay(100)

      pdata = yield multiStorage.getLog(params)
      pdata.length.should.equal(1)
      m = pdata[0]
      m.id.should.equal("testId")
      m.event.should.equal("testEvent")
      m.status.should.equal("ready")
      m.message.text.should.equal("test message")
      m.publishedAt.should.be.ok
      should(m.deliveredAt).not.be.ok

      rdata = yield multiStorage.getRecentPublish(params)
      rdata.length.should.equal(1)
      rdata[0].id.should.equal("testId")

      count = 0
      consume ch, (msg) ->
        count++
      yield Promise.delay(200)

      count.should.be.equal(1)
      pdata = yield multiStorage.getLog(params)
      pdata.length.should.equal(1)
      m = pdata[0]
      m.publishedAt.should.be.ok
      m.deliveredAt.should.be.ok
      m.deliveredAt.should.above(m.publishedAt)

      rdata = yield multiStorage.getRecentDeliver(params)
      rdata.length.should.equal(1)
      rdata[0].id.should.equal("testId")




