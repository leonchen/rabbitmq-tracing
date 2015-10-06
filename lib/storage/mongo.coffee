Promise = require 'bluebird'
MongoDB = require 'mongodb'
Promise.promisifyAll(MongoDB)
{MongoClient} = MongoDB

class MongoStorage
  constructor: (@options) ->
    @dbs = {}
    @collections = {}

  savePublish: (m, cb) ->
    {domain, app, role, event, status, id, data} = m
    @getCollection domain, app, role, (err, coll) =>
      return cb(err) if err
      update = {id, event, status, message: data}
      update.publishedAt = Date.now()
      coll.update {id}, {$set: update, $unset: {deliveredAt: ""}}, {upsert: true}, cb

  saveDeliver: (m, cb) ->
    {domain, app, role, event, status, id} = m
    @getCollection domain, app, role, (err, coll) =>
      return cb(err) if err
      update =
        deliveredAt: Date.now()
      coll.update {id}, {$set: update}, {upsert: true}, cb

  get: (params, cb) ->
    {domain, app, role, event, status} = params
    @getCollection domain, app, role, (err, coll) =>
      return cb(err) if err
      q = {event, status}
      coll.find(q).toArray(cb)

  getCollection: (domain, app, role, cb) ->
    @collections[domain] = {} unless @collections[domain]
    @collections[domain][app] = {} unless @collections[domain][app]
    return cb(null, @collections[domain][app][role]) if @collections[domain][app][role]
    @getDB domain, (err, db) =>
      return cb(err) if err
      coll = db.collection "#{app}_#{role}"
      @ensureIndexes coll
      @collections[domain][app][role] = coll
      cb(null, coll)

  getDB: (domain, cb) ->
    {host, port, db} = @options
    dbName = db || "my-#{domain}-traces"
    return cb(null, @dbs[dbName]) if @dbs[dbName]
    url = "mongodb://#{host}:#{port}/#{dbName}"
    MongoClient.connect url, (err, db) =>
      return cb(err) if err
      @dbs[dbName] = db
      cb(null, db)

  ensureIndexes: (coll) ->
    coll.ensureIndex {event: 1, status: 1, id: 1}, ->
    coll.ensureIndex {publishedAt: 1}, ->
    coll.ensureIndex {deliveredAt: 1}, ->

  purge: (params, cb) ->
    {domain, app, role} = params
    @getCollection domain, app, role, (err, coll) =>
      return cb(err) if err
      coll.drop(cb)

  close: ->
    for db in @dbs
      db.close()
    @dbs = {}
    @collections = {}

Promise.promisifyAll(MongoStorage.prototype)
module.exports = MongoStorage
