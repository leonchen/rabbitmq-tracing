fs = require 'fs'
yaml = require 'js-yaml'

CONFIG_FILE = __dirname + "/config.yaml"

module.exports = (configFile) ->
  configFile = configFile || CONFIG_FILE
  data = fs.readFileSync configFile
  return yaml.safeLoad(data)
