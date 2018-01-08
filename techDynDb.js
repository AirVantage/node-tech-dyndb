var _ = require('lodash');
var AWS = require('aws-sdk');
var BPromise = require('bluebird');
var dynTypes = require('dynamodb-data-types');
var events = require('events');
var logger = require('node-tech-logger');
var techTime = require('node-tech-time');

/**
 * Wrapper around AWS DynanomDB.
 *
 * @param configuration.dynDB.endpoint {String}
 * @param configuration.dynDB.accessKeyId {String}
 * @param configuration.dynDB.secretAccessKey {String}
 * @param configuration.dynDB.region {String}
 */
module.exports = function(configuration) {
  var dynDBOptions = {
    region: configuration.dynDB.region
  };

  // Endpoint and credentials are used only in development mode
  // In production, no endpoint is required and credentials will be automatically provided
  if (configuration.dynDB.local) {
    dynDBOptions.endpoint = new AWS.Endpoint(configuration.dynDB.endpoint);
    dynDBOptions.accessKeyId = configuration.dynDB.accessKeyId;
    dynDBOptions.secretAccessKey = configuration.dynDB.secretAccessKey;
  }
  var dyn = new AWS.DynamoDB(dynDBOptions);
  var emitter = new events.EventEmitter();

  function notify(ruuid, key, category, duration) {
    emitter.emit('dynamodb', {
      category: category,
      ruuid: ruuid || 'unknown',
      key: key,
      duration: duration
    });
  }

  function dynObjectToJson(dynObject, key) {
    // All keys are stored with types ( { foo : { 'S" : "bar" } } )
    var res = dynTypes.AttributeValue.unwrap(dynObject);

    // We now have to remove the 'key' part, since it is not JSON-encoded data
    _.forEach(key, function(v, k) {
      if (res && res[k]) {
        delete res[k];
      }
    });

    // And we assume all values are stored as String to be parsed
    // (this makes it easier to store arbitrary maps)
    res = _strMapToJson(res);
    return res;
  }

  function _strMapToJson(strMap) {
    return _.reduce(
      strMap,
      function(memo, value, key) {
        try {
          memo[key] = JSON.parse(value);
        } catch (e) {
          // NOTE(pht)
          // Typically, this is the the case where I don't know how we want to handle
          // the error.
          logger.error('Unable to parse key', e, 'of strMap', strMap);
        }
        return memo;
      },
      {}
    );
  }

  function _jsonToStrMap(json) {
    return _.reduce(
      json,
      function(memo, value, key) {
        memo[key] = JSON.stringify(value);
        return memo;
      },
      {}
    );
  }

  function wrapUpdateItem(ruuid, tableName, key, newValues) {
    var start = techTime.start();

    var dynKey = dynTypes.AttributeValue.wrap(key);

    var strMap = _jsonToStrMap(newValues);
    var dynData = dynTypes.AttributeValueUpdate.put(strMap);

    var updateParams = {
      Key: dynKey,
      AttributeUpdates: dynData,
      TableName: tableName,
      ReturnValues: 'ALL_NEW'
    };

    return new BPromise(function(resolve, reject) {
      dyn.updateItem(updateParams, function(err, data) {
        notify('ruuid', key, 'Update', techTime.end(start));

        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  /**
   * Returns an object compatible with dynamoDB
   * @param  {Object} data 
   * @param  {Array} jsonKeys   JSON content keys to be wrapped as string
   * @return {Object}
   */
  function getDynData(data, jsonKeys) {
    var jsonItems = _.pick(data, jsonKeys);
    var jsonStringItems = _jsonToStrMap(jsonItems);
    return _.assignIn(jsonStringItems, _.omit(data, jsonKeys));
  }

  /**
   * Put an item with mixed valued (String / JSON)
   * @param  {String} ruuid
   * @param  {String} tableName
   * @param  {String} key
   * @param  {Object} newValues          
   * @param  {Array} jsonKeys       List of JSON Keys
   * @return {BPromise}
   */
  function wrapUpdateMixedItem(ruuid, tableName, key, newValues, jsonKeys) {
    var start = techTime.start();

    var updateParams = {
      Key: dynTypes.AttributeValue.wrap(key),
      AttributeUpdates: dynTypes.AttributeValueUpdate.put(getDynData(newValues, jsonKeys)),
      TableName: tableName
    };

    return new BPromise(function(resolve, reject) {
      dyn.updateItem(updateParams, function(err, data) {
        notify('ruuid', key, 'Update', techTime.end(start));

        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  function wrapPutItem(ruuid, tableName, key, item) {
    var start = techTime.start();

    var dynKey = dynTypes.AttributeValue.wrap(key);

    // All flags data should be encoded as a single JSON-string
    // (otherwise, we would have to recursively specify the type of each data...)
    var strMap = _jsonToStrMap(item);
    var dynData = dynTypes.AttributeValue.wrap(strMap);

    return new BPromise(function(resolve, reject) {
      dyn.putItem(
        {
          Item: _.assignIn(dynKey, dynData),
          TableName: tableName
        },
        function(err, data) {
          notify('ruuid', key, 'Create', techTime.end(start));

          if (err) {
            reject(err);
          } else {
            resolve();
          }
        }
      );
    });
  }

  /**
   * Put an item with mixed valued (String / JSON)
   * @param  {String} ruuid
   * @param  {String} tableName
   * @param  {String} key
   * @param  {Object} item          
   * @param  {Array} jsonKeys       List of JSON Keys
   * @return {BPromise}
   */
  function wrapPutMixedItem(ruuid, tableName, key, item, jsonKeys) {
    var start = techTime.start();

    var dynKey = dynTypes.AttributeValue.wrap(key);
    var dynData = dynTypes.AttributeValue.wrap(item ? getDynData(item, jsonKeys) : {});

    return new BPromise(function(resolve, reject) {
      dyn.putItem(
        {
          Item: _.assignIn(dynKey, dynData),
          TableName: tableName
        },
        function(err, data) {
          notify('ruuid', key, 'Create', techTime.end(start));

          if (err) {
            reject(err);
          } else {
            resolve();
          }
        }
      );
    });
  }

  function wrapListTables() {
    return new BPromise(function(resolve, reject) {
      var start = techTime.start();

      dyn.listTables({}, function(err, data) {
        notify('ruuid', {}, 'Read', techTime.end(start));

        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      });
    });
  }

  function wrapCreateTable(tableDef) {
    return new BPromise(function(resolve, reject) {
      var start = techTime.start();
      dyn.createTable(tableDef, function(err, data) {
        notify('ruuid', {}, 'Create', techTime.end(start));

        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      });
    });
  }

  function wrapDeleteTable(ruuid, tableName) {
    return new BPromise(function(resolve, reject) {
      var start = techTime.start();
      dyn.deleteTable(
        {
          TableName: tableName
        },
        function(err, data) {
          notify('ruuid', {}, 'Delete', techTime.end(start));

          if (err) {
            reject(err);
          } else {
            resolve(data);
          }
        }
      );
    });
  }

  function wrapGetItem(ruuid, tableName, key) {
    var start = techTime.start();

    var dynKey = dynTypes.AttributeValue.wrap(key);

    var params = {
      Key: dynKey,
      TableName: tableName
    };

    return new BPromise(function(resolve, reject) {
      logger.debug('getItem called with params ');
      logger.debug(params);

      dyn.getItem(params, function(err, data) {
        notify('ruuid', key, 'Read', techTime.end(start));

        if (err) {
          reject(err);
        } else {
          // DynDB API returns a single "Item" object
          var item = data.Item;
          item = dynObjectToJson(item, key);
          resolve(item);
        }
      });
    });
  }

  function wrapDeleteAttribute(ruuid, tableName, key, attributeKey) {
    var start = techTime.start();

    var dynKey = dynTypes.AttributeValue.wrap(key);

    var attributeValueUpdate = {};
    attributeValueUpdate[attributeKey] = {
      Action: 'DELETE'
    };
    var params = {
      Key: dynKey,
      TableName: tableName,
      AttributeUpdates: attributeValueUpdate,
      ReturnValues: 'ALL_OLD'
    };

    return new BPromise(function(resolve, reject) {
      logger.debug('[wrapDeleteAttribute] Will update attribute with params', params);

      dyn.updateItem(params, function(err, data) {
        logger.debug('[wrapDeleteAttribute] return from update', data);

        if (err) {
          reject(err);
          return;
        }

        notify('ruuid', key, 'Update', techTime.end(start));

        var item = data.Attributes;
        item = dynObjectToJson(item, key);
        resolve(item);
      });
    });
  }

  function wrapRemoveItem(ruuid, tableName, key) {
    var start = techTime.start();

    var dynKey = dynTypes.AttributeValue.wrap(key);

    var params = {
      Key: dynKey,
      TableName: tableName
    };

    return new BPromise(function(resolve, reject) {
      dyn.deleteItem(params, function(err, data) {
        notify('ruuid', key, 'Delete', techTime.end(start));

        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  function queryTable(ruuid, params) {
    var start = techTime.start();

    return new BPromise(function(resolve, reject) {
      dyn.query(params, function(err, data) {
        if (err) {
          console.error('Unable to query. Error:', JSON.stringify(err, null, 2));
          reject(err, data);
        } else {
          notify('Query succeeded.', techTime.end(start));
          var items = [];
          data.Items.forEach(function(item) {
            items.push(dynTypes.AttributeValue.unwrap(item));
          });
          resolve(items);
        }
      });
    });
  }

  return {
    on: emitter.on.bind(emitter),
    listTables: wrapListTables,
    getItem: wrapGetItem,
    putItem: wrapPutItem,
    putMixedItem: wrapPutMixedItem,
    createTable: wrapCreateTable,
    deleteTable: wrapDeleteTable,
    deleteAttribute: wrapDeleteAttribute,
    updateItem: wrapUpdateItem,
    updateMixedItem: wrapUpdateMixedItem,
    removeItem: wrapRemoveItem,
    queryTable: queryTable,
    isLocal: function() {
      return !!(configuration.dynDB && configuration.dynDB.local);
    }
  };
};
