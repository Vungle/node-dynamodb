var should = require('should');
var step = require('step');
var events = require('events');
var crypto = require('crypto');

var dynaTableName = 'DYNAMO_TEST_TABLE2';

var fileQ = require('../lib/fileQueue');
var ddb = require('../lib/ddb').ddb({ accessKeyId:     process.env.AWS_KEY,
                                 secretAccessKey: process.env.AWS_SECRET,
                                 queue: fileQ.queue({ filePath: './test/dynaQueue' }) });

var options = {};
var queuedCount = 0;
var successCount = 0;
var items = [];
var itemsDir = {};

var generateNewItem = function(i) {
  var item = { 
    data: 'Bryant is cool',
    order: i
  };

  var rand = Math.random() * 10000000;
  item.sha = crypto.createHash('md5').update(rand + item.data + item.order).digest('hex');
  return item;
}

var add = function(i, ee) {
  var item = generateNewItem(i);
  console.log('adding: %s', item.sha);
  ddb.putItem(dynaTableName, item, options, function(err, res, cap) {
    if(err) {
      if (err.queued) {
        console.log('%d PutItem queued: %s', item.order, JSON.stringify(item));
        queuedCount++;
        ee.emit('done', item);
      }
      else {
        throw err;
      }
    }
    else {
      console.log('%d PutItem success: %s', item.order, JSON.stringify(item));
      successCount++;
      ee.emit('done', item);
    }
  });

  items.push(item);
  items[item.sha] = item;
};

var get = function(hash, ee) {
  ddb.getItem(dynaTableName, hash, null, {}, function(err, res, cap) {
    if (err) {
      if (err.queued) {
        
      }
      else {
        throw err;
      }
    }
    else {
      console.log('GetItem success: %s', JSON.stringify(res));
      ee.emit('done', res);
    }
  });
};

var ITEMS = 5;

/**
 * This test exercises the PutItem's fileQueue fire and forget logic.
 * ITEMS should be set at least 2X your provisioned read and write throughput
 */
describe('fileQueue', function() {
  describe(ITEMS + ' operations', function() {
    it('processes all ' + ITEMS + ' put operations', function(done) {
  	  this.timeout(ITEMS * 700);
      var i = 0;
      var countEventEmitter = new events.EventEmitter();
      var dones = 0;
      countEventEmitter.on('done', function(item) {
      	dones++;
        console.log('put done: %d', dones);
      	if (dones == ITEMS) {
      		setTimeout(function() {
            return done();
          }, ITEMS * 200);
      	}
      });
      for (; i < ITEMS; i++) {
      	add(i, countEventEmitter);
      }
    });

    it('gets all ' + ITEMS + ' puts', function(done) {
      this.timeout(4000);
      var countEventEmitter = new events.EventEmitter();
        var dones = 0;
        countEventEmitter.on('done', function(item) {
          console.log('get done: %d', dones)
          dones++;
          if (dones == ITEMS) {
            console.log('finished getting %d items', ITEMS);
            done();
          }
        });
    	for(var i in items) {
        var item = items[i];
        if (!item || !item.sha) {
          continue;
        }
        get(item.sha, countEventEmitter);
      }
    });
  });
});