var should = require('should');
var step = require('step');
var events = require('events');

var dynaTableName = 'Installs';

var fileQ = require('../lib/fileQueue');
var ddb = require('../lib/ddb').ddb({ accessKeyId:     process.env.AWS_KEY,
                                 secretAccessKey: process.env.AWS_SECRET,
                                 queue: fileQ.queue({ filePath: './test/dynaQueue' }) });

console.log('checking table ...');
ddb.describeTable(dynaTableName, function(err, res) {
  console.log(res);
});
console.log('finished checking table ...');

var options = null;
var queuedCount = 0;
var successCount = 0;
var items = [];
var itemsDir = {};
var add = function(i, ee) {
  var item = { 
    isu: (new Date().getTime() + Math.random()).toString(),
    data: 'Bryant is cool',
    order: false
  };
  item.order = i;
  console.log('adding: %s', item.isu);
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
  items[item.isu] = item;
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
    }
  });
};

var ITEMS = 100;

// Start test description
describe('fileQueue', function() {
  describe('100 operations', function() {
    it('processes all 100 put operations', function(done) {
	  this.timeout(120000);
      var i = 0;
      var countEventEmitter = new events.EventEmitter();
      var dones = 0;
      countEventEmitter.on('done', function(item) {
      	dones++;
        console.log('put done: %d', dones);
      	if (dones == ITEMS) {
      		console.log('finished');
      		done();
      	}
      });
      for (; i < ITEMS; i++) {
      	add(i, countEventEmitter);
      }
    });

    it('gets all 100 puts', function(done) {
      this.timeout(120000);
      var countEventEmitter = new events.EventEmitter();
        var dones = 0;
        countEventEmitter.on('done', function(item) {
          console.log('get done: %d', dones)
          dones++;
          if (dones == ITEMS) {
            console.log('finished getting 100 items');
            done();
          }
        });
    	for(var i in items) {
        var item = items[i];
        if (!item || !item.isu) {
          continue;
        }
        get(item.isu, countEventEmitter);
      }
    });
  });
});


