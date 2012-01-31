var should = require('should');
var step = require('step');
var events = require('events');

var dynaTableName = 'Installs';

var fileQ = require('../lib/fileQueue');
var ddb = require('../lib/ddb').ddb({ accessKeyId:     process.env.AWS_KEY,
                                 secretAccessKey: process.env.AWS_SECRET,
                                 queue: fileQ.queue({ filePath: './dynaQueue' }) });

ddb.describeTable(dynaTableName, function(err, res) {
  console.log(res);
});

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
      console.log('GetItem success: %s', res);
    }
  });
};

var ITEMS = 100;

// Start test description
describe('fileQueue', function() {
  describe('100 put operations', function() {
    it('processes all 100 put operations', function(done) {
	  this.timeout(120000);
      var i = 0;
      var countEventEmitter = new events.EventEmitter();
      var dones = 0;
      countEventEmitter.on('done', function(item) {
      	dones++;
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
    	for(var item in items) {
        var countEventEmitter = new events.EventEmitter();
        var dones = 0;
        countEventEmitter.on('done', function(item) {
          if (dones == ITEMS) {
            console.log('finished getting 100 items');
            done();
          }
        });
        getI(item.isu, ee);
      }
    });
  });
});


