var fileQ = require('../lib/fileQueue');
var ddb = require('../lib/ddb').ddb({ accessKeyId:     process.env.AWS_KEY,
                                 secretAccessKey: process.env.AWS_SECRET,
                                 queue: fileQ.queue({ filePath: '../examples/dynaQueue' }) });

ddb.describeTable('Installs', function(err, res) {
  console.log(res);
});

var options = null;
var items = [];
var ITEMS_TO_ADD = 100;
var queuedCount = 0;
var successCount = 0;

var add = function(i) {
  var item = { 
    isu: (new Date().getTime() + Math.random()).toString(),
    data: 'Bryant is cool',
    order: false
  };
  item.order = i;
  ddb.putItem('Installs', item, options, function(err, res, cap) {
    if(err) {
      if (err.queued) {
        console.log('%d PutItem queued: %s', item.order, JSON.stringify(item));
        queuedCount++;
      }
    }
    else {
      console.log('%d PutItem success: %s', item.order, JSON.stringify(item));
      successCount++;
    }

    items[item.isu] = item;
  });
};

var assertKeys = function(items, done) {
  for(var i in items) {
    var item = items[i];
    if (!item || !item.isu) {
      continue;
    }
    
    var isu = item.isu;
    console.log('getting: ' + isu);
    
    ddb.getItem('Installs', isu, null, {}, function(err, res, cap) {
      if (err) {
        if (err.queued) {
          console.log('getItem request queued');
        }
        else {
          console.log('error: ' + err);
        }
      }
      else {
        console.log('GetItem success: isu: %s res:%s', res.isu, JSON.stringify(res));
        item.found = true;
      }

      if (i == ITEMS_TO_ADD - 1) {
        done();
      }
    });
  }
};

var i = 0;
for(;i < ITEMS_TO_ADD; i++) {
  add(i);
}

// var wait = function() {
//   // console.log('waiting...');
// };

// while(items.length < ITEMS_TO_ADD) {
//   wait();
// }

setTimeout(function() {
  console.log('starting key assertions');
  assertKeys(items, function() {
    for(var i in items) {
      var item = items[i];
      if (!item.found) {
        console.error('missed: %s' + item.isu);
      }
    }
  });  
}, 9000);