var fs = require('fs');
var events = require('events');
var fwk = require('fwk');
var crypto = require('crypto');

var queue = function(spec, my) {
  my = my || {};
  my.salt = my.salt || 'super-monkey-bonanzas';
  var _super = {};
  my.filePath = spec.filePath;
  my.retryInterval = 5; // 5 seconds to wait before retry-ing

  var that = new events.EventEmitter();
  that.setMaxListeners(0);

  var fileEventsMapping = {};
  var retryCounts = {};
  var callbackMapping = {};
  var fileEvents = new events.EventEmitter();

  // Create the directory
  fs.mkdir(my.filePath, 0777, function(err) {
  });

  // public methods
  var enqueue;

  // private methods
  var createFilename;

  init = function(ddb) {
    my.ddb = ddb;
    fs.readdir(my.filePath, function(err, files) {
      if (err) throw err;
      var i = 0;
      for (; i < files.length; i++) {
        var file = files[i];

        constructNewEvent(file);
        fileEvents.emit(file, file);
      }
    });
  };

  enqueue = function(op, data, cb) {
    var nfo = { op: op, data: data };
    var filename = createFilename(op, data);
    fs.writeFile(my.filePath + "/" + filename, JSON.stringify(nfo), function(err) {
      if (err) {
        console.error(err);
        throw err;
      }

      console.log('%s queued op:%s', filename, op);
      constructNewEvent(filename, cb);
      fileEvents.emit(filename, filename);
    });
  };

  constructNewEvent = function(filename, cb) {
    fileEventsMapping[filename] = retry;
    fileEvents.on(filename, retry);
    retryCounts[filename] = 0;
    callbackMapping[filename] = cb;
  };

  retry = function(filename) {
    console.log('retrying %s', filename);
    fs.readFile(my.filePath + '/' + filename, 'utf-8', function(err, data) {
      if (err) throw err;
      var request = false;
      try {
        request = JSON.parse(data);  
      } catch (err) {
        console.error(err);
        throw err;
      }
      
      if (!request || !request.op || !request.data) {
        console.error('malformed request');
        deleteFile(filename);
      }
      
      var postExec = function(err, json) {
        if (err && err.statusCode >= 400) {

          retryCounts[filename]++;
          var timeout = Math.pow(my.retryInterval, retryCounts[filename]);
          console.log('failed again: %s, new retry in %d seconds', file, timeout);

          setTimeout(function() {
            fileEvents.emit(filename, filename);
          }, timeout * 1000);
        }
        else {
          console.log('retried %s times. success: %s', retryCounts[filename], filename);
          deleteFile(filename);

          var cb = callbackMapping[filename];
          if (cb) {
            cb(null, json);
          }
        }
      };

      my.ddb.execute(request.op, request.data, postExec, true);
    });
  };

  deleteFile = function(filename) {
    if (!filename) {
      return;
    }

    fs.unlink(my.filePath + "/" + filename, function(err) {
      if (err) {
        // if (err.code === 'ENOENT') {
        //   return console.warn(err);
        // }

        throw err;
      }

      console.log('deleted: %s', filename);
      fileEvents.removeListener(filename, fileEventsMapping[filename]);
      delete fileEventsMapping[filename];
    });
  };

  createFilename = function(op, data) {
    var timestamp = new Date().getTime();
    var sha = crypto.createHash('sha256');
    sha.update(new Buffer(op + JSON.stringify(data) + timestamp,'utf8'));
    var hmac = crypto.createHmac('md5', my.salt);
    hmac.update(sha.digest());     
    var hash = hmac.digest(encoding='hex');

    return timestamp + '-' + hash + '.json';
  };

  fwk.method(that, 'enqueue', enqueue, _super);
  fwk.method(that, 'init', init, _super);

  return that;
};

exports.queue = queue;
