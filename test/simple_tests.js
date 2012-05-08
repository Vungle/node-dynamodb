var should = require('should');
var step = require('step');
var events = require('events');

var ddb = require('../lib/ddb').ddb({ accessKeyId: process.env.AWS_KEY,
                                 secretAccessKey: process.env.AWS_SECRET,
                                 sessionLength: 129600});

var dynaTableName = 'DYNAMODB_TEST_TABLE1';
var table = false;

describe('describes table', function() {
  it('returns the table specifics', function(done) {
    ddb.describeTable(dynaTableName, function(err, res) {
      should.not.exist(err);
      should.exist(res);
      res.TableName.should.equal(dynaTableName);
      console.log(JSON.stringify(res));
      table = res;
      done();
    });
  });
});

describe('list tables', function() {
  it('returns a list of tables', function(done) {
    ddb.listTables({}, function(err, res) {
      should.not.exist(err);
      // console.log('list tables: ' + JSON.stringify(res));
      should.exist(res);
      res.TableNames.length.should.be.above(0);
      done();
    });
  });
});

describe('PutItem, GetItem, then DeleteItem', function() {
  var key;
  var name;
  it('adds an item', function(done) {
    should.exist(table, "table");
    should.exist(table.KeySchema, "table.KeySchema");
    name = table.KeySchema.HashKeyElement.AttributeName;
    var item = {};
    item.sha = new Date().getTime() + "";
    key = item.sha;
    item.data = "This is  nice.";
    ddb.putItem(dynaTableName, item, {}, function(err, res, cap) {
      should.not.exist(err);
      should.not.exist(res);
      should.exist(cap);
      cap.should.equal(1);
      done();
    });
  });

  it('then retrieves an item from DynamoDb', function(done) {
    should.exist(key);
    ddb.getItem(dynaTableName, key, null, false, function(err, item) {
      should.not.exist(err);
      should.exist(item);
      item.data.should.equal("This is  nice.");
      item[name].should.equal(key);
      done();
    });
  });

  it('then retrieves an item from DynamoDb', function(done) {
    should.exist(key);
    ddb.getItem(dynaTableName, key, null, false, function(err, item) {
      should.not.exist(err);
      should.exist(item);
      item.data.should.equal("This is  nice.");
      item[name].should.equal(key);
      done();
    });
  });

  it('then deletes the item', function(done) {
    should.exist(key);
    ddb.deleteItem(dynaTableName, key, null, {}, function(err, attributes, cap) {
      should.not.exist(err);
      should.not.exist(attributes);
      done();
    });
  });

  it('then tries to get the deleted item', function(done) {
    should.exist(key);
    ddb.getItem(dynaTableName, key, null, false, function(err, item) {
      should.not.exist(err);
      should.not.exist(item);
      done();
    });
  });

  describe('scans table', function() {
    it('returns 4 items', function(done) {
      this.timeout(5000);
      var options = {
        limit: 4
      };
      ddb.scan(dynaTableName, options, function(err, res) {
        should.not.exist(err);
        should.exist(res);
        res.items.length.should.equal(0);
        res.scannedCount.should.equal(0);
        // console.log(res);
        done();
      });
    });
  });
});

// var name = 'testTable';
// describe('table manipulation', function() {
//   it('removes the table from DynamoDb', function(done) {
//     ddb.deleteTable(name, function(err, tab) {
//       should.exist(err);
//       err.type.should.equal('com.amazonaws.dynamodb.v20111205#ResourceNotFoundException');
//       done();
//     });
//   });  
  
//   it('returns the table specifics', function(done) {
//     var keySchema = {
//       hash: ['hash', 'S']
//     };
//     var provisionedThroughput = {
//       write: 5,
//       read: 5
//     }
//     ddb.createTable(name, keySchema, provisionedThroughput, function(err, res) {
//       should.not.exist(err);
//       should.exist(res);
//       // console.log(res);
//       res.TableName.should.equal(name);
//       done();
//     });
//   });
// });
