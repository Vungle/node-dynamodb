#!/usr/bin/env node

var program = require('commander');

program
  .version('0.0.1')
  .option('-l, --list', 'List Tables')
  .option('-s, --scan [table]', 'Scans a table')
  .option('-g, --get [table]', 'GetItem from a table')
  .option('-c, --create', 'Creates a table')
  .parse(process.argv);

var ddb = require('./lib/ddb').ddb({ accessKeyId: process.env.AWS_KEY,
                                 secretAccessKey: process.env.AWS_SECRET });

function handleResponse(err, data) {
  if (err) {
    console.log('There was an error with your request:');
    console.log(err.message);
    exit();
  }
};

function list() {
  ddb.listTables({}, function(err, res) {
    console.log('Tables: %s', JSON.stringify(res, null, '\t'));
  });  
};


function scan(table) {
  console.log('scanning: %s', program.scan);
  program.prompt('Enter attributes []: ', function(attributes) {
    program.prompt('Enter limit (10): ', Number, function(limit) {
      program.confirm('count?: ', function(count) {
        program.prompt('Enter filter {}: ', function(filter) {
          console.log('attributes: %s filter: %s', attributes, filter);
          if (attributes) {
            attributes = JSON.parse(attributes);
          }
          if (filter) {
            filter = JSON.parse(filter);
          }
          limit = limit || 10;
          var options = {
            attributesToGet: attributes,
            limit: limit,
            count: count,
            filter: filter
          };
          ddb.scan(program.scan, options, function(err, res, cap) {
            console.log('%s', JSON.stringify(res, null, '\t'));
            console.log('cap used: %s', JSON.stringify(cap, null, '\t'));
            scan(table);
          });
        });
      });
    });
  });
};

function getItem(table) {
  program.prompt('Enter key: ', function(key) {
    var start = new Date();
    ddb.getItem(table, key, null, false, function(err, item) {
      if (err) {
        return handleResponse(err, item);
      }
      console.dir(item);
      console.log('--took: %dms', new Date() - start);
      getItem(table);
    });
  });
};

if (program.list) {
  list();
}

if (program.scan) {
  scan(program.scan);
}

if (program.get) {
  getItem(program.get);
}