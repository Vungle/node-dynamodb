# node.js dynamodb driver

This library is a fork off of https://github.com/spolu/node-dynamodb, which was a great start. This library supports queuing of PutItem operations, which is essential for any data-critical web application where writes must succeed 100%.

I've changed the organization of the main class, "ddb", to not use the fwk library, which is dangerous in node.js environments. 

Please see the mocha tests for usage examples.

-sansMischevia
