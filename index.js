var mod_bunyan = require('bunyan');
var mod_restify = require('restify');

var mod_api = require('./lib/api');
var log = mod_bunyan.createLogger({
    name: 'keyval',
    level: 'info',
    src: process.env.LOG_DEBUG,
    serializers: mod_bunyan.stdSerializers
});

var server = mod_restify.createServer({
    name: 'keyval',
    log: log,
    version: '1.0.0'
});

var datastore = {};

server.listen(1337);

server.use(mod_restify.queryParser());
server.on('after', mod_restify.auditLogger({
    log: mod_bunyan.createLogger({
        name: 'audit',
        stream: process.stdout
    })
}));

// API

server.put('/keys/:name', mod_api.putKey.bind(this, datastore));
server.get('/keys/:name', mod_api.getKey.bind(this, datastore));
server.get('/keys', mod_api.listKeys.bind(this, datastore));
