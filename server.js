var mod_bunyan = require('bunyan');
var mod_restify = require('restify');

var mod_api = require('./lib/api');
var mod_cassandra = require('./lib/cassandra.js');

(function main () {
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

    var port = 1337;
    var datastore = mod_cassandra.createDatastore({
        cassandra: {
            keyspace: 'yunong',
            table: 'urls',
            contactPoints: ['localhost'],
            host: 'localhost',
            port: 9042,
            cql_version: '3.2.0',
            username: 'cassandra',
            password: 'cassandra',
        },
        log: log
    }, function (err) {
        if (err) {
            throw err;
        }

        server.listen(port);
        log.info('server started on port ' + port);
    });


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
    server.del('/keys/:name', mod_api.delKey.bind(this, datastore));
    server.get('/keys', mod_api.listKeys.bind(this, datastore));
})();
