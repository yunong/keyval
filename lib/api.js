var mod_restify = require('restify');

module.exports = {
    putKey: function putKey(datastore, req, res, next) {
        var log = req.log;
        var err;
        if (!req.params.name) {
            err = new mod_restify.InvalidArgumentError('key name must ' +
                                                             'be supplied');
            return next(err);
        }

        if (!req.params.value) {
            err = new mod_restify.InvalidArgumentError('key value must ' +
                                                             'be supplied');
            return next(err);
        }
        datastore[req.params.name] = req.params.value;
        res.send();
        return next();
    },

    getKey: function getKey(datastore, req, res, next) {
        var log = req.log;
        if (!req.params.name) {
            return next(new mod_restify.InvalidArgumentError('key name must ' +
                                                             'be supplied'));
        }
        var value = datastore[req.params.name];
        if (!value) {
            return next(new mod_restify.ResourceNotFoundError('key does not ' +
                                                              'exist'));
        }
        res.send(value);
        return next();
    },

    listKeys:  function listKeys(datastore, req, res, next) {
        var log = req.log;
        res.send(datastore);
        return next();
    }
};

