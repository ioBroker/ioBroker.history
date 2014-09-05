# ioBroker.history

This adapter saves state history in a two-staged process. At first datapoints are stored in Redis, as soon as they
reach maxLength they are trimmed to minLength and moved to CouchDB.