const { MongoClient } = require('mongodb');

class MongoDBStorage {
    constructor(url, dbName) {
        this.url = url;
        this.dbName = dbName;
        this.client = null;
        this.db = null;
    }

    async connect() {
        try {
            this.client = await MongoClient.connect(this.url);
            this.db = this.client.db(this.dbName);
            return true;
        } catch (error) {
            console.error(`Error connecting to MongoDB: ${error}`);
            return false;
        }
    }

    async close() {
        if (this.client) {
            await this.client.close();
        }
    }

    async storeValue(id, state) {
        if (!this.db) return;
        
        const collection = this.db.collection('history');
        const entry = {
            id,
            ts: new Date(state.ts),
            val: state.val,
            ack: state.ack || false,
            from: state.from,
            q: state.q || 0
        };

        try {
            await collection.insertOne(entry);
            return true;
        } catch (error) {
            console.error(`Error storing value in MongoDB: ${error}`);
            return false;
        }
    }

    async getHistory(id, options) {
        if (!this.db) return [];

        const collection = this.db.collection('history');
        const query = {
            id,
            ts: {}
        };

        if (options.start) {
            query.ts.$gte = new Date(options.start);
        }
        if (options.end) {
            query.ts.$lte = new Date(options.end);
        }

        if (!Object.keys(query.ts).length) {
            delete query.ts;
        }

        try {
            const result = await collection.find(query)
                .sort({ ts: options.sort || 1 })
                .toArray();

            return result.map(entry => ({
                ts: entry.ts.getTime(),
                val: entry.val,
                ack: entry.ack,
                from: entry.from,
                q: entry.q
            }));
        } catch (error) {
            console.error(`Error getting history from MongoDB: ${error}`);
            return [];
        }
    }

    async storeState(id, state) {
        return this.storeValue(id, state);
    }
}

module.exports = MongoDBStorage;
