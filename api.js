const express = require('express');
const mongoose = require('mongoose');
const zookeeper = require('node-zookeeper-client');
const { createHash } = require('crypto');
const axios = require('axios');
const morgan = require('morgan');

const app = express();
const port = process.argv[3]
const uri = `mongodb://localhost:${process.argv[4]}/database`;

app.use(express.json());
app.use(morgan('combined'))

// Mongo Configuration
const dataSchema = new mongoose.Schema({
    key: String,
    value: String
});

const Data = mongoose.model('Data', dataSchema);

// generate key
const key = generateKey(process.argv[2] + 'localhost' + process.argv[3] + process.argv[4]);
function generateKey(str) {
    return parseInt(createHash('sha256').update(str).digest('hex'),16)%2000;
}

function generateNodeInfo() {
    var nodeData = {
        "host": "localhost",
        "port": port,
        "name": process.argv[2],
        "key": key
    }
    return nodeData;
}

// Zookeeper
var initialFetchComplete = false;
const path = `/data/${process.argv[2]}`;
const client = zookeeper.createClient('localhost:8000,localhost:8001,localhost:8002');
var mongoNodes = [];

async function listChildren(client, path) {
    client.getChildren(
        path,
        function (event) {
            console.log('Got watcher event: %s', event);
            listChildren(client, path);
        },
        async function (error, children, stat) {
            if (error) {
                console.log(
                    'Failed to list children of %s due to: %s.',
                    path,
                    error
                );
                return;
            }

            console.log('Children of %s are: %j.', path, children);
            mongoNodes=[];
            var cal = new Promise((resolve, reject) => {
                children.forEach(async (child, index, children) => {
                    await getNodeData(client, `${path}/${child}`);
                    if (index == children.length - 1) {
                        resolve();
                    }
                })
                if (children.length == 0) {
                    resolve();
                }
            })
            cal.then(() => {
                mongoNodes.sort((a, b) => parseInt(a.key) - parseInt(b.key));
                initialFetchComplete = true;
            })
        }
    );
}

async function createNode(client, path, data) {
    await client.exists(
        path,
        async function (err, stat) {
            if (err) {
                console.log(err.stack);
                return;
            }

            if (stat) {
                console.log('Node Already exists. Deleting and recreating.');
                removeNode(client, path);
            }

            await client.create(
                path,
                Buffer.from(data),
                zookeeper.CreateMode.EPHEMERAL,
                function (err, path) {
                    if (err) {
                        console.log(err.stack);
                        return;
                    }
                    console.log('Node: %s is successfully created.', path);
                }
            );

        }
    )


}

async function getNodeData(client, path) {
    await client.getData(
        path,
        function (err, data, stat) {
            if (err) {
                console.log(err.stack);
                return;
            }
            console.log('Got data: %s', data.toString('utf8'));
            mongoNodes.push(JSON.parse(data.toString('utf8')));
        }
    );
}

function removeNode(client, path) {
    client.remove(
        path,
        function (err) {
            if (err) {
                console.log(err.stack);
                return;
            }
            console.log('Node: %s is successfully removed.', path);
        }
    );
}

client.once('connected', async function () {
    console.log('Connected to ZooKeeper.');

    //set watch on /data
    await listChildren(client, "/data");

    //create node
    const waitForFetch = new Promise((resolve, reject) => {
        const interval = setInterval(() => {
            if (initialFetchComplete) {
                clearInterval(interval);
                resolve();
            }
        }, 1000)
    })
    waitForFetch.then(() => {
        createNode(client, path, JSON.stringify(generateNodeInfo()));
    })

});

client.connect();

function getKeyIndex(key) {
    mongoNodes.sort((a, b) => parseInt(a.key) - parseInt(b.key));
    var index = 0;
    for (var i = 0; i < mongoNodes.length; i++) {
        if (mongoNodes[i].key > key) {
            index = i;
            break;
        }
    }
    return index;
}

// API Endpoints
app.get('/api/data', (req, res) => {
    
    Data.find({}).then(data => {
        res.status(200).json(data);
    }
    ).catch(err => {
        console.log(err);
        res.status(400).send('data not found');
    });
})

app.post('/api/data', (req, res) => {
    
    var value = req.body;
    const data_key = generateKey(JSON.stringify(req.body));
    // console.log(data_key);
    var index = getKeyIndex(data_key);
    // console.log(index);
    if(mongoNodes[index].key === key){
        const data = new Data({ key: data_key, value:JSON.stringify(value) });
        data.save().then(data => {
            res.status(200).json({ 'data': 'data added successfully' });
        }).catch(err => {
            console.log(err);
            res.status(400).send('adding new data failed');
        });
    }
    else{
        axios.post(`http://${mongoNodes[index].host}:${mongoNodes[index].port}/api/data/node`, { key: data_key, value:JSON.stringify(value) })
        .then(data => {
            // console.log(data.data)
            res.status(data.status).send(data.data)
        })
        .catch(error => {
            console.error(error)
            res.status(400).send('adding new data failed');
        })
    }
})

app.post('/api/data/node', (req, res) => {

    const data = new Data(req.body);
    data.save().then(data => {
        res.status(200).json({ 'data': 'data added successfully' });
    }).catch(err => {
        console.log(err);
        res.status(400).send('adding new data failed');
    });
})


const start = async () => {
    try {
        await mongoose
            .connect(uri, {
                useNewUrlParser: true,
                useUnifiedTopology: true
            })
            .then(() => {
                console.log("Successfully connected to Mongo database");
            })
            .catch((error) => {
                console.log("database connection failed. exiting now...");
                console.error(error);
                process.exit(1);
            });
        const server = app.listen(port, () => {
            console.log(`Listening on port ${port}`);
        })

        process.on('SIGINT' || 'SIGTERM', () => {
            console.log('Closing http server.');
            server.close((err) => {
                console.log('Http server closed.');
                client.close();
                process.exit(err ? 1 : 0);
            });
        });
    }
    catch (err) {
        console.log(err);
    }
}

start();


//data to reach node 1 "key":"oknm njknjn"