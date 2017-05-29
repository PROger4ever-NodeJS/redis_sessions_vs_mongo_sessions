const redis = require("redis");
const mongo = require("mongodb");

const TEST_REPEATS = 10000;
let benchUnique = "" + Math.random();

let data = [];
for (let i = 0; i < TEST_REPEATS; i++) {
  data.push({
    key: `bench:${benchUnique}:${Math.random()}`,
    value: "value" + Math.random()
  });
}

let randomKeys = [];
for (let i = 0; i < TEST_REPEATS; i++) {
  randomKeys.push(data[Math.floor(Math.random() * TEST_REPEATS)].key);
}

function addLotOfRecordsRedis(client, i, mainCB) {
  if (++i < TEST_REPEATS) {
    client.set(data[i].key, data[i].value,
      addLotOfRecordsRedis.bind(null, client, i, mainCB));
  } else {
    mainCB();
  }
}

function addLotOfRecordsMongo(collection, i, mainCB) {
  if (++i < TEST_REPEATS) {
    collection.insertOne({
        _id: data[i].key,
        value: data[i].value
      },
      addLotOfRecordsMongo.bind(null, collection, i, mainCB));
  } else {
    mainCB();
  }
}

function getLotOfRecordsByPrefixRedis(client, mainCB) {
  client.keys(`bench:${benchUnique}:*`, function (err, keys) {
    let results = new Array(keys.length);
    let i = 0;

    function cb() {
      client.get(keys[i], function (err, value) {
        results[i] = {
          key: keys[i],
          value
        };
        if (++i < keys.length) {
          cb();
        } else {
          mainCB(null, results);
        }
      });
    }

    cb();
  });
}

let regexPrefix = new RegExp(`bench:${benchUnique}:`);
function getLotOfRecordsByPrefixMongo(collection, mainCB) {
  collection.find({
    _id: regexPrefix
  }, mainCB.bind(null))
}

function getRandomRecordsRedis(client, i, mainCB) {
  if (++i < TEST_REPEATS) {
    client.get(randomKeys[i], getRandomRecordsRedis.bind(null, client, i, mainCB));
  } else {
    mainCB();
  }
}

function getRandomRecordsMongo(collection, i, mainCB) {
  if (++i < TEST_REPEATS) {
    collection.findOne({
        _id: randomKeys[i]
      },
      getRandomRecordsMongo.bind(null, collection, i, mainCB)
    );
  } else {
    mainCB();
  }
}

function addLotOfFieldsRedis(client, i, mainCB) {
  if (++i < TEST_REPEATS) {
    client.hset(`bench:${benchUnique}:hash`, ("field" + Math.random()).replace(".", ","), "value" + Math.random(),
      addLotOfFieldsRedis.bind(null, client, i, mainCB));
  } else {
    mainCB();
  }
}


function addLotOfFieldsMongo(collection, i, mainCB) {
  if (++i < TEST_REPEATS) {
    collection.updateOne({
        _id: `bench:${benchUnique}:hash`
      }, {
        $set: {
          [("field" + Math.random()).replace(".", ",")]: "value" + Math.random()
        }
      }, { upsert: true },
      addLotOfFieldsMongo.bind(null, collection, i, mainCB));
  } else {
    mainCB();
  }
}

function getLotOfFieldsRedis(client, mainCB) {
  client.hgetall(`bench:${benchUnique}:hash`, mainCB.bind(null));
}

function getLotOfFieldsMongo(collection, mainCB) {
  collection.findOne({
    _id: `bench:${benchUnique}:hash`
  }, mainCB.bind(null));
}


function benchRedis(mainCB) {
  let client = redis.createClient({
    "host": "redis",
    "port": 6379,
    "db": 1,
    "options": {
      "user": "admin",
      "pass": "admin"
    }
  });

  console.log("start redis...");
  let timeStart = new Date();
  addLotOfRecordsRedis(client, -1, function (err, res) {
    let diff = new Date() - timeStart;
    console.log("addLotOfRecordsRedis... ", diff / 1000);

    timeStart = new Date();
    getLotOfRecordsByPrefixRedis(client, function (err, res) {
      let diff = new Date() - timeStart;
      console.log("getLotOfRecordsByPrefixRedis... ", diff / 1000);

      timeStart = new Date();
      getRandomRecordsRedis(client, -1, function (err, res) {
        let diff = new Date() - timeStart;
        console.log("getRandomRecordsRedis... ", diff / 1000);

        timeStart = new Date();
        addLotOfFieldsRedis(client, -1, function (err, res) {
          let diff = new Date() - timeStart;
          console.log("addLotOfFieldsRedis... ", diff / 1000);


          timeStart = new Date();
          getLotOfFieldsRedis(client, function (err, res) {
            let diff = new Date() - timeStart;
            console.log("getLotOfFieldsRedis... ", diff / 1000);

            client.quit(mainCB.bind(null));
          });
        });
      });
    });
  });
}

function benchMongo(mainCB) {
  mongo.connect("mongodb://mongo:27017/bench", function (err, db) {
    let collection = db.collection("benchVsRedis");
    console.log("start mongo...");
    let timeStart = new Date();
    addLotOfRecordsMongo(collection, -1, function (err, res) {
      let diff = new Date() - timeStart;
      console.log("addLotOfRecordsMongo... ", diff / 1000);

      timeStart = new Date();
      getLotOfRecordsByPrefixMongo(collection, function (err, res) {
        res.toArray(function (err, res) {
          let diff = new Date() - timeStart;
          console.log("getLotOfRecordsByPrefixMongo... ", diff / 1000);

          getRandomRecordsMongo(collection, -1, function (err, res) {
            let diff = new Date() - timeStart;
            console.log("getRandomRecordsMongo... ", diff / 1000);

            timeStart = new Date();
            addLotOfFieldsMongo(collection, -1, function (err, res) {
              let diff = new Date() - timeStart;
              console.log("addLotOfFieldsMongo... ", diff / 1000);

              timeStart = new Date();
              getLotOfFieldsMongo(collection, function (err, res) {
                let diff = new Date() - timeStart;
                console.log("getLotOfFields... ", diff / 1000);

                db.close(mainCB.bind(null));
              });
            });
          });
        });
      });
    });
  });
}


function start() {
  benchRedis(function () {
    benchMongo(function () {
      console.log("end");
    });
  });
}


start();