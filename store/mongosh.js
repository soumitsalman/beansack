//  DB
use("beansack");

// collections
db.getCollection("beans").countDocuments({});
db.getCollection("keywords").countDocuments({});
db.getCollection("medianoise").countDocuments({});
db.getCollection("digests").countDocuments({});

//  vector index
//  https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/vcore/vector-search
db.runCommand(
  {
    "createIndexes": "beans",
    "indexes": [
      {
        "name": "wholebeans_vec_search",
        "key": 
        {
          "embeddings": "cosmosSearch"
        },
        "cosmosSearchOptions": 
        {
          "kind": "vector-ivf",
          "numLists": 10,
          "similarity": "COS",
          "dimensions": 512
        }
      }
    ]
  }
)

// scalar index
db.getCollection("beans").createIndex({url:1, updated:1})
db.getCollection("beans").createIndex({kind:1})
