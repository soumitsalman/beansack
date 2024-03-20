# DB
use beansack

# collections
db.beans.countDocuments({})
db.keywords.countDocuments({})
db.medianoise.countDocuments({})
db.digests.countDocuments({})

# vector index
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


db.runCommand(
  {
    "createIndexes": "digests",
    "indexes": [
      {
        "name": "bean_vec_search",
        "key": 
        {
          "embeddings": "cosmosSearch"
        },
        "cosmosSearchOptions": 
        {
          "kind": "vector-ivf",
          "numLists": 1,
          "similarity": "COS",
          "dimensions": 384
        }
      }
    ]
  }
)

# scalar index
db.beans.createIndex({url:1, updated:1})
db.beans.createIndex({kind:1})
db.digests.createIndex({url:1, updated:1})

# https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/vcore/vector-search