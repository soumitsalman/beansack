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
);

db.runCommand(
  {
    "createIndexes": "beans",
    "indexes": [
      {
        "name": "beans_category_search",
        "key": 
        {
          "category_embeddings": "cosmosSearch"
        },
        "cosmosSearchOptions": 
        {
          "kind": "vector-ivf",
          "numLists": 10,
          "similarity": "COS",
          "dimensions": 768
        }
      }
    ]
  }
);

db.runCommand(
  {
    "createIndexes": "beans",
    "indexes": [
      {
        "name": "beans_query_search",
        "key": 
        {
          "search_embeddings": "cosmosSearch"
        },
        "cosmosSearchOptions": 
        {
          "kind": "vector-ivf",
          "numLists": 10,
          "similarity": "COS",
          "dimensions": 768
        }
      }
    ]
  }
);


// scalar index - these need to exist if i want to use these as filters in vector search
db.getCollection("beans").createIndex(
  {
    updated: -1, // latest stuff should be at the top
    keywords: 1 // although this may seem like it should be "text", that doesnt work for vector search 
  },
  {
    name: "updated_and_keywords"
  }
)
db.getCollection("beans").createIndex({kind:1})
