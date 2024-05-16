//  DB
use("beansack");

// collections
db.getCollection("beans").countDocuments({});
db.getCollection("concepts").countDocuments({});
db.getCollection("medianoise").countDocuments({});
db.getCollection("digests").countDocuments({});

//  vector index
//  https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/vcore/vector-search


// INDEXES FOR BEANS
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
db.beans.createIndex(
  {
    updated: -1, // latest stuff should be at the top
    kind: 1 // although this may seem like it should be "text", that doesnt work for vector search 
  },
  {
    name: "beans_scalar_search"
  }
);

db.beans.createIndex(
  {
      title: "text",
      summary: "text",
      topic: "text",        
      keywords: "text"
  },
  {
      name: "beans_text_search"
  }
);

// INDEXES FOR CONCEPTS/NEWS NUGGETS
// text searching news nuggets
db.concepts.createIndex(
  {
      keyphrase: "text",
      event: "text"
  },
  {
      name: "concept_text_search"
  }
);

// Create a new index in the collection.
db.concepts.createIndex(
  {
    updated: -1,
    match_count: -1
  }, 
  {
    name: "concept_scalar_search"
  }
);

db.concepts.createIndex(
  { mapped_urls: 1 }, 
  { name: "concept_scalar_search_url"}
);

db.runCommand(
  {
    "createIndexes": "concepts",
    "indexes": [
      {
        "name": "concept_vector_search",
        "key": 
        {
          "embeddings": "cosmosSearch"
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