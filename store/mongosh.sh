# DB
use beansack

# collections
db.beans.countDocuments({})
db.keywords.countDocuments({})
db.medianoise.countDocuments({})
db.digests.countDocuments({})

# vector index
db.digests.createIndex(
{
  name: 'digest_search',
  key: 
  {
    "embeddings": "cosmosSearch"
  },
  cosmosSearchOptions: 
  {
    kind: 'vector-ivf',
    numLists: 1,
    similarity: 'COS',
    dimensions: 384
  }
})
