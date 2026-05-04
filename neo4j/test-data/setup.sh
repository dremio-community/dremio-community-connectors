#!/bin/bash
# Start Neo4j test container
docker run -d \
  --name neo4j-test \
  --network dremio-net \
  -e NEO4J_AUTH=neo4j/testpass \
  neo4j:5.26-community

# Wait for Neo4j to be ready
echo "Waiting 30s for Neo4j to start..."
sleep 30

# Load test data via cypher-shell
docker exec neo4j-test cypher-shell -u neo4j -p testpass -d neo4j <<'CYPHER'
// Person nodes
CREATE (:Person {name:"Alice Smith", born:1985, email:"alice@example.com", score:98.5, active:true});
CREATE (:Person {name:"Bob Jones", born:1990, email:"bob@example.com", score:72.0, active:true});
CREATE (:Person {name:"Carol Chen", born:1988, email:"carol@example.com", score:75.5, active:false});
CREATE (:Person {name:"Dave Kim", born:1992, email:"dave@example.com", score:88.0, active:true});
CREATE (:Person {name:"Emma Wilson", born:1995, email:"emma@example.com", score:65.0, active:false});

// Movie nodes
CREATE (:Movie {title:"The Matrix", released:1999, tagline:"Welcome to the Real World", rating:8.7});
CREATE (:Movie {title:"Inception", released:2010, tagline:"Your mind is the scene of the crime", rating:8.8});
CREATE (:Movie {title:"Interstellar", released:2014, tagline:"Mankind was born on Earth", rating:8.6});
CREATE (:Movie {title:"The Dark Knight", released:2008, tagline:"Why So Serious?", rating:9.0});
CREATE (:Movie {title:"Parasite", released:2019, tagline:"Act like you own the place", rating:8.5});

// Relationships
MATCH (a:Person {name:"Alice Smith"}), (m:Movie {title:"The Matrix"}) CREATE (a)-[:ACTED_IN]->(m);
MATCH (a:Person {name:"Bob Jones"}), (m:Movie {title:"Inception"}) CREATE (a)-[:ACTED_IN]->(m);
MATCH (a:Person {name:"Carol Chen"}), (m:Movie {title:"The Matrix"}) CREATE (a)-[:ACTED_IN]->(m);
CYPHER

echo "Test data loaded successfully."
