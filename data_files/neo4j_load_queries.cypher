
-- ============================================================
-- Neo4j AuraDB — Load Guide
-- ============================================================
-- After copying your CSV files to a public URL (e.g. GitHub raw,
-- Google Drive with sharing, or Neo4j's import folder), run:

-- 1. Load Segments
LOAD CSV WITH HEADERS FROM 'https://github.com/tonijhanel/fec-georgia-2022/tree/main/data_files/segments.csv' AS row
MERGE (:Segment {name: row.segment});

-- 2. Load Candidates
LOAD CSV WITH HEADERS FROM 'https://github.com/tonijhanel/fec-georgia-2022/tree/main/data_files/candidates.csv' AS row
MERGE (:Candidate {id: row.candidate_id, name: row.name,
       party: row.party, state: row.state});

-- 3. Load Committees
LOAD CSV WITH HEADERS FROM 'https://github.com/tonijhanel/fec-georgia-2022/tree/main/data_files/committees.csv' AS row
MERGE (c:Committee {id: row.committee_id})
  SET c.name = row.committee_name, c.type = row.committee_type
WITH c, row
MATCH (cand:Candidate {id: row.candidate_id})
MERGE (c)-[:SUPPORTS]->(cand);

-- 4. Load Donors
LOAD CSV WITH HEADERS FROM 'https://github.com/tonijhanel/fec-georgia-2022/tree/main/data_files/donors.csv' AS row
MERGE (d:Donor {id: row.donor_id})
  SET d.name = row.donor_name, d.city = row.city,
      d.state = row.state, d.employer = row.employer,
      d.occupation = row.occupation;

-- 5. Load Donor → Segment edges
LOAD CSV WITH HEADERS FROM 'https://github.com/tonijhanel/fec-georgia-2022/tree/main/data_files/donor_segment_edges.csv' AS row
MATCH (d:Donor {id: row.donor_id})
MATCH (s:Segment {name: row.segment})
MERGE (d)-[:WORKS_IN]->(s);

-- 6. Load Donations
LOAD CSV WITH HEADERS FROM 'https://github.com/tonijhanel/fec-georgia-2022/tree/main/data_files/donations.csv' AS row
MATCH (d:Donor {id: row.donor_id})
MATCH (c:Committee {id: row.committee_id})
CREATE (d)-[:DONATED_TO {amount: toFloat(row.amount), date: row.date}]->(c);

-- ============================================================
-- Example queries to try once loaded
-- ============================================================

-- Who are the top 10 donors by total amount?
MATCH (d:Donor)-[r:DONATED_TO]->()
RETURN d.name, sum(r.amount) AS total
ORDER BY total DESC LIMIT 10;

-- Which candidates share the most donors?
MATCH (d:Donor)-[:DONATED_TO]->(:Committee)-[:SUPPORTS]->(c:Candidate)
WITH d, collect(DISTINCT c.name) AS candidates
WHERE size(candidates) > 1
RETURN d.name, candidates;

-- Which advocacy segment contributes most to each candidate?
MATCH (seg:Segment)<-[:WORKS_IN]-(d:Donor)-[r:DONATED_TO]->
      (:Committee)-[:SUPPORTS]->(c:Candidate)
RETURN c.name, seg.name, round(sum(r.amount)) AS total
ORDER BY c.name, total DESC;
