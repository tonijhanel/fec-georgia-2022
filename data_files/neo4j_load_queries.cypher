LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tonijhanel/fec-georgia-2022/main/data_files/segments.csv' AS row
MERGE (:Segment {name: row.segment});

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tonijhanel/fec-georgia-2022/main/data_files/candidates.csv' AS row
MERGE (:Candidate {id: row.candidate_id, name: row.name,
       party: row.party, state: row.state});


LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tonijhanel/fec-georgia-2022/main/data_files/committees.csv' AS row
MERGE (c:Committee {id: row.committee_id})
  SET c.name = row.committee_name, c.type = row.committee_type
WITH c, row
MATCH (cand:Candidate {id: row.candidate_id})
MERGE (c)-[:SUPPORTS]->(cand);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tonijhanel/fec-georgia-2022/main/data_files/donors.csv' AS row
MERGE (d:Donor {id: row.donor_id})
  SET d.name = row.donor_name, d.city = row.city,
      d.state = row.state, d.employer = row.employer,
      d.occupation = row.occupation;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tonijhanel/fec-georgia-2022/main/data_files/donor_segment_edges.csv' AS row
MATCH (d:Donor {id: row.donor_id})
MATCH (s:Segment {name: row.segment})
MERGE (d)-[:WORKS_IN]->(s);

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/tonijhanel/fec-georgia-2022/main/data_files/donations.csv' AS row
MATCH (d:Donor {id: row.donor_id})
MATCH (c:Committee {id: row.committee_id})
CREATE (d)-[:DONATED_TO {amount: toFloat(row.amount), date: row.date}]->(c);

