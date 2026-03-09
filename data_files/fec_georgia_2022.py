"""
FEC Data Fetcher & Cleaner — Georgia 2022 Senate Race
======================================================
Pulls individual contribution data from the FEC API and cleans it
for loading into a Neo4j graph database.

Nodes produced:
  - Candidate   (name, party, office, state)
  - Donor       (name, city, state, employer, occupation)
  - Committee   (name, committee_id, type, designation)
  - Segment     (label — derived from donor occupation/employer)

Relationships produced:
  - (Donor)-[:DONATED_TO {amount, date, cycle}]->(Committee)
  - (Committee)-[:SUPPORTS]->(Candidate)
  - (Donor)-[:WORKS_IN]->(Segment)

Output files:
  - candidates.csv
  - committees.csv
  - donors.csv
  - donations.csv
  - segments.csv
  - donor_segment_edges.csv

Requirements:
  pip install requests pandas tqdm

Usage:
  1. Get a free FEC API key at https://api.data.gov/signup/
  2. Run: python fec_georgia_2022.py --api-key YOUR_KEY
  3. Optional flags:
       --limit 500       # max donations to fetch (default 1000)
       --out ./data      # output directory (default: current dir)
"""

import argparse
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# ── Constants ────────────────────────────────────────────────────────────────

FEC_BASE = "https://api.open.fec.gov/v1"
CYCLE = 2022
STATE = "GA"

# Georgia 2022 Senate candidates (Warnock special + regular)
# We filter by committee IDs linked to these candidates via the API,
# but these names are used for display / fallback matching.
TARGET_CANDIDATES = [
    "WARNOCK, RAPHAEL",
    "WALKER, HERSCHEL",
]

# Occupation → advocacy segment mapping (expand as you explore the data)
SEGMENT_MAP = {
    # Finance
    r"invest|financ|hedge|equity|asset|capital|fund|bank|wealth": "Finance & Investment",
    r"insurance|insur": "Insurance",
    r"real estate|realtor|realt|property": "Real Estate",
    # Legal
    r"attorney|lawyer|law firm|legal|counsel|litigat": "Legal",
    # Healthcare
    r"physician|doctor|surgeon|medical|health|hospital|pharma|dentist|nurse": "Healthcare",
    # Tech
    r"software|engineer|tech|developer|data|cyber|ai |machine learn|cloud": "Technology",
    # Energy
    r"energy|oil|gas|petroleum|utility|solar|wind|renewable": "Energy",
    # Education
    r"professor|teacher|educator|school|university|college|academ": "Education",
    # Nonprofit / Advocacy
    r"nonprofit|ngo|foundation|advocacy|activist|organiz": "Nonprofit & Advocacy",
    # Retired
    r"retired|retiree|not employed|unemployed|homemaker": "Retired / Not Employed",
    # Business / Executive
    r"ceo|cfo|coo|president|executive|owner|founder|entrepreneur|consultant|manage": "Business & Executive",
    # Media
    r"journal|media|news|broadcast|publish|writer|author|communicat": "Media & Communications",
    # Government
    r"government|federal|state|county|city|municipal|public sector|civil serv": "Government & Public Sector",
}

# ── API helpers ───────────────────────────────────────────────────────────────

def fec_get(endpoint: str, params: dict, api_key: str) -> dict:
    """Single paginated GET against FEC API with basic retry logic."""
    params["api_key"] = api_key
    url = f"{FEC_BASE}/{endpoint}"
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
    return {}


def fetch_all_pages(endpoint: str, base_params: dict, api_key: str,
                    max_records: int = 1000) -> list[dict]:
    """Iterate through FEC pagination until max_records reached."""
    records = []
    page = 1
    per_page = min(100, max_records)

    with tqdm(total=max_records, desc=f"  {endpoint}") as pbar:
        while len(records) < max_records:
            params = {**base_params, "page": page, "per_page": per_page}
            data = fec_get(endpoint, params, api_key)

            results = data.get("results", [])
            if not results:
                break

            records.extend(results)
            pbar.update(len(results))
            page += 1

            pagination = data.get("pagination", {})
            if page > pagination.get("pages", 1):
                break

    return records[:max_records]


# ── Fetch functions ───────────────────────────────────────────────────────────

def fetch_candidates(api_key: str) -> pd.DataFrame:
    """Fetch Georgia 2022 Senate candidates."""
    print("\n[1/4] Fetching candidates...")
    raw = fetch_all_pages(
        "candidates/",
        {"state": STATE, "election_year": CYCLE, "office": "S", "per_page": 100},
        api_key,
        max_records=50,
    )
    if not raw:
        print("  ⚠ No candidates returned. Check your API key.")
        return pd.DataFrame()

    df = pd.DataFrame(raw)
    cols = {
        "candidate_id": "candidate_id",
        "name": "name",
        "party": "party",
        "office": "office",
        "state": "state",
        "incumbent_challenge_full": "incumbent_status",
    }
    df = df[[c for c in cols if c in df.columns]].rename(columns=cols)
    df["cycle"] = CYCLE
    print(f"  ✓ {len(df)} candidates found")
    return df


def fetch_committees(candidates_df: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """Fetch principal campaign committees for each candidate."""
    print("\n[2/4] Fetching committees...")
    all_committees = []

    for _, cand in candidates_df.iterrows():
        raw = fetch_all_pages(
            "candidate/{candidate_id}/committees/".replace(
                "{candidate_id}", cand["candidate_id"]
            ),
            {"cycle": CYCLE},
            api_key,
            max_records=20,
        )
        for c in raw:
            c["candidate_id"] = cand["candidate_id"]
            c["candidate_name"] = cand["name"]
        all_committees.extend(raw)

    if not all_committees:
        return pd.DataFrame()

    df = pd.DataFrame(all_committees)
    cols = {
        "committee_id": "committee_id",
        "name": "committee_name",
        "committee_type_full": "committee_type",
        "designation_full": "designation",
        "candidate_id": "candidate_id",
        "candidate_name": "candidate_name",
    }
    df = df[[c for c in cols if c in df.columns]].rename(columns=cols)
    df = df.drop_duplicates(subset=["committee_id"])
    print(f"  ✓ {len(df)} committees found")
    return df


def fetch_donations(committees_df: pd.DataFrame, api_key: str,
                    limit: int = 1000) -> pd.DataFrame:
    """Fetch individual contributions for each committee."""
    print("\n[3/4] Fetching donations...")
    all_donations = []
    per_committee = max(50, limit // max(len(committees_df), 1))

    for _, comm in committees_df.iterrows():
        raw = fetch_all_pages(
            "schedules/schedule_a/",
            {
                "committee_id": comm["committee_id"],
                "two_year_transaction_period": CYCLE,
                "min_amount": 200,   # FEC itemization threshold
            },
            api_key,
            max_records=per_committee,
        )
        for d in raw:
            d["committee_id"] = comm["committee_id"]
            d["candidate_name"] = comm.get("candidate_name", "")
        all_donations.extend(raw)

    if not all_donations:
        print("  ⚠ No donations returned.")
        return pd.DataFrame()

    df = pd.DataFrame(all_donations)
    print(f"  ✓ {len(df)} raw donation records fetched")
    return df


# ── Cleaning ──────────────────────────────────────────────────────────────────

def clean_text(s) -> str:
    """Normalize whitespace and title-case a string."""
    if pd.isna(s) or s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip().title()


def infer_segment(occupation: str, employer: str) -> str:
    """Map occupation/employer text to an advocacy segment."""
    text = f"{occupation} {employer}".lower()
    for pattern, label in SEGMENT_MAP.items():
        if re.search(pattern, text):
            return label
    return "Other"


def clean_donations(raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Clean raw FEC schedule A data.
    Returns: (donors_df, donations_df, segments_df)
    """
    print("\n[4/4] Cleaning data...")

    df = raw_df.copy()

    # ── Standardise column names ──
    rename = {
        "contributor_name": "donor_name",
        "contributor_city": "city",
        "contributor_state": "state",
        "contributor_zip": "zip",
        "contributor_employer": "employer",
        "contributor_occupation": "occupation",
        "contribution_receipt_amount": "amount",
        "contribution_receipt_date": "date",
        "committee_id": "committee_id",
        "candidate_name": "candidate_name",
        "transaction_id": "transaction_id",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    # ── Keep only needed columns (gracefully) ──
    keep = ["transaction_id", "donor_name", "city", "state", "zip",
            "employer", "occupation", "amount", "date",
            "committee_id", "candidate_name"]
    df = df[[c for c in keep if c in df.columns]]

    # ── Text cleaning ──
    for col in ["donor_name", "city", "employer", "occupation", "candidate_name"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_text)

    # ── Amount ──
    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)
    df = df[df["amount"] > 0]

    # ── Date ──
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # ── Donor ID (stable hash from name + zip) ──
    df["donor_id"] = (
        df.get("donor_name", "").fillna("") + "|" +
        df.get("zip", "").fillna("").astype(str).str[:5]
    ).apply(lambda x: "D" + str(abs(hash(x)))[:10])

    # ── Segments ──
    df["segment"] = df.apply(
        lambda r: infer_segment(
            r.get("occupation", ""), r.get("employer", "")
        ),
        axis=1,
    )

    # ── Build output tables ──

    donors_df = df[[
        "donor_id", "donor_name", "city", "state", "zip",
        "employer", "occupation"
    ]].drop_duplicates(subset=["donor_id"])

    donations_df = df[[
        "transaction_id", "donor_id", "committee_id",
        "candidate_name", "amount", "date"
    ]].drop_duplicates(subset=["transaction_id"] if "transaction_id" in df.columns else None)

    segments_df = pd.DataFrame(
        {"segment": df["segment"].unique()}
    ).reset_index(drop=True)

    donor_segment_df = df[["donor_id", "segment"]].drop_duplicates()

    print(f"  ✓ {len(donors_df)} unique donors")
    print(f"  ✓ {len(donations_df)} donations")
    print(f"  ✓ {len(segments_df)} advocacy segments identified")

    return donors_df, donations_df, segments_df, donor_segment_df


# ── Neo4j Cypher hints ────────────────────────────────────────────────────────

CYPHER_HINTS = """
-- ============================================================
-- Neo4j AuraDB — Load Guide
-- ============================================================
-- After copying your CSV files to a public URL (e.g. GitHub raw,
-- Google Drive with sharing, or Neo4j's import folder), run:

-- 1. Load Segments
LOAD CSV WITH HEADERS FROM 'file:///segments.csv' AS row
MERGE (:Segment {name: row.segment});

-- 2. Load Candidates
LOAD CSV WITH HEADERS FROM 'file:///candidates.csv' AS row
MERGE (:Candidate {id: row.candidate_id, name: row.name,
       party: row.party, state: row.state});

-- 3. Load Committees
LOAD CSV WITH HEADERS FROM 'file:///committees.csv' AS row
MERGE (c:Committee {id: row.committee_id})
  SET c.name = row.committee_name, c.type = row.committee_type
WITH c, row
MATCH (cand:Candidate {id: row.candidate_id})
MERGE (c)-[:SUPPORTS]->(cand);

-- 4. Load Donors
LOAD CSV WITH HEADERS FROM 'file:///donors.csv' AS row
MERGE (d:Donor {id: row.donor_id})
  SET d.name = row.donor_name, d.city = row.city,
      d.state = row.state, d.employer = row.employer,
      d.occupation = row.occupation;

-- 5. Load Donor → Segment edges
LOAD CSV WITH HEADERS FROM 'file:///donor_segment_edges.csv' AS row
MATCH (d:Donor {id: row.donor_id})
MATCH (s:Segment {name: row.segment})
MERGE (d)-[:WORKS_IN]->(s);

-- 6. Load Donations
LOAD CSV WITH HEADERS FROM 'file:///donations.csv' AS row
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
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch & clean FEC data for Georgia 2022 Senate race"
    )
    parser.add_argument("--api-key", required=True, help="FEC API key from api.data.gov")
    parser.add_argument("--limit", type=int, default=1000,
                        help="Max donations to fetch (default: 1000)")
    parser.add_argument("--out", default=".", help="Output directory (default: current dir)")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 55)
    print("  FEC Georgia 2022 Senate — Data Pipeline")
    print("=" * 55)

    # ── Fetch ──
    candidates_df = fetch_candidates(args.api_key)
    if candidates_df.empty:
        print("\n❌ No candidates found. Exiting.")
        return

    committees_df = fetch_committees(candidates_df, args.api_key)
    if committees_df.empty:
        print("\n❌ No committees found. Exiting.")
        return

    raw_donations_df = fetch_donations(committees_df, args.api_key, limit=args.limit)

    # ── Clean ──
    if not raw_donations_df.empty:
        donors_df, donations_df, segments_df, donor_segment_df = clean_donations(raw_donations_df)
    else:
        donors_df = donations_df = segments_df = donor_segment_df = pd.DataFrame()

    # ── Save ──
    print("\n[Saving CSV files...]")
    files = {
        "candidates.csv": candidates_df,
        "committees.csv": committees_df,
        "donors.csv": donors_df,
        "donations.csv": donations_df,
        "segments.csv": segments_df,
        "donor_segment_edges.csv": donor_segment_df,
    }
    for fname, df in files.items():
        if not df.empty:
            path = out_dir / fname
            df.to_csv(path, index=False)
            print(f"  ✓ {fname} ({len(df)} rows) → {path}")
        else:
            print(f"  ⚠ {fname} — no data, skipped")

    # ── Cypher hints ──
    hints_path = out_dir / "neo4j_load_queries.cypher"
    hints_path.write_text(CYPHER_HINTS)
    print(f"  ✓ neo4j_load_queries.cypher → {hints_path}")

    print("\n✅ Done! Next steps:")
    print("   1. Open Neo4j AuraDB → Import tab")
    print("   2. Upload the CSV files")
    print("   3. Run the Cypher queries in neo4j_load_queries.cypher")
    print("   4. Explore in the Neo4j Browser with the example queries at the bottom\n")


if __name__ == "__main__":
    main()
