import json, os, string, numpy as np
from sentence_transformers import SentenceTransformer
from rank_bm25 import BM25Okapi
from qdrant_client import QdrantClient, models

# Paths (use the same as in build_vector_db.py)
CHUNKS_PATH = "complaints_chunks.jsonl"
DB_DIR = "qdrant_nhtsa_db"
COLLECTION_NAME = "nhtsa_complaints"
EMBED_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# 1. Load the complaints data to build a BM25 corpus
docs = []      # full text of each complaint
ids = []       # list of complaint IDs (int or str, matching what was used in Qdrant)
with open(CHUNKS_PATH, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        obj = json.loads(line)
        doc_id = obj.get("id") or obj.get("chunk_id")
        try:
            doc_id = int(doc_id)
        except ValueError:
            doc_id = str(doc_id)
        text = obj.get("full_text") or obj.get("text") or ""
        docs.append(text)
        ids.append(doc_id)

# Tokenize the documents for BM25
def simple_tokenize(text):
    """Lowercase and split text into tokens, removing punctuation."""
    text = text.lower().translate(str.maketrans('', '', string.punctuation))
    return text.split()

tokenized_docs = [simple_tokenize(doc) for doc in docs]
bm25 = BM25Okapi(tokenized_docs)  # build BM25 index in memory
print(f"Built BM25 index for {len(docs)} documents.")

# Create a mapping from ID to index in the docs list for quick lookup
id_to_index = {doc_id: idx for idx, doc_id in enumerate(ids)}

# Load the embedding model for query encoding
model = SentenceTransformer(EMBED_MODEL_NAME)

# Connect to Qdrant (embedded mode) to query vectors
client = QdrantClient(path=DB_DIR)

def hybrid_search(query, top_k=5, alpha=0.5):
    """
    Search the complaints using a combination of BM25 and vector similarity.
    alpha = weight for BM25 vs vector (0.5 = equal weight).
    Returns a list of (id, score, metadata) for top_k results.
    """
    # Encode query to vector
    query_vec = model.encode(query, normalize_embeddings=True).tolist()

    # BM25: get scores for all docs and identify top candidates
    query_tokens = simple_tokenize(query)
    bm25_scores = bm25.get_scores(query_tokens)  # BM25 score for each doc
    # Get top candidates from BM25
    bm25_top_n = 20  # consider top-N BM25 docs for combination
    bm25_top_idxs = np.argsort(bm25_scores)[::-1][:bm25_top_n]
    bm25_top_ids = {ids[i] for i in bm25_top_idxs}
    max_bm25 = bm25_scores.max() if bm25_scores.max() > 0 else 1.0
    min_bm25 = bm25_scores.min()
    
    # Vector search: get top vector-similar docs (with some limit)
    vec_top_n = 20
    search_result = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vec,
        limit=vec_top_n,
        with_payload=True
    )
    # Qdrant returns a list of ScoredPoint objects
    vector_top_ids = set()
    vector_scores = {}
    max_vec_score = 0.0
    min_vec_score = 0.0
    for point in search_result:
        pid = point.id  # complaint ID
        vector_top_ids.add(pid)
        score = point.score  # higher = more similar (cosine similarity)
        vector_scores[pid] = score
        # track max/min score among retrieved
        if score > max_vec_score: 
            max_vec_score = score
        if score < min_vec_score:
            min_vec_score = score

    # Combine the candidates from both methods
    candidate_ids = bm25_top_ids | vector_top_ids

    results = []
    for pid in candidate_ids:
        # Normalize BM25 and vector scores to [0,1] range
        idx = id_to_index.get(pid)
        bm25_score = 0.0
        if idx is not None:
            bm25_score = (bm25_scores[idx] - min_bm25) / (max_bm25 - min_bm25) if max_bm25 > min_bm25 else 0.0
        vec_score = 0.0
        if pid in vector_scores:
            # Normalize vector score similarly (if min_vec_score is negative, handle accordingly)
            if max_vec_score > min_vec_score:
                vec_score = (vector_scores[pid] - min_vec_score) / (max_vec_score - min_vec_score)
            else:
                vec_score = vector_scores[pid]
        # Hybrid weighted score
        hybrid_score = alpha * bm25_score + (1 - alpha) * vec_score

        # Get payload/metadata (if we have it from vector search or need to fetch)
        meta = None
        if pid in vector_scores:
            # If came from vector search, payload is in search_result
            # Find that point to get payload:
            for point in search_result:
                if point.id == pid:
                    meta = point.payload
                    break
        if meta is None:
            # If not from vector result, fetch payload from Qdrant by ID
            res = client.retrieve(collection_name=COLLECTION_NAME, ids=[pid], with_payload=True)
            if res:
                meta = res[0].payload

        results.append((pid, hybrid_score, meta or {}))
    
    # Sort candidates by hybrid score descending and return top_k
    results.sort(key=lambda x: x[1], reverse=True)
    return results[:top_k]

# Example usage:
query = "2021 Tesla beam lighting issue"
hits = hybrid_search(query, top_k=5, alpha=0.5)
for pid, score, meta in hits:
    print(f"\nResult ID: {pid} | Hybrid Score: {score:.3f}")
    # Print some details from metadata
    if meta:
        year = meta.get("vehicle_year")
        vehicle = meta.get("vehicle_raw")
        component = meta.get("component")
        narrative = meta.get("full_text", "")[:200]  # first 200 chars of complaint
        print(f"Vehicle: {year} {vehicle}, Component: {component}")
        print(f"Narrative snippet: {narrative}...")
