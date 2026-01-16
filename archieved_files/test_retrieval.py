#!/usr/bin/env python3
"""
Quick smoke-test for retrieval quality.

- Uses your existing Qdrant DB if available (qdrant_nhtsa_db or qdrant_db)
- Falls back to vector-only ranking by embedding + dot product if Qdrant isn't found
- Prints Top-K results with key metadata so you can judge relevance and ordering fast

Run:
  python test_retrieval.py "2021 tesla low beams too bright automatic high beams" --k 5
"""

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from sentence_transformers import SentenceTransformer

EMBED_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

# Change this if your source JSONL filename differs
DEFAULT_JSONL = "nhtsa_merged_complaints.jsonl"

# If you used Qdrant earlier, put the path here (we'll auto-detect too)
QDRANT_CANDIDATE_DIRS = ["qdrant_nhtsa_db", "qdrant_db", "qdrant_nhtsa"]


def read_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def detect_qdrant_dir() -> Optional[str]:
    for d in QDRANT_CANDIDATE_DIRS:
        if Path(d).exists():
            return d
    return None


def try_qdrant_search(qdrant_dir: str, collection: str, query_vec: List[float], limit: int):
    try:
        from qdrant_client import QdrantClient
    except Exception:
        return None

    client = QdrantClient(path=qdrant_dir)
    try:
        hits = client.query_points(
            collection_name=collection,
            query=query_vec,
            limit=limit,
            with_payload=True,
        )
    except Exception as e:
        print(f"[warn] Qdrant query failed: {e}")
        return None

    results = []
    for rank, p in enumerate(hits, start=1):
        payload = p.payload or {}
        # We store full_text sometimes in payload; if not present, still show metadata
        full_text = payload.get("full_text") or payload.get("text") or ""
        results.append(
            {
                "rank": rank,
                "id": str(p.id),
                "score": float(p.score),
                "metadata": payload,
                "full_text": full_text,
            }
        )
    return results


def fallback_vector_search_from_jsonl(jsonl_path: Path, model: SentenceTransformer, query: str, k: int):
    # Load docs (this is OK for 6,984 docs)
    docs = []
    for obj in read_jsonl(jsonl_path):
        doc_id = str(obj.get("id"))
        full_text = obj.get("full_text") or ""
        meta = obj.get("metadata", {}) or {}
        docs.append((doc_id, full_text, meta))

    texts = [t for _, t, _ in docs]
    emb = model.encode(texts, normalize_embeddings=True, convert_to_numpy=True).astype("float32")
    q = model.encode([query], normalize_embeddings=True, convert_to_numpy=True).astype("float32")[0]
    scores = emb @ q
    top_idx = np.argsort(scores)[::-1][:k]

    results = []
    for rank, i in enumerate(top_idx, start=1):
        doc_id, full_text, meta = docs[i]
        results.append(
            {
                "rank": rank,
                "id": doc_id,
                "score": float(scores[i]),
                "metadata": meta,
                "full_text": full_text,
            }
        )
    return results


def print_results(results: List[Dict[str, Any]], max_chars: int = 600):
    if not results:
        print("No results.")
        return

    for r in results:
        md = r.get("metadata", {}) or {}
        # Support both payload style and your JSONL style
        vehicle_year = md.get("vehicle_year")
        vehicle_raw = md.get("vehicle_raw")
        component = md.get("component")
        crash = md.get("crash")
        fire = md.get("fire")
        incident = md.get("incidentDate_raw") or md.get("incidentDate_iso")
        reported = md.get("reportedDate_raw") or md.get("reportedDate_iso")
        loc = md.get("consumerLocation")

        print(
            f"\n#{r['rank']}  id={r['id']}  score={r['score']:.4f}\n"
            f"  year={vehicle_year} | vehicle={vehicle_raw} | component={component}\n"
            f"  crash={crash} | fire={fire} | incident={incident} | reported={reported} | location={loc}"
        )

        txt = (r.get("full_text") or "").strip()
        if txt:
            snippet = txt[:max_chars] + ("..." if len(txt) > max_chars else "")
            print("  ---")
            print(snippet)
        else:
            print("  (no full_text in payload)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("query", help="Search query")
    ap.add_argument("--k", type=int, default=5)
    ap.add_argument("--jsonl", default=DEFAULT_JSONL, help="Fallback JSONL path if Qdrant isn't available")
    ap.add_argument("--collection", default="nhtsa_complaints", help="Qdrant collection name")
    args = ap.parse_args()

    print("Loading embed model...")
    model = SentenceTransformer(EMBED_MODEL_NAME)

    qvec = model.encode([args.query], normalize_embeddings=True).tolist()[0]

    qdrant_dir = detect_qdrant_dir()
    if qdrant_dir:
        print(f"Using Qdrant persistent DB at: {qdrant_dir}")
        results = try_qdrant_search(qdrant_dir, args.collection, qvec, args.k)
        if results is not None:
            print_results(results)
            return
        print("[warn] Qdrant available but query failed. Falling back to JSONL vector search...")

    jsonl_path = Path(args.jsonl)
    if not jsonl_path.exists():
        raise SystemExit(f"JSONL not found: {jsonl_path}. Provide --jsonl <path>.")

    print(f"Using JSONL fallback: {jsonl_path}")
    results = fallback_vector_search_from_jsonl(jsonl_path, model, args.query, args.k)
    print_results(results)


if __name__ == "__main__":
    main()
