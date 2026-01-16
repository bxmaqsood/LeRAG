import os
import json
from pathlib import Path
from typing import Dict, Any, Iterator, Tuple, Union, List

from sentence_transformers import SentenceTransformer

from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance


# -----------------------------
# Config (edit if needed)
# -----------------------------
INPUT_JSONL = "nhtsa_merged_complaints.jsonl"
DB_DIR = "qdrant_nhtsa_db"
COLLECTION_NAME = "nhtsa_complaints"

EMBED_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BATCH_SIZE = 256
VECTOR_SIZE = 384  # MiniLM-L6-v2 output dimension


def read_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def safe_point_id(raw_id: Any) -> Union[int, str]:
    """
    Qdrant supports int or str IDs.
    Your NHTSA IDs look numeric (e.g. '11666886'), so we store as int when possible.
    """
    try:
        return int(str(raw_id))
    except Exception:
        return str(raw_id)


def main():
    in_path = Path(INPUT_JSONL)
    if not in_path.exists():
        raise SystemExit(f"❌ Input JSONL not found: {in_path.resolve()}")

    print("Loading embedding model...")
    model = SentenceTransformer(EMBED_MODEL_NAME)

    print("Opening Qdrant persistent client...")
    os.makedirs(DB_DIR, exist_ok=True)
    client = QdrantClient(path=DB_DIR)

    print(f"Creating/recreating collection: {COLLECTION_NAME}")
    # recreate_collection wipes old collection for clean rebuild
    client.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=VECTOR_SIZE, distance=Distance.COSINE),
    )

    batch_ids: List[Union[int, str]] = []
    batch_texts: List[str] = []
    batch_payloads: List[Dict[str, Any]] = []
    total = 0

    for obj in read_jsonl(in_path):
        doc_id = safe_point_id(obj.get("id"))
        full_text = obj.get("full_text", "") or ""
        meta = obj.get("metadata", {}) or {}

        # Store metadata + full complaint text in payload
        payload = dict(meta)
        payload["id"] = str(obj.get("id"))
        payload["full_text"] = full_text
        payload["source"] = "nhtsa_complaints"

        batch_ids.append(doc_id)
        batch_texts.append(full_text)
        batch_payloads.append(payload)

        if len(batch_ids) >= BATCH_SIZE:
            embeddings = model.encode(batch_texts, normalize_embeddings=True).tolist()
            points = [
                PointStruct(id=pid, vector=vec, payload=pl)
                for pid, vec, pl in zip(batch_ids, embeddings, batch_payloads)
            ]
            client.upsert(collection_name=COLLECTION_NAME, points=points)
            total += len(points)
            print(f"Upserted {total} complaints...")

            batch_ids, batch_texts, batch_payloads = [], [], []

    # final flush
    if batch_ids:
        embeddings = model.encode(batch_texts, normalize_embeddings=True).tolist()
        points = [
            PointStruct(id=pid, vector=vec, payload=pl)
            for pid, vec, pl in zip(batch_ids, embeddings, batch_payloads)
        ]
        client.upsert(collection_name=COLLECTION_NAME, points=points)
        total += len(points)

    print("\n✅ Done.")
    print(f"Total complaints stored: {total}")
    print(f"DB directory: {DB_DIR}")
    print(f"Collection: {COLLECTION_NAME}")


if __name__ == "__main__":
    main()
