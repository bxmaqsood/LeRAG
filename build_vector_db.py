import os
import json
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

# Paths and settings
CHUNKS_PATH = "complaints_chunks.jsonl"  # your merged JSONL with one complaint per line
DB_DIR = "qdrant_nhtsa_db"              # directory for Qdrant persistent storage
COLLECTION_NAME = "nhtsa_complaints"
EMBED_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BATCH_SIZE = 256

def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)

def main():
    # Load embedding model
    print("Loading embedding model...")
    model = SentenceTransformer(EMBED_MODEL_NAME)

    # Initialize Qdrant in embedded persistent mode
    print("Starting Qdrant (embedded mode)...")
    os.makedirs(DB_DIR, exist_ok=True)
    client = QdrantClient(path=DB_DIR)  # Persists data to disk:contentReference[oaicite:1]{index=1}

    # Recreate collection (deletes if exists to start fresh)
    client.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(size=384, distance=Distance.COSINE)
    )

    ids_batch = []
    vectors_batch = []
    payloads_batch = []
    total = 0

    # Read each complaint from JSONL
    for obj in read_jsonl(CHUNKS_PATH):
        # Use complaint ID as the document ID (ensure it's an int for Qdrant)
        doc_id = obj.get("id") or obj.get("chunk_id")
        try:
            doc_id = int(doc_id)
        except ValueError:
            # If not purely numeric, keep as string
            doc_id = str(doc_id)

        # Full text of the complaint (combine all parts, including narrative)
        text = obj.get("full_text") or obj.get("text") 
        if text is None:
            continue  # skip if no text

        # Prepare metadata payload (include all relevant fields)
        meta = obj.get("metadata", {}) or {}
        # If not already separate, you can parse vehicle info into make/model.
        # e.g., meta["make"] = meta.get("vehicle_raw", "").split()[0]  # "TESLA", etc.
        meta["full_text"] = text  # store full text for retrieval context
        meta["id"] = doc_id       # store the ID as well

        ids_batch.append(doc_id)
        payloads_batch.append(meta)
        vectors_batch.append(text)  # will encode later

        # Process in batches for efficiency
        if len(ids_batch) >= BATCH_SIZE:
            # Embed the batch of texts
            embeddings = model.encode(vectors_batch, normalize_embeddings=True)
            # Prepare point structures for Qdrant
            points = [
                PointStruct(id=id_val, vector=emb.tolist(), payload=payload)
                for id_val, emb, payload in zip(ids_batch, embeddings, payloads_batch)
            ]
            # Upsert points into Qdrant
            client.upsert(collection_name=COLLECTION_NAME, points=points)
            total += len(points)
            print(f"Indexed {total} complaints...")
            # Reset batches
            ids_batch, vectors_batch, payloads_batch = [], [], []

    # Flush any remaining
    if ids_batch:
        embeddings = model.encode(vectors_batch, normalize_embeddings=True)
        points = [
            PointStruct(id=id_val, vector=emb.tolist(), payload=payload)
            for id_val, emb, payload in zip(ids_batch, embeddings, payloads_batch)
        ]
        client.upsert(collection_name=COLLECTION_NAME, points=points)
        total += len(points)

    print(f"✅ Completed indexing. Total complaints indexed: {total}")

if __name__ == "__main__":
    main()
