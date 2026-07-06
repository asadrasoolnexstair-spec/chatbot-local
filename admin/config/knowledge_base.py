# # # =============================================================================
# # # KNOWLEDGE BASE API
# # # =============================================================================
# # # API endpoints for Knowledge Base management with ChromaDB integration
# # # =============================================================================

# # =============================================================================
# # KNOWLEDGE BASE API (production-grade)
# # =============================================================================
# # API endpoints for Knowledge Base management with ChromaDB integration
# #
# # Key fixes vs. previous version:
# #   - Recursive, structure-aware chunking (was: naive 500-char hard-cut splitter
# #     that regularly severed sentences mid-word -> "trimmed and half" answers)
# #   - Bigger, token-aware default chunk size + real overlap that preserves
# #     context across chunk boundaries
# #   - Structured processors for CSV/JSON (rows/records -> readable sentences
# #     instead of dumping raw text, which destroys retrieval quality)
# #   - pdfplumber-based PDF extraction (PyPDF2 is unmaintained and silently
# #     drops text on many real-world PDFs), with PyPDF2 as a fallback only
# #   - DOCX support added
# #   - Ingestion runs as a background task with a status column, so uploads
# #     don't block/time out and partial failures are visible
# #   - Search endpoint supports score thresholds and returns full chunk text
# #     (never truncated) with sane defaults
# #   - Basic hardening: path traversal protection, retries around ChromaDB
# # =============================================================================

# import os
# import re
# import uuid
# import hmac
# import json
# import logging
# from datetime import datetime
# from typing import Any, Dict, List, Optional, Callable, Awaitable
# from pathlib import Path

# from fastapi import (
#     APIRouter, BackgroundTasks, Depends, HTTPException,
#     UploadFile, File, Form, Query, status
# )
# from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# import asyncpg
# import aiofiles

# logger = logging.getLogger(__name__)

# router = APIRouter(prefix="/api/knowledge-base", tags=["knowledge-base"])
# security = HTTPBearer(auto_error=False)

# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "/app/knowledge_base/data")).resolve()
# CHROMADB_HOST = os.getenv("CHROMADB_HOST", "chromadb")
# CHROMADB_PORT = int(os.getenv("CHROMADB_PORT", "8000"))
# DEFAULT_COLLECTION = os.getenv("KB_COLLECTION", "website_content")

# # Chunking defaults. ~1200 chars / ~250-300 tokens keeps a chunk large enough
# # to contain a full idea/paragraph, while chunk_overlap keeps context glued
# # across boundaries. Tune per embedding model's context window if needed.
# DEFAULT_CHUNK_SIZE = int(os.getenv("KB_CHUNK_SIZE", "1200"))
# DEFAULT_CHUNK_OVERLAP = int(os.getenv("KB_CHUNK_OVERLAP", "200"))

# MAX_UPLOAD_SIZE = int(os.getenv("KB_MAX_UPLOAD_MB", "20")) * 1024 * 1024

# SUPPORTED_FILE_TYPES = {
#     ".txt": "text/plain",
#     ".md": "text/markdown",
#     ".pdf": "application/pdf",
#     ".html": "text/html",
#     ".htm": "text/html",
#     ".json": "application/json",
#     ".csv": "text/csv",
#     ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
# }


# # =============================================================================
# # DATABASE CONNECTION
# #
# # Expected schema additions to `content_sources` vs. the old version:
# #   ALTER TABLE content_sources ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending';
# #   ALTER TABLE content_sources ADD COLUMN IF NOT EXISTS error_message TEXT;
# #   ALTER TABLE content_sources ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
# # status values: 'pending' -> 'processing' -> 'completed' | 'failed'
# # =============================================================================

# db_pool: Optional[asyncpg.Pool] = None


# async def get_pool() -> asyncpg.Pool:
#     global db_pool
#     if db_pool is None:
#         db_pool = await asyncpg.create_pool(
#             host=os.getenv("DB_HOST", "localhost"),
#             port=int(os.getenv("DB_PORT", 5432)),
#             database=os.getenv("DB_NAME", "chatbot"),
#             user=os.getenv("DB_USER", "rasa"),
#             password=os.getenv("DB_PASSWORD"),
#             min_size=2,
#             max_size=10,
#         )
#     return db_pool


# async def get_db():
#     """Dependency for a pooled database connection."""
#     pool = await get_pool()
#     async with pool.acquire() as conn:
#         yield conn


# async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
#     """Verify admin token using JWT or a static ADMIN_TOKEN."""
#     import jwt

#     if credentials is None:
#         raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Authentication required")

#     token = credentials.credentials
#     admin_token = os.getenv("ADMIN_TOKEN")
#     if admin_token and hmac.compare_digest(token, admin_token):
#         return {"user_id": "admin", "email": "admin@local", "role": "admin"}

#     secret = os.getenv("JWT_SECRET")
#     if not secret:
#         raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "JWT_SECRET not configured")

#     try:
#         payload = jwt.decode(token, secret, algorithms=["HS256"])
#         return {
#             "user_id": payload.get("sub"),
#             "email": payload.get("email"),
#             "role": payload.get("role", "viewer"),
#         }
#     except jwt.ExpiredSignatureError:
#         raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token expired")
#     except jwt.InvalidTokenError:
#         raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")


# # =============================================================================
# # CHROMADB CLIENT
# # =============================================================================

# class ChromaDBClient:
#     """ChromaDB client for vector operations, with a small retry wrapper."""

#     def __init__(self):
#         self._client = None

#     def get_client(self):
#         if self._client is None:
#             try:
#                 import chromadb
#                 from chromadb.config import Settings

#                 self._client = chromadb.HttpClient(
#                     host=CHROMADB_HOST,
#                     port=CHROMADB_PORT,
#                     settings=Settings(anonymized_telemetry=False),
#                 )
#                 logger.info(f"Connected to ChromaDB at {CHROMADB_HOST}:{CHROMADB_PORT}")
#             except Exception as e:
#                 logger.error(f"Failed to connect to ChromaDB: {e}")
#                 raise HTTPException(
#                     status.HTTP_503_SERVICE_UNAVAILABLE,
#                     f"ChromaDB not available: {e}",
#                 )
#         return self._client

#     def get_collection(self, name: str = DEFAULT_COLLECTION):
#         client = self.get_client()
#         try:
#             from chromadb.utils.embedding_functions import DefaultEmbeddingFunction
#             embedding_fn = DefaultEmbeddingFunction()
#         except ImportError:
#             logger.warning("Default embedding function not available; collection may fail on add()")
#             embedding_fn = None

#         return client.get_or_create_collection(
#             name=name,
#             metadata={"hnsw:space": "cosine"},
#             embedding_function=embedding_fn,
#         )


# async def _with_retries(fn: Callable[[], Any], attempts: int = 3, label: str = "chromadb op") -> Any:
#     """Run a sync chromadb call with basic retry/backoff. Runs in a thread so it
#     doesn't block the event loop."""
#     import asyncio

#     last_err = None
#     for attempt in range(1, attempts + 1):
#         try:
#             return await asyncio.to_thread(fn)
#         except Exception as e:
#             last_err = e
#             logger.warning(f"{label} failed (attempt {attempt}/{attempts}): {e}")
#             if attempt < attempts:
#                 await asyncio.sleep(0.5 * attempt)
#     raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"{label} failed after {attempts} attempts: {last_err}")


# chroma_client = ChromaDBClient()


# # =============================================================================
# # CHUNKING
# #
# # Recursive, separator-aware splitter (same family as LangChain's
# # RecursiveCharacterTextSplitter). Tries paragraph breaks first, then lines,
# # then sentence punctuation, then words -- only falling back to a hard
# # character cut if a single "atom" of text is still bigger than chunk_size.
# # This is what actually fixes "trimmed and half" answers: no more mid-word,
# # mid-sentence severing, and real overlap so a fact split across a boundary
# # still appears whole in at least one chunk.
# # =============================================================================

# DEFAULT_SEPARATORS = ["\n\n", "\n", ". ", "! ", "? ", "; ", ", ", " "]


# def chunk_text(
#     text: str,
#     chunk_size: int = DEFAULT_CHUNK_SIZE,
#     chunk_overlap: int = DEFAULT_CHUNK_OVERLAP,
#     separators: Optional[List[str]] = None,
# ) -> List[str]:
#     text = (text or "").strip()
#     if not text:
#         return []
#     if len(text) <= chunk_size:
#         return [text]

#     seps = separators or DEFAULT_SEPARATORS

#     def split_recursive(chunk: str, remaining_seps: List[str]) -> List[str]:
#         if len(chunk) <= chunk_size:
#             return [chunk] if chunk.strip() else []

#         if not remaining_seps:
#             # Last resort: hard character split.
#             return [chunk[i:i + chunk_size] for i in range(0, len(chunk), chunk_size)]

#         sep = remaining_seps[0]
#         parts = chunk.split(sep)
#         pieces = [
#             (p + sep) if (sep and i < len(parts) - 1) else p
#             for i, p in enumerate(parts)
#         ]

#         merged: List[str] = []
#         current = ""
#         for piece in pieces:
#             if len(piece) > chunk_size:
#                 if current:
#                     merged.append(current)
#                     current = ""
#                 merged.extend(split_recursive(piece, remaining_seps[1:]))
#                 continue
#             if len(current) + len(piece) <= chunk_size:
#                 current += piece
#             else:
#                 if current:
#                     merged.append(current)
#                 current = piece
#         if current:
#             merged.append(current)
#         return merged

#     raw_chunks = [c.strip() for c in split_recursive(text, seps) if c.strip()]

#     if chunk_overlap <= 0 or len(raw_chunks) <= 1:
#         return raw_chunks

#     overlapped = [raw_chunks[0]]
#     for i in range(1, len(raw_chunks)):
#         tail = raw_chunks[i - 1][-chunk_overlap:]
#         overlapped.append(f"{tail} {raw_chunks[i]}".strip())

#     return overlapped


# # =============================================================================
# # DOCUMENT PROCESSING
# #
# # Each processor returns a list of (text, extra_metadata) pairs rather than
# # one giant blob. This lets us keep e.g. one CSV row or one JSON record as a
# # coherent unit before the chunker gets to it, which matters a lot for
# # structured data -- see the "data format" guidance in the chat response.
# # =============================================================================

# async def process_text_file(file_path: Path) -> List[tuple]:
#     async with aiofiles.open(file_path, "r", encoding="utf-8", errors="ignore") as f:
#         content = await f.read()
#     return [(content, {})]


# async def process_markdown_file(file_path: Path) -> List[tuple]:
#     async with aiofiles.open(file_path, "r", encoding="utf-8", errors="ignore") as f:
#         content = await f.read()

#     # Split on top-level headers so each section keeps its own heading as
#     # context, then strip markdown syntax within each section.
#     sections = re.split(r"(?m)^(#{1,3}\s.+)$", content)
#     results = []
#     if len(sections) <= 1:
#         results.append((_clean_markdown(content), {}))
#     else:
#         # sections alternates: [preamble, header, body, header, body, ...]
#         preamble = sections[0].strip()
#         if preamble:
#             results.append((_clean_markdown(preamble), {}))
#         for i in range(1, len(sections), 2):
#             header = sections[i].lstrip("#").strip()
#             body = sections[i + 1] if i + 1 < len(sections) else ""
#             text = f"{header}\n{_clean_markdown(body)}".strip()
#             if text:
#                 results.append((text, {"section": header}))
#     return results


# def _clean_markdown(content: str) -> str:
#     content = re.sub(r"```[\s\S]*?```", "", content)
#     content = re.sub(r"`[^`]+`", "", content)
#     content = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", content)
#     content = re.sub(r"^#+\s*", "", content, flags=re.MULTILINE)
#     return content.strip()


# async def process_html_file(file_path: Path) -> List[tuple]:
#     async with aiofiles.open(file_path, "r", encoding="utf-8", errors="ignore") as f:
#         content = await f.read()
#     text = _extract_html_text(content)
#     return [(text, {})]


# def _extract_html_text(content: str) -> str:
#     try:
#         from bs4 import BeautifulSoup
#         soup = BeautifulSoup(content, "html.parser")
#         for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
#             tag.decompose()
#         main = soup.find("main") or soup.find("article") or soup
#         return main.get_text(separator="\n", strip=True)
#     except ImportError:
#         content = re.sub(r"<script[^>]*>[\s\S]*?</script>", "", content)
#         content = re.sub(r"<style[^>]*>[\s\S]*?</style>", "", content)
#         content = re.sub(r"<[^>]+>", " ", content)
#         return " ".join(content.split())


# async def process_pdf_file(file_path: Path) -> List[tuple]:
#     """Extract text page-by-page so each chunk can carry a page number.
#     Prefers pdfplumber (much better layout/text handling); falls back to
#     PyPDF2 only if pdfplumber isn't installed."""
#     try:
#         import pdfplumber
#         results = []
#         with pdfplumber.open(file_path) as pdf:
#             for page_num, page in enumerate(pdf.pages, start=1):
#                 text = page.extract_text() or ""
#                 if text.strip():
#                     results.append((text, {"page": page_num}))
#         if not results:
#             raise HTTPException(
#                 status.HTTP_400_BAD_REQUEST,
#                 "No extractable text found (this may be a scanned/image PDF that needs OCR).",
#             )
#         return results
#     except ImportError:
#         logger.warning("pdfplumber not installed, falling back to PyPDF2")
#         try:
#             import PyPDF2
#             results = []
#             with open(file_path, "rb") as f:
#                 reader = PyPDF2.PdfReader(f)
#                 for page_num, page in enumerate(reader.pages, start=1):
#                     text = page.extract_text() or ""
#                     if text.strip():
#                         results.append((text, {"page": page_num}))
#             return results
#         except ImportError:
#             raise HTTPException(
#                 status.HTTP_400_BAD_REQUEST,
#                 "PDF processing not available. Install pdfplumber (recommended) or PyPDF2.",
#             )


# async def process_docx_file(file_path: Path) -> List[tuple]:
#     try:
#         import docx
#     except ImportError:
#         raise HTTPException(
#             status.HTTP_400_BAD_REQUEST,
#             "DOCX processing not available. Install python-docx.",
#         )
#     document = docx.Document(str(file_path))
#     parts = []
#     for para in document.paragraphs:
#         if para.text.strip():
#             parts.append(para.text.strip())
#     for table in document.tables:
#         for row in table.rows:
#             cells = [c.text.strip() for c in row.cells if c.text.strip()]
#             if cells:
#                 parts.append(" | ".join(cells))
#     return [("\n".join(parts), {})]


# async def process_csv_file(file_path: Path) -> List[tuple]:
#     """Turn each row into a readable 'column: value' sentence instead of
#     dumping raw CSV text (raw CSV embeds/retrieves very poorly)."""
#     import csv as csv_module

#     async with aiofiles.open(file_path, "r", encoding="utf-8", errors="ignore") as f:
#         raw = await f.read()

#     reader = csv_module.DictReader(raw.splitlines())
#     results = []
#     for i, row in enumerate(reader, start=1):
#         parts = [f"{k.strip()}: {v.strip()}" for k, v in row.items() if k and v and v.strip()]
#         if parts:
#             results.append((". ".join(parts), {"row": i}))
#     if not results:
#         raise HTTPException(status.HTTP_400_BAD_REQUEST, "CSV appears to have no usable rows/headers.")
#     return results


# async def process_json_file(file_path: Path) -> List[tuple]:
#     """Handles two common shapes well:
#       1. A list of Q&A / FAQ-style records -> 'Q: ... A: ...' text per item
#       2. Arbitrary JSON -> pretty-printed per top-level record
#     Both keep one logical record as one unit before chunking."""
#     async with aiofiles.open(file_path, "r", encoding="utf-8", errors="ignore") as f:
#         raw = await f.read()

#     try:
#         data = json.loads(raw)
#     except json.JSONDecodeError as e:
#         raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Invalid JSON: {e}")

#     results = []
#     if isinstance(data, list):
#         for i, item in enumerate(data, start=1):
#             if isinstance(item, dict):
#                 q = item.get("question") or item.get("q") or item.get("title")
#                 a = item.get("answer") or item.get("a") or item.get("content") or item.get("text")
#                 if q and a:
#                     results.append((f"Q: {q}\nA: {a}", {"record": i}))
#                 else:
#                     text = "\n".join(f"{k}: {v}" for k, v in item.items() if v not in (None, ""))
#                     results.append((text, {"record": i}))
#             else:
#                 results.append((str(item), {"record": i}))
#     elif isinstance(data, dict):
#         results.append((json.dumps(data, indent=2, ensure_ascii=False), {}))
#     else:
#         results.append((str(data), {}))

#     return [(t, m) for t, m in results if t and t.strip()]


# PROCESSORS: Dict[str, Callable[[Path], Awaitable[List[tuple]]]] = {
#     ".txt": process_text_file,
#     ".md": process_markdown_file,
#     ".html": process_html_file,
#     ".htm": process_html_file,
#     ".pdf": process_pdf_file,
#     ".json": process_json_file,
#     ".csv": process_csv_file,
#     ".docx": process_docx_file,
# }


# async def process_file(file_path: Path, file_type: str) -> List[tuple]:
#     processor = PROCESSORS.get(file_type, process_text_file)
#     return await processor(file_path)


# # =============================================================================
# # INGESTION (runs in the background so uploads don't block/time out)
# # =============================================================================

# async def _ingest_document(
#     doc_id: str,
#     file_path: Path,
#     filename: str,
#     file_ext: str,
#     collection: str,
#     size_bytes: int,
# ):
#     pool = await get_pool()
#     async with pool.acquire() as conn:
#         try:
#             await conn.execute(
#                 "UPDATE content_sources SET status = 'processing' WHERE id = $1", doc_id
#             )

#             sections = await process_file(file_path, file_ext)
#             all_chunks: List[str] = []
#             all_metadatas: List[dict] = []

#             for section_text, section_meta in sections:
#                 section_chunks = chunk_text(section_text)
#                 for idx, c in enumerate(section_chunks):
#                     all_chunks.append(c)
#                     meta = {
#                         "source": filename,
#                         "doc_id": doc_id,
#                         "chunk_index": len(all_chunks) - 1,
#                         "file_type": file_ext,
#                         **section_meta,
#                     }
#                     all_metadatas.append(meta)

#             if not all_chunks:
#                 raise ValueError("Document produced no usable text/chunks")

#             chroma_collection = chroma_client.get_collection(collection)
#             chunk_ids = [f"{doc_id}_chunk_{i}" for i in range(len(all_chunks))]

#             # Batch adds to avoid overly large single requests to ChromaDB.
#             BATCH = 100
#             for i in range(0, len(all_chunks), BATCH):
#                 batch_ids = chunk_ids[i:i + BATCH]
#                 batch_docs = all_chunks[i:i + BATCH]
#                 batch_meta = all_metadatas[i:i + BATCH]
#                 await _with_retries(
#                     lambda: chroma_collection.add(ids=batch_ids, documents=batch_docs, metadatas=batch_meta),
#                     label=f"chromadb.add[{doc_id}]",
#                 )

#             metadata_str = json.dumps({"file_type": file_ext, "size_bytes": size_bytes})
#             await conn.execute(
#                 """
#                 UPDATE content_sources
#                 SET document_count = $2, chunk_count = $3, last_ingested = NOW(),
#                     status = 'completed', error_message = NULL, metadata = $4::jsonb
#                 WHERE id = $1
#                 """,
#                 doc_id, 1, len(all_chunks), metadata_str,
#             )
#             logger.info(f"Ingested {filename} ({doc_id}): {len(all_chunks)} chunks")

#         except Exception as e:
#             logger.error(f"Ingestion failed for {doc_id} ({filename}): {e}")
#             await conn.execute(
#                 "UPDATE content_sources SET status = 'failed', error_message = $2 WHERE id = $1",
#                 doc_id, str(e)[:2000],
#             )


# # =============================================================================
# # API ENDPOINTS
# # =============================================================================

# @router.get("/stats")
# async def get_knowledge_base_stats(_: dict = Depends(verify_token)) -> Dict[str, Any]:
#     try:
#         client = chroma_client.get_client()
#         collections = await _with_retries(client.list_collections, label="list_collections")

#         total_chunks = 0
#         collection_info = []
#         for col in collections:
#             count = await _with_retries(col.count, label=f"count[{col.name}]")
#             total_chunks += count
#             collection_info.append({"name": col.name, "count": count})

#         return {
#             "total_collections": len(collections),
#             "total_chunks": total_chunks,
#             "collections": collection_info,
#             "chromadb_status": "connected",
#         }
#     except HTTPException:
#         raise
#     except Exception as e:
#         return {
#             "total_collections": 0,
#             "total_chunks": 0,
#             "collections": [],
#             "chromadb_status": f"error: {e}",
#         }


# @router.get("/documents")
# async def list_documents(
#     limit: int = Query(50, ge=1, le=200),
#     offset: int = Query(0, ge=0),
#     status_filter: Optional[str] = Query(None, alias="status"),
#     conn: asyncpg.Connection = Depends(get_db),
#     _: dict = Depends(verify_token),
# ) -> Dict[str, Any]:
#     if status_filter:
#         rows = await conn.fetch(
#             """
#             SELECT id, name, source_type, location, collection_name, enabled,
#                    status, error_message, last_ingested, document_count,
#                    chunk_count, metadata, created_at
#             FROM content_sources
#             WHERE status = $1
#             ORDER BY created_at DESC
#             LIMIT $2 OFFSET $3
#             """,
#             status_filter, limit, offset,
#         )
#         total = await conn.fetchval("SELECT COUNT(*) FROM content_sources WHERE status = $1", status_filter)
#     else:
#         rows = await conn.fetch(
#             """
#             SELECT id, name, source_type, location, collection_name, enabled,
#                     last_ingested, document_count,
#                    chunk_count, metadata, created_at
#             FROM content_sources
#             ORDER BY created_at DESC
#             LIMIT $1 OFFSET $2
#             """,
#             limit, offset,
#         )
#         total = await conn.fetchval("SELECT COUNT(*) FROM content_sources")

#     return {"documents": [dict(r) for r in rows], "total": total, "limit": limit, "offset": offset}


# @router.get("/documents/{doc_id}")
# async def get_document_status(
#     doc_id: str,
#     conn: asyncpg.Connection = Depends(get_db),
#     _: dict = Depends(verify_token),
# ) -> Dict[str, Any]:
#     row = await conn.fetchrow("SELECT * FROM content_sources WHERE id = $1", doc_id)
#     if not row:
#         raise HTTPException(status.HTTP_404_NOT_FOUND, "Document not found")
#     return dict(row)


# def _safe_filename(name: str, doc_id: str) -> str:
#     """Prevent path traversal / weird filenames from escaping UPLOAD_DIR."""
#     name = Path(name).name  # strips any directory components
#     name = re.sub(r"[^\w.\-]", "_", name)
#     return f"{doc_id}_{name}"


# @router.post("/upload")
# async def upload_document(
#     background_tasks: BackgroundTasks,
#     file: UploadFile = File(...),
#     collection: str = Form(DEFAULT_COLLECTION),
#     conn: asyncpg.Connection = Depends(get_db),
#     user: dict = Depends(verify_token),
# ) -> Dict[str, Any]:
#     """Upload a document. Ingestion (extraction, chunking, embedding) runs in
#     the background -- poll GET /documents/{doc_id} for status. This avoids
#     request timeouts on large files and keeps partial failures visible
#     instead of silently truncating results."""
#     contents = await file.read()
#     if len(contents) > MAX_UPLOAD_SIZE:
#         raise HTTPException(
#             status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
#             f"File too large. Maximum size is {MAX_UPLOAD_SIZE // (1024 * 1024)}MB",
#         )
#     if not contents:
#         raise HTTPException(status.HTTP_400_BAD_REQUEST, "Uploaded file is empty")

#     file_ext = Path(file.filename).suffix.lower()
#     if file_ext not in SUPPORTED_FILE_TYPES:
#         raise HTTPException(
#             status.HTTP_400_BAD_REQUEST,
#             f"Unsupported file type: {file_ext}. Supported: {list(SUPPORTED_FILE_TYPES.keys())}",
#         )

#     UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
#     doc_id = str(uuid.uuid4())[:8]
#     file_path = UPLOAD_DIR / _safe_filename(file.filename, doc_id)

#     # Guard against path traversal resolving outside UPLOAD_DIR
#     if UPLOAD_DIR not in file_path.resolve().parents:
#         raise HTTPException(status.HTTP_400_BAD_REQUEST, "Invalid filename")

#     async with aiofiles.open(file_path, "wb") as f:
#         await f.write(contents)

#     await conn.execute(
#         """
#         INSERT INTO content_sources (id, name, source_type, location, collection_name,
#                                       document_count, chunk_count, status, metadata)
#         VALUES ($1, $2, 'file', $3, $4, 0, 0, 'pending', $5::jsonb)
#         ON CONFLICT (id) DO UPDATE SET status = 'pending'
#         """,
#         doc_id, file.filename, str(file_path), collection,
#         json.dumps({"file_type": file_ext, "size_bytes": len(contents)}),
#     )

#     background_tasks.add_task(
#         _ingest_document, doc_id, file_path, file.filename, file_ext, collection, len(contents)
#     )

#     return {
#         "success": True,
#         "document_id": doc_id,
#         "filename": file.filename,
#         "collection": collection,
#         "status": "pending",
#         "message": "Document queued for processing. Poll GET /documents/{doc_id} for status.",
#     }


# @router.post("/import-url")
# async def import_from_url(
#     background_tasks: BackgroundTasks,
#     url: str = Form(...),
#     collection: str = Form(DEFAULT_COLLECTION),
#     conn: asyncpg.Connection = Depends(get_db),
#     user: dict = Depends(verify_token),
# ) -> Dict[str, Any]:
#     """Import content from a URL, tried against multiple fetch strategies to
#     handle bot-protected sites. Extraction/chunking happens in the background
#     like /upload."""
#     content, fetch_method = await _fetch_url_content(url)

#     if content is None:
#         raise HTTPException(
#             status.HTTP_400_BAD_REQUEST,
#             f"Failed to fetch URL: {url}. The website may be blocking automated access. "
#             "Try downloading the page as an HTML file and uploading it instead.",
#         )

#     title = _extract_title(content) or url

#     UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
#     doc_id = str(uuid.uuid4())[:8]
#     file_path = UPLOAD_DIR / f"{doc_id}_url_import.html"
#     async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
#         await f.write(content)

#     await conn.execute(
#         """
#         INSERT INTO content_sources (id, name, source_type, location, collection_name,
#                                       document_count, chunk_count, status, metadata)
#         VALUES ($1, $2, 'url', $3, $4, 0, 0, 'pending', $5::jsonb)
#         ON CONFLICT (id) DO UPDATE SET status = 'pending'
#         """,
#         doc_id, title[:255], url, collection,
#         json.dumps({"title": title, "fetch_method": fetch_method}),
#     )

#     background_tasks.add_task(
#         _ingest_document, doc_id, file_path, title, ".html", collection, len(content.encode("utf-8"))
#     )

#     return {
#         "success": True,
#         "document_id": doc_id,
#         "title": title,
#         "url": url,
#         "collection": collection,
#         "fetch_method": fetch_method,
#         "status": "pending",
#     }


# async def _fetch_url_content(url: str):
#     """Try progressively more capable fetch strategies."""
#     # Strategy 1: cloudscraper (handles Cloudflare/bot protection)
#     try:
#         import cloudscraper
#         scraper = cloudscraper.create_scraper(
#             browser={"browser": "chrome", "platform": "windows", "desktop": True}
#         )
#         response = scraper.get(url, timeout=30)
#         response.raise_for_status()
#         return response.text, "cloudscraper"
#     except Exception as e:
#         logger.warning(f"cloudscraper failed for {url}: {e}")

#     # Strategy 2: httpx with browser-like headers
#     try:
#         import httpx
#         headers = {
#             "User-Agent": (
#                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
#                 "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
#             ),
#             "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
#             "Accept-Language": "en-US,en;q=0.9",
#         }
#         async with httpx.AsyncClient(timeout=30.0, headers=headers, follow_redirects=True) as client:
#             response = await client.get(url)
#             response.raise_for_status()
#             return response.text, "httpx"
#     except Exception as e:
#         logger.warning(f"httpx failed for {url}: {e}")

#     # Strategy 3: urllib fallback
#     try:
#         import urllib.request
#         req = urllib.request.Request(url, headers={
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#         })
#         resp = urllib.request.urlopen(req, timeout=30)
#         return resp.read().decode("utf-8", errors="ignore"), "urllib"
#     except Exception as e:
#         logger.warning(f"urllib failed for {url}: {e}")

#     return None, None


# def _extract_title(html_content: str) -> Optional[str]:
#     try:
#         from bs4 import BeautifulSoup
#         soup = BeautifulSoup(html_content, "html.parser")
#         return soup.title.string.strip() if soup.title and soup.title.string else None
#     except ImportError:
#         match = re.search(r"<title[^>]*>(.*?)</title>", html_content, re.IGNORECASE | re.DOTALL)
#         return match.group(1).strip() if match else None


# @router.delete("/documents/{doc_id}")
# async def delete_document(
#     doc_id: str,
#     conn: asyncpg.Connection = Depends(get_db),
#     user: dict = Depends(verify_token),
# ) -> Dict[str, Any]:
#     row = await conn.fetchrow(
#         "SELECT collection_name, location, source_type FROM content_sources WHERE id = $1",
#         doc_id,
#     )
#     if not row:
#         raise HTTPException(status.HTTP_404_NOT_FOUND, "Document not found")

#     try:
#         collection = chroma_client.get_collection(row["collection_name"])
#         results = await _with_retries(
#             lambda: collection.get(where={"doc_id": doc_id}), label=f"get[{doc_id}]"
#         )
#         if results["ids"]:
#             await _with_retries(
#                 lambda: collection.delete(ids=results["ids"]), label=f"delete[{doc_id}]"
#             )

#         if row["source_type"] in ("file", "url"):
#             file_path = Path(row["location"])
#             if file_path.exists() and UPLOAD_DIR in file_path.resolve().parents:
#                 file_path.unlink()

#         await conn.execute("DELETE FROM content_sources WHERE id = $1", doc_id)
#         return {"success": True, "deleted_id": doc_id}

#     except HTTPException:
#         raise
#     except Exception as e:
#         logger.error(f"Error deleting document {doc_id}: {e}")
#         raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Error deleting document: {e}")


# @router.post("/search")
# async def search_knowledge_base(
#     query: str = Form(...),
#     collection: str = Form(DEFAULT_COLLECTION),
#     top_k: int = Form(8, ge=1, le=50),
#     min_score: float = Form(0.0, ge=0.0, le=1.0),
#     _: dict = Depends(verify_token),
# ) -> Dict[str, Any]:
#     """Search the knowledge base. Returns full chunk text (never truncated) so
#     the calling LLM/chatbot gets complete context. `top_k` defaults higher
#     than before (8 vs 5) since chunks are now larger and more coherent;
#     `min_score` lets callers filter out weak matches instead of always
#     getting exactly top_k results regardless of relevance."""
#     chroma_collection = chroma_client.get_collection(collection)

#     results = await _with_retries(
#         lambda: chroma_collection.query(query_texts=[query], n_results=top_k),
#         label="chromadb.query",
#     )

#     formatted_results = []
#     if results["documents"] and results["documents"][0]:
#         for i, doc in enumerate(results["documents"][0]):
#             metadata = results["metadatas"][0][i] if results["metadatas"] else {}
#             distance = results["distances"][0][i] if results.get("distances") else 0
#             score = 1 - distance
#             if score < min_score:
#                 continue
#             formatted_results.append({
#                 "content": doc,
#                 "source": metadata.get("source", "Unknown"),
#                 "score": round(score, 4),
#                 "metadata": metadata,
#             })

#     return {
#         "query": query,
#         "results": formatted_results,
#         "result_count": len(formatted_results),
#         "collection": collection,
#     }


# @router.get("/collections")
# async def list_collections(_: dict = Depends(verify_token)) -> Dict[str, Any]:
#     try:
#         client = chroma_client.get_client()
#         collections = await _with_retries(client.list_collections, label="list_collections")
#         info = []
#         for col in collections:
#             count = await _with_retries(col.count, label=f"count[{col.name}]")
#             info.append({"name": col.name, "count": count})
#         return {"collections": info}
#     except HTTPException:
#         raise
#     except Exception as e:
#         return {"collections": [], "error": str(e)}


# @router.post("/collections")
# async def create_collection(name: str = Form(...), _: dict = Depends(verify_token)) -> Dict[str, Any]:
#     try:
#         collection = chroma_client.get_collection(name)
#         count = await _with_retries(collection.count, label=f"count[{name}]")
#         return {"success": True, "collection": name, "count": count}
#     except HTTPException:
#         raise
#     except Exception as e:
#         raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Failed to create collection: {e}")




#OLD CODE
import os
import uuid
import hmac
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import asyncpg
import aiofiles

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/knowledge-base", tags=["knowledge-base"])
security = HTTPBearer(auto_error=False)

# Configuration
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "/app/knowledge_base/data"))
CHROMADB_HOST = os.getenv("CHROMADB_HOST", "chromadb")
CHROMADB_PORT = int(os.getenv("CHROMADB_PORT", "8000"))
DEFAULT_COLLECTION = os.getenv("KB_COLLECTION", "website_content")

# Supported file types
SUPPORTED_FILE_TYPES = {
    ".txt": "text/plain",
    ".md": "text/markdown", 
    ".pdf": "application/pdf",
    ".html": "text/html",
    ".json": "application/json",
    ".csv": "text/csv",
}


# =============================================================================
# DATABASE CONNECTION
# =============================================================================

db_pool: Optional[asyncpg.Pool] = None


async def get_db():
    """Dependency for database connection."""
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", 5432)),
            database=os.getenv("DB_NAME", "chatbot"),
            user=os.getenv("DB_USER", "rasa"),
            password=os.getenv("DB_PASSWORD"),
            min_size=2,
            max_size=10
        )
    async with db_pool.acquire() as conn:
        yield conn


async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify admin token using JWT or static ADMIN_TOKEN."""
    import jwt
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required"
        )
    token = credentials.credentials
    # Accept static ADMIN_TOKEN for dashboard / development use
    admin_token = os.getenv("ADMIN_TOKEN")
    if admin_token and hmac.compare_digest(token, admin_token):
        return {"user_id": "admin", "email": "admin@local", "role": "admin"}
    try:
        secret = os.getenv("JWT_SECRET")
        if not secret:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="JWT_SECRET not configured"
            )
        payload = jwt.decode(token, secret, algorithms=["HS256"])
        return {"user_id": payload.get("sub"), "email": payload.get("email"), "role": payload.get("role", "viewer")}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")


# =============================================================================
# CHROMADB CLIENT
# =============================================================================

class ChromaDBClient:
    """ChromaDB client for vector operations."""
    
    def __init__(self):
        self._client = None
    
    def get_client(self):
        """Get or create ChromaDB client."""
        if self._client is None:
            try:
                import chromadb
                from chromadb.config import Settings
                
                self._client = chromadb.HttpClient(
                    host=CHROMADB_HOST,
                    port=CHROMADB_PORT,
                    settings=Settings(anonymized_telemetry=False)
                )
                logger.info(f"Connected to ChromaDB at {CHROMADB_HOST}:{CHROMADB_PORT}")
            except Exception as e:
                logger.error(f"Failed to connect to ChromaDB: {e}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"ChromaDB not available: {str(e)}"
                )
        return self._client
    
    def get_collection(self, name: str = DEFAULT_COLLECTION):
        """Get or create a collection with default embedding function."""
        client = self.get_client()
        try:
            from chromadb.utils.embedding_functions import DefaultEmbeddingFunction
            embedding_fn = DefaultEmbeddingFunction()
        except ImportError:
            logger.warning("Default embedding function not available, collection may fail on add")
            embedding_fn = None
        
        return client.get_or_create_collection(
            name=name,
            metadata={"hnsw:space": "cosine"},
            embedding_function=embedding_fn
        )


chroma_client = ChromaDBClient()


# =============================================================================
# DOCUMENT PROCESSING
# =============================================================================

def chunk_text(text: str, chunk_size: int = 500, overlap: int = 50) -> List[str]:
    """Split text into chunks with overlap."""
    if len(text) <= chunk_size:
        return [text]
    
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        
        # Try to break at sentence boundary
        if end < len(text):
            last_period = chunk.rfind('. ')
            last_newline = chunk.rfind('\n')
            break_point = max(last_period, last_newline)
            if break_point > chunk_size // 2:
                chunk = chunk[:break_point + 1]
                end = start + break_point + 1
        
        chunks.append(chunk.strip())
        start = end - overlap
    
    return [c for c in chunks if c]


async def process_text_file(file_path: Path) -> str:
    """Read text from a file."""
    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        return await f.read()


async def process_markdown_file(file_path: Path) -> str:
    """Process markdown file."""
    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = await f.read()
    # Simple markdown cleanup
    import re
    # Remove code blocks but keep content
    content = re.sub(r'```[\s\S]*?```', '', content)
    # Remove inline code
    content = re.sub(r'`[^`]+`', '', content)
    # Remove markdown links but keep text
    content = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', content)
    # Remove headers markers
    content = re.sub(r'^#+\s*', '', content, flags=re.MULTILINE)
    return content


async def process_html_file(file_path: Path) -> str:
    """Extract text from HTML file."""
    async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = await f.read()
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')
        # Remove scripts and styles
        for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
            tag.decompose()
        return soup.get_text(separator='\n', strip=True)
    except ImportError:
        # Fallback: simple regex-based extraction
        import re
        content = re.sub(r'<script[^>]*>[\s\S]*?</script>', '', content)
        content = re.sub(r'<style[^>]*>[\s\S]*?</style>', '', content)
        content = re.sub(r'<[^>]+>', ' ', content)
        return ' '.join(content.split())


async def process_pdf_file(file_path: Path) -> str:
    """Extract text from PDF file."""
    try:
        import PyPDF2
        text_parts = []
        with open(file_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                text_parts.append(page.extract_text())
        return '\n'.join(text_parts)
    except ImportError:
        logger.warning("PyPDF2 not installed, skipping PDF processing")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="PDF processing not available. Install PyPDF2."
        )


async def process_file(file_path: Path, file_type: str) -> str:
    """Process file based on type."""
    processors = {
        ".txt": process_text_file,
        ".md": process_markdown_file,
        ".html": process_html_file,
        ".pdf": process_pdf_file,
        ".json": process_text_file,
        ".csv": process_text_file,
    }
    
    processor = processors.get(file_type, process_text_file)
    return await processor(file_path)


# =============================================================================
# API ENDPOINTS
# =============================================================================

@router.get("/stats")
async def get_knowledge_base_stats(
    _: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Get knowledge base statistics."""
    try:
        client = chroma_client.get_client()
        collections = client.list_collections()
        
        total_chunks = 0
        collection_info = []
        
        for col in collections:
            count = col.count()
            total_chunks += count
            collection_info.append({
                "name": col.name,
                "count": count
            })
        
        return {
            "total_collections": len(collections),
            "total_chunks": total_chunks,
            "collections": collection_info,
            "chromadb_status": "connected"
        }
    except Exception as e:
        return {
            "total_collections": 0,
            "total_chunks": 0,
            "collections": [],
            "chromadb_status": f"error: {str(e)}"
        }


@router.get("/documents")
async def list_documents(
    conn: asyncpg.Connection = Depends(get_db),
    _: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """List all documents in the knowledge base."""
    rows = await conn.fetch("""
        SELECT id, name, source_type, location, collection_name,
               enabled, last_ingested, document_count, chunk_count, metadata
        FROM content_sources
        ORDER BY created_at DESC
    """)
    
    documents = [dict(row) for row in rows]
    return {"documents": documents, "total": len(documents)}


@router.post("/upload")
async def upload_document(
    file: UploadFile = File(...),
    collection: str = Form(DEFAULT_COLLECTION),
    conn: asyncpg.Connection = Depends(get_db),
    user: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Upload and process a document."""
    # Validate file size (max 10MB)
    MAX_UPLOAD_SIZE = 10 * 1024 * 1024
    contents = await file.read()
    if len(contents) > MAX_UPLOAD_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large. Maximum size is {MAX_UPLOAD_SIZE // (1024*1024)}MB"
        )
    await file.seek(0)

    # Validate file type
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in SUPPORTED_FILE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported file type: {file_ext}. Supported: {list(SUPPORTED_FILE_TYPES.keys())}"
        )
    
    # Create upload directory if needed
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    # Generate unique ID and save file
    doc_id = str(uuid.uuid4())[:8]
    safe_filename = f"{doc_id}_{file.filename.replace(' ', '_')}"
    file_path = UPLOAD_DIR / safe_filename
    
    try:
        # Save file
        async with aiofiles.open(file_path, 'wb') as f:
            content = await file.read()
            await f.write(content)
        
        # Process document
        text_content = await process_file(file_path, file_ext)
        
        if not text_content or len(text_content.strip()) < 10:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Document appears to be empty or could not be processed"
            )
        
        # Chunk the content
        chunks = chunk_text(text_content)
        
        # Add to ChromaDB
        chroma_collection = chroma_client.get_collection(collection)
        chunk_ids = [f"{doc_id}_chunk_{i}" for i in range(len(chunks))]
        metadatas = [
            {
                "source": file.filename,
                "doc_id": doc_id,
                "chunk_index": i,
                "file_type": file_ext
            }
            for i in range(len(chunks))
        ]
        
        chroma_collection.add(
            ids=chunk_ids,
            documents=chunks,
            metadatas=metadatas
        )
        
        # Save metadata to database
        import json as _json
        metadata_str = _json.dumps({"file_type": file_ext, "size_bytes": len(content)})
        await conn.execute("""
            INSERT INTO content_sources (id, name, source_type, location, collection_name,
                                        document_count, chunk_count, last_ingested, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                document_count = $6,
                chunk_count = $7,
                last_ingested = NOW()
        """, doc_id, file.filename, 'file', str(file_path), collection,
             1, len(chunks), metadata_str)
        
        return {
            "success": True,
            "document_id": doc_id,
            "filename": file.filename,
            "chunks_created": len(chunks),
            "collection": collection,
            "content_preview": text_content[:200] + "..." if len(text_content) > 200 else text_content
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing document: {e}")
        # Cleanup file on error
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing document: {str(e)}"
        )


@router.post("/import-url")
async def import_from_url(
    url: str = Form(...),
    collection: str = Form(DEFAULT_COLLECTION),
    conn: asyncpg.Connection = Depends(get_db),
    user: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Import content from a URL."""
    
    content = None
    fetch_method = None
    
    # -------------------------------------------------------------------------
    # Strategy 1: cloudscraper (handles Cloudflare/bot protection)
    # -------------------------------------------------------------------------
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True,
            }
        )
        response = scraper.get(url, timeout=30)
        response.raise_for_status()
        content = response.text
        fetch_method = "cloudscraper"
        logger.info(f"URL fetched successfully with cloudscraper: {url}")
    except Exception as e:
        logger.warning(f"cloudscraper failed for {url}: {e}")
    
    # -------------------------------------------------------------------------
    # Strategy 2: httpx with browser-like headers
    # -------------------------------------------------------------------------
    if content is None:
        try:
            import httpx
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Cache-Control": "max-age=0",
            }
            async with httpx.AsyncClient(
                timeout=30.0,
                headers=headers,
                follow_redirects=True,
                http2=True,
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
                content = response.text
                fetch_method = "httpx"
                logger.info(f"URL fetched successfully with httpx: {url}")
        except Exception as e:
            logger.warning(f"httpx failed for {url}: {e}")
    
    # -------------------------------------------------------------------------
    # Strategy 3: urllib with cookie jar (basic fallback)
    # -------------------------------------------------------------------------
    if content is None:
        try:
            import urllib.request
            import http.cookiejar
            cj = http.cookiejar.CookieJar()
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            })
            resp = opener.open(req, timeout=30)
            content = resp.read().decode('utf-8', errors='ignore')
            fetch_method = "urllib"
            logger.info(f"URL fetched successfully with urllib: {url}")
        except Exception as e:
            logger.warning(f"urllib failed for {url}: {e}")
    
    # All strategies failed
    if content is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"Failed to fetch URL: {url}. The website may be blocking automated access. "
                "Try downloading the page as an HTML file and uploading it instead."
            )
        )
    
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content, 'html.parser')
        
        # Get title
        title = soup.title.string if soup.title else url
        
        # Remove unwanted elements
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            tag.decompose()
        
        # Get main content
        main_content = soup.find('main') or soup.find('article') or soup.find('body')
        text_content = main_content.get_text(separator='\n', strip=True) if main_content else soup.get_text()
        
    except ImportError:
        # Fallback
        import re
        text_content = re.sub(r'<[^>]+>', ' ', content)
        text_content = ' '.join(text_content.split())
        title = url
    
    if not text_content or len(text_content.strip()) < 50:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Could not extract meaningful content from URL"
        )
    
    # Chunk and add to ChromaDB
    doc_id = str(uuid.uuid4())[:8]
    chunks = chunk_text(text_content)
    
    try:
        chroma_collection = chroma_client.get_collection(collection)
        chunk_ids = [f"{doc_id}_chunk_{i}" for i in range(len(chunks))]
        metadatas = [
            {
                "source": url,
                "title": title,
                "doc_id": doc_id,
                "chunk_index": i,
                "source_type": "url"
            }
            for i in range(len(chunks))
        ]
        
        chroma_collection.add(
            ids=chunk_ids,
            documents=chunks,
            metadatas=metadatas
        )
        
        # Save to database
        import json as _json
        metadata_str = _json.dumps({"title": title})
        await conn.execute("""
            INSERT INTO content_sources (id, name, source_type, location, collection_name,
                                        document_count, chunk_count, last_ingested, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                document_count = $6,
                chunk_count = $7,
                last_ingested = NOW()
        """, doc_id, title[:255], 'url', url, collection,
             1, len(chunks), metadata_str)
        
        return {
            "success": True,
            "document_id": doc_id,
            "title": title,
            "url": url,
            "chunks_created": len(chunks),
            "collection": collection,
            "fetch_method": fetch_method
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing URL content: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing URL content: {str(e)}"
        )


@router.delete("/documents/{doc_id}")
async def delete_document(
    doc_id: str,
    conn: asyncpg.Connection = Depends(get_db),
    user: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Delete a document from the knowledge base."""
    # Get document info
    row = await conn.fetchrow(
        "SELECT collection_name, location, source_type FROM content_sources WHERE id = $1",
        doc_id
    )
    
    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document not found"
        )
    
    try:
        # Delete from ChromaDB
        collection = chroma_client.get_collection(row['collection_name'])
        # Get all chunk IDs for this document
        results = collection.get(where={"doc_id": doc_id})
        if results['ids']:
            collection.delete(ids=results['ids'])
        
        # Delete file if it's a local file
        if row['source_type'] == 'file':
            file_path = Path(row['location'])
            if file_path.exists():
                file_path.unlink()
        
        # Delete from database
        await conn.execute("DELETE FROM content_sources WHERE id = $1", doc_id)
        
        return {"success": True, "deleted_id": doc_id}
        
    except Exception as e:
        logger.error(f"Error deleting document: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting document: {str(e)}"
        )


@router.post("/search")
async def search_knowledge_base(
    query: str = Form(...),
    collection: str = Form(DEFAULT_COLLECTION),
    top_k: int = Form(5),
    _: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Search the knowledge base."""
    try:
        chroma_collection = chroma_client.get_collection(collection)
        
        results = chroma_collection.query(
            query_texts=[query],
            n_results=top_k
        )
        
        # Format results
        formatted_results = []
        if results['documents'] and results['documents'][0]:
            for i, doc in enumerate(results['documents'][0]):
                metadata = results['metadatas'][0][i] if results['metadatas'] else {}
                distance = results['distances'][0][i] if results.get('distances') else 0
                
                formatted_results.append({
                    "content": doc,
                    "source": metadata.get('source', 'Unknown'),
                    "score": 1 - distance,  # Convert distance to similarity
                    "metadata": metadata
                })
        
        return {
            "query": query,
            "results": formatted_results,
            "collection": collection
        }
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search failed: {str(e)}"
        )


@router.get("/collections")
async def list_collections(
    _: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """List all collections."""
    try:
        client = chroma_client.get_client()
        collections = client.list_collections()
        
        return {
            "collections": [
                {"name": col.name, "count": col.count()}
                for col in collections
            ]
        }
    except Exception as e:
        return {"collections": [], "error": str(e)}


@router.post("/collections")
async def create_collection(
    name: str = Form(...),
    _: dict = Depends(verify_token)
) -> Dict[str, Any]:
    """Create a new collection."""
    try:
        collection = chroma_client.get_collection(name)
        return {"success": True, "collection": name, "count": collection.count()}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create collection: {str(e)}"
        )
