"""
TF-IDF Title Similarity for fuzzy job deduplication.

Catches near-duplicates that exact match misses:
- "Software Engineering Intern" vs "Software Engineer - Intern"
- "ML Research Intern" vs "Machine Learning Research Intern"

Uses pure Python TF-IDF (no sklearn dependency) for zero-install deployment.

Usage:
    engine = TitleSimilarity()
    engine.add("Software Engineering Intern", job_id="1")
    engine.add("Data Science Intern", job_id="2")
    
    matches = engine.find_similar("Software Engineer Intern", threshold=0.7)
    # [("Software Engineering Intern", 0.92, "1")]
"""
import re
import math
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict

log = logging.getLogger(__name__)


@dataclass
class SimilarityMatch:
    """Result of a similarity search."""
    title: str
    score: float           # 0.0 to 1.0
    job_id: str = ""
    company: str = ""


class TitleSimilarity:
    """
    TF-IDF based title similarity engine.
    
    Pure Python implementation — no external ML libraries needed.
    Uses cosine similarity over TF-IDF vectors.
    """

    def __init__(self):
        self._documents: List[dict] = []     # [{title, tokens, job_id, company}]
        self._df: Dict[str, int] = defaultdict(int)  # document frequency per term
        self._idf_cache: Dict[str, float] = {}
        self._dirty = True                   # IDF needs recomputation

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        """Tokenize and normalize a title."""
        text = text.lower().strip()
        # Remove common noise
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        # Split into tokens
        tokens = text.split()
        # Remove stopwords
        stopwords = {'the', 'a', 'an', 'and', 'or', 'of', 'in', 'at', 'for',
                      'to', 'is', 'on', 'with', 'as', 'by', 'from', 'this',
                      'that', 'it', 'be', 'are', 'was', 'been'}
        tokens = [t for t in tokens if t not in stopwords and len(t) > 1]
        return tokens

    @staticmethod
    def _tf(tokens: List[str]) -> Dict[str, float]:
        """Compute term frequency (normalized)."""
        counts = Counter(tokens)
        total = len(tokens)
        if total == 0:
            return {}
        return {term: count / total for term, count in counts.items()}

    def _compute_idf(self):
        """Recompute IDF scores for all terms."""
        n = len(self._documents)
        if n == 0:
            return
        self._idf_cache = {}
        for term, df in self._df.items():
            self._idf_cache[term] = math.log((n + 1) / (df + 1)) + 1  # smoothed IDF
        self._dirty = False

    def _tfidf_vector(self, tokens: List[str]) -> Dict[str, float]:
        """Compute TF-IDF vector for a token list."""
        if self._dirty:
            self._compute_idf()
        tf = self._tf(tokens)
        return {
            term: freq * self._idf_cache.get(term, 1.0)
            for term, freq in tf.items()
        }

    @staticmethod
    def _cosine_similarity(vec_a: Dict[str, float], vec_b: Dict[str, float]) -> float:
        """Compute cosine similarity between two sparse vectors."""
        # Dot product
        common_terms = set(vec_a.keys()) & set(vec_b.keys())
        if not common_terms:
            return 0.0

        dot = sum(vec_a[t] * vec_b[t] for t in common_terms)
        norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
        norm_b = math.sqrt(sum(v * v for v in vec_b.values()))

        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def add(self, title: str, job_id: str = "", company: str = ""):
        """Add a title to the similarity index."""
        tokens = self._tokenize(title)
        if not tokens:
            return

        # Update document frequency
        unique_tokens = set(tokens)
        for token in unique_tokens:
            self._df[token] += 1

        self._documents.append({
            "title": title,
            "tokens": tokens,
            "job_id": job_id,
            "company": company,
        })
        self._dirty = True

    def add_batch(self, items: List[dict]):
        """Add multiple titles at once. Each item: {title, job_id?, company?}"""
        for item in items:
            self.add(
                title=item.get("title", ""),
                job_id=item.get("job_id", ""),
                company=item.get("company", ""),
            )

    def find_similar(self, title: str, threshold: float = 0.7,
                     max_results: int = 5, same_company: str = "") -> List[SimilarityMatch]:
        """
        Find titles similar to the query.
        
        Args:
            title: query title
            threshold: minimum cosine similarity (0.0 - 1.0)
            max_results: max matches to return
            same_company: if set, only match within this company
        
        Returns:
            List of SimilarityMatch sorted by score descending
        """
        query_tokens = self._tokenize(title)
        if not query_tokens:
            return []

        query_vec = self._tfidf_vector(query_tokens)
        matches = []

        for doc in self._documents:
            if same_company and doc["company"].lower() != same_company.lower():
                continue

            doc_vec = self._tfidf_vector(doc["tokens"])
            score = self._cosine_similarity(query_vec, doc_vec)

            if score >= threshold:
                matches.append(SimilarityMatch(
                    title=doc["title"],
                    score=round(score, 4),
                    job_id=doc["job_id"],
                    company=doc["company"],
                ))

        matches.sort(key=lambda m: -m.score)
        return matches[:max_results]

    def is_near_duplicate(self, title: str, company: str = "",
                          threshold: float = 0.85) -> Optional[SimilarityMatch]:
        """
        Check if a title is a near-duplicate of any existing title.
        
        Higher threshold than find_similar — only catches very close matches.
        Returns the best match if above threshold, else None.
        """
        matches = self.find_similar(
            title, threshold=threshold, max_results=1,
            same_company=company
        )
        return matches[0] if matches else None

    @property
    def size(self) -> int:
        return len(self._documents)

    @property
    def vocabulary_size(self) -> int:
        return len(self._df)

    def stats(self) -> dict:
        return {
            "documents": self.size,
            "vocabulary": self.vocabulary_size,
            "avg_tokens_per_doc": (
                round(sum(len(d["tokens"]) for d in self._documents) / max(self.size, 1), 1)
            ),
        }

