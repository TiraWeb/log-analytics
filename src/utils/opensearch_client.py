"""OpenSearch / OpenObserve HTTP client.

Handles:
  - Basic-auth sessions with automatic retry (3×, exponential backoff)
  - Health check  →  GET /_cluster/health  (OS) or  GET /healthz  (OO)
  - Date-histogram aggregations  →  server-side bucketing (avoids pulling raw docs)
  - Scroll API  →  paginated raw-doc retrieval for services that need field-level parsing
  - Service discovery via terms aggregation on `service_name`

Designed for both vanilla OpenSearch (AWS OpenSearch Service, self-hosted)
and OpenObserve's OpenSearch-compatible /_search endpoint.

Usage:
    from src.utils.opensearch_client import OpenSearchClient
    client = OpenSearchClient()          # reads config/config.yaml
    client.check_health()
    buckets = client.date_histogram_agg(...)
    docs    = list(client.scroll_docs(...))
"""
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# Maximum docs per scroll page — keep memory pressure low
_DEFAULT_SCROLL_SIZE = 1_000
# Scroll context TTL
_SCROLL_TTL = "2m"
# Retry config
_MAX_RETRIES = 3
_BACKOFF_FACTOR = 1.5


class OpenSearchClient:
    """Thin HTTP client for OpenSearch-compatible APIs."""

    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

        os_cfg = cfg.get("opensearch", {})

        self.base_url   = os_cfg.get("url", "http://localhost:9200").rstrip("/")
        self.username   = os_cfg.get("username", "admin")
        self.password   = os_cfg.get("password", "admin")
        self.index      = os_cfg.get("index", "microservice_logs")
        self.timeout    = os_cfg.get("timeout", 30)
        self.scroll_size = os_cfg.get("scroll_size", _DEFAULT_SCROLL_SIZE)
        # OpenObserve uses a different health endpoint
        self.is_openobserve = os_cfg.get("is_openobserve", False)
        # Field name mappings (override if your index uses different names)
        self.field_timestamp   = os_cfg.get("field_timestamp",   "_timestamp")
        self.field_service     = os_cfg.get("field_service",      "service_name")
        self.field_level       = os_cfg.get("field_level",        "log_level")
        self.field_latency     = os_cfg.get("field_latency",      "latency_ms")
        self.field_status_code = os_cfg.get("field_status_code",  "status_code")

        self.session = self._build_session()
        logger.info(f"OpenSearchClient → {self.base_url}  index={self.index}")

    # ── Session ──────────────────────────────────────────────────────────────

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.auth = (self.username, self.password)
        session.headers.update({
            "Content-Type": "application/json",
            "Accept":       "application/json",
        })
        retry = Retry(
            total=_MAX_RETRIES,
            backoff_factor=_BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://",  adapter)
        session.mount("https://", adapter)
        return session

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get(self, path: str, **kwargs) -> Optional[Dict]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            r = self.session.get(url, timeout=self.timeout, **kwargs)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"GET {url} failed: {e}")
            return None

    def _post(self, path: str, body: Dict, **kwargs) -> Optional[Dict]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            r = self.session.post(url, json=body, timeout=self.timeout, **kwargs)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"POST {url} failed: {e}")
            return None

    def _delete(self, path: str, **kwargs) -> bool:
        url = f"{self.base_url}/{path.lstrip('/')}"
        try:
            r = self.session.delete(url, timeout=self.timeout, **kwargs)
            r.raise_for_status()
            return True
        except Exception as e:
            logger.warning(f"DELETE {url} failed: {e}")
            return False

    @staticmethod
    def _to_epoch_ms(dt: datetime) -> int:
        """Convert aware or naive datetime → epoch milliseconds."""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    # ── Public API ────────────────────────────────────────────────────────────

    def check_health(self) -> bool:
        """Return True if the cluster / server is reachable and green/yellow."""
        if self.is_openobserve:
            resp = self._get("/healthz")
            return resp is not None
        resp = self._get("/_cluster/health")
        if resp is None:
            return False
        status = resp.get("status", "red")
        ok = status in ("green", "yellow")
        logger.info(f"Cluster health: {status}")
        return ok

    def discover_services(self, start: datetime, end: datetime,
                          size: int = 500) -> List[str]:
        """Return distinct service_name values seen between start and end."""
        body = {
            "size": 0,
            "query": {
                "range": {
                    self.field_timestamp: {
                        "gte": self._to_epoch_ms(start),
                        "lte": self._to_epoch_ms(end),
                        "format": "epoch_millis",
                    }
                }
            },
            "aggs": {
                "services": {
                    "terms": {
                        "field": f"{self.field_service}.keyword",
                        "size":  size,
                    }
                }
            },
        }
        resp = self._post(f"/{self.index}/_search", body)
        if not resp:
            return []
        buckets = resp.get("aggregations", {}).get("services", {}).get("buckets", [])
        services = [b["key"] for b in buckets]
        logger.info(f"Discovered {len(services)} services in OpenSearch")
        return services

    def date_histogram_agg(
        self,
        service_name: str,
        start: datetime,
        end: datetime,
        interval_minutes: int = 5,
    ) -> List[Dict[str, Any]]:
        """Run a date_histogram aggregation for one service over the time window.

        Returns a list of bucket dicts, each with:
            timestamp_ms  : bucket start in epoch-ms
            doc_count     : total log lines in bucket
            error_count   : logs with log_level ERROR/FATAL/CRITICAL
            avg_latency   : avg latency_ms (0 if field absent)
            p95_latency   : 95th-pct latency_ms via percentiles agg
            count_5xx     : logs with status_code >= 500
        """
        body = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                self.field_timestamp: {
                                    "gte": self._to_epoch_ms(start),
                                    "lte": self._to_epoch_ms(end),
                                    "format": "epoch_millis",
                                }
                            }
                        },
                        {"term": {f"{self.field_service}.keyword": service_name}},
                    ]
                }
            },
            "aggs": {
                "by_time": {
                    "date_histogram": {
                        "field":            self.field_timestamp,
                        "fixed_interval":   f"{interval_minutes}m",
                        "min_doc_count":    1,
                    },
                    "aggs": {
                        "error_count": {
                            "filter": {
                                "terms": {
                                    f"{self.field_level}.keyword": [
                                        "ERROR", "FATAL", "CRITICAL",
                                        "error", "fatal", "critical",
                                    ]
                                }
                            }
                        },
                        "avg_latency": {
                            "avg": {"field": self.field_latency}
                        },
                        "p95_latency": {
                            "percentiles": {
                                "field":    self.field_latency,
                                "percents": [95],
                            }
                        },
                        "count_5xx": {
                            "filter": {
                                "range": {
                                    self.field_status_code: {"gte": 500}
                                }
                            }
                        },
                    },
                }
            },
        }

        resp = self._post(f"/{self.index}/_search", body)
        if not resp:
            return []

        raw_buckets = (
            resp.get("aggregations", {})
                .get("by_time", {})
                .get("buckets", [])
        )

        out = []
        for b in raw_buckets:
            p95_values = b.get("p95_latency", {}).get("values", {})
            p95 = p95_values.get("95.0") or p95_values.get("95") or 0.0
            out.append({
                "timestamp_ms": b["key"],
                "doc_count":    b["doc_count"],
                "error_count":  b["error_count"]["doc_count"],
                "avg_latency":  b["avg_latency"].get("value") or 0.0,
                "p95_latency":  p95 if p95 else 0.0,
                "count_5xx":    b["count_5xx"]["doc_count"],
            })
        return out

    def scroll_docs(
        self,
        service_name: str,
        start: datetime,
        end: datetime,
        fields: Optional[List[str]] = None,
    ) -> Generator[Dict, None, None]:
        """Yield raw log documents via the Scroll API.

        Use this only when you need field-level data that aggregations
        can't produce (e.g. custom regex parsing).  For metric extraction
        prefer date_histogram_agg() — it's orders of magnitude cheaper
        at 6.5 TB/month scale.

        Args:
            service_name : filter to this service
            start / end  : inclusive time window
            fields       : _source fields to include (None = all)

        Yields:
            Raw _source dicts for each matching document.
        """
        body: Dict[str, Any] = {
            "size": self.scroll_size,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                self.field_timestamp: {
                                    "gte": self._to_epoch_ms(start),
                                    "lte": self._to_epoch_ms(end),
                                    "format": "epoch_millis",
                                }
                            }
                        },
                        {"term": {f"{self.field_service}.keyword": service_name}},
                    ]
                }
            },
            "sort": [{self.field_timestamp: "asc"}],
        }
        if fields:
            body["_source"] = fields

        # Initial scroll request
        resp = self._post(
            f"/{self.index}/_search?scroll={_SCROLL_TTL}",
            body,
        )
        if not resp:
            return

        scroll_id = resp.get("_scroll_id")
        hits = resp.get("hits", {}).get("hits", [])

        while hits:
            for hit in hits:
                yield hit.get("_source", {})

            if not scroll_id:
                break

            # Fetch next page
            resp = self._post(
                "/_search/scroll",
                {"scroll": _SCROLL_TTL, "scroll_id": scroll_id},
            )
            if not resp:
                break
            scroll_id = resp.get("_scroll_id", scroll_id)
            hits = resp.get("hits", {}).get("hits", [])

        # Clean up scroll context to free memory on the cluster
        if scroll_id:
            self._delete(f"/_search/scroll/{scroll_id}")

    def get_index_stats(self) -> Optional[Dict]:
        """Return basic stats for the configured index (doc count, store size)."""
        resp = self._get(f"/{self.index}/_stats/docs,store")
        if not resp:
            return None
        idx = resp.get("indices", {}).get(self.index, {})
        primaries = idx.get("primaries", {})
        return {
            "doc_count":    primaries.get("docs",  {}).get("count",        0),
            "size_bytes":   primaries.get("store", {}).get("size_in_bytes", 0),
        }
