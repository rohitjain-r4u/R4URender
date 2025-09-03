from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple
from urllib.parse import urlencode


def _to_int(value, default):
    try:
        v = int(value)
        if v <= 0:
            return default
        return v
    except Exception:
        return default


def sanitize_page_params(page_value, per_page_value, *, default_per_page: int = 25, max_per_page: int = 200) -> tuple[int, int]:
    page = _to_int(page_value, 1)
    per_page = _to_int(per_page_value, default_per_page)
    if per_page > max_per_page:
        per_page = max_per_page
    return page, per_page


@dataclass(frozen=True)
class Paginator:
    total: int
    page: int
    per_page: int
    base_url: str
    args: Dict[str, str]

    @property
    def pages(self) -> int:
        return max(1, (self.total + self.per_page - 1) // self.per_page)

    @property
    def has_prev(self) -> bool:
        return self.page > 1

    @property
    def has_next(self) -> bool:
        return self.page < self.pages

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.per_page

    @property
    def limit(self) -> int:
        return self.per_page

    def url_for_page(self, page: int) -> str:
        params = {k: v for k, v in self.args.items() if k not in {"page"}}
        params.update({"page": str(page), "per_page": str(self.per_page)})
        query = urlencode(params, doseq=True)
        return f"{self.base_url}?{query}" if query else self.base_url

    def page_links(self) -> Dict[str, str]:
        links = {}
        if self.pages <= 1:
            return links
        links["first"] = self.url_for_page(1)
        links["last"] = self.url_for_page(self.pages)
        if self.has_prev:
            links["prev"] = self.url_for_page(self.page - 1)
        if self.has_next:
            links["next"] = self.url_for_page(self.page + 1)
        return links

    def windowed_pages(self, *, window: int = 2) -> List[Tuple[int, str, bool]]:
        total_pages = self.pages
        if total_pages <= 1:
            return [(1, self.url_for_page(1), True)]

        left = max(1, self.page - window)
        right = min(total_pages, self.page + window)
        pages: List[int] = []

        def add_unique(p):
            if not pages or pages[-1] != p:
                pages.append(p)

        add_unique(1)
        for p in range(left, right + 1):
            add_unique(p)
        add_unique(total_pages)

        return [(p, self.url_for_page(p), p == self.page) for p in pages]
