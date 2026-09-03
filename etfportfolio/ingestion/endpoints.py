from dataclasses import dataclass
from typing import Any

_DETAILS_EXCLUDED = frozenset({"landing", "sentiment"})


@dataclass(frozen=True)
class Endpoint:
    name: str
    url_prefix: str
    slug_template: str
    gated: bool

    @property
    def url_template(self) -> str:
        return f"{self.url_prefix}{self.slug_template}"

    def resolve(self, **kwargs: Any) -> tuple[str, str, str]:
        slug = self.slug_template.format(**kwargs)
        full_url = f"{self.url_prefix}{slug}"
        return self.url_prefix, slug, full_url


ENDPOINTS: list[Endpoint] = [
    Endpoint(
        name="landing",
        url_prefix="/tws.proxy/fundamentals/landing/",
        slug_template="{product_id}?widgets=objective,keyProfile,lipper_ratings,holdings,mf_key_ratios,mstar&lang=en",
        gated=False,
    ),
    Endpoint(
        name="holdings",
        url_prefix="/tws.proxy/fundamentals/mf_holdings/",
        slug_template="{product_id}?lang=en",
        gated=True,
    ),
    Endpoint(
        name="ratios",
        url_prefix="/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        slug_template="{product_id}?lang=en",
        gated=True,
    ),
    Endpoint(
        name="profile",
        url_prefix="/tws.proxy/fundamentals/mf_profile_and_fees/",
        slug_template="{product_id}?lang=en",
        gated=True,
    ),
    Endpoint(
        name="lipper",
        url_prefix="/tws.proxy/fundamentals/mf_lip_ratings/",
        slug_template="{product_id}?lang=en",
        gated=True,
    ),
    Endpoint(
        name="mstar",
        url_prefix="/tws.proxy/mstar/fund/detail?conid=",
        slug_template="{product_id}&lang=en",
        gated=True,
    ),
    Endpoint(
        name="esg",
        url_prefix="/tws.proxy/impact/esg/",
        slug_template="{product_id}?accounts={account_id}&lang=en",
        gated=False,
    ),
    Endpoint(
        name="theme_weights",
        url_prefix="/tws.proxy/knowledge-graph/ui/fund?conid=",
        slug_template="{product_id}&max=999999999&lang=en",
        gated=False,
    ),
    Endpoint(
        name="sentiment",
        url_prefix="/tws.proxy/sma/request?type=search&conid=",
        slug_template="{product_id}&from={from_date}%2000:00&to={to_date}%2000:00&bar_size=1D&lang=en",
        gated=False,
    ),
]

ENDPOINTS_BY_NAME: dict[str, Endpoint] = {ep.name: ep for ep in ENDPOINTS}

# Snapshot endpoints the details phase actually fetches (excludes landing + sentiment).
DETAILS_ENDPOINTS: list[Endpoint] = [ep for ep in ENDPOINTS if ep.name not in _DETAILS_EXCLUDED]
GATED_ENDPOINTS: list[Endpoint] = [ep for ep in DETAILS_ENDPOINTS if ep.gated]
UNGATED_ENDPOINTS: list[Endpoint] = [ep for ep in DETAILS_ENDPOINTS if not ep.gated]
