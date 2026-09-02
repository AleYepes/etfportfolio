from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Endpoint:
    name: str
    url_prefix: str
    slug_template: str
    shape: str  # "snapshot" | "series"
    gated: bool
    delay_before_request: float = 0.0

    @property
    def url_template(self) -> str:
        return f"{self.url_prefix}{self.slug_template}"

    def resolve(self, **kwargs: Any) -> tuple[str, str, str]:
        slug = self.slug_template.format(**kwargs)
        full_url = f"{self.url_prefix}{slug}"
        return self.url_prefix, slug, full_url


ENDPOINTS: list[Endpoint] = [
    Endpoint(
        name="holdings",
        url_prefix="/tws.proxy/fundamentals/mf_holdings/",
        slug_template="{product_id}?lang=en",
        shape="snapshot",
        gated=True,
    ),
    Endpoint(
        name="ratios",
        url_prefix="/tws.proxy/fundamentals/mf_ratios_fundamentals/",
        slug_template="{product_id}?lang=en",
        shape="snapshot",
        gated=True,
    ),
    Endpoint(
        name="profile",
        url_prefix="/tws.proxy/fundamentals/mf_profile_and_fees/",
        slug_template="{product_id}?lang=en",
        shape="snapshot",
        gated=True,
    ),
    Endpoint(
        name="lipper",
        url_prefix="/tws.proxy/fundamentals/mf_lip_ratings/",
        slug_template="{product_id}?lang=en",
        shape="snapshot",
        gated=True,
    ),
    Endpoint(
        name="mstar",
        url_prefix="/tws.proxy/mstar/fund/detail?conid=",
        slug_template="{product_id}&lang=en",
        shape="snapshot",
        gated=True,
    ),
    Endpoint(
        name="ownership",
        url_prefix="/tws.proxy/fundamentals/ownership/",
        slug_template="{product_id}?fields=owners_types,institutional_owners,insider_owners,institutional_total,insider_total,institutional_summary,insider_summary,others_summary&lang=en",
        shape="snapshot",
        gated=False,
    ),
    Endpoint(
        name="esg",
        url_prefix="/tws.proxy/impact/esg/",
        slug_template="{product_id}?accounts={account_id}&lang=en",
        shape="snapshot",
        gated=False,
    ),
    Endpoint(
        name="theme_weights",
        url_prefix="/tws.proxy/knowledge-graph/ui/fund?conid=",
        slug_template="{product_id}&max=999999999&lang=en",
        shape="snapshot",
        gated=False,
    ),
    Endpoint(
        name="sentiment",
        url_prefix="/tws.proxy/sma/request?type=search&conid=",
        slug_template="{product_id}&from={from_date}%2000:00&to={to_date}%2000:00&bar_size=1D&lang=en",
        shape="series",
        gated=False,
        delay_before_request=0,
    ),
]

ENDPOINTS_BY_NAME: dict[str, Endpoint] = {ep.name: ep for ep in ENDPOINTS}
