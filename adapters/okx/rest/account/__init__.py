from adapters.okx.rest.account.balances import OkxRestAccountBalancesMixin
from adapters.okx.rest.account.parsers import OkxRestAccountParsersMixin
from adapters.okx.rest.account.positions import OkxRestAccountPositionsMixin
from adapters.okx.rest.account.summaries import OkxRestAccountSummariesMixin


class OkxRestAccountMixin(
    OkxRestAccountParsersMixin,
    OkxRestAccountBalancesMixin,
    OkxRestAccountPositionsMixin,
    OkxRestAccountSummariesMixin,
):
    pass


__all__ = ["OkxRestAccountMixin"]
