from pkg_resources import DistributionNotFound, get_distribution

from .core import read_snowflake, to_snowflake

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    pass
