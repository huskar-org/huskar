from __future__ import absolute_import

import re


rfc1738_pattern = re.compile(r'''
    (?P<name>[\w\+]+)://
    (?:
        (?P<username>[^:/]*)
        (?::(?P<password>.*))?
    @)?
    (?:
        (?:
            \[(?P<ipv6host>[^/]+)\] |
            (?P<ipv4host>[^/:]+)
        )?
        (?::(?P<port>[^/]*))?
    )?
    (?:/(?P<path>.*))?
    ''', re.X)


def parse_rfc1738_args(url):
    """Parse URL with the RFC 1738."""
    m = rfc1738_pattern.match(url)
    if m is None:
        raise ValueError('Cannot parse RFC 1738 URL: {!r}'.format(url))
    return m.groupdict()


def extract_application_name(url):
    """Parses the Sam URL and returns its application name.

    :param url: The URL string.
    :returns: The application name, or ``None`` if this is not a Sam URL.
    """
    try:
        args = parse_rfc1738_args(url)
    except ValueError:
        return
    scheme = args['name'] or ''
    if scheme.startswith('sam+'):
        return args['ipv4host'] or args['ipv6host']


def extract_application_names(urls):
    """Parses the Sam URLs and returns names of valid applications.

    :param urls: The list or dictionary of Sam URLs.
    :returns: The list or dictionary of application names.
    """
    if isinstance(urls, dict):
        iterator = (
            (key, extract_application_name(url))
            for key, url in urls.iteritems())
        return {key: name for key, name in iterator if name}
    iterator = (extract_application_name(url) for url in urls)
    return [name for name in iterator if name]
