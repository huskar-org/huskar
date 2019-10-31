Assets
======

Email Snapshots
---------------

{% for group in indices|groupby(1) -%}
* {{ group.grouper[1] }}
    {%- for id, _, relpath in group.list %}
    * :download:`快照范例 {{ id + 1 }} <{{ relpath }}>`
    {%- endfor %}
{% endfor %}
