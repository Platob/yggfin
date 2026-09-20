{#
  The expressions more than one product needs, kept in one place.
#}

{#
  Sixteen ordered bytes over the parts that scope one identity.

  `fix.silver` already carries the identities the parser could name --
  `curruuid` for an event and `crossuuid` for the chain it belongs to -- so a
  digest is written here only where SQL has to name something the parser had no
  word for. Every part is cast and coalesced before it is joined, so a null
  part and an empty one are the same identity and neither shifts the parts
  after it; MD5 is what DuckDB spells, and sixteen bytes is what an identity is.
#}
{% macro rekep_digest(parts) -%}
from_hex(md5(
    {%- for part in parts %}
    coalesce(cast({{ part }} as varchar), ''){% if not loop.last %} || '|' ||{% endif %}
    {%- endfor %}
))
{%- endmacro %}

{#
  The normalized states an order is no longer live in.

  `state` and `exectype` reach `fix.silver` as the codec's own sortable
  spelling of an OrdStatus(39) or ExecType(150) code, not as the wire's
  character: `2` Filled is `80FILLED`, `3` DoneForDay is `80DONEDAY`, `B`
  Calculated is `80CALCULAT`, `4` Canceled is `90CANCELED`, `8` Rejected is
  `95REJECTED` and `C` Expired is `95EXPIRED`. `5` Replaced (`70REPLACED`) is
  not here: a replaced order's chain continues under the replacement, so the
  event that closes it is the replacement's own terminal state.
#}
{% macro rekep_terminal_states() -%}
('80FILLED', '80DONEDAY', '80CALCULAT', '90CANCELED', '95REJECTED', '95EXPIRED')
{%- endmacro %}

{#
  The normalized states an order opens on: `0` New (`20NEW`) and `D` Accepted
  for bidding (`20ACCEPTED`). A pending state is not an opening: an order that
  never leaves `10PENDNEW` was never live.
#}
{% macro rekep_opening_states() -%}
('20NEW', '20ACCEPTED')
{%- endmacro %}
