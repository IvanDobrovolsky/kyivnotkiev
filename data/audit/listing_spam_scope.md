# Affiliate listing spam — scope and why 8 pairs were not rebuilt

A booking network publishes one YouTube description per property. Every row
differs by hotel name and address, so text-hash dedup keeps all of them:

    Guest House on Chkalova 79 - Feodosiya - country
    Save up to 25% with Smart Booking. Book it now: http://smart-booking.club/...
    Address: per Pervomaiskaya 4.; zip code: 98000  Set in Feodosiya, 9 km from ...

`pipeline/filters._LISTING_SPAM` now drops it, matching the network's call to
action rather than any hotel name, so a genuine travel description that happens
to say "Set in Feodosiya" survives.

## Where it mattered

feodosiia only. 2.3% of its YouTube rows, in a pair with 518 canonical records —
enough for the template to own the vocabulary profile. Before the filter its
solo profile was `guest, code, zip, ship, sevastopol`. After rebuilding:

    crimea 55.5 · lessons 42.2 · sea 32.6 · sevastopol 28.8 · ship 28.4
    crimean 28.0 · kerch 23.8 · plague 22.5 · black 21.2 · peninsula 19.5
    ivan 19.8 · landing 19.3

which is the real discourse: Crimean geography, the Black Sea fleet, the 1941–42
Kerch–Feodosia landings, the Black Death entering Europe at Caffa, and Ivan
Aivazovsky, who was born there.

## Where it did not

The other 8 affected pairs were NOT rebuilt, on a measurement rather than an
assumption. Spam share of canonical records:

    kyiv 0.02%   kharkiv 0.03%   odesa 0.10%   lviv 0.17%
    zaporizhzhia 0.01%   chornobyl 0.00%   dnipro-river 0.04%   babyn-yar 0.02%

and for every chip currently shipped by those pairs, the share of its token mass
coming from spam rows is under 1%. At those volumes the rows cannot move a
z-score past MIN_Z, so a rebuild would reproduce the same chips at a cost of
about three hours. The filter is live, so the next rebuild removes them anyway;
until then the corpus carries 252 spam rows across those 8 pairs and no
published result depends on them.

Measured 2026-09-13.
