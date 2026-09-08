<p align="center">
  <img src="logo.svg" width="80" alt="Kyiv chestnut">
</p>

<h1 align="center">#KyivNotKiev</h1>

<p align="center">
  <strong>A Computational Study of Ukrainian Toponym Adoption</strong><br>
  Measuring how English switched from Russian-derived to Ukrainian spellings, 2010–2025.
</p>

<p align="center">
  <a href="https://kyivnotkiev.org">kyivnotkiev.org</a> ·
  <a href="https://huggingface.co/datasets/KyivNotKiev/toponym-adoption-data">Dataset</a> ·
  <a href="https://www.bbc.com/ukrainian/news-45718643">BBC Coverage</a>
</p>

---

<!-- AUTO:metrics -->
| Metric | Value |
|--------|-------|
| Toponym matches | **1.5M** |
| Toponym pairs | **24** |
| Data sources | **7** |
| Time span | **2010-2025** |
| CL corpus | **151.6K** verified English texts |
<!-- /AUTO:metrics -->

Everything else — per-pair charts, methodology, collocations, discourse clusters,
and the verified holdout exhibits — lives on [kyivnotkiev.org](https://kyivnotkiev.org).
Pairs are configured in [`config/pairs.yaml`](config/pairs.yaml); all numbers here are
regenerated from the data manifest by `pipeline/update_readme.py` — never edit them by hand.
