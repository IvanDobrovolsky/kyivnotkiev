# ARR October 2026 — submission requirements

Target: ARR October cycle → commit to NAACL 2027 (San Francisco, 1–5 Jun 2027).
Sources are ARR's own site and NAACL 2027's CFP. Items marked ⚠️ could not be
verified from a primary source and must be re-checked before submitting.

---

## Dates

| What | When | Note |
|---|---|---|
| **Submission** | **12 Oct 2026** | ARR + NAACL agree |
| **Reviewer registration — every author** | **12 Oct 2026** | NAACL says this date; ARR's general rule says 48h after the deadline. **Assume 12 Oct.** |
| Reviews due | ~16 Nov 2026 | ⚠️ Projected from the August cycle. ARR lists it as TBA. |
| Author response | ~23 Nov – 3 Dec | ⚠️ Same — projected, not published. |
| Meta-reviews released | 18 Dec 2026 | From NAACL's CFP; ARR says TBA |
| **Commitment deadline** | **20 Dec 2026** | ⚠️ ARR says 20 Dec, NAACL says 23 Dec. **Use 20 Dec.** |
| NAACL notification | 10 Feb 2027 | |
| Conference | 1–5 Jun 2027 | San Francisco |

Re-check ARR's dates page in late November — most October-cycle dates are still
TBA and will be filled in.

---

## Desk rejections — these kill the paper before anyone reads it

Ordered by how easy each is to do by accident.

1. **Any author fails to file the reviewer registration form.** Automatic. This is
   a ten-minute form and the single most likely way to lose the submission.
2. **No Limitations section.** It is mandatory, does not count toward the page
   limit, and must not introduce new methods, analysis or results.
3. **Key material hidden in an appendix.** Appendices are unlimited, but anything
   needed to judge novelty, claims or technical correctness must be in the eight
   pages. Burying it reads as evading the page limit.
4. **Appendices not double-column.** Desk-reject trigger since July 2025.
   Exception for math-heavy sections.
5. **Anonymity violation.** Author names, affiliations, acknowledgements, or a
   link to a repository that identifies the author.
6. **Responsible NLP checklist incorrect, incomplete or misleading.**
7. **Template modified**, or the `[review]` setting left off.
8. **Selecting the binding "we do not intend to release a preprint" option** and
   then preprinting.
9. Exceeding the page limit; dual submission; >10% text reuse from prior work;
   resubmission that fails to acknowledge its earlier version.

Desk rejection can happen at any point in the cycle, including after reviews.

---

## Format

- **8 pages of content.** Accepted papers get a 9th for camera-ready.
- **Unlimited references.**
- **Unlimited Limitations section** — required, does not count toward the limit.
- **Unlimited appendices**, after the references, double-column, not counted —
  subject to the rule in desk-rejection item 3.
- Official ACL style files, unmodified. Run the ACL `pubcheck` tool.
- No submission fee found anywhere. ⚠️ No positive statement to that effect
  either — inferred from the total absence of fee language.

### What this means for 24 pairs

The argument has to stand on **4–6 representative pairs in the main eight pages**.
The other 18 go in the appendix. Choosing which 4–6 is a real editorial decision,
not a formatting one: they have to carry the claim on their own.

---

## Anonymisation — the awkward part

The paper's strength is a public pipeline and a published dataset. The review PDF
must not reveal either.

| Asset | Handling |
|---|---|
| Code | Mirror to **Anonymous GitHub** — ARR names this tool explicitly |
| Dataset | Upload as a `.zip` with the submission. **Do not link HuggingFace** — the handle identifies the author |
| Site | **Do not cite kyivnotkiev.org** in the PDF |
| Release claim | State that the dataset will be released, without saying where |

That last line is worth points rather than costing them. The review form scores
**Datasets** separately, and the top mark goes to a release that "should affect
other people's choice of research or development projects to undertake."

⚠️ ARR's anonymity rules never mention HuggingFace; only Anonymous GitHub is
named. The guidance above is inference from the general rule. If unsure, email
editors@aclrollingreview.org before the deadline.

**The SSRN preprint is fine.** There has been no anonymity period since 15 Feb
2024, and NAACL's CFP repeats this. Reviewers are told that identifying an author
from a preprint is not a violation. One real cost: ARR gives anonymous
submissions "priority in acceptance decisions for borderline papers", so
preprinting forfeits a tiebreaker.

---

## Responsible NLP checklist

Section B is the substantial one for this paper. It requires, per artifact:

- the licence of every asset used
- **for scraped or API-collected data, the copyright and terms of service of the
  source** — eight separate positions: GDELT, Google Trends, Wikipedia, Reddit,
  YouTube, Google Books Ngrams, OpenAlex, Telegram
- whether intended use is compatible with the original licence
- **steps taken to check for and anonymise personally identifying content** —
  Reddit and Telegram text need an answer
- documentation, which the checklist explicitly ties to dataset cards
- dataset statistics

Section E requires disclosing AI writing and coding assistance.

Start section B early. Eight terms-of-service positions is not a last-week task.

---

## Reviewing obligation

**Registration: mandatory, every author, no exemption.** Desk rejection if missed.
Needs an up-to-date OpenReview profile with affiliation and, where they exist,
Semantic Scholar / DBLP / ACL Anthology links.

**Actually reviewing: almost certainly not required here.** ARR's bar is two or
more papers in main ACL venues or Findings, plus one more in the Anthology or a
major ML venue. A first-time author meets neither clause, and the CFP names
"authors new to the community" as an exemption. Assignment is unlikely but not
impossible — ARR considers candidates case-by-case when a paper has no qualified
reviewers.

No institutional affiliation is required to submit. ARR states it "welcomes
scholars from other communities."

---

## Scores and what follows

Overall Assessment maps directly to outcomes:

```
5    Consider for Award        3    Findings
4.5  Borderline Award          2.5  Borderline Findings
4    Conference                2    Resubmit next cycle
3.5  Borderline Conference     1.5  Resubmit after next cycle
                               1    Do not resubmit
```

Findings is judged on **soundness and reproducibility**; the conference track also
weighs novelty and impact. That split favours this paper.

⚠️ NAACL 2027's CFP does not mention Findings at all. It ran in 2022, 2024 and
2025 — Findings of NAACL 2025 carried 476 papers against 720 in the main
conference — so it is likely but unconfirmed for 2027.

**After the meta-review** the paper is free: commit to a venue, resubmit to a
later ARR cycle, or take it elsewhere. These are mutually exclusive.
Being rejected after committing carries no penalty.

**Two hard gates:** a meta-review of 1.5 blocks resubmission to the next cycle;
1 blocks it permanently for that paper.

**Withdrawal trap:** withdrawing more than 48h after the deadline blocks
resubmission until the *second* subsequent cycle. There is rarely a reason to
withdraw — the paper is already free once the meta-review lands.

---

## Venue choice at commitment

Commit to **NAACL as primary**, tick the option to be considered by COLING if
NAACL declines. One primary venue only, no dual commitment, and the choice cannot
be changed afterwards. The fallback is explicitly not guaranteed.

⚠️ COLING 2027 cannot be verified at all: `coling2027.org` is a parked domain
with no content. Dates, location, page limits and Findings status are unknown.
The only evidence it exists is ARR's venue table and NAACL's CFP.

---

## Framing — the thing most likely to decide the outcome

**Excitement** is a scored axis and the bottom of its scale reads "does not
resonate with me... in no way related to computational processing of language."
Adoption curves invite that reaction from a reviewer pool whose default paper is
about language models.

Lead with the **measurement failures**, not the adoption curves:

- GDELT's `AllNames` canonicalises entity names, manufacturing variant
  distributions that were never in the text. Usyk's 85%-Russian series was an
  artifact of `Oleksandr → Alexander`.
- YouTube's search API saturates for high-volume terms, so counts measure
  sampling depth rather than usage. The Russian variant cannot spike.
- A Google Trends joint query quantises the minority variant to zero — 191 of
  192 months for chornobyl.

ARR's CFP explicitly invites this genre: negative results reporting
"non-reproducibility... misattribution ('right for the wrong reasons')". The
GDELT result is exactly that, about a corpus the NLP community uses as ground
truth. Adoption curves become the application that motivates and validates the
critique.

**Bibliography.** ARR judges community fit partly by whether the reference list
contains work from that community. A bibliography anchored on Murray (2014) and
Bikelienė (2023) reads as an outside submission. Monroe et al. (2008) is an
ACL anchor and needs company. This is the cheapest available intervention and
should be done before 12 October.

---

## Before submitting — open data issues

- 60,534 GDELT URLs were released back for retry after the ledger fix; the retry
  pass has not run. Per-outlet claims depend on it.
- 36,810 fetched bodies classify as `neither` variant and have not been dropped
  from the texts or the derived series.
- YouTube: only chornobyl is trustworthy. Eleven pairs are ~90% incomplete at
  legacy depth; twelve were never collected.
