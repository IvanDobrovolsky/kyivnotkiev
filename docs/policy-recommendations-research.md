# Policy Recommendations Research: #KyivNotKiev Paper
## Compiled for Language Policy Journal Submission
### Research date: March 2026

---

## 1. Writing Tools Enforcing Ukrainian Spellings

### 1.1 Grammarly
- **Status: Likely enforces, not publicly documented.**
- Grammarly is a Ukrainian-founded company (offices in Kyiv, New York, Vancouver) and consistently uses "Kyiv" in all its own communications.
- Grammarly has invested heavily in Ukrainian-language NLP, including the UA-GEC (Grammatical Error Correction) dataset for Ukrainian.
- No public documentation was found confirming a specific Kiev->Kyiv correction rule, but given the company's Ukrainian identity and its "sensitive language" features, it is highly probable that Grammarly flags "Kiev" in favor of "Kyiv."
- **Recommendation for paper:** Test directly and cite result. Contact Grammarly press team for confirmation.

### 1.2 Microsoft Word (Office 365)
- **Status: Flags "Kiev" conditionally -- requires opt-in setting.**
- Both "Kyiv" and "Kiev" pass the basic English spellcheck (neither is red-underlined).
- However, under **Grammar and Refinements > Sensitive Geopolitical References**, Word warns: *"It's best to use current place names"* and suggests "Kyiv."
- **Critical finding:** This setting is **not enabled by default** in all configurations. Users must opt in or have it pre-selected.
- Source: [Office Watch (2022)](https://office-watch.com/2022/spell-and-say-kiev-or-kyiv-according-to-microsoft-word/)
- **Policy recommendation:** Advocate for Microsoft to enable "Sensitive Geopolitical References" by default, or at minimum surface a one-time prompt to users.

### 1.3 Google Docs
- **Status: No evidence of flagging Kiev.**
- Google Docs' spellcheck accepts both "Kiev" and "Kyiv" without flagging either.
- Google Docs does not have an equivalent to Microsoft's "Sensitive Geopolitical References" feature.
- **Policy recommendation:** Advocate for Google to implement geopolitical sensitivity checks similar to Microsoft Word.

### 1.4 Apple (iOS/macOS)
- **Status: No evidence of autocorrect intervention.**
- Apple's autocorrect and spellcheck accept both "Kiev" and "Kyiv" as valid proper nouns.
- No geopolitical sensitivity feature exists in Apple's text correction system.
- **Policy recommendation:** Apple should update its dictionaries to prefer "Kyiv" in predictive text suggestions when a user types "Kie-".

### 1.5 LanguageTool (Open Source)
- **Status: Actively enforces Kyiv and other Ukrainian spellings.**
- LanguageTool published a dedicated article: ["How to Spell Certain Ukrainian Words and Names in English"](https://blog.languagetool.org/insights/post/ukraine/).
- Explicitly recommends "Kyiv" over "Kiev" and covers other Ukrainian place name transliterations.
- As an open-source tool, its rules can be inspected and cited in the GitHub repository ([languagetool-org/languagetool](https://github.com/languagetool-org/languagetool)).
- **Significance:** This is the most transparent and verifiable enforcement mechanism among writing tools.

### 1.6 AP Stylebook (Digital)
- **Status: Changed to "Kyiv" on August 14, 2019.**
- The AP Stylebook is available as a searchable online subscription, updated throughout the year.
- The entry specifies: "Kyiv" for the capital; "chicken Kiev" retained for the culinary dish; "Kievan Rus" retained for historical context.
- AP Stylebook digital tools are integrated into many newsroom CMS systems.
- Sources: [KyivPost (2019)](https://www.kyivpost.com/ukraine-politics/kiev-no-more-ap-stylebook-changes-spelling-of-ukrainian-capital-to-kyiv.html), [AP Stylebook Facebook](https://www.facebook.com/apstylebook/photos/a.118833031473125/2623534357669634/)

### 1.7 Summary Table: Tool Enforcement

| Tool | Flags "Kiev"? | Default behavior | Notes |
|------|--------------|-----------------|-------|
| Grammarly | Probable | Unknown | Ukrainian-founded; needs direct testing |
| Microsoft Word | Yes (conditional) | Opt-in setting | "Sensitive Geopolitical References" |
| Google Docs | No | Accepts both | No geopolitical sensitivity feature |
| Apple iOS/macOS | No | Accepts both | No preference in autocorrect |
| LanguageTool | Yes | Active rule | Open source, verifiable |
| AP Stylebook | Yes (editorial) | Standard entry | Since August 2019 |

---

## 2. Major Outlets and Platforms Still Using Old Spellings

### 2.1 English-Language News Media
- **Most major English-language outlets have switched to "Kyiv"** since 2019-2022, including: AP, Reuters, BBC, CNN, The Guardian, NYT, Washington Post, Wall Street Journal, Al Jazeera, Financial Times, The Economist, The Daily Telegraph, Euronews.
- **No major English-language outlet was found still using "Kiev" as standard style in 2025-2026.**

### 2.2 German-Language Media (Significant Holdouts)
- Several major German outlets continued using "Kiew" (Russian-derived German transliteration) into late 2024:
  - Frankfurter Allgemeine Zeitung, Die Welt, Suddeutsche Zeitung, Funke Media Group, Ippen Holding (Frankfurter Rundschau, Munchner Merkur), Der Standard, Neue Zurcher Zeitung.
- **Die Zeit** switched to "Kyjiw" (Ukrainian-derived German transliteration) on October 30, 2024.
- The **German Foreign Ministry** officially adopted "Kyjiw" from February 24, 2024.
- Source: [UNITED24 Media](https://united24media.com/latest-news/german-outlet-zeit-adopts-ukrainian-spelling-of-kyiv-in-support-of-ukrainian-identity-3391)

### 2.3 Dutch-Language Media
- **De Standaard** (Belgium) kept "Kiev" as one of "a very small set of exceptions."
- The Flemish government's **Team Taaladvies** still listed "Kiev" as the commonly accepted name in Dutch, though noting that "Kyiv" has been increasingly used since 2022.

### 2.4 Recipe/Food Platforms ("Chicken Kiev/Kyiv")

| Platform | Spelling Used | Notes |
|----------|--------------|-------|
| Jamie Oliver | **Chicken Kyiv** | Fully switched |
| Milk Street (Kimball) | **Chicken Kyiv** | Fully switched |
| Food52 | **Chicken Kiev (Chicken Kyiv)** | Dual listing |
| BBC Good Food | Chicken Kiev | Traditional spelling retained |
| RecipeTin Eats | Chicken Kiev | Traditional spelling |
| Natasha's Kitchen | Chicken Kiev | Traditional spelling |
| Rick Stein | Chicken Kiev | Traditional spelling |
| Food Network | Chicken Kiev | Traditional spelling |
| Delicious Magazine | Chicken Kiev/kievs | Traditional spelling |

- **Key finding:** Food platforms are split. Premium/progressive brands (Jamie Oliver, Milk Street) have switched; most mainstream recipe databases retain "Chicken Kiev."
- **AP Stylebook position:** Retains "chicken Kiev" as the dish name, treating it as an established English culinary term.

### 2.5 Sports Databases

| Platform | Current Name | Notes |
|----------|-------------|-------|
| Transfermarkt | **Dynamo Kyiv** | Switched; URL still shows "kiew" (German legacy) |
| ESPN | **Dynamo Kyiv** | Switched in display name; some URLs retain "kiev" |
| Sofascore | **Dynamo Kyiv** | Switched |
| UEFA | **Dynamo Kyiv** | Official name |

- **Key finding:** Major sports databases have uniformly adopted "Dynamo Kyiv" in display names. Legacy URL slugs sometimes retain old spellings (a common technical debt pattern worth noting in the paper).

### 2.6 Map Services

| Service | Displayed Name | Notes |
|---------|---------------|-------|
| Google Maps | **Kyiv** | Switched |
| Apple Maps | **Kyiv, Ukraine** | Switched |
| OpenStreetMap | **Kyiv** | Switched |

- All three major mapping platforms display "Kyiv" as the city label.

### 2.7 Travel Platforms

| Platform | Spelling Used | Notes |
|----------|--------------|-------|
| TripAdvisor | **Kyiv** | Switched; some user-generated business names still show "Kiev" (e.g., "UKRAINE AND KIEV TRAVEL GUIDES," "Stay in Kiev") |
| Expedia | **Kyiv** | Switched |
| Booking.com | **Kyiv** | Switched (based on campaign adoption reports) |
| Lonely Planet | **Kyiv** | Switched |

- **Key finding:** All major travel platforms have switched official city labels to "Kyiv." However, user-generated content (business names, reviews) on TripAdvisor still frequently contains "Kiev," creating an inconsistency gap.

---

## 3. Social Media Gaps (Novel Contribution Opportunity)

### 3.1 Existing Research
- **No comprehensive study was found** that systematically tracks Kiev/Kyiv adoption across Twitter/X, Reddit, TikTok, and YouTube.
- One study (Nature Communications, 2024) created dictionaries of Ukrainian vs. Russian identity mentions for social media analysis using 1.6M posts from Facebook and Twitter, where "Kyiv" was part of the Ukrainian identity dictionary.
- Multiple studies examine Ukraine-related social media content but focus on war discourse, disinformation, or propaganda -- not specifically on toponymic adoption patterns.
- **This represents a clear gap in the literature and a novel contribution opportunity for the paper.**

### 3.2 Platform API Access for Research (as of 2025-2026)

#### Twitter/X
- **Free tier:** Severely limited (essentially unusable for research).
- **Basic tier:** $200/month (was $100), 10K tweets/month.
- **Pro tier:** $5,000/month, 1M tweets/month -- minimum viable for historical research.
- **Enterprise:** $42,000+/month.
- **Pay-as-you-go (new Feb 2026):** ~$0.01/tweet, buy credits.
- **Academic Research Program:** Formerly free; now effectively defunct. Researchers face "impossible costs" for data that was once freely available (CJR reporting).
- **Historical data:** Full archive search requires Pro tier or above.
- **Recommendation:** Historical X/Twitter data may be accessible through existing academic datasets or institutional agreements. The EU's DSA Article 40 nominally requires platforms to enable vetted researcher access.

#### Reddit
- **Official API:** Still free for non-commercial, non-competitive use. Viable for academic research projects.
- **Pushshift:** Shut down by Reddit in 2024 (CFAA-related). Historical archives remain partially accessible.
- **Arctic Shift:** Academic project hosting Reddit data dumps via Academic Torrents; publishes monthly archives (posts, comments, metadata).
- **Recommendation:** Reddit remains the most accessible major platform for this research. Historical data via Arctic Shift/Academic Torrents is the best path for longitudinal analysis.

#### TikTok
- **Research API:** Available to researchers at non-profit/academic institutions.
- **Requirements:** Endorsement letter, registration, approval, detailed application (requirements tightened in 2025).
- **Available data:** Public video metadata, comments, engagement metrics, account information.
- **Known limitations:** Incomplete data delivery (missing metadata for ~1 in 8 videos), server instability, unreliable pagination (reported by AI Forensics and academic researchers).
- **Recommendation:** Usable but unreliable. Results should be treated as a lower-bound estimate. Pair with manual sampling for validation.

#### YouTube
- **Data API v3:** Available through YouTube's academic research program.
- **Requirements:** Affiliation with accredited higher-learning institution; verification required.
- **Known limitations:** Search endpoint returns highly inconsistent results; randomizes based on topic popularity; not suitable for representative historical sampling (documented in ACM IMC 2025 paper and Information, Communication & Society 2025 paper).
- **Recommendation:** Useful for metadata analysis of known channels/videos (e.g., news channels, cooking channels). Not reliable for search-based sampling.

### 3.3 Suggested Research Design
Given API constraints, the paper could propose or pilot:
1. **Reddit longitudinal study** (most feasible): Track "Kiev" vs "Kyiv" usage in r/ukraine, r/worldnews, r/soccer, r/cooking, r/travel over time using Arctic Shift historical dumps.
2. **YouTube channel audit:** Sample major news channels, cooking channels, and travel vloggers; analyze title/description text for Kiev vs Kyiv using the Data API on known channel IDs.
3. **Twitter/X:** If budget allows, a Pro-tier analysis of the #KyivNotKiev hashtag adoption and general usage trends pre/post 2022 invasion.
4. **TikTok:** Exploratory analysis via Research API, acknowledging data completeness limitations.

---

## 4. The Borscht/Borshch Debate

### 4.1 UNESCO Official Spelling
- UNESCO uses **"borscht"** in the official inscription: **"Culture of Ukrainian borscht cooking"** (inscribed July 1, 2022, on the List of Intangible Cultural Heritage in Need of Urgent Safeguarding).
- Element number: 01852.
- Source: [UNESCO ICH](https://ich.unesco.org/en/USL/culture-of-ukrainian-borscht-cooking-01852)
- **Note:** UNESCO clarified this "does not imply exclusivity, nor ownership."

### 4.2 Ukrainian English-Language Media Usage

| Outlet | Preferred Spelling | Notes |
|--------|-------------------|-------|
| Kyiv Independent | **borsch** | Used in their explainer: "Borsch, a Ukrainian staple, explained" |
| Ukrainska Pravda (English) | **borsch/borshch** | Uses both, with discussion of the spelling question |
| Etnocook (Ukrainian food site) | **borshch** | Closest phonetic transliteration from Ukrainian |

### 4.3 Dictionary Entries

| Dictionary | Entry Spelling | Etymology Given |
|-----------|---------------|----------------|
| Merriam-Webster | **borscht** | "from Yiddish borsht, from Ukrainian & Russian borshch"; first known use 1828 |
| Oxford | **borscht** (primary) | Via Yiddish from Russian/Ukrainian |
| Cambridge | **borscht** (primary), borsch (variant) | -- |
| Collins | **borscht** | -- |
| Dictionary.com | **borscht** | Dated 1880-85 |
| Wiktionary | **borscht** (primary) | Lists full etymological chain |

### 4.4 Is "Borscht" an English Loanword or a Transliteration?
- **"Borscht" is a fully established English loanword**, not a direct transliteration from Ukrainian.
- The English word entered via **Yiddish** (borsht/borscht), brought by Ashkenazi Jewish immigrants to North America. The Yiddish form added the final "-t" sound.
- The Ukrainian word is borshch (no final "t"); the Russian is borshch (also no final "t").
- **This makes "borscht" etymologically parallel to other Yiddish-mediated loanwords** (e.g., "kibbutz," "chutzpah") -- it is an English word in its own right, not a Russian or Ukrainian spelling.
- Source: [Etymonline](https://www.etymonline.com/word/borscht)

### 4.5 The Naming Tension
- Ukrainian sources (Kyiv Independent, Etnocook) prefer **"borsch"** or **"borshch"** as direct transliterations from Ukrainian.
- English dictionaries uniformly list **"borscht"** as the standard English form.
- UNESCO used **"borscht"** in its inscription, aligning with English convention rather than Ukrainian transliteration.
- **This creates a different dynamic than Kiev/Kyiv:** "Borscht" did not enter English via Russian -- it entered via Yiddish. The argument for changing it is weaker because:
  1. It is not a Russian-imposed spelling (it is Yiddish-mediated).
  2. It is an established English loanword present in all major dictionaries since the 19th century.
  3. Even the Ukrainian form "borshch" differs from "borsch" -- there is no single "correct" transliteration.
- **However,** the Kyiv Independent and Ukrainian cultural institutions clearly prefer "borsch" as a matter of cultural assertion.

### 4.6 Policy Recommendation for Paper
- Treat borscht/borshch as a **distinct case** from Kiev/Kyiv in the paper.
- Kiev->Kyiv is a clear-cut case: replacing a Russian transliteration with a Ukrainian one for a sovereign nation's capital.
- Borscht->borshch is more complex: replacing an established English loanword (via Yiddish) with a Ukrainian transliteration.
- The paper could argue for a **spectrum model**: some terms (capital city names) warrant immediate and universal correction; others (long-established culinary loanwords) may evolve more gradually and may not require prescriptive intervention.

---

## 5. KyivNotKiev Campaign Timeline (Key Dates for Paper)

| Date | Event |
|------|-------|
| Oct 2, 2018 | MFA Ukraine launches #KyivNotKiev campaign |
| Aug 14, 2019 | AP Stylebook changes to "Kyiv" |
| Jun 2019 | US Board on Geographic Names officially adopts "Kyiv" |
| Oct 2019 | IATA switches to "Kyiv" |
| Jan 2020 | 63 airports and 3 airlines using "Kyiv" |
| Sep 2020 | English Wikipedia formally adopts "Kyiv" |
| Feb 24, 2022 | Russian full-scale invasion -- accelerates global adoption |
| Jul 1, 2022 | UNESCO inscribes "Culture of Ukrainian borscht cooking" |
| Feb 24, 2024 | German Foreign Ministry adopts "Kyjiw" |
| Oct 30, 2024 | Die Zeit switches to "Kyjiw" |

---

## 6. Actionable Policy Recommendations Summary

### For the paper's conclusion:

1. **Writing tool vendors** should implement Ukrainian spelling preferences as default-on (not opt-in) in their geopolitical sensitivity features. Microsoft's existing framework is a model; Google and Apple should follow.

2. **Food media** represents the largest remaining gap in English-language adoption. A targeted campaign similar to #KyivNotKiev could advocate for "Chicken Kyiv" on major recipe platforms, though the AP Stylebook's retention of "chicken Kiev" as a culinary term complicates this.

3. **Social media is an unstudied frontier.** The paper should flag the absence of systematic research on Kiev/Kyiv adoption in user-generated content across Twitter/X, Reddit, TikTok, and YouTube. Reddit (via Arctic Shift) and YouTube (via Data API) are the most feasible platforms for academic study.

4. **The borscht/borshch case** should be presented as a separate, more nuanced linguistic question -- one of loanword evolution rather than decolonial renaming.

5. **URL/slug technical debt** in sports databases and travel platforms (where display names show "Kyiv" but URLs still contain "kiev") represents a measurable persistence of the old form that could be quantified in the paper.

6. **German-language media** remains the most significant holdout in major Western press. The paper could draw a comparison between English-language adoption (largely complete by 2022) and German-language adoption (still contested into 2025).

7. **EU Digital Services Act Article 40** could be leveraged to advocate for platform data access for researchers studying linguistic adoption patterns on social media.

---

## Sources

- [Grammarly - Wikipedia](https://en.wikipedia.org/wiki/Grammarly)
- [Grammarly UA-GEC Dataset](https://www.grammarly.com/blog/engineering/ua-gec-2/)
- [Office Watch: Spell and say Kiev or Kyiv according to Microsoft Word](https://office-watch.com/2022/spell-and-say-kiev-or-kyiv-according-to-microsoft-word/)
- [LanguageTool: How to Spell Certain Ukrainian Words](https://blog.languagetool.org/insights/post/ukraine/)
- [LanguageTool GitHub](https://github.com/languagetool-org/languagetool)
- [KyivPost: AP Stylebook changes spelling](https://www.kyivpost.com/ukraine-politics/kiev-no-more-ap-stylebook-changes-spelling-of-ukrainian-capital-to-kyiv.html)
- [KyivNotKiev - Wikipedia](https://en.wikipedia.org/wiki/KyivNotKiev)
- [Kyiv Independent: Kyiv, not Kiev](https://kyivindependent.com/kyiv-not-kiev-how-ukrainians-reclaimed-their-capital-and-their-future/)
- [Atlantic Council: Why spelling matters](https://www.atlanticcouncil.org/blogs/ukrainealert/kyiv-not-kiev-why-spelling-matters-in-ukraines-quest-for-an-independent-identity/)
- [UNITED24 Media: Die Zeit adopts Ukrainian spelling](https://united24media.com/latest-news/german-outlet-zeit-adopts-ukrainian-spelling-of-kyiv-in-support-of-ukrainian-identity-3391)
- [Kyiv Independent: German Foreign Ministry adopts Kyjiw](https://kyivindependent.com/german-foreign-ministry-kyjiw/)
- [Jamie Oliver: Chicken Kyiv](https://www.jamieoliver.com/recipes/chicken/chicken-kyiv/)
- [Transfermarkt: Dynamo Kyiv](https://www.transfermarkt.us/dynamo-kiew/startseite/verein/338)
- [ESPN: Dynamo Kyiv](https://www.espn.com/soccer/team/squad/_/id/440/dynamo-kiev)
- [Lonely Planet: Kyiv](https://www.lonelyplanet.com/destinations/ukraine/kyiv)
- [OpenStreetMap Wiki: Kyiv](https://wiki.openstreetmap.org/wiki/Kyiv)
- [TripAdvisor: Kyiv](https://www.tripadvisor.com/Tourism-g294474-Kyiv-Vacations.html)
- [Tour Kyiv: Kyiv vs Kiev](https://tourkyiv.com/2025/12/17/kyiv-vs-kiev-whats-the-difference-and-why-it-matters-more-than-ever/)
- [Nature Communications: Social identity and Ukraine](https://www.nature.com/articles/s41467-024-52179-8)
- [TikTok Research API](https://developers.tiktok.com/products/research-api/)
- [AI Forensics: TikTok API Problems](https://aiforensics.org/work/tk-api)
- [Reddit API Academic Tools](https://subjectguides.library.american.edu/c.php?g=1238130&p=9060342)
- [YouTube API Academic Audit (ACM 2025)](https://dl.acm.org/doi/10.1145/3730567.3764492)
- [X/Twitter API Pricing](https://twitterapi.io/blog/twitter-api-pricing-2025)
- [CJR: Academic research on Twitter](https://www.cjr.org/tow_center/qa-what-happened-to-academic-research-on-twitter.php)
- [UNESCO: Ukrainian borscht cooking](https://ich.unesco.org/en/USL/culture-of-ukrainian-borscht-cooking-01852)
- [Kyiv Independent: Borsch, a Ukrainian staple](https://kyivindependent.com/borsch-a-ukrainan-staple/)
- [Merriam-Webster: Borscht](https://www.merriam-webster.com/dictionary/borscht)
- [Etymonline: Borscht](https://www.etymonline.com/word/borscht)
- [Wiktionary: Borscht](https://en.wiktionary.org/wiki/borscht)
- [NPR: UNESCO declares borsch cooking endangered](https://www.npr.org/2022/07/01/1109319174/unesco-declares-ukraine-borsch-ukrainian-heritage)
