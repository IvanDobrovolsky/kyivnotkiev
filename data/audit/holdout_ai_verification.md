# Holdout AI verification

Per-pair verification agents reviewed **4,956** holdout entries across **24** pairs.

- kept: **3,586**
- dropped: **1,370** (27.6%)
- distinct drop patterns proposed: **417** across 24 pairs

Source of record: the verification workflow journal (`wf_79c37602-920/journal.jsonl`). Every drop below carries the reviewing agent's own reason; `drop_pattern` is the reusable regex it proposed, and is absent where nothing generalises.

## Drops per pair

| pair | drops | share of all drops |
| --- | ---: | ---: |
| odesa | 172 | 12.6% |
| kyiv | 127 | 9.3% |
| lviv | 123 | 9.0% |
| mykola-hohol | 112 | 8.2% |
| borscht | 91 | 6.6% |
| donbas | 89 | 6.5% |
| luhansk | 84 | 6.1% |
| chornobyl | 82 | 6.0% |
| dynamo-kyiv | 60 | 4.4% |
| chicken-kyiv | 58 | 4.2% |
| kharkiv | 57 | 4.2% |
| volodymyr-zelenskyy | 49 | 3.6% |
| ihor-sikorsky | 48 | 3.5% |
| kyivan-rus | 39 | 2.8% |
| babyn-yar | 33 | 2.4% |
| varenyky | 31 | 2.3% |
| serhii-korolyov | 24 | 1.8% |
| oleksandr-usyk | 20 | 1.5% |
| kazymyr-malevych | 19 | 1.4% |
| ternopil | 13 | 0.9% |
| feodosiia | 12 | 0.9% |
| volodymyr-the-great | 12 | 0.9% |
| bakhmut | 9 | 0.7% |
| dnipro-river | 6 | 0.4% |
| **total** | **1370** | **100.0%** |

## Drops per class

| class | drops | share of all drops |
| --- | ---: | ---: |
| non-english | 410 | 29.9% |
| spam | 288 | 21.0% |
| frozen-brand | 234 | 17.1% |
| wrong-referent | 158 | 11.5% |
| person-name | 154 | 11.2% |
| other | 96 | 7.0% |
| city-usage | 23 | 1.7% |
| metalinguistic | 7 | 0.5% |
| **total** | **1370** | **100.0%** |

## Drop entries by class

### non-english — 410 drops

**odesa** (19)

- `openalex` [https://openalex.org/W4409143349](https://openalex.org/W4409143349) — French book-review title: 'Géographie des ténèbres. Bucarest-Transnistrie-Odessa'. _(pattern: `géographie des ténèbres`)_
- `openalex` [https://openalex.org/W4415280516](https://openalex.org/W4415280516) — French review of the same Caraion book; title and abstract entirely in French. _(pattern: `géographie des ténèbres`)_
- `reddit` [https://reddit.com/r/trabzonspor/comments/1hvmdfs](https://reddit.com/r/trabzonspor/comments/1hvmdfs) — Turkish post on Bob Dylan's ancestry: 'Trabzon'dan Odessa'ıya'.
- `youtube` [https://youtube.com/watch?v=0JazTp5wf5E](https://youtube.com/watch?v=0JazTp5wf5E) — Spanish hashtag-only Short ('Hija de #marisela #marilyn #odessa #singer').
- `youtube` [https://youtube.com/watch?v=5p_kvWQZdLA](https://youtube.com/watch?v=5p_kvWQZdLA) — BILD, German: 'RUSSISCHER ANGRIFF AUF ODESSA'; description entirely German.
- `youtube` [https://youtube.com/watch?v=C1gtmwH8DXk](https://youtube.com/watch?v=C1gtmwH8DXk) — German hashtag-only Short from the same channel; no English prose.
- `youtube` [https://youtube.com/watch?v=FiNcK_GG9-w](https://youtube.com/watch?v=FiNcK_GG9-w) — Russian-language meme Short (#Привоз #Одесса #бабушка); hashtag soup, no English prose.
- `youtube` [https://youtube.com/watch?v=WrZup7TrHj4](https://youtube.com/watch?v=WrZup7TrHj4) — German joke Short ('Odessa-Witz über den Krieg'); entire description in German.
- `youtube` [https://youtube.com/watch?v=YS1uIOS-xdY](https://youtube.com/watch?v=YS1uIOS-xdY) — LCI, French: 'Odessa : nouvelle cible de l'acharnement russe'; description in French.
- `youtube` [https://youtube.com/watch?v=e3-azoCfsuY](https://youtube.com/watch?v=e3-azoCfsuY) — German hashtag-only Short (#kampf #krieg #russlandukrainekrieg #odessa); no English prose.
- `youtube` [https://youtube.com/watch?v=g3aWPVf3x1c](https://youtube.com/watch?v=g3aWPVf3x1c) — Indonesian: 'Bro Z Sikat Dua Kapal Tanker di Odessa'.
- `youtube` [https://youtube.com/watch?v=iwoxUxAHPzs](https://youtube.com/watch?v=iwoxUxAHPzs) — Russian channel 'Тудой-сюдой по Одессе'; hashtag-only title.
- `youtube` [https://youtube.com/watch?v=leGQ4-knNuI](https://youtube.com/watch?v=leGQ4-knNuI) — AFP Deutsch, German: 'Verletzte bei russischem Angriff auf Odessa'.
- `youtube` [https://youtube.com/watch?v=oVASgBovTM0](https://youtube.com/watch?v=oVASgBovTM0) — Russian-language Odesa kennel channel; description is Cyrillic ('Немецкие овчарки... Одесса'). _(pattern: `smart\s+and\s+strong\s+dogs`)_
- `youtube` [https://youtube.com/watch?v=s6moSrsTVmA](https://youtube.com/watch?v=s6moSrsTVmA) — Channel 'Подземная Одесса'; title 'Незабаром / coming soon' - Ukrainian/Russian, no prose.
- `youtube` [https://youtube.com/watch?v=sh187TA4q_E](https://youtube.com/watch?v=sh187TA4q_E) — Description is Russian hashtags (#одесса #украина) from the same Odesa kennel channel.
- `youtube` [https://youtube.com/watch?v=uM1gjFpLPdw](https://youtube.com/watch?v=uM1gjFpLPdw) — Same Russian-language kennel channel; description in Cyrillic ('Одесса 2025'). _(pattern: `smart\s+and\s+strong\s+dogs`)_
- `youtube` [https://youtube.com/watch?v=uaeC6sen2ms](https://youtube.com/watch?v=uaeC6sen2ms) — Russian-language channel (Юлия Кульчицкая); hashtag-only title, no prose.
- `youtube` [https://youtube.com/watch?v=zqDeNsk2zlw](https://youtube.com/watch?v=zqDeNsk2zlw) — Indonesian (Tribun): 'Fasilitas Pelabuhan di Odessa Terbakar Hebat'.

**kyiv** (76)

- `news` [https://www.giornaledibrescia.it/italia-e-estero/kiev-ci-siamo-ritirati-da-5-insediamenti-a-zaporizhzhia-recjqv7b](https://www.giornaledibrescia.it/italia-e-estero/kiev-ci-siamo-ritirati-da-5-insediamenti-a-zaporizhzhia-recjqv7b) — Italian body text; sole match is the Italian headline. _(pattern: `giornaledibrescia\.it`)_
- `news` [https://www.giornaledibrescia.it/italia-e-estero/kiev-grazie-a-nebbia-circa-300-russi-penetrati-a-pokrovsk-r3oulxvb](https://www.giornaledibrescia.it/italia-e-estero/kiev-grazie-a-nebbia-circa-300-russi-penetrati-a-pokrovsk-r3oulxvb) — Italian body text; sole match is the Italian headline. _(pattern: `giornaledibrescia\.it`)_
- `news` [https://www.giornaledibrescia.it/italia-e-estero/kiev-sospende-ministro-toccato-dall-indagine-anticorruzione-f3nwwujv](https://www.giornaledibrescia.it/italia-e-estero/kiev-sospende-ministro-toccato-dall-indagine-anticorruzione-f3nwwujv) — Body is Italian; the only "Kiev" is in the Italian headline. _(pattern: `giornaledibrescia\.it`)_
- `openalex` [https://openalex.org/W4416847926](https://openalex.org/W4416847926) — French (OpenAlex language=fr), La Nouvelle Revue francaise.
- `openalex` [https://openalex.org/W4417278357](https://openalex.org/W4417278357) — French title "Retour de Kiev" in the French journal Servir.
- `openalex` [https://openalex.org/W7113778576](https://openalex.org/W7113778576) — Italian title and abstract (OpenAlex language=it).
- `openalex` [https://openalex.org/W7125266541](https://openalex.org/W7125266541) — French title and abstract (OpenAlex language=fr).
- `reddit` [https://reddit.com/r/Bragantino/comments/1hvu2a5](https://reddit.com/r/Bragantino/comments/1hvu2a5) — Portuguese title; the match is "Dinamo de Kiev", which belongs to the dynamo-kyiv pair. _(pattern: `d[ií]namo\s+de\s+kiev`)_
- `reddit` [https://reddit.com/r/HotSpotQR/comments/1hy14sl](https://reddit.com/r/HotSpotQR/comments/1hy14sl) — French post; both matches are inside a euractiv.fr URL slug, not prose.
- `reddit` [https://reddit.com/r/Italia/comments/1hrdgoh](https://reddit.com/r/Italia/comments/1hrdgoh) — Italian title ("Kiev ferma il gas russo per l'Europa").
- `reddit` [https://reddit.com/r/ItaliaRossa/comments/1htplb6](https://reddit.com/r/ItaliaRossa/comments/1htplb6) — Italian title (brigata "Anna di Kiev").
- `reddit` [https://reddit.com/r/Roumanie/comments/1hxkmrv](https://reddit.com/r/Roumanie/comments/1hxkmrv) — Romanian post body (duplicate of the r/moldova text).
- `reddit` [https://reddit.com/r/TR_News/comments/1hr1vz9](https://reddit.com/r/TR_News/comments/1hr1vz9) — Turkish title, no English body.
- `reddit` [https://reddit.com/r/TarihiSeyler/comments/1hwo13i](https://reddit.com/r/TarihiSeyler/comments/1hwo13i) — Turkish title; also refers to Kievan Rus, which config excludes from this pair's holdouts.
- `reddit` [https://reddit.com/r/TarihiSeyler/comments/1hx4xlr](https://reddit.com/r/TarihiSeyler/comments/1hx4xlr) — Turkish WWII history post.
- `reddit` [https://reddit.com/r/actualite/comments/1hs0oug](https://reddit.com/r/actualite/comments/1hs0oug) — French title (brigade "Anne de Kiev").
- `reddit` [https://reddit.com/r/actualite/comments/1hy18nf](https://reddit.com/r/actualite/comments/1hy18nf) — French title, no English body.
- `reddit` [https://reddit.com/r/france/comments/1hskbpe](https://reddit.com/r/france/comments/1hskbpe) — French title (brigade Anne de Kiev).
- `reddit` [https://reddit.com/r/france/comments/1htin4z](https://reddit.com/r/france/comments/1htin4z) — French title ("FICO MENACE KIEV").
- `reddit` [https://reddit.com/r/france6/comments/1hu78g3](https://reddit.com/r/france6/comments/1hu78g3) — French title (brigade "Anne de Kiev").
- `reddit` [https://reddit.com/r/futebol/comments/1hvu2k7](https://reddit.com/r/futebol/comments/1hvu2k7) — Portuguese crosspost of the same item; "Dinamo de Kiev" is the dynamo-kyiv pair. _(pattern: `d[ií]namo\s+de\s+kiev`)_
- `reddit` [https://reddit.com/r/hakan_nsfw_hikaye/comments/1ht9v31](https://reddit.com/r/hakan_nsfw_hikaye/comments/1ht9v31) — Turkish erotic-fiction post.
- `reddit` [https://reddit.com/r/moldova/comments/1hxfunc](https://reddit.com/r/moldova/comments/1hxfunc) — Romanian post body.
- `reddit` [https://reddit.com/r/oknotizie/comments/1hsm23i](https://reddit.com/r/oknotizie/comments/1hsm23i) — Italian title (brigata "Anna di Kiev").
- `reddit` [https://reddit.com/r/oknotizie/comments/1htlvch](https://reddit.com/r/oknotizie/comments/1htlvch) — Italian title ("brigata anna di kiev").
- `reddit` [https://reddit.com/r/portugal2/comments/1hr1yd0](https://reddit.com/r/portugal2/comments/1hr1yd0) — Portuguese title and body.
- `reddit` [https://reddit.com/r/tjournal_refugees/comments/1hsstnq](https://reddit.com/r/tjournal_refugees/comments/1hsstnq) — Russian-language post; the only match is inside a dw.com URL slug, not prose.
- `reddit` [https://reddit.com/r/tjournal_refugees/comments/1hves8k](https://reddit.com/r/tjournal_refugees/comments/1hves8k) — Russian-language post; the only match is inside a dw.com URL slug.
- `reddit` [https://reddit.com/r/u_sarah0inconnue/comments/1htm62u](https://reddit.com/r/u_sarah0inconnue/comments/1htm62u) — French title, no English body.
- `reddit` [https://reddit.com/r/u_sarahloup/comments/1hs9nht](https://reddit.com/r/u_sarahloup/comments/1hs9nht) — French title, no English body.
- `youtube` [https://youtube.com/watch?v=-NWJpVlniMA](https://youtube.com/watch?v=-NWJpVlniMA) — Romanian ("Live din Kiev").
- `youtube` [https://youtube.com/watch?v=-NlAd7Pr8Ww](https://youtube.com/watch?v=-NlAd7Pr8Ww) — Italian hashtag-only title ("#pensioni #kiev"), empty description, no running text.
- `youtube` [https://youtube.com/watch?v=-vNwZBHVByM](https://youtube.com/watch?v=-vNwZBHVByM) — Spanish title and description.
- `youtube` [https://youtube.com/watch?v=0ek17Qf1lis](https://youtube.com/watch?v=0ek17Qf1lis) — Italian hashtag-only title ("#viktororban #kiev"), no running text.
- `youtube` [https://youtube.com/watch?v=2QoBRH_IH60](https://youtube.com/watch?v=2QoBRH_IH60) — Portuguese title and description (BandNews TV).
- `youtube` [https://youtube.com/watch?v=2wKNhNsJ3dE](https://youtube.com/watch?v=2wKNhNsJ3dE) — Italian title and description (Nicolai Lilin).
- `youtube` [https://youtube.com/watch?v=3a5WOeJCjjQ](https://youtube.com/watch?v=3a5WOeJCjjQ) — Uzbek title ("Tezkor xabar(rossiya kiev)").
- `youtube` [https://youtube.com/watch?v=4Pfu2aDfnnA](https://youtube.com/watch?v=4Pfu2aDfnnA) — French title and description (BFMTV).
- `youtube` [https://youtube.com/watch?v=4fbRCVUOeTU](https://youtube.com/watch?v=4fbRCVUOeTU) — Kinyarwanda title and description.
- `youtube` [https://youtube.com/watch?v=8qdJBA48WOA](https://youtube.com/watch?v=8qdJBA48WOA) — Romanian ("Live din Kiev").
- `youtube` [https://youtube.com/watch?v=ENjdAV1jF2Y](https://youtube.com/watch?v=ENjdAV1jF2Y) — Swedish title and description (Aftonbladet).
- `youtube` [https://youtube.com/watch?v=Hw4lInbNAI4](https://youtube.com/watch?v=Hw4lInbNAI4) — Italian hashtag-only title ("#kiev #ucraina"), no running text.
- `youtube` [https://youtube.com/watch?v=JSCdNjrx6kg](https://youtube.com/watch?v=JSCdNjrx6kg) — Indonesian title, no description.
- `youtube` [https://youtube.com/watch?v=Jmu0cxo5kps](https://youtube.com/watch?v=Jmu0cxo5kps) — Indonesian title and description (Tribunnews).
- `youtube` [https://youtube.com/watch?v=Ka1oCWLVrwQ](https://youtube.com/watch?v=Ka1oCWLVrwQ) — Italian title and description (Danilo Torresi).
- `youtube` [https://youtube.com/watch?v=M2SK131oTPU](https://youtube.com/watch?v=M2SK131oTPU) — Italian title ("Devastante attacco Russo su Kiev").
- `youtube` [https://youtube.com/watch?v=MvQePIjone0](https://youtube.com/watch?v=MvQePIjone0) — Italian title and description (TG La7).
- `youtube` [https://youtube.com/watch?v=O-ZILTf9MB4](https://youtube.com/watch?v=O-ZILTf9MB4) — French title and description (LCI).
- `youtube` [https://youtube.com/watch?v=O1cbp_Zsc8Y](https://youtube.com/watch?v=O1cbp_Zsc8Y) — Indonesian title, no description.
- `youtube` [https://youtube.com/watch?v=OOETcfDlQr8](https://youtube.com/watch?v=OOETcfDlQr8) — Portuguese title.
- `youtube` [https://youtube.com/watch?v=TMO7ctDwDRI](https://youtube.com/watch?v=TMO7ctDwDRI) — Dutch title and description (VRT NWS).
- `youtube` [https://youtube.com/watch?v=VdMP_iYxVrg](https://youtube.com/watch?v=VdMP_iYxVrg) — French title and description (RTBF).
- `youtube` [https://youtube.com/watch?v=ZSqrfSxX9ng](https://youtube.com/watch?v=ZSqrfSxX9ng) — Vietnamese hashtags and description.
- `youtube` [https://youtube.com/watch?v=cV_WfFDHyz8](https://youtube.com/watch?v=cV_WfFDHyz8) — Italian title and description.
- `youtube` [https://youtube.com/watch?v=d7YyaYb5KHA](https://youtube.com/watch?v=d7YyaYb5KHA) — Indonesian title, no description.
- `youtube` [https://youtube.com/watch?v=dARpfv1vLK4](https://youtube.com/watch?v=dARpfv1vLK4) — Italian title ("Basta soldi a Kiev!").
- `youtube` [https://youtube.com/watch?v=eE8gxYZK4lU](https://youtube.com/watch?v=eE8gxYZK4lU) — Italian title and description (Nicolai Lilin).
- `youtube` [https://youtube.com/watch?v=eKmyztKO0IU](https://youtube.com/watch?v=eKmyztKO0IU) — French title and description (franceinfo).
- `youtube` [https://youtube.com/watch?v=f3s2crveUY8](https://youtube.com/watch?v=f3s2crveUY8) — Spanish title and description (N+).
- `youtube` [https://youtube.com/watch?v=fBQVU74Z_Jk](https://youtube.com/watch?v=fBQVU74Z_Jk) — Vietnamese hashtags and description.
- `youtube` [https://youtube.com/watch?v=fuEOwdPNOcE](https://youtube.com/watch?v=fuEOwdPNOcE) — French title and description (ARTE).
- `youtube` [https://youtube.com/watch?v=gX40tz9rP5A](https://youtube.com/watch?v=gX40tz9rP5A) — French title (franceinfo).
- `youtube` [https://youtube.com/watch?v=hsNktCilqfI](https://youtube.com/watch?v=hsNktCilqfI) — Italian title and description (Visione TV).
- `youtube` [https://youtube.com/watch?v=huxkCEWsjFs](https://youtube.com/watch?v=huxkCEWsjFs) — Indonesian title and description (Tribunnews).
- `youtube` [https://youtube.com/watch?v=iOtm2HdWnIE](https://youtube.com/watch?v=iOtm2HdWnIE) — Swedish title and description (Aftonbladet).
- `youtube` [https://youtube.com/watch?v=ipZdQFRS5sQ](https://youtube.com/watch?v=ipZdQFRS5sQ) — French title and description (BFMTV).
- `youtube` [https://youtube.com/watch?v=jHWHHveXSs4](https://youtube.com/watch?v=jHWHHveXSs4) — Indonesian title ("Kiev Bergetar!").
- `youtube` [https://youtube.com/watch?v=kvdTW-bpe9g](https://youtube.com/watch?v=kvdTW-bpe9g) — French title and description (africanews en francais).
- `youtube` [https://youtube.com/watch?v=o42bb5zWM_A](https://youtube.com/watch?v=o42bb5zWM_A) — Indonesian title and description.
- `youtube` [https://youtube.com/watch?v=otc-T8L7GFc](https://youtube.com/watch?v=otc-T8L7GFc) — Indonesian title and description (Tribun Lombok).
- `youtube` [https://youtube.com/watch?v=s8bwwRD63bo](https://youtube.com/watch?v=s8bwwRD63bo) — Indonesian title, no description.
- `youtube` [https://youtube.com/watch?v=uKiFKLPj67E](https://youtube.com/watch?v=uKiFKLPj67E) — Indonesian title, no description.
- `youtube` [https://youtube.com/watch?v=voyaIR4g7BI](https://youtube.com/watch?v=voyaIR4g7BI) — Tagalog crime-story narration ("Princess Olga Ng Kiev").
- `youtube` [https://youtube.com/watch?v=w0NX6y6BQeU](https://youtube.com/watch?v=w0NX6y6BQeU) — Swahili title (RS SWAHILI TV).
- `youtube` [https://youtube.com/watch?v=w5sG9SoXreg](https://youtube.com/watch?v=w5sG9SoXreg) — Italian title and description (Radio Radio TV).
- `youtube` [https://youtube.com/watch?v=yUqHqSiPPqQ](https://youtube.com/watch?v=yUqHqSiPPqQ) — Italian title and description (TG2000).

**lviv** (5)

- `openalex` [https://openalex.org/W7127363564](https://openalex.org/W7127363564) — Three-word French title 'Berlioz et Lvov' with no abstract or body; the referent is the composer Alexey Lvov whom Berlioz wrote about, and there is no English running text to quote. _(pattern: `\bberlioz\s+et\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Iulisep_Canal/comments/1i9txkt](https://reddit.com/r/Iulisep_Canal/comments/1i9txkt) — Yak-3 post is entirely Spanish ('la operacion Lvov-Sandomierz', 'el IAD de Lvov de la Fuerza Aerea'); Spanish convention, not an English choice.
- `reddit` [https://reddit.com/r/tjournal_refugees/comments/1jpyeys](https://reddit.com/r/tjournal_refugees/comments/1jpyeys) — Post body is Russian and the only 'lvov' is inside a URL slug (korrespondent.net/city/lvov/...); not running text at all. _(pattern: `/city/lvov/`)_
- `youtube` [https://youtube.com/watch?v=LnPjVTAFPD4](https://youtube.com/watch?v=LnPjVTAFPD4) — Title and whole description are French ('Offensive Lvov-Sandomir', 'l'Armee rouge lance...'); 'Sandomir' is the French/Russian form, so this measures French convention. _(pattern: `\blvov[\s-]sandomir\b`)_
- `youtube` [https://youtube.com/watch?v=YvvzQ2gGSrg](https://youtube.com/watch?v=YvvzQ2gGSrg) — Description is entirely Indonesian ('Pertandingan ini dilaksanakan di Lvov, Ukraina selama penyelenggaraan turnamen Lvov'); Indonesian, not English, convention. _(pattern: `\blvov\s*,?\s*ukraina\b`)_

**mykola-hohol** (11)

- `reddit` [https://reddit.com/r/RecomandariCarti_RO/comments/1icr6qh](https://reddit.com/r/RecomandariCarti_RO/comments/1icr6qh) — Romanian-language book list in r/RecomandariCarti_RO. _(pattern: `^r/recomandaricarti_ro`)_
- `reddit` [https://reddit.com/r/transbr/comments/1hzcaif](https://reddit.com/r/transbr/comments/1hzcaif) — Portuguese-language post in r/transbr listing favourite authors. _(pattern: `^r/transbr`)_
- `youtube` [https://youtube.com/watch?v=11KgiOU5Y0A](https://youtube.com/watch?v=11KgiOU5Y0A) — Romanian radio-theatre upload ('Suflete moarte', teatru radiofonic). _(pattern: `teatru radiofonic`)_
- `youtube` [https://youtube.com/watch?v=3y2K7c6cO6o](https://youtube.com/watch?v=3y2K7c6cO6o) — Portuguese quote card ('Quanto mais fundo o homem desce...'). _(pattern: `quanto mais fundo`)_
- `youtube` [https://youtube.com/watch?v=BrFbvFEsEuo](https://youtube.com/watch?v=BrFbvFEsEuo) — Portuguese audiobook of 'O Capote'; title and description are Portuguese. _(pattern: `audiolivro|clássico da literatura russa`)_
- `youtube` [https://youtube.com/watch?v=ByXYcQpaNcg](https://youtube.com/watch?v=ByXYcQpaNcg) — Portuguese BookTube video about 'O Capote'. _(pattern: `literatura russa|história mais triste`)_
- `youtube` [https://youtube.com/watch?v=EK7wbKjQW6I](https://youtube.com/watch?v=EK7wbKjQW6I) — Romanian audiobook of 'Mirgorod' ('Carte Audio', Romanian description). _(pattern: `carte audio|povestiri`)_
- `youtube` [https://youtube.com/watch?v=T37V6ZbJmC0](https://youtube.com/watch?v=T37V6ZbJmC0) — Romanian vlog ('"Viy", de Nikolai Gogol & "Demon" - film & #ootd'). _(pattern: `,\s*de\s+nikolai\s+gogol`)_
- `youtube` [https://youtube.com/watch?v=qeb5TBM0LUY](https://youtube.com/watch?v=qeb5TBM0LUY) — Portuguese audiobook 'Almas Mortas'. _(pattern: `almas mortas`)_
- `youtube` [https://youtube.com/watch?v=yZGRF2-fV2w](https://youtube.com/watch?v=yZGRF2-fV2w) — Albanian bookshop video ('3 klasiket e vitit 2025', 'Frymë të vdekura - Nikolai Gogol'). _(pattern: `klasiket|frymë të vdekura`)_
- `youtube` [https://youtube.com/watch?v=yek5pSJZVA0](https://youtube.com/watch?v=yek5pSJZVA0) — Romanian audiobook channel ('Despre vindecarea spirituală', 'Cărți pentru suflet'). _(pattern: `despre vindecarea`)_

**borscht** (3)

- `reddit` [https://reddit.com/r/BGBMBERLIN/comments/1j06e9l](https://reddit.com/r/BGBMBERLIN/comments/1j06e9l) — Entirely German press release ("so Gartendirektor Thomas Borsch"), and all five hits are the botanical-garden director's surname. _(pattern: `thomas\s+borsch`)_
- `reddit` [https://reddit.com/r/forolibre/comments/1lh8ubm](https://reddit.com/r/forolibre/comments/1lh8ubm) — Spanish-language pro-LNR propaganda: "Gastronomía: Borsch, varenyky (empanadillas hervidas)" — right referent, but Spanish, so it measures Spanish convention.
- `reddit` [https://reddit.com/r/recetaschilenas/comments/1jp2e9g](https://reddit.com/r/recetaschilenas/comments/1jp2e9g) — Spanish-language recipe post: "perfecto para acompañar platos típicos como el borsch" — right referent, but Spanish, so it measures Spanish convention, not English.

**donbas** (59)

- `news` [https://www.giornaledibrescia.it/italia-e-estero/partigiani-filo-ucraini-sabotano-ferrovia-nel-donbass-p2157cik](https://www.giornaledibrescia.it/italia-e-estero/partigiani-filo-ucraini-sabotano-ferrovia-nel-donbass-p2157cik) — giornaledibrescia.it: article body is Italian (langdetect it:0.86); the only English fragment carrying the token is a boilerplate EPA photo caption. _(pattern: `giornaledibrescia\.it`)_
- `news` [https://www.irishdentist.ie/taiwan-under-siege-the-fear-of-chinese-invasion/11359/](https://www.irishdentist.ie/taiwan-under-siege-the-fear-of-chinese-invasion/11359/) — irishdentist.ie: machine-translated Italian Taiwan feature ('an Ukrainian flag came from the front of the Donbass With the signatures'); not quotable English. _(pattern: `irishdentist\.ie`)_
- `news` [https://www.irishdentist.ie/the-issues-on-donbass-nato-and-nuclear-power/14039/](https://www.irishdentist.ie/the-issues-on-donbass-nato-and-nuclear-power/14039/) — irishdentist.ie: machine-translated Italian war report republished on a hijacked Irish dentistry domain (MT artifact 'the knot Donbasssafety guarantees'); spelling reflects Italian convention. _(pattern: `irishdentist\.ie`)_
- `news` [https://www.irishdentist.ie/we-may-not-be-satisfied-with-kievs-revised-peace-plan/13595/](https://www.irishdentist.ie/we-may-not-be-satisfied-with-kievs-revised-peace-plan/13595/) — irishdentist.ie: machine-translated Italian peace-plan piece on the same hijacked dentistry domain (guillemets, 'starting pointnot', 'Zelenskyduring'). _(pattern: `irishdentist\.ie`)_
- `news` [https://www.odnako.org/explosion-in-a-skyscraper-of-moscow-one-dead-and-wounded-including-the-founder-of-the-militia-philorussian-in-donbass/](https://www.odnako.org/explosion-in-a-skyscraper-of-moscow-one-dead-and-wounded-including-the-founder-of-the-militia-philorussian-in-donbass/) — odnako.org: machine-translated Italian ('Battalion of Volunteer Filorussi Fighters'); Italian convention, not an English spelling choice. _(pattern: `odnako\.org`)_
- `news` [https://www.odnako.org/rare-lands-an-11-billion-treasure-ukraine-boasts-a-reserve-of-2-6-billion-tons-many-in-donbass/](https://www.odnako.org/rare-lands-an-11-billion-treasure-ukraine-boasts-a-reserve-of-2-6-billion-tons-many-in-donbass/) — odnako.org: machine-translated Italian on the same content farm; not usable English ('The rare earth I am 17 elements from the periodic table'). _(pattern: `odnako\.org`)_
- `news` [https://www.odnako.org/two-bridges-in-russia-collapses-7-dead-indagates-from-terrorism-filraine-partisans-sabotage-the-railway-in-donbass/](https://www.odnako.org/two-bridges-in-russia-collapses-7-dead-indagates-from-terrorism-filraine-partisans-sabotage-the-railway-in-donbass/) — odnako.org: machine-translated Italian (ANSA) on an AI content farm — 'Filraine partisans Sabotano railway in Donbass', 'Cremlin logistics'. _(pattern: `odnako\.org`)_
- `reddit` [https://reddit.com/r/Italia/comments/1i07r0t](https://reddit.com/r/Italia/comments/1i07r0t) — r/Italia: Italian post — "Il miliziano italiano catturato nel Donbass dalle forze ucraine".
- `reddit` [https://reddit.com/r/NuovaItalia/comments/1in038v](https://reddit.com/r/NuovaItalia/comments/1in038v) — r/NuovaItalia: Italian post — "Ucraina: il fronte del Donbass rischia di collassare".
- `reddit` [https://reddit.com/r/VietNamNation/comments/1ibebfx](https://reddit.com/r/VietNamNation/comments/1ibebfx) — r/VietNamNation: Vietnamese post — "Họ chiếm các thành phố quan trọng nhất của Donbass".
- `reddit` [https://reddit.com/r/VietnamToanCau/comments/1ibechc](https://reddit.com/r/VietnamToanCau/comments/1ibechc) — r/VietnamToanCau: Vietnamese crosspost of the same text; measures Vietnamese convention, not English.
- `reddit` [https://reddit.com/r/oknotizie/comments/1ignzdp](https://reddit.com/r/oknotizie/comments/1ignzdp) — r/oknotizie: Italian post — "capo delle milizie che combattono in Kursk e Donbass".
- `reddit` [https://reddit.com/r/opinionnonpopulaire/comments/1if8o9g](https://reddit.com/r/opinionnonpopulaire/comments/1if8o9g) — r/opinionnonpopulaire: French post — "les républiques séparatistes du Donbass".
- `reddit` [https://reddit.com/r/tjournal_refugees/comments/1il4or1](https://reddit.com/r/tjournal_refugees/comments/1il4or1) — r/tjournal_refugees: Russian-language (Cyrillic) post; the sole match is inside a dw.com URL slug (/ru/donbass/t-18060364), not running text. _(pattern: `dw\.com/ru/donbass`)_
- `reddit` [https://reddit.com/r/wahrheitkommtanslicht/comments/1iq5v9g](https://reddit.com/r/wahrheitkommtanslicht/comments/1iq5v9g) — r/wahrheitkommtanslicht: German post — "besuchte Roman Silantjew die Region Donbass".
- `youtube` [https://youtube.com/watch?v=-HAEPpNdJ3k](https://youtube.com/watch?v=-HAEPpNdJ3k) — Gazeta do Mundo: Portuguese video — "Tropas russas tomam cidade estratégica em Donbass"; Portuguese convention.
- `youtube` [https://youtube.com/watch?v=2COpSwmWXLo](https://youtube.com/watch?v=2COpSwmWXLo) — Angin Timur: Indonesian video — "Serangan Satelit Presisi di Donbass"; Indonesian convention.
- `youtube` [https://youtube.com/watch?v=2X7EGvlc4Og](https://youtube.com/watch?v=2X7EGvlc4Og) — Deutsches Wissen: German video; match sits in a German SEO keyword list ("Donbass Berichterstattung").
- `youtube` [https://youtube.com/watch?v=6W2GEAdJeKc](https://youtube.com/watch?v=6W2GEAdJeKc) — eingeSCHENKt.tv: German video — "10 Jahre Krieg im Donbass"; German convention.
- `youtube` [https://youtube.com/watch?v=70bqZ1LlwzI](https://youtube.com/watch?v=70bqZ1LlwzI) — WELT Nachrichtensender: German video — "Donbass müsse jedoch russisch bleiben!"; German convention.
- `youtube` [https://youtube.com/watch?v=8ZNiysijB_s](https://youtube.com/watch?v=8ZNiysijB_s) — Interesting Productions: Portuguese video — "…atacar os territórios de Donbass…"; Portuguese convention.
- `youtube` [https://youtube.com/watch?v=AjcvI3FFVjA](https://youtube.com/watch?v=AjcvI3FFVjA) — Paix et Guerre par Caroline Galactéros: French video — "Comprendre le Donbass, Macron et la Narration"; French convention.
- `youtube` [https://youtube.com/watch?v=BKSAlG53NFY](https://youtube.com/watch?v=BKSAlG53NFY) — Kriegsblick: German video — "Ukrainischer Panzer durchbricht die Donbass-Front"; German convention, not English usage.
- `youtube` [https://youtube.com/watch?v=Bym6_y3-hpk](https://youtube.com/watch?v=Bym6_y3-hpk) — franceinfo: French video — "Guerre en Ukraine : l'enjeu du Donbass"; French convention.
- `youtube` [https://youtube.com/watch?v=D_NiXrWq0SQ](https://youtube.com/watch?v=D_NiXrWq0SQ) — OMERTA: French description — "le journaliste et spécialiste du Donbass, Laurent Brayard"; French convention.
- `youtube` [https://youtube.com/watch?v=E3qYfLOL1mo](https://youtube.com/watch?v=E3qYfLOL1mo) — GRD Film: Turkish description — "Donbass'ta bir savaş başlar"; Turkish convention.
- `youtube` [https://youtube.com/watch?v=F0hFqpE9gUU](https://youtube.com/watch?v=F0hFqpE9gUU) — ARTEde: German video — "Ukraine: Donbass, auf Leben und Tod | ARTE Reportage"; measures German convention, not English usage.
- `youtube` [https://youtube.com/watch?v=FbMy7X_y0xw](https://youtube.com/watch?v=FbMy7X_y0xw) — Reyalidad ng Militar: Tagalog description — "…ang mga Ruso sa Donbass…"; not English usage.
- `youtube` [https://youtube.com/watch?v=H6IM7TKMHZE](https://youtube.com/watch?v=H6IM7TKMHZE) — Ostwind: German video/description — "…den Krieg im Donbass begonnen habe"; German convention.
- `youtube` [https://youtube.com/watch?v=JUqPeLALfUM](https://youtube.com/watch?v=JUqPeLALfUM) — Muka Nev: Indonesian video — "Pentingnya Donbass bagi Russia"; Indonesian convention.
- `youtube` [https://youtube.com/watch?v=KtMJQ4TZ45E](https://youtube.com/watch?v=KtMJQ4TZ45E) — ARTEde: German video — "Donbass: entmilitarisierte Zone? | Mit offenen Karten"; German convention.
- `youtube` [https://youtube.com/watch?v=LtGMfLm9prU](https://youtube.com/watch?v=LtGMfLm9prU) — LCI: French video — "Moscou fond sur le Donbass｜LCI"; French convention, not English usage.
- `youtube` [https://youtube.com/watch?v=M9H1Mb7B5Zo](https://youtube.com/watch?v=M9H1Mb7B5Zo) — junior ferreiradonascimento: Portuguese video — "A escalada no Donbass"; Portuguese convention.
- `youtube` [https://youtube.com/watch?v=MS9BKPoTfvk](https://youtube.com/watch?v=MS9BKPoTfvk) — Snack Podcast: German video — "Atomgefahr durch deutsche Panzer im Donbass!"; German convention.
- `youtube` [https://youtube.com/watch?v=Oaf0C-htJ1w](https://youtube.com/watch?v=Oaf0C-htJ1w) — Enfoque Global: Spanish video — "Rusia avanza y marca fase clave en Donbass"; Spanish convention.
- `youtube` [https://youtube.com/watch?v=OpLGmLPF2JQ](https://youtube.com/watch?v=OpLGmLPF2JQ) — Fakta Unick: Indonesian video/description — "…akhir dominasi Ukraina di Donbass?" (only the DMCA boilerplate is English).
- `youtube` [https://youtube.com/watch?v=Oqw7DA46u78](https://youtube.com/watch?v=Oqw7DA46u78) — LCI: French video — 'Zelensky : "notre Donbass, notre Crimée"'; French convention.
- `youtube` [https://youtube.com/watch?v=PvhtKL9uE44](https://youtube.com/watch?v=PvhtKL9uE44) — TV5MONDE Info: French video — "ces habitants qui refusent de quitter le Donbass"; French convention.
- `youtube` [https://youtube.com/watch?v=QcARTbt_UyY](https://youtube.com/watch?v=QcARTbt_UyY) — Macronomist: German video — "Russen mit vielen Vorstößen in Donbass"; German convention.
- `youtube` [https://youtube.com/watch?v=U_Y_-lNwVXI](https://youtube.com/watch?v=U_Y_-lNwVXI) — MUHIRE MUNANA OFFICIAL: Kinyarwanda video/description — "…Putin arashaka Donbass byihariye…"; not English usage.
- `youtube` [https://youtube.com/watch?v=Ugr4l7SB_3c](https://youtube.com/watch?v=Ugr4l7SB_3c) — WELT Nachrichtensender: German video — "…WELT-Reporter berichtet von der Front im Donbass"; German convention.
- `youtube` [https://youtube.com/watch?v=XAZ-YVHyQuU](https://youtube.com/watch?v=XAZ-YVHyQuU) — Kopi Pait: Indonesian video — "Putin Tegaskan Rusia Akan Bebaskan Seluruh Donbass"; Indonesian convention.
- `youtube` [https://youtube.com/watch?v=_9oayD1uagk](https://youtube.com/watch?v=_9oayD1uagk) — ZDFheute Nachrichten: German video — "Warum Putin nicht auf den Donbass verzichten kann"; German convention.
- `youtube` [https://youtube.com/watch?v=_JDLPThDUdg](https://youtube.com/watch?v=_JDLPThDUdg) — Desde El Capitolio - Epoch Times: Spanish video — "¡Ucrania dice NO al Donbass!"; Spanish convention.
- `youtube` [https://youtube.com/watch?v=_UyUPL4r3Hc](https://youtube.com/watch?v=_UyUPL4r3Hc) — Ukraine TV: French description — "…que Kyiv remette tout le Donbass à la Russie…"; French convention.
- `youtube` [https://youtube.com/watch?v=dvDXKQP7snQ](https://youtube.com/watch?v=dvDXKQP7snQ) — TG2000: Italian video — "Kiev pronta ad accettare zona demilitarizzata in Donbass"; Italian convention.
- `youtube` [https://youtube.com/watch?v=fL9om2Dm4qM](https://youtube.com/watch?v=fL9om2Dm4qM) — ZDFheute Nachrichten: German video — "Selenskyj warnt vor neuer Offensive im Donbass"; German convention.
- `youtube` [https://youtube.com/watch?v=fOIcNHrheWg](https://youtube.com/watch?v=fOIcNHrheWg) — FrontStory di Claudio Locatelli: Italian video — "COMBATTENTE del DONBASS nel 2014"; Italian convention.
- `youtube` [https://youtube.com/watch?v=hiiClAlR_UA](https://youtube.com/watch?v=hiiClAlR_UA) — AFP Deutsch: German video — "Warum der Donbass so wichtig ist"; German convention.
- `youtube` [https://youtube.com/watch?v=kczWG_bA3NI](https://youtube.com/watch?v=kczWG_bA3NI) — Alur Singkat Cerita Film: Indonesian description — "…sebagai penembak jitu di Donbass, Ukraina…"; Indonesian convention.
- `youtube` [https://youtube.com/watch?v=kgayVkvZ8O0](https://youtube.com/watch?v=kgayVkvZ8O0) — Ostwind: German video — "Putin: Nicht Russland hat den Krieg im Donbass begonnen"; German convention.
- `youtube` [https://youtube.com/watch?v=nrkDpG_dVLU](https://youtube.com/watch?v=nrkDpG_dVLU) — BundesTalk: German video/description — "…schon 2014/15 im Donbass"; German convention.
- `youtube` [https://youtube.com/watch?v=pch5w0mHfnA](https://youtube.com/watch?v=pch5w0mHfnA) — Mark Reicher: German video — "DRAMATISCHE LAGE! GRÖSSTER KESSEL im Donbass!"; German convention, not English usage.
- `youtube` [https://youtube.com/watch?v=rC88X4_50Ls](https://youtube.com/watch?v=rC88X4_50Ls) — randy dandy random: Italian description — "il sostegno ai separatisti del Donbass"; Italian convention.
- `youtube` [https://youtube.com/watch?v=s88JSCLZHug](https://youtube.com/watch?v=s88JSCLZHug) — Warzonder: Indonesian video — "DRONE KAMIKAZE MENARGETKAN KOMPLEKS INDUSTRI DI DONBASS"; Indonesian convention.
- `youtube` [https://youtube.com/watch?v=tEnnxdhCZqA](https://youtube.com/watch?v=tEnnxdhCZqA) — DustyIMax: German video — "Donbass: Wer muss geschützt werden? Eine Analyse"; German convention.
- `youtube` [https://youtube.com/watch?v=uARnqCtMpy8](https://youtube.com/watch?v=uARnqCtMpy8) — Fakta Unick: Indonesian description — "…pengurangan pasukan di Donbass…"; Indonesian convention.
- `youtube` [https://youtube.com/watch?v=vcRUteHqpF0](https://youtube.com/watch?v=vcRUteHqpF0) — WELT Nachrichtensender: German video — "Heftiger Streit um Donbass! USA drängen Ukraine auf Verzicht"; German convention.
- `youtube` [https://youtube.com/watch?v=xC1ddNF-aOg](https://youtube.com/watch?v=xC1ddNF-aOg) — ZonaGeopolitik: Indonesian video — "Pertempuran di Donbass kian sengit!"; Indonesian convention.

**luhansk** (56)

- `reddit` [https://reddit.com/r/OpinionesPolemicas/comments/1j0nr4e](https://reddit.com/r/OpinionesPolemicas/comments/1j0nr4e) — Spanish opinion post ('el este ucraniano -Donetsk, Lugansk, Jarkov-')
- `reddit` [https://reddit.com/r/ProRussia_news/comments/1igg0n4](https://reddit.com/r/ProRussia_news/comments/1igg0n4) — Spanish crosspost ('Que se queden con Crimea, Donetsk y Lugansk')
- `reddit` [https://reddit.com/r/Slovenia/comments/1kfyks4](https://reddit.com/r/Slovenia/comments/1kfyks4) — Slovenian post ('podpira pošiljanje evropskih vojakov v Lugansk in Donetsk')
- `reddit` [https://reddit.com/r/Sp1Ro/comments/1jjfgr3](https://reddit.com/r/Sp1Ro/comments/1jjfgr3) — Romanian news repost ('regiunilor ucrainene Donețk, Lugansk, Zaporojie')
- `reddit` [https://reddit.com/r/Sp1Ro/comments/1l4jvgl](https://reddit.com/r/Sp1Ro/comments/1l4jvgl) — Romanian news repost ('Harkov, Sumîi şi Lugansk')
- `reddit` [https://reddit.com/r/TroChuyenLinhTinh/comments/1koluxi](https://reddit.com/r/TroChuyenLinhTinh/comments/1koluxi) — Vietnamese post ('Lạng Sơn, Quảng Ninh giống Donetsk và Lugansk')
- `reddit` [https://reddit.com/r/TroChuyenLinhTinh/comments/1l24fo6](https://reddit.com/r/TroChuyenLinhTinh/comments/1l24fo6) — Vietnamese repost of the same memorandum text
- `reddit` [https://reddit.com/r/VietNamNation/comments/1l24du0](https://reddit.com/r/VietNamNation/comments/1l24du0) — Vietnamese post ('các tỉnh Donetsk, Lugansk, Kherson và Zaporizhya')
- `reddit` [https://reddit.com/r/VietnamToanCau/comments/1ixz6rx](https://reddit.com/r/VietnamToanCau/comments/1ixz6rx) — Vietnamese news digest ('bao gồm cả ở Donetsk, Lugansk, Kherson')
- `reddit` [https://reddit.com/r/fcporto/comments/1lcwiof](https://reddit.com/r/fcporto/comments/1lcwiof) — Portuguese scouting thread; the only hit is 'Zorya Lugansk' inside Portuguese prose
- `reddit` [https://reddit.com/r/u_Plastic-Giraffe7255/comments/1ito9ag](https://reddit.com/r/u_Plastic-Giraffe7255/comments/1ito9ag) — Spanish personal essay ('Lugansk estaba siendo defendida con uñas y dientes')
- `reddit` [https://reddit.com/r/u_valzan67/comments/1jnn93n](https://reddit.com/r/u_valzan67/comments/1jnn93n) — Italian title ('La distruzione di un T-90M russo nel Lugansk')
- `youtube` [https://youtube.com/watch?v=0BMKhzDJv7o](https://youtube.com/watch?v=0BMKhzDJv7o) — Spanish ('Control total ruso en Lugansk consolidado')
- `youtube` [https://youtube.com/watch?v=1FEXSe77qt4](https://youtube.com/watch?v=1FEXSe77qt4) — Swahili ('vikosi vya Russia vimeyatwaa maeneo ya mikoa ya Donetsk, Lugansk')
- `youtube` [https://youtube.com/watch?v=4LnfsPQJWYw](https://youtube.com/watch?v=4LnfsPQJWYw) — Italian ('referendum di Lugansk e Donetsk')
- `youtube` [https://youtube.com/watch?v=7D8Q6cGQHN4](https://youtube.com/watch?v=7D8Q6cGQHN4) — Indonesian ('Ia lahir di Lugansk pada masa Uni Soviet')
- `youtube` [https://youtube.com/watch?v=7RDtPdBscuE](https://youtube.com/watch?v=7RDtPdBscuE) — Indonesian ('Ia lahir di Lugansk pada masa Uni Soviet')
- `youtube` [https://youtube.com/watch?v=7Yr4ORat9nY](https://youtube.com/watch?v=7Yr4ORat9nY) — Italian ('il corrispondente da Lugansk di International Reporters')
- `youtube` [https://youtube.com/watch?v=9Tni6Jv5VCQ](https://youtube.com/watch?v=9Tni6Jv5VCQ) — Ukrainian/Russian-language short ('ПРОБЛЕМА'); 'lugansk' only in the hashtag block
- `youtube` [https://youtube.com/watch?v=AGvv4WA9Bow](https://youtube.com/watch?v=AGvv4WA9Bow) — Portuguese ('LUGANSK CAIU? & RÚSSIA ANUNCIA CONTROLE TOTAL')
- `youtube` [https://youtube.com/watch?v=BFoKDx78irE](https://youtube.com/watch?v=BFoKDx78irE) — Spanish ('TERRIBLE ATAQUE RUSO EN LUGANSK')
- `youtube` [https://youtube.com/watch?v=BuOaGrDgUx4](https://youtube.com/watch?v=BuOaGrDgUx4) — Italian ('le regioni ucraine di Kherson, Zaporizhzhia, Donetsk e Lugansk')
- `youtube` [https://youtube.com/watch?v=CJOS4287nvo](https://youtube.com/watch?v=CJOS4287nvo) — Spanish ('retirar tropas de Donetsk y Lugansk')
- `youtube` [https://youtube.com/watch?v=E8C6upa4K88](https://youtube.com/watch?v=E8C6upa4K88) — Tagalog ('Pagpapasya sa Kalayaan ng Donetsk at Lugansk')
- `youtube` [https://youtube.com/watch?v=EsRjpZgBqDU](https://youtube.com/watch?v=EsRjpZgBqDU) — Indonesian channel; 'lugansk' appears only inside an Indonesian SEO keyword dump
- `youtube` [https://youtube.com/watch?v=FioVh7HrlQw](https://youtube.com/watch?v=FioVh7HrlQw) — Russian-language children's dance digest; the English half mirrors the Russian original ('"ДвижDance", Луганск') and the token is a credit line, not prose
- `youtube` [https://youtube.com/watch?v=GIZRzHHQBJc](https://youtube.com/watch?v=GIZRzHHQBJc) — Russian-language channel and description ('поездка через город Луганск'); the Latin token is a bare title stamp, not English prose
- `youtube` [https://youtube.com/watch?v=Glfah_UHRsY](https://youtube.com/watch?v=Glfah_UHRsY) — Spanish ('las provincias de Donetsk y Lugansk')
- `youtube` [https://youtube.com/watch?v=Gybt3yzN5kc](https://youtube.com/watch?v=Gybt3yzN5kc) — Italian model-railway description ('la fabbrica di Lugansk iniziò a lavorare nel 1972')
- `youtube` [https://youtube.com/watch?v=HRmNfurmO1A](https://youtube.com/watch?v=HRmNfurmO1A) — Portuguese ('Zelensky fala em ceder territórios'; '#donetsk #lugansk')
- `youtube` [https://youtube.com/watch?v=N_6k1mULymk](https://youtube.com/watch?v=N_6k1mULymk) — Hmong ('UKRAINE TXEEB TAU 1 LUB ZOS HAUV LUGANSK')
- `youtube` [https://youtube.com/watch?v=OliQvyjYq6Y](https://youtube.com/watch?v=OliQvyjYq6Y) — Portuguese ('Futuro da Guerra Quem Controla Lugansk')
- `youtube` [https://youtube.com/watch?v=RTA9FQpat-4](https://youtube.com/watch?v=RTA9FQpat-4) — German ('Er wurde in Lugansk während der Sowjetzeit geboren')
- `youtube` [https://youtube.com/watch?v=Uz23OsX0jDY](https://youtube.com/watch?v=Uz23OsX0jDY) — Italian model-railway description ('la fabbrica di locomotrici Luhanskteplovoz di Lugansk')
- `youtube` [https://youtube.com/watch?v=YqHlZPG4EII](https://youtube.com/watch?v=YqHlZPG4EII) — Swahili ('miji mingine inayotajwa kuwa itaachiwa ni pamoja na Lugansk na Donetsk')
- `youtube` [https://youtube.com/watch?v=ZZFZZnXNPbk](https://youtube.com/watch?v=ZZFZZnXNPbk) — Italian ('scene drammatiche a Lugansk')
- `youtube` [https://youtube.com/watch?v=ZbOueAz64N4](https://youtube.com/watch?v=ZbOueAz64N4) — Spanish description; 'Lugansk' is a bullet in a car-race roster of territories, not prose
- `youtube` [https://youtube.com/watch?v=aNJPWEytAds](https://youtube.com/watch?v=aNJPWEytAds) — Serbian ('Ukrajina predaje Rusiji regione Donjeck i Lugansk')
- `youtube` [https://youtube.com/watch?v=ad2hPTQvL0w](https://youtube.com/watch?v=ad2hPTQvL0w) — Spanish-language short ('Lugansk, Donetsk, Zaporiyia y Jersón'); measures Spanish convention, not English
- `youtube` [https://youtube.com/watch?v=b7L-PTavotQ](https://youtube.com/watch?v=b7L-PTavotQ) — German ('Er wurde in Lugansk während der Sowjetzeit geboren')
- `youtube` [https://youtube.com/watch?v=bhRPrJ9jKzs](https://youtube.com/watch?v=bhRPrJ9jKzs) — Spanish ('retirarse completamente de las regiones de Donetsk, Lugansk')
- `youtube` [https://youtube.com/watch?v=cF7pKiBVbWY](https://youtube.com/watch?v=cF7pKiBVbWY) — Indonesian ('pengakuan wilayah Krimea dan Lugansk')
- `youtube` [https://youtube.com/watch?v=gCax59m2Z-c](https://youtube.com/watch?v=gCax59m2Z-c) — Italian newscast rundown ('Lugansk, si ricordano i caduti')
- `youtube` [https://youtube.com/watch?v=iOkUqSz_ES4](https://youtube.com/watch?v=iOkUqSz_ES4) — Spanish ('Moscú centró sus esfuerzos en el Donbás, especialmente en el oblast de Lugansk')
- `youtube` [https://youtube.com/watch?v=kSdRrx3au7U](https://youtube.com/watch?v=kSdRrx3au7U) — Spanish ('las provincias ocupadas de Donetsk, Lugansk, Kherson')
- `youtube` [https://youtube.com/watch?v=kp2xcu5FPks](https://youtube.com/watch?v=kp2xcu5FPks) — Indonesian ('Pembebasan Donbass yang terdiri wilayah Donetsk dan Lugansk')
- `youtube` [https://youtube.com/watch?v=lnXpsyExp1g](https://youtube.com/watch?v=lnXpsyExp1g) — Spanish description ('regiones estratégicas como Donetsk, Lugansk, Jersón')
- `youtube` [https://youtube.com/watch?v=lwXxhkfHLBw](https://youtube.com/watch?v=lwXxhkfHLBw) — Description is majority Russian Cyrillic song lyrics ('За Донецк и Луганск') with an appended fan translation; measures the Russian source's convention
- `youtube` [https://youtube.com/watch?v=mAuQQ4Vba6o](https://youtube.com/watch?v=mAuQQ4Vba6o) — Italian newscast rundown ('Lugansk, attentato uccide l'ex sindaco della città')
- `youtube` [https://youtube.com/watch?v=m_7O5UEQgQY](https://youtube.com/watch?v=m_7O5UEQgQY) — Spanish ('LUGANSK 98% OCUPADO OFICIALMENTE')
- `youtube` [https://youtube.com/watch?v=pQzopvCQ5T0](https://youtube.com/watch?v=pQzopvCQ5T0) — Italian ('permesso di soggiorno temporaneo nella Repubblica Popolare di Lugansk')
- `youtube` [https://youtube.com/watch?v=pb3g945qDKw](https://youtube.com/watch?v=pb3g945qDKw) — Indonesian ('pengakuan wilayah Krimea dan Lugansk')
- `youtube` [https://youtube.com/watch?v=pcD8c3clgs0](https://youtube.com/watch?v=pcD8c3clgs0) — Russian-language channel; title is bare hashtags ('#automobile #lugansk #music#вахта') with an empty description
- `youtube` [https://youtube.com/watch?v=sGIDTkosYk4](https://youtube.com/watch?v=sGIDTkosYk4) — Spanish description ('recuperaron un territorio clave en Lugansk')
- `youtube` [https://youtube.com/watch?v=vcQudmNk0e4](https://youtube.com/watch?v=vcQudmNk0e4) — Italian ('Pesanti attacchi ucraini su DOneck, Lugansk e perfino a Izevsk')
- `youtube` [https://youtube.com/watch?v=wZ3IyQiwBGE](https://youtube.com/watch?v=wZ3IyQiwBGE) — Spanish description; 'Lugansk' is an entry in a car-race roster of territories

**chornobyl** (16)

- `news` [https://www.okaz.com.sa/variety/na/2225423](https://www.okaz.com.sa/variety/na/2225423) — Article body is Arabic (تشيرنوبيل); the English block is an appended translation, not attested usage.
- `openalex` [https://openalex.org/W4411807606](https://openalex.org/W4411807606) — OpenAlex language=ru; Russian-language MSU journal article with a translated English title.
- `reddit` [https://reddit.com/r/HUEstation/comments/1hr3fkt](https://reddit.com/r/HUEstation/comments/1hr3fkt) — Portuguese-language Brazilian meme sub (r/HUEstation); bare one-word video title, no English text.
- `reddit` [https://reddit.com/r/Slovakia/comments/1hso0pf](https://reddit.com/r/Slovakia/comments/1hso0pf) — Slovak-language post about watching 'seriál Chernobyl z roku 2019'.
- `reddit` [https://reddit.com/r/filmeseseries/comments/1hr3dcf](https://reddit.com/r/filmeseseries/comments/1hr3dcf) — Portuguese post; 'o desastre de Chernobyl' used to describe a bad film.
- `youtube` [https://youtube.com/watch?v=73LzdJSUgWM](https://youtube.com/watch?v=73LzdJSUgWM) — Tamil voiceover short (#tamilvoiceover #tamilshorts). _(pattern: `#tamil`)_
- `youtube` [https://youtube.com/watch?v=87NcMLP3Crk](https://youtube.com/watch?v=87NcMLP3Crk) — Romanised Hindi short ('Sab Kuch Badal Gaya', '1986 ka ek blast…').
- `youtube` [https://youtube.com/watch?v=CMNHBYaU2Ts](https://youtube.com/watch?v=CMNHBYaU2Ts) — Indonesian short ('sejarah chernobyl').
- `youtube` [https://youtube.com/watch?v=DP08U9Zh500](https://youtube.com/watch?v=DP08U9Zh500) — Portuguese short ('5 curiosidades sobre Chernobyl').
- `youtube` [https://youtube.com/watch?v=RG6J7lKU7Yo](https://youtube.com/watch?v=RG6J7lKU7Yo) — Indonesian news short (Tribun Jogja).
- `youtube` [https://youtube.com/watch?v=RY4-qxsBveI](https://youtube.com/watch?v=RY4-qxsBveI) — Indonesian short ('MISTERI CHERNOBYL: Kutukan Wormwood…').
- `youtube` [https://youtube.com/watch?v=Rw7mpEkx3N4](https://youtube.com/watch?v=Rw7mpEkx3N4) — Portuguese short ('Chernobyl Sob Ataque: Uma Nova Ameaça Surge').
- `youtube` [https://youtube.com/watch?v=fKO1hjvXmzU](https://youtube.com/watch?v=fKO1hjvXmzU) — Tamil-language short; the only English token is the name plus #tamil hashtags. _(pattern: `#tamil`)_
- `youtube` [https://youtube.com/watch?v=fpxCYU7iCJA](https://youtube.com/watch?v=fpxCYU7iCJA) — Portuguese short; 'O Chernobyl Brasileiro' is the Goiânia caesium-137 accident, not Ukraine.
- `youtube` [https://youtube.com/watch?v=psoaBgo9ySI](https://youtube.com/watch?v=psoaBgo9ySI) — Portuguese channel/short ('Sussurros da Noite', #historia #relatos).
- `youtube` [https://youtube.com/watch?v=t998f-hRpJk](https://youtube.com/watch?v=t998f-hRpJk) — Portuguese short ('reator chernobyl #fisica').

**dynamo-kyiv** (30)

- `reddit` [https://reddit.com/r/SportingCP/comments/1olrkb3](https://reddit.com/r/SportingCP/comments/1olrkb3) — Portuguese post ('O sucesso da equipa B ... só o Dynamo Kiev fez em 1998'); Portuguese convention, not English. _(pattern: `\b(equipa|arriscamos|ganhar a primeira liga|já agora)\b`)_
- `reddit` [https://reddit.com/r/galatasaray/comments/1i2iqi5](https://reddit.com/r/galatasaray/comments/1i2iqi5) — Turkish post ('Dynamo kiev maci biletleri ne zaman satisa cikar?' = when do tickets go on sale?); Turkish convention, not English. _(pattern: `\b(maci biletleri|ne zaman|satisa cikar)\b`)_
- `youtube` [https://youtube.com/watch?v=0T_W78ZERZE](https://youtube.com/watch?v=0T_W78ZERZE) — Italian results round-up ('SINTESI PLAYOFF EUROPA LEAGUE'); Italian convention. _(pattern: `\b(sintesi|andata|ritorno|calcio europeo)\b`)_
- `youtube` [https://youtube.com/watch?v=0XvAX3tumxw](https://youtube.com/watch?v=0XvAX3tumxw) — Spanish betting previews ('PRONÓSTICOS UEFA CONFERENCE LEAGUE | JORNADA 6'); Spanish convention. _(pattern: `(pron[oó]sticos?|apuestas deportivas|futbol europeo|jornada \d)`)_
- `youtube` [https://youtube.com/watch?v=3Esz84iXLDw](https://youtube.com/watch?v=3Esz84iXLDw) — Romanian ('Meci amical' = friendly match) from GOLAZO.ro; Romanian convention, not English. _(pattern: `\bmeci amical\b`)_
- `youtube` [https://youtube.com/watch?v=6LBzida4f_w](https://youtube.com/watch?v=6LBzida4f_w) — French 1987 Téléfoot archive ('Dynamo Kiev-Besiktas'); French convention. _(pattern: `\b(téléfoot|1/[24] de finale|coupe des clubs champions)\b`)_
- `youtube` [https://youtube.com/watch?v=Alx_vA6cY7w](https://youtube.com/watch?v=Alx_vA6cY7w) — Italian results round-up ('SINTESI 3° TURNO ... RITORNO') with Italian/French club names; Italian convention. _(pattern: `\b(sintesi|andata|ritorno|calcio europeo)\b`)_
- `youtube` [https://youtube.com/watch?v=DBl62LLNZh0](https://youtube.com/watch?v=DBl62LLNZh0) — Portuguese live-stream title ('AO VIVO: DYNAMO KIEV x HAMRUN - SEM DELAY'); Portuguese convention. _(pattern: `\b(ao vivo|sem delay|palpites|análises do|jogos do dia)\b`)_
- `youtube` [https://youtube.com/watch?v=DC1CDzwxpU0](https://youtube.com/watch?v=DC1CDzwxpU0) — Turkish ('GALATASARAY DYNAMO KİEV SANCHEZİN GOLÜ' = Sanchez's goal); also 'KİEV' with the Turkish dotted I. _(pattern: `\b(golü|sanchezin|maçi|maçı)\b`)_
- `youtube` [https://youtube.com/watch?v=GRPwb32Rbbs](https://youtube.com/watch?v=GRPwb32Rbbs) — Indonesian title and description ('Ketika Shevchenko Menguasai Camp Nou'); Indonesian convention. _(pattern: `\b(hasil|tadi malam|ketika|semua pemain|kualifikasi|pertandingan|melawan|mempertemukan|menewaskan|menang lagi)\b`)_
- `youtube` [https://youtube.com/watch?v=KjMV7Mt8r_w](https://youtube.com/watch?v=KjMV7Mt8r_w) — French betting previews ('MES PRONOSTICS LIGUE DES CHAMPIONS'); French convention. _(pattern: `\b(pronostics?|téléfoot|ligue des champions|ligue europa)\b`)_
- `youtube` [https://youtube.com/watch?v=MgNd1KfeRfQ](https://youtube.com/watch?v=MgNd1KfeRfQ) — Turkish match-summary title ('DYNAMO KIEV ÖZET' = summary); measures Turkish convention, not English. _(pattern: `\b(özet|maçi|maçı|maçini|rakibi|rakibimizi|rakipleri|golü|türkçe|spiker|kariyer önerisi|tepki videosu|biletleri|kayıtları|satisa|tanıyalım|izlerse)\b`)_
- `youtube` [https://youtube.com/watch?v=PrkAPa0bSL8](https://youtube.com/watch?v=PrkAPa0bSL8) — Indonesian retelling of the 1942 Kyiv 'Death Match' ('Semua Pemain Ini Habis Main Langsung Dibunuh'); Indonesian convention. _(pattern: `\b(semua pemain|klub lokal|dipaksa|tentara|pertandingan)\b`)_
- `youtube` [https://youtube.com/watch?v=RCsqC2KexLs](https://youtube.com/watch?v=RCsqC2KexLs) — Turkish title ('Samsun'da Coulibaly şov') with Turkish tags (#futbol); Turkish convention. _(pattern: `\b(özet|maçi|maçı|rakibi|golü|türkçe|spiker|şov|futbol)\b`)_
- `youtube` [https://youtube.com/watch?v=Ytsr9H25Z7c](https://youtube.com/watch?v=Ytsr9H25Z7c) — Turkish Football Manager video ('FM 26 | KARİYER ÖNERİSİ'); also spelled 'KİEV' with the Turkish dotted I, which is not the pair's ASCII surface form. _(pattern: `\b(kariyer önerisi|fm2[0-9]kariyer|türkçe|spiker)\b`)_
- `youtube` [https://youtube.com/watch?v=ZjENe2wnIag](https://youtube.com/watch?v=ZjENe2wnIag) — French betting previews ('MES PRONOSTICS LIGUE EUROPA'); French convention. _(pattern: `\b(pronostics?|téléfoot|ligue des champions|ligue europa)\b`)_
- `youtube` [https://youtube.com/watch?v=ahM_se4M_i0](https://youtube.com/watch?v=ahM_se4M_i0) — Turkish reaction video ('GALATASARAY 3-3 DYNAMO KIEV MAÇINI İZLERSE'); Turkish convention. _(pattern: `\b(maçini|izlerse|fanatik|rezillik|tepki videosu)\b`)_
- `youtube` [https://youtube.com/watch?v=ahggRrvtZwE](https://youtube.com/watch?v=ahggRrvtZwE) — French betting previews ('MES PRONOSTICS LIGUE DES CHAMPIONS'); French style is 'Kiev' regardless. _(pattern: `\b(pronostics?|téléfoot|ligue des champions|ligue europa)\b`)_
- `youtube` [https://youtube.com/watch?v=bPqd940Yplw](https://youtube.com/watch?v=bPqd940Yplw) — Turkish EA FC commentary video ('TÜRKÇE SPİKER'); also 'KİEV' with the Turkish dotted I. _(pattern: `\b(türkçe|spiker|maçi|maçı)\b`)_
- `youtube` [https://youtube.com/watch?v=bublenM9FjM](https://youtube.com/watch?v=bublenM9FjM) — Indonesian description of Lewandowski's 2021 bicycle kick ('mempertemukan Dynamo Kiev melawan FC Bayern'); Indonesian convention. _(pattern: `\b(mempertemukan|melawan|gol salto|pada tanggal)\b`)_
- `youtube` [https://youtube.com/watch?v=dHtSRmaFKz4](https://youtube.com/watch?v=dHtSRmaFKz4) — Spanish betting previews ('PRONÓSTICOS ... APUESTAS DEPORTIVAS'); Spanish convention, which is 'Kiev' regardless. _(pattern: `(pron[oó]sticos?|apuestas deportivas|futbol europeo|jornada \d)`)_
- `youtube` [https://youtube.com/watch?v=gKkuXQg2BGk](https://youtube.com/watch?v=gKkuXQg2BGk) — French 1987 Téléfoot archive listing 'FC Porto-Dynamo Kiev'; French convention. _(pattern: `\b(téléfoot|pronostics?|1/[24] de finale|coupe de france)\b`)_
- `youtube` [https://youtube.com/watch?v=kNOgkGsWXho](https://youtube.com/watch?v=kNOgkGsWXho) — French betting previews ('Pronostic foot Ligue Europa Conference'); French convention. _(pattern: `\b(pronostics?|pronostic foot|ligue europa conference)\b`)_
- `youtube` [https://youtube.com/watch?v=kx7bcWbrr0M](https://youtube.com/watch?v=kx7bcWbrr0M) — Turkish reaction video ('YİNE REZİLLİK ... MAÇI TEPKİ VİDEOSU'); Turkish convention. _(pattern: `\b(rezillik|tepki videosu|maçi|maçı)\b`)_
- `youtube` [https://youtube.com/watch?v=l1xTBBzDwxs](https://youtube.com/watch?v=l1xTBBzDwxs) — Portuguese betting-tips channel ('Análises do Ortega | Jogos do dia'); Portuguese convention. _(pattern: `\b(palpites|análises do|ao vivo|sem delay|jogos do dia|o sucesso da equipa)\b`)_
- `youtube` [https://youtube.com/watch?v=qHpLQOvneL4](https://youtube.com/watch?v=qHpLQOvneL4) — Turkish opponent preview ('AVRUPA LİGİ'NDEKİ RAKİBİ DYNAMO KİEV! | RAKİBİMİZİ TANIYALIM'); also 'KİEV' with the Turkish dotted I. _(pattern: `\b(rakibi|rakibimizi|tanıyalım|rakipleri)\b`)_
- `youtube` [https://youtube.com/watch?v=s0VA9LNqajI](https://youtube.com/watch?v=s0VA9LNqajI) — French betting previews ('Pronostic Ligue des Champions ... Pronostic Foot Pafos Dynamo Kiev'); French convention. _(pattern: `\b(pronostics?|pronostic foot|ligue des champions)\b`)_
- `youtube` [https://youtube.com/watch?v=vHL25h8NSMU](https://youtube.com/watch?v=vHL25h8NSMU) — Indonesian results round-up ('HASIL LIGA CHAMPIONS TADI MALAM'); Indonesian convention. _(pattern: `\b(hasil|tadi malam|kualifikasi|jadwal|pertandingan)\b`)_
- `youtube` [https://youtube.com/watch?v=vSo3NDkYl1o](https://youtube.com/watch?v=vSo3NDkYl1o) — Malay ('crystal palace menang lagi menewaskan dynamo kiev 2-0..padu mat'); Malay convention, not English. _(pattern: `\b(menang lagi|menewaskan|padu mat|kalah)\b`)_
- `youtube` [https://youtube.com/watch?v=xKayqhi1GrE](https://youtube.com/watch?v=xKayqhi1GrE) — Italian results round-up ('SINTESI ... ANDATA'); Italian convention. _(pattern: `\b(sintesi|andata|ritorno|calcio europeo)\b`)_

**chicken-kyiv** (2)

- `reddit` [https://reddit.com/r/kuhinja/comments/1k93q80](https://reddit.com/r/kuhinja/comments/1k93q80) — r/kuhinja is a Croatian/Serbian-language cooking subreddit ('kuhinja' = kitchen); the only English is the borrowed dish label in the title, with no English running text. _(pattern: `r/kuhinja\b`)_
- `youtube` [https://youtube.com/watch?v=Zn010b9TREs](https://youtube.com/watch?v=Zn010b9TREs) — Russian-language channel; the title is an English tag but the whole description is Russian ('КОТЛЕТЫ ПО-КИЕВСКИ ... 1 Куриная грудка'), so this measures Russian, not English, usage. _(pattern: `[а-яё]{4,}`)_

**kharkiv** (19)

- `reddit` [https://reddit.com/r/TarihiSeyler/comments/1hx4xlr](https://reddit.com/r/TarihiSeyler/comments/1hx4xlr) — Turkish-language post; the Kharkov mention sits inside Turkish prose.
- `youtube` [https://youtube.com/watch?v=-85TZFfRXMA](https://youtube.com/watch?v=-85TZFfRXMA) — Indonesian (Tribun Timur): 'UAV Geran Rusia Hancurkan Titik Kontrol... di Kharkov'.
- `youtube` [https://youtube.com/watch?v=-LB8c8HDqtQ](https://youtube.com/watch?v=-LB8c8HDqtQ) — Indonesian (ZonaGeopolitik): 'PASUKAN SINYAL RUSIA KUASAI KHARKOV!'.
- `youtube` [https://youtube.com/watch?v=5eam1pnifUM](https://youtube.com/watch?v=5eam1pnifUM) — Italian (Nicolai Lilin): 'ZELENSKIJ STA PER ABBANDONARE KHARKOV'.
- `youtube` [https://youtube.com/watch?v=5lrmqjT8Qyo](https://youtube.com/watch?v=5lrmqjT8Qyo) — Indonesian (Tribun Timur): 'Kota Kharkov Meledak Dahsyat Dihantam Rudal Rusia'.
- `youtube` [https://youtube.com/watch?v=6Mmp-nT-quA](https://youtube.com/watch?v=6Mmp-nT-quA) — Indonesian (Tribun Timur): 'Serangan Malam Hari di Arah Kharkov'.
- `youtube` [https://youtube.com/watch?v=6jx1hIkBc-s](https://youtube.com/watch?v=6jx1hIkBc-s) — Spanish-language channel (Intereconomia): 'Dron FPV sobre Kharkov muestra ataque en primera persona'.
- `youtube` [https://youtube.com/watch?v=C7gS_l9thZ4](https://youtube.com/watch?v=C7gS_l9thZ4) — French (Caroline Galacteros): 'son avancee dans les regions de Soumy, Kharkov et Kherson'.
- `youtube` [https://youtube.com/watch?v=FnSvwKArZ8g](https://youtube.com/watch?v=FnSvwKArZ8g) — Indonesian (Tribun Papua Barat): 'Amunisi Lancet Rusia Hancurkan Radar Ukraina RADA di Kharkov'.
- `youtube` [https://youtube.com/watch?v=G1prwzoS4VA](https://youtube.com/watch?v=G1prwzoS4VA) — Indonesian (Tribun Timur): 'Chechnya Luncurkan Serangan Drone Presisi di Arah Kharkov'.
- `youtube` [https://youtube.com/watch?v=LS4DEX0u2kg](https://youtube.com/watch?v=LS4DEX0u2kg) — Indonesian (Tribun Papua Barat): 'Drone Rusia Hancurkan 2 Tank Leopard Ukraina di Kharkov'.
- `youtube` [https://youtube.com/watch?v=LfRCPYRmQV0](https://youtube.com/watch?v=LfRCPYRmQV0) — Indonesian (Awan Militer): 'Iskander Rusia Hajar Pabrik Tank Kharkov!'.
- `youtube` [https://youtube.com/watch?v=P9Ti-smjQIc](https://youtube.com/watch?v=P9Ti-smjQIc) — Portuguese: 'Ataques da Russia castigam Odessa e Kharkov'.
- `youtube` [https://youtube.com/watch?v=PoX0F6FYMTM](https://youtube.com/watch?v=PoX0F6FYMTM) — Italian (Nicolai Lilin): 'I russi bombardano Kharkov e avanzano su tutta la linea'.
- `youtube` [https://youtube.com/watch?v=e55kt5jc8iQ](https://youtube.com/watch?v=e55kt5jc8iQ) — Indonesian (Kompas.com): 'Rusia Rebut Permukiman di Kharkov'.
- `youtube` [https://youtube.com/watch?v=m9x3IUi5kvs](https://youtube.com/watch?v=m9x3IUi5kvs) — Indonesian (Tribun Timur): 'Kibarkan Bendera di Kharkov Usai Musuh Mundur'.
- `youtube` [https://youtube.com/watch?v=n-wQ0_loLE0](https://youtube.com/watch?v=n-wQ0_loLE0) — Italian (Nicolai Lilin): 'URGENTE! PUTIN ASFALTA KHARKOV' with an all-Italian description.
- `youtube` [https://youtube.com/watch?v=qjL-hiC8yxY](https://youtube.com/watch?v=qjL-hiC8yxY) — Swahili (RS Swahili TV): 'WANAJESHI WA UKRAINE HUKO KHARKOV...'.
- `youtube` [https://youtube.com/watch?v=u_m9Xdgqc60](https://youtube.com/watch?v=u_m9Xdgqc60) — Indonesian (Angin Timur): 'Aksi Nekat di Kharkov'.

**volodymyr-zelenskyy** (20)

- `reddit` [https://reddit.com/r/Lima_Peru/comments/1kv2no7](https://reddit.com/r/Lima_Peru/comments/1kv2no7) — r/Lima_Peru: the sentence carrying the name is Spanish — “Es un actor cómico. ¿Pero no lo es también Vladimir Zelensky?” _(pattern: `\b(?:presidencial|candidato|pero no lo es también)\b`)_
- `reddit` [https://reddit.com/r/NepalSocial/comments/1newvi8](https://reddit.com/r/NepalSocial/comments/1newvi8) — r/NepalSocial: the sentence carrying the name is Roman-script Nepali — “Ukraine ko President Vladimir zelensky pani entertainment industry bata nai thiye.” _(pattern: `\b(?:pani|bata nai thiye|dherai similarities xa)\b`)_
- `reddit` [https://reddit.com/r/u_RDSolenodonte/comments/1p4l6he](https://reddit.com/r/u_RDSolenodonte/comments/1p4l6he) — r/u_RDSolenodonte: the post is Spanish — “…calificó de "postura" las declaraciones de Vladimir Zelensky…” _(pattern: `\b(?:el enviado especial|calificó de|declaraciones de)\b`)_
- `youtube` [https://youtube.com/watch?v=3rTvvMm-htM](https://youtube.com/watch?v=3rTvvMm-htM) — tvOnenewscom: the sentence carrying the name is Indonesian — “Presiden Ukraina Vladimir Zelensky dan lingkaran terdekatnya…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=9vk9oiCnt2U](https://youtube.com/watch?v=9vk9oiCnt2U) — A2 CNN: the sentence carrying the name is Albanian — “…siç raportoi agjencia RBC-Ukrainë, Vladimir Zelensky deklaroi se…” _(pattern: `\b(?:presidentin|deklaroi|takimi|mund të)\b`)_
- `youtube` [https://youtube.com/watch?v=BYpHDoPWQFk](https://youtube.com/watch?v=BYpHDoPWQFk) — FACTROVA: the title is Roman-script Hindi — “Ab Vladimir Zelensky INDIA Ane vale hai”. _(pattern: `\b(?:ane vale hai|ab .{0,20} india ane)\b`)_
- `youtube` [https://youtube.com/watch?v=DmZnhensG6w](https://youtube.com/watch?v=DmZnhensG6w) — Tribun Jogja Official: the sentence carrying the name is Indonesian — “…telah meminta pemimpin Ukraina Vladimir Zelensky untuk memperbaiki hubungan…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=JdwaF_51dlE](https://youtube.com/watch?v=JdwaF_51dlE) — Mundo da Fantasia: the description is Portuguese — “Vladimir Zelensky indo à luta”. _(pattern: `\b(?:indo à luta|guarda costas)\b`)_
- `youtube` [https://youtube.com/watch?v=Kjoiq-iUVCw](https://youtube.com/watch?v=Kjoiq-iUVCw) — KONTAN TV: the sentence carrying the name is Indonesian — “…pertemuan dengan Presiden Ukraina Vladimir Zelensky dan pemimpin negara-negara Eropa…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=MMntnbdILF8](https://youtube.com/watch?v=MMntnbdILF8) — Kompas.com: the sentence carrying the name is Indonesian — “Presiden Ukraina, Vladimir Zelensky pun tidak menepis pernyataan…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=MncQEy9S05o](https://youtube.com/watch?v=MncQEy9S05o) — Pojok Negeri Media: the sentence carrying the name is Indonesian — “…pertengkaran publiknya dengan Vladimir Zelensky…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=PPZdb9QbG78](https://youtube.com/watch?v=PPZdb9QbG78) — Tribun Lombok: the sentence carrying the name is Indonesian — “Presiden Ukraina Vladimir Zelensky menolak mendukung usulan gencatan senjata…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=QhLyqJqwXMg](https://youtube.com/watch?v=QhLyqJqwXMg) — Focus News Tanzania Tv 1: the sentence carrying the name is Swahili — “…tangazo la Rais wa Ukraine Vladimir Zelensky kwamba alikuwa tayari…” _(pattern: `\b(?:rais wa|amesema|kutoka kwa|akijibu)\b`)_
- `youtube` [https://youtube.com/watch?v=REJbsFB_oTE](https://youtube.com/watch?v=REJbsFB_oTE) — Tribun Pekanbaru Official: the title and description are Indonesian — “Vladimir Zelensky Negosiasi Damai Rusia dengan Meminta Tambahan Senjata…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=Z5SuLqXEuK8](https://youtube.com/watch?v=Z5SuLqXEuK8) — La Repubblica: the sentence carrying the name is Italian, not English — “…riferendosi all'incontro imminente con Vladimir Zelensky…” _(pattern: `\b(?:incontrer|riferendosi|settimane|abbiamo)\b`)_
- `youtube` [https://youtube.com/watch?v=ZHSIwlnGWaM](https://youtube.com/watch?v=ZHSIwlnGWaM) — Gorakh Dhandha with Abrar Qureshi: Roman-script Urdu title with the name appended as a pipe segment — “Burqa molvi ki phir amad qu | kon sy 10 safihu par pabandi | Donald Trump Vs Vladimir Zelensky”. _(pattern: `\b(?:ki phir amad|kon sy|par pabandi)\b`)_
- `youtube` [https://youtube.com/watch?v=np9Q0mfLhW0](https://youtube.com/watch?v=np9Q0mfLhW0) — Warta Kota Production: the sentence carrying the name is Indonesian — “…Presiden Ukraina Vladimir Zelensky, hingga Presiden Prancis Emmanuel Macron…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=uuQjZH_-7GA](https://youtube.com/watch?v=uuQjZH_-7GA) — Baku Hantam: the sentence carrying the name is Indonesian — “Vladimir Zelensky menolak untuk meminta maaf atas…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_
- `youtube` [https://youtube.com/watch?v=vsJuvt5Qtgc](https://youtube.com/watch?v=vsJuvt5Qtgc) — Mapa at Mga Dahilan: the sentence carrying the name is Tagalog — “nagkita sina Donald Trump at Vladimir Zelensky sa Washington!” _(pattern: `\b(?:ang|ng|sina|nagkita|kasama)\b`)_
- `youtube` [https://youtube.com/watch?v=yQDQ5uHSxyw](https://youtube.com/watch?v=yQDQ5uHSxyw) — Amanda Chanel: the sentence carrying the name is Indonesian — “Pemimpin Ukraina Vladimir Zelensky memperingatkan bahwa Kiev…” _(pattern: `\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b`)_

**ihor-sikorsky** (20)

- `reddit` [https://reddit.com/r/u_Key_Bobcat9407/comments/1ijrxus](https://reddit.com/r/u_Key_Bobcat9407/comments/1ijrxus) — Portuguese post ('Em 1939, o primeiro helicóptero surgiu, criado por Igor Sikorsky, um engenheiro russo-americano'). _(pattern: `\bhelic[oó]ptero\b`)_
- `youtube` [https://youtube.com/watch?v=1J589yIhIRY](https://youtube.com/watch?v=1J589yIhIRY) — Indonesian ('seorang insinyur penerbangan keturunan Rusia-Amerika yang mengembangkan dan menerbangkan helikopter'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=39JWZkw9OP4](https://youtube.com/watch?v=39JWZkw9OP4) — Indonesian title only ('penemuan helikopter modern (igor sikorsky) #sejarahdunia'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=4KJkwATlxGg](https://youtube.com/watch?v=4KJkwATlxGg) — Indonesian ('Igor Sikorsky adalah insinyur penerbangan kelahiran Kiev pada 1889'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=9fkXKajAY0o](https://youtube.com/watch?v=9fkXKajAY0o) — Portuguese description ('O VS-300, desenvolvido por Igor Sikorsky, demonstrou pela primeira vez o voo vertical'). _(pattern: `\bhelic[oó]ptero\b`)_
- `youtube` [https://youtube.com/watch?v=P4uqrIGntwI](https://youtube.com/watch?v=P4uqrIGntwI) — Indonesian description ('Sejarah Helikopter Pertama di Dunia... Diciptakan oleh Igor Sikorsky'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=_JGIj1gIras](https://youtube.com/watch?v=_JGIj1gIras) — Russian-language channel (Путь Мастера); the only text is the bare Latin-script name plus hashtags, so it evidences Russian-language convention, not English usage.
- `youtube` [https://youtube.com/watch?v=c7zJFHmDyfg](https://youtube.com/watch?v=c7zJFHmDyfg) — Indonesian ('Helikopter modern ditemukan oleh Igor Sikorsky, yang berhasil merancang dan menerbangkan VS-300'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=cVtj9XgEXnU](https://youtube.com/watch?v=cVtj9XgEXnU) — Indonesian ('Inilah kisah luar biasa tentang Igor Sikorsky, sosok jenius dari luar negeri'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=dlnl50wVcA8](https://youtube.com/watch?v=dlnl50wVcA8) — Indonesian ('Tapi baru pada 1939, Igor Sikorsky berhasil menerbangkannya'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=htKycBw3Fyo](https://youtube.com/watch?v=htKycBw3Fyo) — Indonesian ('Kisah Igor Sikorsky adalah bukti bahwa kegagalan hanyalah penundaan'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=k3lHASBEgQM](https://youtube.com/watch?v=k3lHASBEgQM) — Indonesian ('hingga prototipe sukses buatan Igor Sikorsky di tahun 1939'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=li24ER2Tbu4](https://youtube.com/watch?v=li24ER2Tbu4) — Indonesian ('kisah inspiratif Igor Sikorsky Penemu Helikopter... Selamat datang di Channel Tokoh Inspiratif'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=nn9z34vibZ8](https://youtube.com/watch?v=nn9z34vibZ8) — Indonesian ('tanggal 14 Mei 1939 jadi hari bersejarah karena Igor Sikorsky berhasil terbangkan helikopter VS-300'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=oOwxqWRyUh0](https://youtube.com/watch?v=oOwxqWRyUh0) — Indonesian ('Pada tahun 1939, Igor Sikorsky menerbangkan helikopter praktis pertama di dunia'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=qyemVnmNVlw](https://youtube.com/watch?v=qyemVnmNVlw) — Indonesian title only ('Penemuan Helikopter - Igor Sikorsky (1939, Amerika)'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=rAQseWcVZWQ](https://youtube.com/watch?v=rAQseWcVZWQ) — Indonesian ('Igor Sikorsky: Bapak Helikopter Modern... Bayangkan seorang anak kecil di Kiev'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=rroQugr3P68](https://youtube.com/watch?v=rroQugr3P68) — Indonesian ('Penemu Helikopter Modern Igor Sikorsky... Helikopter modern adalah hasil dari perkembangan panjang'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=zUMn_Co2PxI](https://youtube.com/watch?v=zUMn_Co2PxI) — Indonesian ('akhirnya helikopter modern berhasil diciptakan oleh Igor Sikorsky'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_
- `youtube` [https://youtube.com/watch?v=zrzdFlhKQSA](https://youtube.com/watch?v=zrzdFlhKQSA) — Indonesian ('Para Pencipta & Penemu... Robert Goddard, Igor Sikorsky, Richard Trevithick'). _(pattern: `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`)_

**kyivan-rus** (1)

- `youtube` [https://youtube.com/watch?v=8UE-bs1onAg](https://youtube.com/watch?v=8UE-bs1onAg) — Title and description are Filipino/Tagalog ('susuriin natin ang buong kasaysayan ng Russia-mula sa Kievan Rus'); not English usage. _(pattern: `\b(?:kasaysayan|paano ito|mula sa|susuriin natin)\b`)_

**babyn-yar** (10)

- `openalex` [https://openalex.org/W7149224234](https://openalex.org/W7149224234) — French-titled item ('Babi Yar et les escaliers Potemkine') whose indexed abstract is about the French housing crisis — not English and not about the Kyiv site. _(pattern: `babi yar et les`)_
- `reddit` [https://reddit.com/r/DeTodoES/comments/1njmgsf](https://reddit.com/r/DeTodoES/comments/1njmgsf) — Spanish-language book blurb: 'podría haber acontecido en Babi Yar donde los nazis ametrallaron a millares de judíos'.
- `reddit` [https://reddit.com/r/DeTodoES/comments/1njn0qt](https://reddit.com/r/DeTodoES/comments/1njn0qt) — Duplicate Spanish-language book blurb (same text as the other r/DeTodoES post).
- `reddit` [https://reddit.com/r/Yermak/comments/1m1m337](https://reddit.com/r/Yermak/comments/1m1m337) — Machine-translated repost of Yermak's Ukrainian post — the same 1,200 chars give 'Babin Yar' twice and 'Babi Yar' once, plus 'Kit Kelloga'/'rabbins'; the spelling is MT output, not an English writer's choice. _(pattern: `babin yar`)_
- `youtube` [https://youtube.com/watch?v=054lSrkHpec](https://youtube.com/watch?v=054lSrkHpec) — German-language description; the only hit sits in German prose: 'hinterliessen Schluchten wie Babi Yar voller Toter'.
- `youtube` [https://youtube.com/watch?v=Fmn-CT85KdY](https://youtube.com/watch?v=Fmn-CT85KdY) — Portuguese short: 'Babi Yar, um dos piores momentos da segunda guerra mundial'; no English text.
- `youtube` [https://youtube.com/watch?v=NYwEKnzScL4](https://youtube.com/watch?v=NYwEKnzScL4) — Indonesian short: 'Babi Yar: Jurang Pembantaian Nazi yang Disembunyikan'; no English text. _(pattern: `jurang pembantaian`)_
- `youtube` [https://youtube.com/watch?v=fWwpCtqNngU](https://youtube.com/watch?v=fWwpCtqNngU) — Spanish op-ed video 'Babi Yar colombiano' — entirely Spanish, using the ravine as a frame for Colombian massacres. _(pattern: `babi yar colombiano`)_
- `youtube` [https://youtube.com/watch?v=gHLJE4zR8A4](https://youtube.com/watch?v=gHLJE4zR8A4) — French-language ASMR short: 'ASMR - Le massacre de Babi Yar'; no English text. _(pattern: `massacre de babi yar`)_
- `youtube` [https://youtube.com/watch?v=n2ecafFc7O4](https://youtube.com/watch?v=n2ecafFc7O4) — Portuguese short 'Fatos curiosos sobre Massacre de Babi Yar', with a keyword-stuffed description about science/nerd culture. _(pattern: `massacre de babi yar`)_

**varenyky** (21)

- `reddit` [https://reddit.com/r/WriteStreakGerman/comments/1hwmchj](https://reddit.com/r/WriteStreakGerman/comments/1hwmchj) — German writing-practice post; the German prose spells it 'Wareniki'/'Waraneki' and the matched 'vareniki' occurs only inside a link URL (supertravelr.com/.../pelmeni-vareniki-and-pierogi). _(pattern: `https?://[^\s)]*vareniki`)_
- `reddit` [https://reddit.com/r/ebooklibri/comments/1ovr0yx](https://reddit.com/r/ebooklibri/comments/1ovr0yx) — Entirely Italian: 'Pelmeni/Vareniki (Ravioli Russi al Burro)' inside an Italian cooking-robot program listing; measures Italian convention. _(pattern: `\bravioli\s+russi\b|\bcucina\s+russa\b`)_
- `youtube` [https://youtube.com/watch?v=-2q0NpfVb-Q](https://youtube.com/watch?v=-2q0NpfVb-Q) — Entirely German: 'Vareniki ohne Ei?! Perfekte vegane Teigtaschen – Omas Mythos zerbröselt!' _(pattern: `\bteigtaschen\b`)_
- `youtube` [https://youtube.com/watch?v=Dy3dz-363nA](https://youtube.com/watch?v=Dy3dz-363nA) — Second Stalik Khankishiev upload: bare-term title, wholly Russian description; no English text. _(pattern: `\bсталик\b`)_
- `youtube` [https://youtube.com/watch?v=HF6SMz3sXcM](https://youtube.com/watch?v=HF6SMz3sXcM) — Turkish title on a Russian-language Turkey channel: 'PEYNİRLİ RUS MANTI "VARENIKI" MANTI'; empty description. _(pattern: `\bpeynirli\b|\brus\s+mant[ıi]\b`)_
- `youtube` [https://youtube.com/watch?v=HuxSwTWolCA](https://youtube.com/watch?v=HuxSwTWolCA) — Duplicate Turkish upload: 'Ukrayna Mantısı Vareniki Tarifi!' with the same full Turkish recipe description. _(pattern: `ukrayna\s+mant[ıi]s[ıi]`)_
- `youtube` [https://youtube.com/watch?v=Je2zqzykFn0](https://youtube.com/watch?v=Je2zqzykFn0) — Turkish short: '#keşfet' (Turkish 'explore') is the only content word beside a generic #food; measures Turkish convention, not an English choice. _(pattern: `#ke[sş]fet`)_
- `youtube` [https://youtube.com/watch?v=PaM669jQ2vQ](https://youtube.com/watch?v=PaM669jQ2vQ) — Stalik Khankishiev's channel: bare-term title, whole description Russian (book promo, mvideo.ru links); no English text. _(pattern: `\bсталик\b`)_
- `youtube` [https://youtube.com/watch?v=QGzT-t3Llf8](https://youtube.com/watch?v=QGzT-t3Llf8) — Whole description is German: 'Slava Berlin! – ukrainische Spezialitäten (Solyanka & Vareniki)'. _(pattern: `\bukrainische\b`)_
- `youtube` [https://youtube.com/watch?v=QWyrlkS3iVI](https://youtube.com/watch?v=QWyrlkS3iVI) — Title is a German hashtag string: '#Ukraine#Essen#sehr#lecker#deutschland #Vareniki'; empty description. _(pattern: `#(?:essen|sehr|lecker|deutschland)\b`)_
- `youtube` [https://youtube.com/watch?v=TOzmE_1DXvg](https://youtube.com/watch?v=TOzmE_1DXvg) — Title 'vareniki s cartoshkoi' is romanised Russian ('vareniki s kartoshkoy') on a Russian-language channel; empty description. _(pattern: `vareniki\s+s\s+[ck]artoshkoi`)_
- `youtube` [https://youtube.com/watch?v=U0MeZPVaWV4](https://youtube.com/watch?v=U0MeZPVaWV4) — Azerbaijani title 'Kartof Pilmeni (Vareniki)' on the channel 'Gürcü_Mətbəxim'; only generic hashtags otherwise, empty description. _(pattern: `\bkartof\s+p[ie]lmeni\b`)_
- `youtube` [https://youtube.com/watch?v=efeuGI-k4P8](https://youtube.com/watch?v=efeuGI-k4P8) — Turkish throughout: 'Ukrayna Mantısı Vareniki Tarifi!' plus a full Turkish recipe description. _(pattern: `ukrayna\s+mant[ıi]s[ıi]`)_
- `youtube` [https://youtube.com/watch?v=fURAYGCpsgs](https://youtube.com/watch?v=fURAYGCpsgs) — Entirely Spanish: 'Los pierogi o vareniki con patatas son unos dumplings tradicionales muy populares en la cocina de Europa del Este'. _(pattern: `\bvareniki\s+con\b`)_
- `youtube` [https://youtube.com/watch?v=frlY5I6y9AA](https://youtube.com/watch?v=frlY5I6y9AA) — Entirely German: 'Wareniki Russische Teigtaschen mit Erdbeere aus Kaufland' / 'In diesem Video teste ich zum ersten Mal russische Teigtaschen, Vareniki'. _(pattern: `\bteigtaschen\b`)_
- `youtube` [https://youtube.com/watch?v=gdNCmp_2y84](https://youtube.com/watch?v=gdNCmp_2y84) — Bare-term title; the entire description is a Russian recipe ('Вареники по рецепту бабушки ... 500гр муки') tagged '#вареникирецепт'. _(pattern: `вареники\s+по\s+рецепту`)_
- `youtube` [https://youtube.com/watch?v=kD8U4jAZ8yY](https://youtube.com/watch?v=kD8U4jAZ8yY) — Uzbek: 'Vareniki kartoshkali' with '#retsept', '#shiriliklar', '#foydali_tavsiyalar', '#maruzalar'; empty description. _(pattern: `#retsept\b|kartoshkali`)_
- `youtube` [https://youtube.com/watch?v=lOu-BUNYeNA](https://youtube.com/watch?v=lOu-BUNYeNA) — Turkish hashtags carry the title: '#tarif' (recipe) and '#pratiktatlitarifleri' (practical dessert recipes); empty description. _(pattern: `#(?:tarif|pratik\w*tarif\w*)\b`)_
- `youtube` [https://youtube.com/watch?v=ocEWnW0y1Vc](https://youtube.com/watch?v=ocEWnW0y1Vc) — Entirely Italian (GialloZafferano): 'Conoscete i vareniki? Sono un piatto tipico della cucina dell'Europa orientale'. _(pattern: `\bvareniki\s+con\b`)_
- `youtube` [https://youtube.com/watch?v=sskWCGfzxX0](https://youtube.com/watch?v=sskWCGfzxX0) — Turkish/Azerbaijani title 'ŞİRİN VARENİKİ' ('sweet vareniki'), spelled with Turkish dotted capital İ; empty description. _(pattern: `\bşirin\s+varenik`)_
- `youtube` [https://youtube.com/watch?v=v4q9VeWWLDo](https://youtube.com/watch?v=v4q9VeWWLDo) — Entirely Spanish: 'Mi favorito platillo ruso: los varenikis' plus a Spanish recipe; measures Spanish convention. _(pattern: `\blos\s+vareniki`)_

**serhii-korolyov** (14)

- `youtube` [https://youtube.com/watch?v=Ac9S-GYWSkU](https://youtube.com/watch?v=Ac9S-GYWSkU) — Spanish (El Sol de Mexico): 'lo comparo al legendario ingeniero de cohetes sovietico Sergei Korolev.'
- `youtube` [https://youtube.com/watch?v=Az1E1dkl7GU](https://youtube.com/watch?v=Az1E1dkl7GU) — Indonesian title, empty description: 'penemuan satelit pertama yang di ciptakan oleh manusia -sergei korolev.'
- `youtube` [https://youtube.com/watch?v=BnBIDhDNXnI](https://youtube.com/watch?v=BnBIDhDNXnI) — Portuguese (UOL): 'o comparou ao lendario engenheiro de foguetes sovietico Sergei Korolev' — not English usage.
- `youtube` [https://youtube.com/watch?v=DfuhEDu2O-c](https://youtube.com/watch?v=DfuhEDu2O-c) — Portuguese (Sabia Nao): 'voce vai conhecer Sergei Korolev, o engenheiro que sobreviveu ao inferno do Gulag.'
- `youtube` [https://youtube.com/watch?v=Ecslw3cmUBk](https://youtube.com/watch?v=Ecslw3cmUBk) — Malay (Astro AWANI): 'menyamakannya dengan Sergei Korolev, tokoh utama di sebalik kejayaan awal angkasa Soviet.'
- `youtube` [https://youtube.com/watch?v=GQ3Xxyter6w](https://youtube.com/watch?v=GQ3Xxyter6w) — Indonesian (ORANG TERKENAL): 'Sergei Pavlovich Korolev adalah kepala teknisi roket dan perancang pesawat luar angkasa Soviet.'
- `youtube` [https://youtube.com/watch?v=Rw2F5g7FeVs](https://youtube.com/watch?v=Rw2F5g7FeVs) — Portuguese (O Liberal): 'o comparou ao lendario engenheiro de foguetes sovietico Sergei Korolev.'
- `youtube` [https://youtube.com/watch?v=XOhBsDdsF3c](https://youtube.com/watch?v=XOhBsDdsF3c) — Malay (Astro AWANI, third duplicate upload): 'menyamakannya dengan Sergei Korolev.'
- `youtube` [https://youtube.com/watch?v=bCT-pUSDbnI](https://youtube.com/watch?v=bCT-pUSDbnI) — Indonesian (RuangPikir): 'perancangan ilmuwan hebat Sergei Korolev.'
- `youtube` [https://youtube.com/watch?v=f89mt5fB6eE](https://youtube.com/watch?v=f89mt5fB6eE) — Malay (Astro AWANI, duplicate upload): 'menyamakannya dengan Sergei Korolev.'
- `youtube` [https://youtube.com/watch?v=rILhPz6Lxac](https://youtube.com/watch?v=rILhPz6Lxac) — Portuguese (Rede TVT): 'o comparou ao lendario engenheiro de foguetes sovietico Sergei Korolev.'
- `youtube` [https://youtube.com/watch?v=uujeIrc7kcM](https://youtube.com/watch?v=uujeIrc7kcM) — Indonesian motivational short: 'Yang mustahil menjadi mungkin ketika tidak ada jalan lain... Sergei Korolev.'
- `youtube` [https://youtube.com/watch?v=vpeWV2mg3Ok](https://youtube.com/watch?v=vpeWV2mg3Ok) — Portuguese (A Voz do Cerrado): 'o comparou a Sergei Korolev, lendario engenheiro sovietico.'
- `youtube` [https://youtube.com/watch?v=xnnpHd6qd8c](https://youtube.com/watch?v=xnnpHd6qd8c) — Portuguese (LIBERDADE NORDESTE): 'Putin compara Elon Musk a pioneiro espacial sovietico Sergei Korolev.'

**oleksandr-usyk** (11)

- `youtube` [https://youtube.com/watch?v=2_qexIasdDg](https://youtube.com/watch?v=2_qexIasdDg) — Swahili title and description ('Amepigana na bondia namba moja Alexander Usyk'). _(pattern: `\bbondia\b|\bamepigana\b`)_
- `youtube` [https://youtube.com/watch?v=DFjf1c4XoL8](https://youtube.com/watch?v=DFjf1c4XoL8) — Indonesian title and description ('Analisis dan Review Usyk vs Dubois | Pembalasan atau Pembuktian !! Tinju Dunia'). _(pattern: `\b(analisis|pembalasan|pembuktian|tinju dunia)\b`)_
- `youtube` [https://youtube.com/watch?v=EDDhZy4bSUQ](https://youtube.com/watch?v=EDDhZy4bSUQ) — Indonesian title and description ('Mengerikan !! Daniel Dubois Siap Menumbangkan Usyk | Prediksi Pertarungan'); Indonesian naming convention, not an English editorial choice. _(pattern: `\b(mengerikan|pertarungan|prediksi|menumbangkan)\b`)_
- `youtube` [https://youtube.com/watch?v=M4CaA1UNHtY](https://youtube.com/watch?v=M4CaA1UNHtY) — Swahili title ('ALEXANDER USYK NDIYE BONDIA NAMBA MOJA DUNIA MZIMA/REKODI ZAKE NI BALAA TUPU/HAJAPOTEZA PAMBANO'). _(pattern: `\bbondia\b|\bpambano\b|\bndiye\b`)_
- `youtube` [https://youtube.com/watch?v=OT1RRRAhJLE](https://youtube.com/watch?v=OT1RRRAhJLE) — German title ('Mahmoud charr beobachtet tyson fury und Alexander Usyk bei dem wiegen') and fully German description. _(pattern: `\bbeobachtet\b|\bbei dem wiegen\b`)_
- `youtube` [https://youtube.com/watch?v=chESCdg6TDU](https://youtube.com/watch?v=chESCdg6TDU) — Indonesian title ('KEKALAHAN TYSON FURY ATAS ALEXANDER USYK' = Tyson Fury's defeat to Usyk). _(pattern: `\bkekalahan\b`)_
- `youtube` [https://youtube.com/watch?v=guOzSBa4zZo](https://youtube.com/watch?v=guOzSBa4zZo) — Indonesian title ('Dua Jagoan Kelas Berat Bertemu ... Siapa Yang Unggul'), repeated verbatim as the description. _(pattern: `\bkelas berat\b|\bsiapa yang\b`)_
- `youtube` [https://youtube.com/watch?v=mdZc4uGn3QM](https://youtube.com/watch?v=mdZc4uGn3QM) — Cyrillic-named Russian-language channel ('БОКС BOXING') with no description; a single English title line from a Russophone channel is not an English-language source. _(pattern: `[Ѐ-ӿ]{3,}`)_
- `youtube` [https://youtube.com/watch?v=p34o69O_57M](https://youtube.com/watch?v=p34o69O_57M) — Uzbek title ('Bahodir Jalolov va Alexander Usyk boks', #mmauzbekistan); Uzbek convention, no English text and no description. _(pattern: `\bboks\b|\bmmauzbekistan\b`)_
- `youtube` [https://youtube.com/watch?v=t9GRUPrbCbY](https://youtube.com/watch?v=t9GRUPrbCbY) — Russophone channel: description is entirely Russian ('Поздравил Усика с Днём рождения!'); the one-line English title measures the Russian-language convention, which the project's English-only rule excludes. _(pattern: `[Ѐ-ӿ]{3,}`)_
- `youtube` [https://youtube.com/watch?v=xdHBu3n3a8c](https://youtube.com/watch?v=xdHBu3n3a8c) — Indonesian title and description ('Usyk Raja Tinju Tak Tertandingi !! Daniel Dubois Harus mengakui Kehebatan Usyk'). _(pattern: `\b(tinju|kehebatan|mengakui|tertandingi)\b`)_

**kazymyr-malevych** (6)

- `reddit` [https://reddit.com/r/DecorReps/comments/1mn9pvv](https://reddit.com/r/DecorReps/comments/1mn9pvv) — Vietnamese post ('5 Tac Pham Truu Tuong Huyen Thoai...'); the match sits in a Vietnamese table row. _(pattern: `t[aáạ]c ph[aẩ]m|hinh vuong den`)_
- `youtube` [https://youtube.com/watch?v=5zmiIphn1OM](https://youtube.com/watch?v=5zmiIphn1OM) — Spanish description ('*Kazimir Malevich*, artista abstracto') on a Spanish-language literary presentation. _(pattern: `artista abstracto`)_
- `youtube` [https://youtube.com/watch?v=BgrtOAfNbHk](https://youtube.com/watch?v=BgrtOAfNbHk) — Spanish: 'Kazimir MALEVICH pintor ruso' (music upload with Spanish description). _(pattern: `malevich pintor ruso`)_
- `youtube` [https://youtube.com/watch?v=H6-s8Zruvdk](https://youtube.com/watch?v=H6-s8Zruvdk) — Title and description are Italian ('il dipinto "Quadrato nero" di Kazimir Malevich rimase appeso capovolto'). _(pattern: `il dipinto|a causa della`)_
- `youtube` [https://youtube.com/watch?v=Kr6tvzZqyao](https://youtube.com/watch?v=Kr6tvzZqyao) — Portuguese title 'Vamos conhecer um pouco de Kazimir Malevich'; description is Portuguese with no English text. _(pattern: `vamos conhecer um pouco de`)_
- `youtube` [https://youtube.com/watch?v=jXfAcnnVwoU](https://youtube.com/watch?v=jXfAcnnVwoU) — Italian description ('Piet Mondrian, Kazimir Malevich e Bridget Riley hanno contribuito significativamente'). _(pattern: `hanno contribuito significativamente`)_

**ternopil** (4)

- `reddit` [https://reddit.com/r/tjournal_refugees/comments/1l425r5](https://reddit.com/r/tjournal_refugees/comments/1l425r5) — Entirely Russian-Cyrillic post; the sole "Ternopol" token is inside a dw.com URL slug (…-dronami-ternopol-ostalsa-bez-sveta/), never in running text. _(pattern: `[а-яё]{4,}`)_
- `youtube` [https://youtube.com/watch?v=T6bjZ2wkMu0](https://youtube.com/watch?v=T6bjZ2wkMu0) — Estonian/Finnish-form label "Ternopol ,Ukraina 19.11 2025" from channel juri ravoit — "Ukraina" marks it non-English, and it is a date stamp rather than running text. _(pattern: `\bukraina\b`)_
- `youtube` [https://youtube.com/watch?v=c7lGEvYpZOQ](https://youtube.com/watch?v=c7lGEvYpZOQ) — Italian-language title ("Attacco russo su Ternopol! Vittime tra i civili") on the Italian channel Agente del Kremlino — right referent, wrong language for an English-adoption holdout.
- `youtube` [https://youtube.com/watch?v=vilM2Zo8FhY](https://youtube.com/watch?v=vilM2Zo8FhY) — Ukrainian-language vlog channel (Сніжана VLOG); the title "Estetica Ternopol" is a two-word romanized salon/location tag, not English prose.

**feodosiia** (3)

- `youtube` [https://youtube.com/watch?v=5ue3C3-6p54](https://youtube.com/watch?v=5ue3C3-6p54) — Title and all 4 hits are Indonesian ('menghantam depo minyak strategis di Feodosiya', 'kota pelabuhan Feodosiya'); measures Indonesian, not English, convention. _(pattern: `\b(?:serangan|ukraina|krimea|jembatan|pelabuhan)\b`)_
- `youtube` [https://youtube.com/watch?v=_1d0LJMRb7Y](https://youtube.com/watch?v=_1d0LJMRb7Y) — Entirely Indonesian ('kilang minyak raksasa Feodosiya', 'Kilang Feodosiya Meledak'); same ET News channel, not English usage. _(pattern: `\b(?:serangan|ukraina|krimea|jembatan|pelabuhan)\b`)_
- `youtube` [https://youtube.com/watch?v=bLYdZ1MOIEI](https://youtube.com/watch?v=bLYdZ1MOIEI) — Azerbaijani throughout ('Feodosiya seherindeki neft bazasi dron hucumuna meruz qalib'); Azerbaijani convention, not English. _(pattern: `\b(?:partlayiş\w*|hücum\w*|vuruldu|neft\s+bazası|şəhərind\w*)\b`)_

**volodymyr-the-great** (2)

- `youtube` [https://youtube.com/watch?v=ki6NQRB87gc](https://youtube.com/watch?v=ki6NQRB87gc) — Somali-language video (title and body Somali); the two English sentences sit in an appended multilingual promo/hashtag block targeting #USA #UK #Canada, followed by ~100 lines of keyword stuffing. _(pattern: `taariikhda`)_
- `youtube` [https://youtube.com/watch?v=nnCGZ8KFXIg](https://youtube.com/watch?v=nnCGZ8KFXIg) — Indonesian-language video and description ('Raja paling brutal di Rus Kuno...'); the English phrase appears only inside the Indonesian SEO keyword blob, never in running text. _(pattern: `\bsejarah\b`)_

**bakhmut** (1)

- `youtube` [https://youtube.com/watch?v=yVYeSjMue2g](https://youtube.com/watch?v=yVYeSjMue2g) — Portuguese-language video; the only occurrence is in the Portuguese description "Imagens das batalhas por Artemovsk pelos olhos dos combatentes ucranianos" — measures Portuguese convention, not an English choice (title also carries Russian-Cyrillic hashtags).

**dnipro-river** (1)

- `youtube` [https://youtube.com/watch?v=qGKFTgktlEk](https://youtube.com/watch?v=qGKFTgktlEk) — Roman-Urdu title and description; 'Dnieper River' occurs only inside an Urdu SEO keyword line ('Volga Trade Route aur Dnieper River ka istemaal'), not English running text. _(pattern: `\b(?:aur|kya|hain|karte|jante|kaun|zaroor|nahi|ka istemaal|ki tijarat)\b`)_

### spam — 288 drops

**odesa** (69)

- `reddit` [https://reddit.com/r/DelawareR4R/comments/1hvilu8](https://reddit.com/r/DelawareR4R/comments/1hvilu8) — Personals: '28m for m in Odessa... In new castle county' - Odessa, Delaware. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/KCm4m/comments/1huwa4r](https://reddit.com/r/KCm4m/comments/1huwa4r) — Kansas City personals: 'between Odessa and Raytown... along I-70' - Odessa, Missouri. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hqxbvo](https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hqxbvo) — Crosspost of the same adult personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hqzf6t](https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hqzf6t) — Crosspost of the same adult personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hr0qtd](https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hr0qtd) — Crosspost of the same personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hr3ib8](https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hr3ib8) — Escort ad with phone number: 'im real in odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hreyo0](https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hreyo0) — 'Marriott Odessa' hookup ad - the Texas hotel. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hro7lr](https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hro7lr) — Crosspost of the bare 'Odessa' personals post; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hurs28](https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1hurs28) — 'Hosting in Odessa' crossdresser personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1huscki](https://reddit.com/r/MIDESSA432WTXSLUTZ/comments/1huscki) — Crosspost of the same 'M4F / Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/Midland432M4A/comments/1hrfbl0](https://reddit.com/r/Midland432M4A/comments/1hrfbl0) — Crosspost of the 'Marriott Odessa' hookup ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/Midland432M4A/comments/1ht48k4](https://reddit.com/r/Midland432M4A/comments/1ht48k4) — 'Hosting in Odessa' crossdresser personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/Midland432M4A/comments/1httv45](https://reddit.com/r/Midland432M4A/comments/1httv45) — Crosspost of the same explicit personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/Midland432M4A/comments/1hurscb](https://reddit.com/r/Midland432M4A/comments/1hurscb) — Crosspost of the same 'Hosting in Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hqwxoi](https://reddit.com/r/OdessaTexxx/comments/1hqwxoi) — r/OdessaTexxx hookup personals ('Can host in Odessa'); Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hqxbij](https://reddit.com/r/OdessaTexxx/comments/1hqxbij) — Adult personals: 'NYE bi oral bttm hosting in Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hqzdxs](https://reddit.com/r/OdessaTexxx/comments/1hqzdxs) — Adult personals: 'Need head east side Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hr0q9e](https://reddit.com/r/OdessaTexxx/comments/1hr0q9e) — Adult personals ('M4F', body just 'Odessa'); Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hrfbdm](https://reddit.com/r/OdessaTexxx/comments/1hrfbdm) — Crosspost of the 'Marriott Odessa' hookup ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hrive4](https://reddit.com/r/OdessaTexxx/comments/1hrive4) — Adult personals: 'in Odessa can't host'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hrncq1](https://reddit.com/r/OdessaTexxx/comments/1hrncq1) — Crosspost of the same 'M4F / Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hrncxm](https://reddit.com/r/OdessaTexxx/comments/1hrncxm) — Crosspost of the same 'M4F / Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hro7z2](https://reddit.com/r/OdessaTexxx/comments/1hro7z2) — Crosspost of the bare 'Odessa' personals post; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hs3hus](https://reddit.com/r/OdessaTexxx/comments/1hs3hus) — Crosspost of the same 'I host Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hs3zvr](https://reddit.com/r/OdessaTexxx/comments/1hs3zvr) — Adult personals: 'Odessa area lmk'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hs6mok](https://reddit.com/r/OdessaTexxx/comments/1hs6mok) — '28 f Odessa' personals post; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hsbd8s](https://reddit.com/r/OdessaTexxx/comments/1hsbd8s) — '30 in Odessa looking to use someone' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hsbf7b](https://reddit.com/r/OdessaTexxx/comments/1hsbf7b) — Repost of the same 'in Odessa can host' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1ht29wp](https://reddit.com/r/OdessaTexxx/comments/1ht29wp) — '30 m stuck in Odessa another night' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1ht4931](https://reddit.com/r/OdessaTexxx/comments/1ht4931) — Crosspost of the same 'Hosting in Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1ht57qr](https://reddit.com/r/OdessaTexxx/comments/1ht57qr) — Adult personals: 'I'm in Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1ht6psl](https://reddit.com/r/OdessaTexxx/comments/1ht6psl) — 'M4a' personals post, body 'Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1httti4](https://reddit.com/r/OdessaTexxx/comments/1httti4) — Explicit personals ad tagged 'Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hu3iwl](https://reddit.com/r/OdessaTexxx/comments/1hu3iwl) — 'I am a HOSTING BTTM in odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1huimug](https://reddit.com/r/OdessaTexxx/comments/1huimug) — 'What's everyone up to around odessa?' in a Texas hookup sub. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1huq05r](https://reddit.com/r/OdessaTexxx/comments/1huq05r) — 'Just got to Odessa tonight' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hursgp](https://reddit.com/r/OdessaTexxx/comments/1hursgp) — Crosspost of the same 'Hosting in Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hurt65](https://reddit.com/r/OdessaTexxx/comments/1hurt65) — 'Looking for BWC NOW ODESSA' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1husc2f](https://reddit.com/r/OdessaTexxx/comments/1husc2f) — 'M4F' personals post, body 'Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hvauax](https://reddit.com/r/OdessaTexxx/comments/1hvauax) — 'In Odessa' personals post; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/OdessaTexxx/comments/1hvv1gg](https://reddit.com/r/OdessaTexxx/comments/1hvv1gg) — 'Hung bull here can host odessa area'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/PermianPlayTime3/comments/1hr0qon](https://reddit.com/r/PermianPlayTime3/comments/1hr0qon) — Crosspost of the same personals ad to a Permian Basin sub; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/PermianPlayTime3/comments/1hrnd5l](https://reddit.com/r/PermianPlayTime3/comments/1hrnd5l) — Crosspost of the same 'M4F / Odessa' ad to a Permian Basin sub. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/PermianPlayTime3/comments/1husccp](https://reddit.com/r/PermianPlayTime3/comments/1husccp) — Crosspost of the same 'M4F / Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/PermianPlayTime3/comments/1huscdw](https://reddit.com/r/PermianPlayTime3/comments/1huscdw) — Duplicate crosspost of the same 'M4F / Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/TexasswingersOfficial/comments/1htvwv0](https://reddit.com/r/TexasswingersOfficial/comments/1htvwv0) — 'Looking for a couple in Odessa' swinger ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/WTX_PnP/comments/1httvgy](https://reddit.com/r/WTX_PnP/comments/1httvgy) — Crosspost of the same explicit personals ad to a West Texas PnP sub. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/midlandTXXX/comments/1hrfbvl](https://reddit.com/r/midlandTXXX/comments/1hrfbvl) — Crosspost of the 'Marriott Odessa' hookup ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/midlandstraight4gaybi/comments/1htrzcz](https://reddit.com/r/midlandstraight4gaybi/comments/1htrzcz) — Adult personals: 'Need head North Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/trapsgonewild/comments/1hrtbo9](https://reddit.com/r/trapsgonewild/comments/1hrtbo9) — Crosspost of the 'M4T Odessa' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hqwjak](https://reddit.com/r/westtexasgonewild/comments/1hqwjak) — West Texas hookup personals; 'I'm in Odessa and can host' is the Permian Basin city. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hr0qfj](https://reddit.com/r/westtexasgonewild/comments/1hr0qfj) — Crosspost of the same 'M4F / Odessa' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hr71sf](https://reddit.com/r/westtexasgonewild/comments/1hr71sf) — Personals post titled 'Odessa', body 'Seeking'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hrlnlm](https://reddit.com/r/westtexasgonewild/comments/1hrlnlm) — 'Bored in Odessa' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hrnclu](https://reddit.com/r/westtexasgonewild/comments/1hrnclu) — 'M4F / Odessa' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hro21f](https://reddit.com/r/westtexasgonewild/comments/1hro21f) — Bare 'Odessa' personals post; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hrtamc](https://reddit.com/r/westtexasgonewild/comments/1hrtamc) — 'M4T Odessa' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hs3h3p](https://reddit.com/r/westtexasgonewild/comments/1hs3h3p) — Adult personals: 'I host Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hs40dz](https://reddit.com/r/westtexasgonewild/comments/1hs40dz) — Adult personals: 'Hung bull can host odessa area'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hta7hs](https://reddit.com/r/westtexasgonewild/comments/1hta7hs) — Adult personals: 'preferably odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1httug6](https://reddit.com/r/westtexasgonewild/comments/1httug6) — Crosspost of the same explicit personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hunfpo](https://reddit.com/r/westtexasgonewild/comments/1hunfpo) — 'Couple in Odessa looking for...' personals ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hursme](https://reddit.com/r/westtexasgonewild/comments/1hursme) — Crosspost of the same 'Hosting in Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1husc6k](https://reddit.com/r/westtexasgonewild/comments/1husc6k) — Crosspost of the same 'M4F / Odessa' ad; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1huu5xz](https://reddit.com/r/westtexasgonewild/comments/1huu5xz) — 'M Odessa' personals post; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hvbvna](https://reddit.com/r/westtexasgonewild/comments/1hvbvna) — 'Top in Odessa' personals post; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hvl1ms](https://reddit.com/r/westtexasgonewild/comments/1hvl1ms) — 'sneaky link fwm from Odessa or Pecos area' - Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `reddit` [https://reddit.com/r/westtexasgonewild/comments/1hvwfmf](https://reddit.com/r/westtexasgonewild/comments/1hvwfmf) — Personals ad, body 'West Odessa'; Odessa, Texas. _(pattern: `^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b`)_
- `youtube` [https://youtube.com/watch?v=bJ2JAXJCS-A](https://youtube.com/watch?v=bJ2JAXJCS-A) — Bihar clay-cup-machine Short stuffing unrelated geo hashtags (#kolkata #odessa #nepal).

**kyiv** (9)

- `news` [https://www.irishdentist.ie/talks-between-the-usa-and-russia-in-abu-dhabi-missiles-rain-on-kiev-during-the-night/13108/](https://www.irishdentist.ie/talks-between-the-usa-and-russia-in-abu-dhabi-missiles-rain-on-kiev-during-the-night/13108/) — Same hijacked domain; machine-translated Italian copy ("as reported by ReutersAlready since yesterday"). _(pattern: `irishdentist\.ie`)_
- `news` [https://www.irishdentist.ie/the-ultimate-test-of-attrition-and-survival/12606/](https://www.irishdentist.ie/the-ultimate-test-of-attrition-and-survival/12606/) — Same hijacked domain; "who among Fly And Kiev" is Italian "Mosca" (Moscow/fly) mistranslated - raw MT, not English editorial usage. _(pattern: `irishdentist\.ie`)_
- `news` [https://www.irishdentist.ie/washington-and-kiev-looking-for-a-solution/13676/](https://www.irishdentist.ie/washington-and-kiev-looking-for-a-solution/13676/) — irishdentist.ie is a hijacked dentistry domain running machine-translated Italian wire copy ("he said Zelenskyand then add"); spelling reflects the Italian source, not an English choice. _(pattern: `irishdentist\.ie`)_
- `reddit` [https://reddit.com/r/AskReddit/comments/1hu0vmx](https://reddit.com/r/AskReddit/comments/1hu0vmx) — Karma-farming post by a recruitment-agency owner advertising vacancies; posted twice within minutes. _(pattern: `recruitment\s+agency\s+esca|raise\s+karma`)_
- `reddit` [https://reddit.com/r/AskReddit/comments/1hu0xe4](https://reddit.com/r/AskReddit/comments/1hu0xe4) — Duplicate karma-farming post from the same recruiter. _(pattern: `recruitment\s+agency\s+esca|raise\s+karma`)_
- `reddit` [https://reddit.com/r/BirdFlu_SOL/comments/1hvmk0i](https://reddit.com/r/BirdFlu_SOL/comments/1hvmk0i) — $BIRDFLU memecoin promo copy ("Born in Dr. Fauci's secret lab in Kiev").
- `reddit` [https://reddit.com/r/LoveHUB/comments/1hvgarw](https://reddit.com/r/LoveHUB/comments/1hvgarw) — Bot-generated escort-ad city tally table ("|Kiev|1|"), not running text. _(pattern: `new escort ads|last week'?s new profiles`)_
- `reddit` [https://reddit.com/r/MassageRepublic_com_/comments/1hvp6yw](https://reddit.com/r/MassageRepublic_com_/comments/1hvp6yw) — Bot-generated escort-directory city tally table ("|Kiev|2|").
- `youtube` [https://youtube.com/watch?v=cnvnOTNXImY](https://youtube.com/watch?v=cnvnOTNXImY) — Keyword-stuffed hashtag-only title mixing war and Call of Duty tags; empty description, no running text.

**lviv** (15)

- `reddit` [https://reddit.com/r/TWStories/comments/1jtbm31](https://reddit.com/r/TWStories/comments/1jtbm31) — Title is the bare token 'lvov'; body is 'What is the most annoying word/phrase that people use?' - no city referent at all.
- `youtube` [https://youtube.com/watch?v=2_K8pLEIwp8](https://youtube.com/watch?v=2_K8pLEIwp8) — '#lvov #shortclips Mistri Labour' - hashtag noise, no city content. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=3nIpA9gNiSI](https://youtube.com/watch?v=3nIpA9gNiSI) — Hashtag salad '#SagarPark#lvov #vikram #mohammad #viral'; no city referent. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=6awABB3vH0Y](https://youtube.com/watch?v=6awABB3vH0Y) — Title is the bare token 'lvov' with empty description and tags on an Indian shorts channel; part of the #lvov='love' typo cluster, zero city evidence.
- `youtube` [https://youtube.com/watch?v=P9rW0-Wo8qI](https://youtube.com/watch?v=P9rW0-Wo8qI) — Bhojpuri dance short with a trailing '#lvov' hashtag. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=ZZLhZHzJNIY](https://youtube.com/watch?v=ZZLhZHzJNIY) — '#lvov #bhojpuri #song' - hashtag-only Bhojpuri short. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=cY3h3WLj7ag](https://youtube.com/watch?v=cY3h3WLj7ag) — '(heart)#lvov #vdo #assamesesong' - Assamese song short, hashtag noise. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=fs-uUsXNcU0](https://youtube.com/watch?v=fs-uUsXNcU0) — '#shortvideos #hinduism #like #pyar #lvov' - '#pyar' (Hindi for 'love') sitting next to '#lvov' confirms the love-typo cluster. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=ipiXbPOpyCQ](https://youtube.com/watch?v=ipiXbPOpyCQ) — '#lvov #bhojpuri #song' - hashtag-only short. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=it4N8CXPSJw](https://youtube.com/watch?v=it4N8CXPSJw) — 'I lvov(heart)e Muhammad' - 'lvov' is a mangled spelling of 'love' in a Roman-Urdu Islamic short; no city referent in title, description or tags. _(pattern: `\bi\s+lvov\W{0,3}e\b`)_
- `youtube` [https://youtube.com/watch?v=ljafbHyBfZ8](https://youtube.com/watch?v=ljafbHyBfZ8) — '#lvoe #lvov' side by side on a Bhojpuri shorts channel - self-evidently a misspelling of 'love'. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=ofHcwwJR4cU](https://youtube.com/watch?v=ofHcwwJR4cU) — Romanised-Hindi sad-status short ending in '#lvov'; no city referent. _(pattern: `#lvov\b`)_
- `youtube` [https://youtube.com/watch?v=uireoznW8Kk](https://youtube.com/watch?v=uireoznW8Kk) — Emoji-only title with 'LVOV' between heart emoji - the 'love' typo again; no description, no city.
- `youtube` [https://youtube.com/watch?v=unwptTK36TI](https://youtube.com/watch?v=unwptTK36TI) — Emoji-only title with 'LVOV' among hearts; same love-typo cluster as the other Raj Kumar upload.
- `youtube` [https://youtube.com/watch?v=zSOO2fMxYjU](https://youtube.com/watch?v=zSOO2fMxYjU) — Title is bare hashtags '#lvov #youtubeshorts #bhojapurihitsong #maghiyasong' on a Bhojpuri shorts channel; no city content. _(pattern: `#lvov\b`)_

**mykola-hohol** (2)

- `reddit` [https://reddit.com/r/u_Budget_Shop1719/comments/1jd2hdj](https://reddit.com/r/u_Budget_Shop1719/comments/1jd2hdj) — Bare promo link 'The Mantle By Nikolai Gogol - Quizlit' on a shop account's own profile page; no body text. _(pattern: `quizlit`)_
- `youtube` [https://youtube.com/watch?v=2sH1Rd3whsk](https://youtube.com/watch?v=2sH1Rd3whsk) — Islamic recitation Short with '#Nikolai Gogol' keyword-stuffed into unrelated hashtags. _(pattern: `islamic video|quranrecitation`)_

**borscht** (42)

- `reddit` [https://reddit.com/r/FreeKarma4You/comments/1jpus8i](https://reddit.com/r/FreeKarma4You/comments/1jpus8i) — Title and body are the bare token "borsch" in a karma-farming subreddit — filler, not usage. _(pattern: `freekarma`)_
- `reddit` [https://reddit.com/r/LeahHasIt/comments/1l957r1](https://reddit.com/r/LeahHasIt/comments/1l957r1) — NSFW promo tag soup: "Cara Delevingne fambase sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/LeahHasIt/comments/1l95hym](https://reddit.com/r/LeahHasIt/comments/1l95hym) — NSFW promo tag soup: "London Reigns borsch amateurvideos". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/LeahHasIt/comments/1lbng30](https://reddit.com/r/LeahHasIt/comments/1lbng30) — NSFW promo tag soup: "Skinny Teen Fuck sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/LeahHasIt/comments/1lbprap](https://reddit.com/r/LeahHasIt/comments/1lbprap) — NSFW promo tag soup: "Gigantic Tits Jodhpurs borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Leah_leah/comments/1l4isyi](https://reddit.com/r/Leah_leah/comments/1l4isyi) — NSFW promo tag soup: "jiggaboo sexy borsch Nadja Stone". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Slutty_TGirls/comments/1l8d2zj](https://reddit.com/r/Slutty_TGirls/comments/1l8d2zj) — NSFW promo tag soup: "Jacob Harris borsch lovely asian moscow girl". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Slutty_TGirls/comments/1l8deii](https://reddit.com/r/Slutty_TGirls/comments/1l8deii) — NSFW promo tag soup: "Ebony Ass Pussy sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_corner/comments/1l11c3l](https://reddit.com/r/Tgirls_corner/comments/1l11c3l) — NSFW promo tag soup, same stuffed template as the r/alexandra_nova posts. _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_corner/comments/1l11z2o](https://reddit.com/r/Tgirls_corner/comments/1l11z2o) — NSFW promo tag soup, same stuffed template ("victoria paris borsch"). _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1l317ch](https://reddit.com/r/Tgirls_hottest/comments/1l317ch) — NSFW promo tag soup: "Wet Pussy Panties sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1l6rd8z](https://reddit.com/r/Tgirls_hottest/comments/1l6rd8z) — NSFW promo tag soup: "multiple camera angles sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1l6t8dx](https://reddit.com/r/Tgirls_hottest/comments/1l6t8dx) — NSFW promo tag soup: "Vintage Classic Full Movie borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lapfyf](https://reddit.com/r/Tgirls_hottest/comments/1lapfyf) — NSFW promo tag soup: "elisa sanches anal sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lf877e](https://reddit.com/r/Tgirls_hottest/comments/1lf877e) — NSFW promo tag soup: "lesbians making love sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lf8bd0](https://reddit.com/r/Tgirls_hottest/comments/1lf8bd0) — NSFW promo tag soup: "Ukraine teen webcam borsch Viet Nam". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lgd5g3](https://reddit.com/r/Tgirls_hottest/comments/1lgd5g3) — NSFW promo tag soup: "bigtitsinbikinis sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lgdnmt](https://reddit.com/r/Tgirls_hottest/comments/1lgdnmt) — NSFW promo tag soup: "pussy whipping ashlye borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lh57kx](https://reddit.com/r/Tgirls_hottest/comments/1lh57kx) — NSFW promo tag soup: "Amateur Mature Nude borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lh5aeh](https://reddit.com/r/Tgirls_hottest/comments/1lh5aeh) — NSFW promo tag soup: "Natalia Fadeev cantik sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lh5sux](https://reddit.com/r/Tgirls_hottest/comments/1lh5sux) — NSFW promo tag soup: "Megan Medellin rebi sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lh67uk](https://reddit.com/r/Tgirls_hottest/comments/1lh67uk) — NSFW promo tag soup: "Neve Campbell borsch Debbie Boyde". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lh6pdx](https://reddit.com/r/Tgirls_hottest/comments/1lh6pdx) — NSFW promo tag soup: "ashley tervot onlyfans ynk sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/TransTemptress/comments/1l7jqy6](https://reddit.com/r/TransTemptress/comments/1l7jqy6) — NSFW promo tag soup, identical stuffed string to the r/Tgirls_hottest posts. _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/TransTemptress/comments/1l8d2e6](https://reddit.com/r/TransTemptress/comments/1l8d2e6) — NSFW promo tag soup, identical stuffed string ("Vintage Classic Full Movie borsch"). _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/alexandra_nova/comments/1kzlgqr](https://reddit.com/r/alexandra_nova/comments/1kzlgqr) — NSFW promo tag soup: "nudelily sexy borsch Fake Taxie" — keyword stuffing, no referent. _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/alexandra_nova/comments/1kznbdd](https://reddit.com/r/alexandra_nova/comments/1kznbdd) — NSFW promo tag soup: "victoria paris borsch Double Dildo" — keyword stuffing. _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/itsemmma/comments/1kt17kb](https://reddit.com/r/itsemmma/comments/1kt17kb) — OnlyFans promo with a keyword-stuffed tag soup; "borsch" sits between "caiu no whatsapp" and "moistcassandra" — no referent at all. _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/itsemmma/comments/1kt1f3j](https://reddit.com/r/itsemmma/comments/1kt1f3j) — Same OnlyFans tag-soup template: "clickoncontent sexy borsch emily rinuado". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lal7jt](https://reddit.com/r/lexiiixxx/comments/1lal7jt) — NSFW promo tag soup: "Old Man Orgy sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lapub6](https://reddit.com/r/lexiiixxx/comments/1lapub6) — NSFW promo tag soup: "Xbox Controller hot ass fuck borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lbxtes](https://reddit.com/r/lexiiixxx/comments/1lbxtes) — NSFW promo tag soup: "hot lady addisonivy sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lceic6](https://reddit.com/r/lexiiixxx/comments/1lceic6) — NSFW promo tag soup: "Vitiligo Teens Sexis borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1le2q6u](https://reddit.com/r/lexiiixxx/comments/1le2q6u) — NSFW promo tag soup: "one night stand anal sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1le36ny](https://reddit.com/r/lexiiixxx/comments/1le36ny) — NSFW promo tag soup: "group masturbation borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1levhlp](https://reddit.com/r/lexiiixxx/comments/1levhlp) — NSFW promo tag soup: "machofucker sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lfkv51](https://reddit.com/r/lexiiixxx/comments/1lfkv51) — NSFW promo tag soup, same stuffed string as 1lal7jt. _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lfoqk3](https://reddit.com/r/lexiiixxx/comments/1lfoqk3) — NSFW promo tag soup, same stuffed string as 1lapub6. _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lghkw5](https://reddit.com/r/lexiiixxx/comments/1lghkw5) — NSFW promo tag soup: "Girls Way sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lgirp5](https://reddit.com/r/lexiiixxx/comments/1lgirp5) — NSFW promo tag soup: "devilsvideos com borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lh95lj](https://reddit.com/r/lexiiixxx/comments/1lh95lj) — NSFW promo tag soup: "no name silvervale borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lh9hld](https://reddit.com/r/lexiiixxx/comments/1lh9hld) — NSFW promo tag soup: "padlizsan sexy borsch". _(pattern: `\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b`)_

**donbas** (5)

- `reddit` [https://reddit.com/r/AlienAbduction/comments/1ilrsvz](https://reddit.com/r/AlienAbduction/comments/1ilrsvz) — r/AlienAbduction: same copy-pasted 'energy healer'/abductee solicitation, crossposted.
- `reddit` [https://reddit.com/r/StrangeEarth/comments/1iln38i](https://reddit.com/r/StrangeEarth/comments/1iln38i) — r/StrangeEarth: same copy-pasted 'energy healer'/reptilian solicitation, crossposted.
- `reddit` [https://reddit.com/r/cosmicexperiences/comments/1ilrocg](https://reddit.com/r/cosmicexperiences/comments/1ilrocg) — r/cosmicexperiences: same copy-pasted 'energy healer'/abductee solicitation, crossposted.
- `reddit` [https://reddit.com/r/reptilians/comments/1illyk6](https://reddit.com/r/reptilians/comments/1illyk6) — r/reptilians: copy-pasted 'energy healer'/David-Icke solicitation cross-spammed to five unrelated subs; one boilerplate Donbass clause. _(pattern: `\breptilians?\b|\benerg(?:y|etic)\s+healer\b`)_
- `reddit` [https://reddit.com/r/reptilians2/comments/1infab3](https://reddit.com/r/reptilians2/comments/1infab3) — r/reptilians2: same copy-pasted 'energy healer'/reptilian solicitation, crossposted.

**luhansk** (18)

- `reddit` [https://reddit.com/r/LeahHasIt/comments/1l5tibs](https://reddit.com/r/LeahHasIt/comments/1l5tibs) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/LeahHasIt/comments/1lbpf6u](https://reddit.com/r/LeahHasIt/comments/1lbpf6u) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/Leah_leah/comments/1l4ic2m](https://reddit.com/r/Leah_leah/comments/1l4ic2m) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/Slutty_TGirls/comments/1l8ejue](https://reddit.com/r/Slutty_TGirls/comments/1l8ejue) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1l0ziog](https://reddit.com/r/Tgirls_hottest/comments/1l0ziog) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1l1r1nt](https://reddit.com/r/Tgirls_hottest/comments/1l1r1nt) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1l3itpr](https://reddit.com/r/Tgirls_hottest/comments/1l3itpr) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1l6q6zu](https://reddit.com/r/Tgirls_hottest/comments/1l6q6zu) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/Tgirls_hottest/comments/1lf76yr](https://reddit.com/r/Tgirls_hottest/comments/1lf76yr) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/TransTemptress/comments/1l7io8k](https://reddit.com/r/TransTemptress/comments/1l7io8k) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/itsemmma/comments/1kt1gx7](https://reddit.com/r/itsemmma/comments/1kt1gx7) — OnlyFans promo post; 'lugansk' buried in a 38k-char porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/itsemmma/comments/1kuscht](https://reddit.com/r/itsemmma/comments/1kuscht) — OnlyFans promo post; 'Lugansk' inside a random-word/keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1laumfq](https://reddit.com/r/lexiiixxx/comments/1laumfq) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1lbz9jq](https://reddit.com/r/lexiiixxx/comments/1lbz9jq) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1le15kh](https://reddit.com/r/lexiiixxx/comments/1le15kh) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `reddit` [https://reddit.com/r/lexiiixxx/comments/1ley4kr](https://reddit.com/r/lexiiixxx/comments/1ley4kr) — OnlyFans promo post; 'lugansk' inside a porn-SEO keyword salad _(pattern: `onlyfans\.com/`)_
- `youtube` [https://youtube.com/watch?v=60gvNd2G8bg](https://youtube.com/watch?v=60gvNd2G8bg) — MBBS-abroad consultancy short; 'LUGANSK STATE MEDICAL UNIVERSITY' sits in a keyword-stuffed university list with WhatsApp lead-gen numbers _(pattern: `mbbs\s+(?:in\s+russia|abroad)`)_
- `youtube` [https://youtube.com/watch?v=elH1MKAl5HQ](https://youtube.com/watch?v=elH1MKAl5HQ) — Same MBBS-abroad consultancy template; 'LUGANSK STATE MEDICAL UNIVERSITY' in a keyword-stuffed list _(pattern: `mbbs\s+(?:in\s+russia|abroad)`)_

**chornobyl** (2)

- `news` [https://www.chilecomparte.cl/foros/topic/5145833-chernobyl-a-bomb-that-keeps-ticking-2024-1080p720p-hevc-x265-megusta/](https://www.chilecomparte.cl/foros/topic/5145833-chernobyl-a-bomb-that-keeps-ticking-2024-1080p720p-hevc-x265-megusta/) — Piracy release post (rapidgator/clicknupload links, duplicated blurb) on a warez forum, not journalism. _(pattern: `chilecomparte\.cl|\[?(?:1080p|720p)\]?.*\bx265\b`)_
- `youtube` [https://youtube.com/watch?v=tU1Q7Y1bh_g](https://youtube.com/watch?v=tU1Q7Y1bh_g) — Video is a generic 'Chemical Leak' clip; #chernobyl is unrelated hashtag farming.

**dynamo-kyiv** (8)

- `reddit` [https://reddit.com/r/ClassicFootballShirts/comments/1nx641j](https://reddit.com/r/ClassicFootballShirts/comments/1nx641j) — Same commercial matchworn-shirt advert as 1nw2cyl, cross-posted to r/ClassicFootballShirts. _(pattern: `anyone looking for matchworn shirts`)_
- `reddit` [https://reddit.com/r/KitSwap/comments/1nx64f8](https://reddit.com/r/KitSwap/comments/1nx64f8) — Same commercial matchworn-shirt advert as 1nw2cyl, cross-posted to r/KitSwap. _(pattern: `anyone looking for matchworn shirts`)_
- `reddit` [https://reddit.com/r/SoccerJerseys/comments/1nw2cyl](https://reddit.com/r/SoccerJerseys/comments/1nw2cyl) — Commercial matchworn-shirt advert cross-posted to four subreddits (1nw2cyl, 1nx63a0, 1nx641j, 1nx64f8) by the same seller; the phrase appears only inside a German-language club/country inventory ('Bosnien-Herzegowina', 'Deutschland', 'FC Bayern München'), not in discourse. _(pattern: `anyone looking for matchworn shirts`)_
- `reddit` [https://reddit.com/r/WagerTalk/comments/1mbdqq2](https://reddit.com/r/WagerTalk/comments/1mbdqq2) — Automated odds-feed post ('Free Sports Pick loaded from Kevin Dolan: (224377) Hamrun Spartans at (224378) Dynamo Kiev'); bot output with rotation numbers, not discourse. _(pattern: `free sports pick loaded from`)_
- `reddit` [https://reddit.com/r/WatchNext4k/comments/1i639yq](https://reddit.com/r/WatchNext4k/comments/1i639yq) — Dutch-language promo for the illegal stream site next4k.com; piracy advertising rather than discourse, and non-English. _(pattern: `next4k`)_
- `reddit` [https://reddit.com/r/soccer/comments/1nx63a0](https://reddit.com/r/soccer/comments/1nx63a0) — Same commercial matchworn-shirt advert as 1nw2cyl, cross-posted to r/soccer; German inventory list, not discourse. _(pattern: `anyone looking for matchworn shirts`)_
- `youtube` [https://youtube.com/watch?v=8Uv2xogchhE](https://youtube.com/watch?v=8Uv2xogchhE) — Keyword-stuffed BGMI mobile-gaming short; the tag list contains 'dynamo kiev clutch in bgmi', a nonsense conflation with the streamer Dynamo Gaming. Not the football club. _(pattern: `\b(bgmi|dynamo gaming|dynamogaming|1v[s]?4 clutch|m416)\b`)_
- `youtube` [https://youtube.com/watch?v=tj0naYLXzN0](https://youtube.com/watch?v=tj0naYLXzN0) — BGMI mobile-gaming clutch short; matched only via keyword-stuffed gaming tags, not the football club. _(pattern: `\b(bgmi|1v[s]?4 clutch|spraygod|m416clutch)\b`)_

**chicken-kyiv** (28)

- `reddit` [https://reddit.com/r/AmateurFoodPorn/comments/1k54fhm](https://reddit.com/r/AmateurFoodPorn/comments/1k54fhm) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/CookeryHQ/comments/1k54rvy](https://reddit.com/r/CookeryHQ/comments/1k54rvy) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/DinnerIdeas/comments/1k58jjc](https://reddit.com/r/DinnerIdeas/comments/1k58jjc) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/EasyPeasyRecipes/comments/1k59iod](https://reddit.com/r/EasyPeasyRecipes/comments/1k59iod) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/EasyRecipesForNoobies/comments/1k5aczy](https://reddit.com/r/EasyRecipesForNoobies/comments/1k5aczy) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/FoodvideosNORULES/comments/1k5c1f9](https://reddit.com/r/FoodvideosNORULES/comments/1k5c1f9) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/HellYeahIdEatThat/comments/1k5cjiv](https://reddit.com/r/HellYeahIdEatThat/comments/1k5cjiv) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/MealPics/comments/1k5dbk3](https://reddit.com/r/MealPics/comments/1k5dbk3) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/RateMyPlate/comments/1k5e17j](https://reddit.com/r/RateMyPlate/comments/1k5e17j) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/RedditInTheKitchen/comments/1k5e1rg](https://reddit.com/r/RedditInTheKitchen/comments/1k5e1rg) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/TheHighChef/comments/1k4g2y9](https://reddit.com/r/TheHighChef/comments/1k4g2y9) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/TodayIAte/comments/1k5eram](https://reddit.com/r/TodayIAte/comments/1k5eram) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/cookingtonight/comments/1k55ir1](https://reddit.com/r/cookingtonight/comments/1k55ir1) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/covidcookery/comments/1k566wv](https://reddit.com/r/covidcookery/comments/1k566wv) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/dailygodfood/comments/1k567kj](https://reddit.com/r/dailygodfood/comments/1k567kj) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/decentfoodporn/comments/1k56x1u](https://reddit.com/r/decentfoodporn/comments/1k56x1u) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/dinner/comments/1k57ov7](https://reddit.com/r/dinner/comments/1k57ov7) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/food/comments/1k5bawz](https://reddit.com/r/food/comments/1k5bawz) — Mass crosspost: u/XRPcook pushed one identical body to 26 subreddits (46 posts) on 2025-04-21/22; kept once at r/stonerfood (1k4ee3p).
- `reddit` [https://reddit.com/r/stonerchefs/comments/1k4f5r7](https://reddit.com/r/stonerchefs/comments/1k4f5r7) — Mass crosspost: u/XRPcook pushed one identical body ('I needed something to go with the potato terrine I made so I went with Chicken Kiev') to 26 subreddits (46 posts) on 2025-04-21/22. Kept once at r/stonerfood (1k4ee3p); every other copy is the same utterance counted again.
- `reddit` [https://reddit.com/r/tonightsdinner/comments/1k5fpb1](https://reddit.com/r/tonightsdinner/comments/1k5fpb1) — Mass crosspost by u/XRPcook (same 2025-04-22 blast, r/tonightsdinner copy); also recorded as removed, so the link is dead.
- `reddit` [https://reddit.com/r/u_USFan2008/comments/1kp5e8j](https://reddit.com/r/u_USFan2008/comments/1kp5e8j) — u/USFan2008 profile post: a fabricated roster of invented US presidents and their dinners ('Josephine Arabella Caldwell (1897-1901) - Chicken Kiev with baby carrots'), markdown-formatted and reading as machine-generated filler rather than attested human prose.
- `youtube` [https://youtube.com/watch?v=2Z5iSZRThSo](https://youtube.com/watch?v=2Z5iSZRThSo) — Second Milena ASMR AI-generated 'flag ball' short; 'Ukraine (Chicken Kiev)' appears only in the auto-generated dish timestamp list. _(pattern: `\bai[\s-]*(?:magic|generated)\b`)_
- `youtube` [https://youtube.com/watch?v=5SXEkcb9rl8](https://youtube.com/watch?v=5SXEkcb9rl8) — Description is a keyword-stuffing blob ('chicken karahi videos chicken kiev videos chicken kulambu video'); the video is an unrelated 'chicken lover vlog' with no dish reference. _(pattern: `chicken\s+kiev\s+videos?\s+chicken\s+\w+\s+video`)_
- `youtube` [https://youtube.com/watch?v=5j5-lNFWtf8](https://youtube.com/watch?v=5j5-lNFWtf8) — Altech Baku 'Chef style #120': same keyword-stuffed dish list as #118/#119, SEO padding rather than a reference to the video. _(pattern: `arepas,\s*chicken kiev,\s*doner kebab`)_
- `youtube` [https://youtube.com/watch?v=7JMQrvOsIeg](https://youtube.com/watch?v=7JMQrvOsIeg) — Milena ASMR AI-generated 'flag dome' short ('Watch satisfying AI magic'); 'Ukraine (Chicken Kiev)' is one line of an auto-generated country/dish timestamp list, not human prose. _(pattern: `\bai[\s-]*(?:magic|generated)\b`)_
- `youtube` [https://youtube.com/watch?v=l2vXKcSdDjo](https://youtube.com/watch?v=l2vXKcSdDjo) — Altech Baku 'Chef style #118': same keyword-stuffed dish list, SEO padding. _(pattern: `arepas,\s*chicken kiev,\s*doner kebab`)_
- `youtube` [https://youtube.com/watch?v=ug0pn4Bkg-s](https://youtube.com/watch?v=ug0pn4Bkg-s) — Soggy Pixel AI cat short, description tagged #AIGenerated - machine-generated content, not attested human usage. _(pattern: `#ai(?:generated|cat)\b`)_
- `youtube` [https://youtube.com/watch?v=wJWkqhdWZSk](https://youtube.com/watch?v=wJWkqhdWZSk) — Altech Baku 'Chef style #119': description is a keyword-stuffed list of 60+ unrelated dishes ('...arepas, chicken kiev, doner kebab, gnocchi...'), identical across the channel's uploads - SEO padding, not a reference to the video's content. _(pattern: `arepas,\s*chicken kiev,\s*doner kebab`)_

**kharkiv** (5)

- `news` [https://www.irishdentist.ie/russia-ukraine-all-the-obstacles-that-brake-peace-after-the-washington-summit/10281/](https://www.irishdentist.ie/russia-ukraine-all-the-obstacles-that-brake-peace-after-the-washington-summit/10281/) — Machine-translated Italian geopolitics copy dumped on an Irish dentistry site ('Crimea, born' = mistranslated 'NATO', 'Zaporiziahere'); MT content-farm text, not English editorial usage. _(pattern: `irishdentist\.ie`)_
- `reddit` [https://reddit.com/r/UKPersonalFinance/comments/1ier89w](https://reddit.com/r/UKPersonalFinance/comments/1ier89w) — Money-begging post with bank details and a monobank link ('help me close the credit'); scam solicitation. _(pattern: `send\.monobank\.ua`)_
- `reddit` [https://reddit.com/r/u_Iam_Tender_Angel/comments/1i6wklc](https://reddit.com/r/u_Iam_Tender_Angel/comments/1i6wklc) — Romance-bait persona post on a user profile sub ('My name is Jessica... born and raised in Kharkov'); fabricated biography. _(pattern: `r/u_iam_tender_angel`)_
- `youtube` [https://youtube.com/watch?v=mqWIFhgdq04](https://youtube.com/watch?v=mqWIFhgdq04) — ARMA 3 milsim footage captioned as real news (same channel and disclaimer as the other MILSIM JOSS clip). _(pattern: `\barma\s?3\b|milsim`)_
- `youtube` [https://youtube.com/watch?v=xhi0sy0vBI4](https://youtube.com/watch?v=xhi0sy0vBI4) — ARMA 3 milsim footage captioned as real news ('DISCLAIMER: ...all videos on this channel are simulations of the Arma 3 game'). _(pattern: `\barma\s?3\b|milsim`)_

**volodymyr-zelenskyy** (22)

- `reddit` [https://reddit.com/r/AlternativeHistory/comments/1j08d0s](https://reddit.com/r/AlternativeHistory/comments/1j08d0s) — r/AlternativeHistory: copy 6 of the same 7-sub 'Great prophecy' crosspost flood. _(pattern: `\bgreat prophecy\b`)_
- `reddit` [https://reddit.com/r/EscapingPrisonPlanet/comments/1j039nh](https://reddit.com/r/EscapingPrisonPlanet/comments/1j039nh) — r/EscapingPrisonPlanet: copy 4 of the same 7-sub 'Great prophecy' crosspost flood. _(pattern: `\bgreat prophecy\b`)_
- `reddit` [https://reddit.com/r/MatrixReality/comments/1j0894n](https://reddit.com/r/MatrixReality/comments/1j0894n) — r/MatrixReality: copy 5 of the same 7-sub 'Great prophecy' crosspost flood. _(pattern: `\bgreat prophecy\b`)_
- `reddit` [https://reddit.com/r/UkraineRussiaReport/comments/1iwnrt7](https://reddit.com/r/UkraineRussiaReport/comments/1iwnrt7) — r/UkraineRussiaReport: 2nd of 2 identical posts in the same subreddit minutes apart (kept 1iwnrcl). _(pattern: `ex-zelensky aide threatens to jail him for life`)_
- `reddit` [https://reddit.com/r/UkraineRussiaReport/comments/1pgbscr](https://reddit.com/r/UkraineRussiaReport/comments/1pgbscr) — r/UkraineRussiaReport: 2nd of 3 reposts of the same 'Flamingo' piece in the same subreddit (kept 1pgbhwz). _(pattern: `the flamingo that doesn'?t fly`)_
- `reddit` [https://reddit.com/r/UkraineRussiaReport/comments/1pgbuln](https://reddit.com/r/UkraineRussiaReport/comments/1pgbuln) — r/UkraineRussiaReport: 3rd of 3 reposts of the same 'Flamingo' piece. _(pattern: `the flamingo that doesn'?t fly`)_
- `reddit` [https://reddit.com/r/conspiracy/comments/1j08eno](https://reddit.com/r/conspiracy/comments/1j08eno) — r/conspiracy: copy 7 of the same 7-sub 'Great prophecy' crosspost flood. _(pattern: `\bgreat prophecy\b`)_
- `reddit` [https://reddit.com/r/gaystories/comments/1knc9zi](https://reddit.com/r/gaystories/comments/1knc9zi) — r/gaystories: 3rd of 3 copies of the same erotica post. _(pattern: `this is the first gay erotic piece i ever wrote`)_
- `reddit` [https://reddit.com/r/gaystoriesgonewild/comments/1knc8p9](https://reddit.com/r/gaystoriesgonewild/comments/1knc8p9) — r/gaystoriesgonewild: 2nd of 3 copies of one erotica post uploaded minutes apart (kept 1knc6z0). _(pattern: `this is the first gay erotic piece i ever wrote`)_
- `reddit` [https://reddit.com/r/interesting_newss/comments/1pk6pu0](https://reddit.com/r/interesting_newss/comments/1pk6pu0) — r/interesting_newss: 2nd of 2 reposts of the same body in the same subreddit (kept 1pk6nqb). _(pattern: `ukraine'?s deaths reach 1 millions`)_
- `reddit` [https://reddit.com/r/proamc/comments/1lwrfv7](https://reddit.com/r/proamc/comments/1lwrfv7) — r/proamc: 2nd of 3 reposts of one article in the same subreddit minutes apart (kept 1lwqytv). _(pattern: `zelensky claimed he ‘never heard of’ ukrainian nazi wwii crimes`)_
- `reddit` [https://reddit.com/r/proamc/comments/1lwrgun](https://reddit.com/r/proamc/comments/1lwrgun) — r/proamc: 3rd of 3 reposts of the same article in the same subreddit. _(pattern: `zelensky claimed he ‘never heard of’ ukrainian nazi wwii crimes`)_
- `reddit` [https://reddit.com/r/reptilians/comments/1j03905](https://reddit.com/r/reptilians/comments/1j03905) — r/reptilians: copy 2 of one 'Great prophecy' post crossposted by a single author to 7 subs within minutes (kept r/AlienAbduction 1j038gb). _(pattern: `\bgreat prophecy\b`)_
- `reddit` [https://reddit.com/r/reptilians2/comments/1j039d6](https://reddit.com/r/reptilians2/comments/1j039d6) — r/reptilians2: copy 3 of the same 7-sub 'Great prophecy' crosspost flood. _(pattern: `\bgreat prophecy\b`)_
- `reddit` [https://reddit.com/r/russiawarinukraine/comments/1j0soyl](https://reddit.com/r/russiawarinukraine/comments/1j0soyl) — r/russiawarinukraine: 2nd of 2 near-identical posts in the same subreddit (only the Vence/Vance typo differs; kept 1j0scig). _(pattern: `trump and j\.d\. v[ae]nce glorified president vladimir zelensky`)_
- `youtube` [https://youtube.com/watch?v=4568CD7jRa0](https://youtube.com/watch?v=4568CD7jRa0) — Steve Deace: the name appears only in a comma-separated topic-keyword list at the end of the description; the show's own prose writes “Putin and Zelensky”. _(pattern: `,\s*vladimir putin,\s*vladimir zelensky,`)_
- `youtube` [https://youtube.com/watch?v=4wDe-YvyNFI](https://youtube.com/watch?v=4wDe-YvyNFI) — BuzzBriefs News: the name appears only inside a 4,970-character space-separated SEO keyword dump at the end of the description. _(pattern: `i stand with ukraine vladimir zelensky`)_
- `youtube` [https://youtube.com/watch?v=8NohXP4AH_k](https://youtube.com/watch?v=8NohXP4AH_k) — News18 Urdu: lowercase keyword injected into the headline (“How the Donald Trump- vladimir zelensky Zelensky talks collapsed”); the description's own prose writes “Volodymyr Zelensky”. _(pattern: `donald trump-\s*vladimir zelensky zelensky`)_
- `youtube` [https://youtube.com/watch?v=JdBhzcCchqo](https://youtube.com/watch?v=JdBhzcCchqo) — Firstpost: the name appears only in the pipe-delimited SEO keyword block; the same description's prose writes “Ukraine's Volodymyr Zelensky”. _(pattern: `\|\s*vladimir zelensky\s*\|`)_
- `youtube` [https://youtube.com/watch?v=eMmMx8K8ces](https://youtube.com/watch?v=eMmMx8K8ces) — News18 Kerala: pipe-delimited SEO keyword title (“…| Vladimir Zelensky | Russia | Ukraine |N18G”) on a Malayalam-language livestream; the name is a tag, not running text. _(pattern: `\|\s*vladimir zelensky\s*\|`)_
- `youtube` [https://youtube.com/watch?v=ii1uD4c8KhI](https://youtube.com/watch?v=ii1uD4c8KhI) — Firstpost: the name appears only in the pipe-delimited SEO keyword block appended to the description; Firstpost's own prose in the same description writes “Ukraine's Volodymyr Zelensky”. _(pattern: `\|\s*vladimir zelensky\s*\|`)_
- `youtube` [https://youtube.com/watch?v=witml9yEvUs](https://youtube.com/watch?v=witml9yEvUs) — Nova Nexus Future: slash-delimited keyword-soup title (“Ukraine Rocked Russia/A World shocking news/ russia vs ukraine gone wrong/ vladimir zelensky's”) with no grammar and a dangling possessive; empty description. _(pattern: `/\s*vladimir zelensky`)_

**ihor-sikorsky** (2)

- `youtube` [https://youtube.com/watch?v=E823XHS_wD8](https://youtube.com/watch?v=E823XHS_wD8) — Name occurs only inside a trailing comma-separated SEO keyword list, not in any sentence.
- `youtube` [https://youtube.com/watch?v=TGgLIQLXpZk](https://youtube.com/watch?v=TGgLIQLXpZk) — Canadian-prairie helicopter clip whose description repeats 'Igor Sikorsky' four times as SEO padding around a pasted encyclopedia lede.

**kyivan-rus** (35)

- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1im70c7](https://reddit.com/r/Chto_ne_skache/comments/1im70c7) — Serial copypasta by u/12nb34 into their own subreddit network: 40 identical 'Modern Russia derives its name from the Kievan Rus' posts in this pair, six of them 23-30s apart. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1imwfe3](https://reddit.com/r/Chto_ne_skache/comments/1imwfe3) — Serial copypasta by u/12nb34 into their own subreddit network (177 posts in this pair, 40 with identical titles). _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ip7xsq](https://reddit.com/r/Chto_ne_skache/comments/1ip7xsq) — One of six identical u/12nb34 'Merry Christmas / Kievan Rus embraced Christianity' posts fired 23-30 seconds apart into their own subreddit. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ip7xzp](https://reddit.com/r/Chto_ne_skache/comments/1ip7xzp) — Duplicate #2 of the same u/12nb34 Christmas copypasta, posted 23s after the previous one. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ip7y65](https://reddit.com/r/Chto_ne_skache/comments/1ip7y65) — Duplicate #3 of the same u/12nb34 Christmas copypasta, 24s later. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ip80vg](https://reddit.com/r/Chto_ne_skache/comments/1ip80vg) — Duplicate #4 of the same u/12nb34 Christmas copypasta. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ip814k](https://reddit.com/r/Chto_ne_skache/comments/1ip814k) — Duplicate #5 of the same u/12nb34 Christmas copypasta. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ip81bj](https://reddit.com/r/Chto_ne_skache/comments/1ip81bj) — Duplicate #6 of the same u/12nb34 Christmas copypasta. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1itey56](https://reddit.com/r/Chto_ne_skache/comments/1itey56) — Another u/12nb34 copypasta variant ('Ever since Kievan Rus embraced Christianity') in their own subreddit. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1itxa4d](https://reddit.com/r/Chto_ne_skache/comments/1itxa4d) — u/12nb34 reposting a scraped news paragraph ('what Russians call the Kievan Rus') into their own subreddit. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ivi5u1](https://reddit.com/r/Chto_ne_skache/comments/1ivi5u1) — u/12nb34 'Modern Russia derives its name from the Kievan Rus' copypasta, one of 40 identical posts. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ivi94m](https://reddit.com/r/Chto_ne_skache/comments/1ivi94m) — Same u/12nb34 copypasta reposted 285 seconds later. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ivic7w](https://reddit.com/r/Chto_ne_skache/comments/1ivic7w) — Same u/12nb34 copypasta reposted again minutes later. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ivio5r](https://reddit.com/r/Chto_ne_skache/comments/1ivio5r) — u/12nb34 variant copypasta ("the Kievan Rus'. The name Rus' ... men who row") in their own subreddit. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1ivj9mv](https://reddit.com/r/Chto_ne_skache/comments/1ivj9mv) — Same u/12nb34 variant copypasta reposted 30 minutes later. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1iw8nw8](https://reddit.com/r/Chto_ne_skache/comments/1iw8nw8) — Same u/12nb34 variant copypasta reposted the next day. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j51l1w](https://reddit.com/r/Chto_ne_skache/comments/1j51l1w) — Same 'Kiev ... capital of the Kievan Rus ... established by Vikings' copypasta, posted 105 seconds before u/12nb34's own copy. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j51mmh](https://reddit.com/r/Chto_ne_skache/comments/1j51mmh) — u/12nb34 copy of the same 'scientific consensus / Kievan Rus' copypasta. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j6cggl](https://reddit.com/r/Chto_ne_skache/comments/1j6cggl) — Reworded repost of the same u/12nb34 'capital of Kievan Rus was established by Vikings' copypasta. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j6dbo5](https://reddit.com/r/Chto_ne_skache/comments/1j6dbo5) — Same u/12nb34 copypasta reposted an hour later with 'But' prefixed. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j6mx82](https://reddit.com/r/Chto_ne_skache/comments/1j6mx82) — u/12nb34 'scholarly consensus ... they formed the state of Kievan Rus' copypasta, one of 14 identical posts. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j7um4t](https://reddit.com/r/Chto_ne_skache/comments/1j7um4t) — u/12nb34 'Modern Russia derives its name from the Kievan Rus' copypasta again. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j7vb8s](https://reddit.com/r/Chto_ne_skache/comments/1j7vb8s) — Same copypasta reposted 52 minutes later by u/12nb34. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j7w1qf](https://reddit.com/r/Chto_ne_skache/comments/1j7w1qf) — Same copypasta reposted again by u/12nb34. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/Chto_ne_skache/comments/1j81uxj](https://reddit.com/r/Chto_ne_skache/comments/1j81uxj) — Same copypasta reposted again by u/12nb34 five hours later. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `reddit` [https://reddit.com/r/The_Way_of_the_DOOMMM/comments/1j7ulyv](https://reddit.com/r/The_Way_of_the_DOOMMM/comments/1j7ulyv) — Same u/12nb34 copypasta cross-posted to another subreddit in their own network, 20s before the r/Chto_ne_skache copy. _(pattern: `^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b`)_
- `youtube` [https://youtube.com/watch?v=9_llGlv0-nM](https://youtube.com/watch?v=9_llGlv0-nM) — Same michigantime marketplace listing reposted with 'Runes' appended; identical store-links-only description. _(pattern: `sterling silver 925|ebay\.us/m/`)_
- `youtube` [https://youtube.com/watch?v=DhZ6kXuoccA](https://youtube.com/watch?v=DhZ6kXuoccA) — ADS Channel 10k pastes the same country list (copied from Curiox, 'N4zi Germany' typo included) on a video about the Pakistani and Indian PMs. _(pattern: `(?:countries|country) (?:i|we) show in this video`)_
- `youtube` [https://youtube.com/watch?v=EDEI21ek9ZI](https://youtube.com/watch?v=EDEI21ek9ZI) — Same Curiox boilerplate country list; the hit is template text, not a description of this video. _(pattern: `countries i show in this video`)_
- `youtube` [https://youtube.com/watch?v=P_LrGLQfQEk](https://youtube.com/watch?v=P_LrGLQfQEk) — Description is a verbatim scrape of See U in History's 'Vikings in the Islamic World' text (same sentences, swapped amzn.to affiliate links); no independent usage. _(pattern: `if you like our content, check out our original comics`)_
- `youtube` [https://youtube.com/watch?v=WM_Wfcla0dw](https://youtube.com/watch?v=WM_Wfcla0dw) — Only hit is inside a 50-term pipe-delimited SEO keyword dump ('Kievan Rus | Kievan Rus Fall | Rus Principalities Mongols'); the video and its prose are about the Mongol Empire. _(pattern: `\|\s*kievan rus\s*\|\s*kievan rus fall\s*\|`)_
- `youtube` [https://youtube.com/watch?v=flQYphR52jo](https://youtube.com/watch?v=flQYphR52jo) — Boilerplate 'Countries I show in this video: ... Kievan Rus ...' list pasted verbatim across unrelated Curiox uploads; not attributable to this video. _(pattern: `countries i show in this video`)_
- `youtube` [https://youtube.com/watch?v=iymv7Tl5_cY](https://youtube.com/watch?v=iymv7Tl5_cY) — Same ADS Channel 10k boilerplate country list, this time on an 'India of Evolution' short. _(pattern: `(?:countries|country) (?:i|we) show in this video`)_
- `youtube` [https://youtube.com/watch?v=tT0VVgzt2dI](https://youtube.com/watch?v=tT0VVgzt2dI) — eBay/Etsy/Depop jewellery listing: 'sterling silver 925 , Viking, Kievan Rus, amulet pendant' is marketplace keyword stuffing; description is only store links. _(pattern: `sterling silver 925|ebay\.us/m/`)_
- `youtube` [https://youtube.com/watch?v=y2lGTx6XkrA](https://youtube.com/watch?v=y2lGTx6XkrA) — Same Curiox boilerplate country list on a video about the Achaemenid Empire - Kievan Rus is not its subject. _(pattern: `countries i show in this video`)_

**babyn-yar** (1)

- `youtube` [https://youtube.com/watch?v=Mks0IglNPNI](https://youtube.com/watch?v=Mks0IglNPNI) — Hindi comedy short 'Do sarabi Yar'; 'Babi Yar massacre' only appears inside a stuffed keyword dump mixing Holocaust terms with 'stand-up comedy, funny videos'. _(pattern: `sarabi yar`)_

**varenyky** (2)

- `reddit` [https://reddit.com/r/u_espanolukraine/comments/1kcd9jp](https://reddit.com/r/u_espanolukraine/comments/1kcd9jp) — Self-promo post on a user-profile page (r/u_espanolukraine, a Spanish-language Ukraine account); the whole post is the two-word stream label 'Vareniki live' with no body. _(pattern: `^r/u_`)_
- `youtube` [https://youtube.com/watch?v=QUH0NQylkr8](https://youtube.com/watch?v=QUH0NQylkr8) — Promo spam: title is a phone number plus a hashtag dump ('☎️998917781703#cherryculinarystudio#chuchvara#kotlet#vareniki#somsa#manti#shorts') over an Uzbek description with a 60-tag block and a Telegram link. _(pattern: `#cherryculinar\w*`)_

**serhii-korolyov** (4)

- `youtube` [https://youtube.com/watch?v=9b-PK2fZ4YA](https://youtube.com/watch?v=9b-PK2fZ4YA) — Name occurs only in a trailing SEO keyword run ('...USSR vs USA space Sergei Korolev Soviet rocket program Cold War...'), never in a sentence. _(pattern: `space\s+sergei korolev\s+soviet rocket program`)_
- `youtube` [https://youtube.com/watch?v=TBdnGsz8qYw](https://youtube.com/watch?v=TBdnGsz8qYw) — Name occurs only inside a comma-separated SEO keyword blob ('...baikonur cosmodrome launch,sergei korolev sputnik,soviet union...'). _(pattern: `,\s*sergei korolev[^,]{0,25},`)_
- `youtube` [https://youtube.com/watch?v=hB9hYk_vNyw](https://youtube.com/watch?v=hB9hYk_vNyw) — Hinglish ISRO short whose description is the same rocket-keyword dump ('...saturn v launch sergei korolev space x launch today...'). _(pattern: `sergei korolev\s+space\s*x`)_
- `youtube` [https://youtube.com/watch?v=mS4ZwJ_ohvM](https://youtube.com/watch?v=mS4ZwJ_ohvM) — Match sits in a pure rocket-keyword dump ('...isro satellite launch sergei korolev space x rocket soyuz...'); no sentence anywhere. _(pattern: `sergei korolev\s+space\s*x`)_

**oleksandr-usyk** (4)

- `reddit` [https://reddit.com/r/FightLibrary/comments/1luko43](https://reddit.com/r/FightLibrary/comments/1luko43) — Identical repost of r/BoxingClassics/1lu6ymx by the same account (New_Primary_9579, whose entire corpus is these 2 posts) one day later in a second full-fight sub, 0 comments — one item counted twice.
- `youtube` [https://youtube.com/watch?v=-aa-2LeyAYg](https://youtube.com/watch?v=-aa-2LeyAYg) — Serie A football lineup video; the entire description is a 5-line keyword list ending 'alexander usyk / asmr'. No prose, wrong subject. _(pattern: `alexander usyk\s+asmr`)_
- `youtube` [https://youtube.com/watch?v=0Ya8VlbcVIw](https://youtube.com/watch?v=0Ya8VlbcVIw) — Same-channel duplicate: BOXI NEWS uploaded this at 2025-08-03T21:30Z with body copy identical to iuoOtaz393c (21:15Z, also in this table) — one script, two uploads 15 minutes apart, counted twice.
- `youtube` [https://youtube.com/watch?v=9UbD0_nssaE](https://youtube.com/watch?v=9UbD0_nssaE) — Chicago rap-single promo; 'Alexander usyk' appears only inside a ~200-term SEO keyword blob alongside 'type beat', 'mr beast', 'Fortnite'. The video has nothing to do with boxing. _(pattern: `\btype beat\b`)_

**kazymyr-malevych** (7)

- `reddit` [https://reddit.com/r/CustomCanvasCurators/comments/1j3f9ey](https://reddit.com/r/CustomCanvasCurators/comments/1j3f9ey) — Storefront self-promo post by u/CustomCanvasCurators: 'Custom Canvas Wall Art Inspired by Kazimir Malevich's Suprematism ... by CustomCanvasCurators'. _(pattern: `customcanvascurators`)_
- `reddit` [https://reddit.com/r/CustomCanvasCurators/comments/1lsd9e8](https://reddit.com/r/CustomCanvasCurators/comments/1lsd9e8) — Duplicate storefront listing from the same promo account four months later. _(pattern: `customcanvascurators`)_
- `reddit` [https://reddit.com/r/artstore/comments/1ohph2g](https://reddit.com/r/artstore/comments/1ohph2g) — Marketplace listing for AI-generated prints ('I'm offering AI-inspired digital minimalist artworks ... Format: PNG (4K resolution, printable) Price'); the name frames a sales pitch.
- `youtube` [https://youtube.com/watch?v=JXfevh248YE](https://youtube.com/watch?v=JXfevh248YE) — Drop-ship product listing: 'our Kazimir Malevich Inspired Abstract Dolphin Metal Wall Art ... premium white aluminum composite'; the name is a marketing style tag. _(pattern: `kazimir malevich[ -]*(?:inspired|style)[^\n]{0,80}(?:metal )?wall (?:art|decor|sign)`)_
- `youtube` [https://youtube.com/watch?v=Klgc8RPnzN8](https://youtube.com/watch?v=Klgc8RPnzN8) — Same drop-ship storefront: 'our Kazimir Malevich inspired metal wall decor featuring humming birds'; product copy, not usage about the painter. _(pattern: `kazimir malevich[ -]*(?:inspired|style)[^\n]{0,80}(?:metal )?wall (?:art|decor|sign)`)_
- `youtube` [https://youtube.com/watch?v=TolSWBysuRc](https://youtube.com/watch?v=TolSWBysuRc) — Same storefront: 'Kazimir Malevich Style Wildlife Abstract Metal Wall Art Sign ... premium white aluminum composite'. _(pattern: `kazimir malevich[ -]*(?:inspired|style)[^\n]{0,80}(?:metal )?wall (?:art|decor|sign)`)_
- `youtube` [https://youtube.com/watch?v=rf4nYks4gvU](https://youtube.com/watch?v=rf4nYks4gvU) — Generic drawing short whose description is a 200+ artist-name SEO keyword pile ('... pierre auguste renoir kazimir malevich maurizio cattelan malevich eugene delacroix ...'); no semantic use. _(pattern: `kazimir malevich\s+maurizio cattelan`)_

**ternopil** (1)

- `reddit` [https://reddit.com/r/u_rommy2012/comments/1k1kkvb](https://reddit.com/r/u_rommy2012/comments/1k1kkvb) — OnlyFans spam post: body is repeated "Let's Chat On My Free Onlyfans" links plus filter-evasion dictionary word-salad in which "Ternopol" sits between "peckier" and "cresyl" — no referent at all. _(pattern: `onlyfans\.com`)_

**feodosiia** (1)

- `youtube` [https://youtube.com/watch?v=SL9xg3k4Zbc](https://youtube.com/watch?v=SL9xg3k4Zbc) — Mass-produced personalised birthday-song template ('FEODOSIYA Happy Birthday Song | Ultimate Happy Birthday to You Remix'); FEODOSIYA fills the given-name slot, no city referent. _(pattern: `happy\s+birthday\s+song\b|#makemyday`)_

**volodymyr-the-great** (2)

- `youtube` [https://youtube.com/watch?v=Gxy05drKqpc](https://youtube.com/watch?v=Gxy05drKqpc) — AI-generated occult word-salad ('House of Huisache', 'Chinacate Resurrection Brokers'); the phrase is name-dropped inside vampire-lore gibberish, not a usage about the prince. _(pattern: `house of huisache|tuatha de acacia|chinacate`)_
- `youtube` [https://youtube.com/watch?v=uuz7ZxMqAzg](https://youtube.com/watch?v=uuz7ZxMqAzg) — Description is nothing but a comma-separated SEO keyword dump ('vladimir the great,east slavic state,#what is the biggest country in the world?,...') duplicated into the tags; no running text anywhere in the document.

**bakhmut** (3)

- `reddit` [https://reddit.com/r/itsemmma/comments/1ku2629](https://reddit.com/r/itsemmma/comments/1ku2629) — OnlyFans promo post (37,879 chars) padded with a random dictionary word-salad; "Artemovsk" appears as one isolated word between "Ramillies" and "caboceer" — no referent, no running text. _(pattern: `onlyfans\.com`)_
- `youtube` [https://youtube.com/watch?v=62Z0vxJOsrc](https://youtube.com/watch?v=62Z0vxJOsrc) — Hashtag-stuffed Short with an empty description; the whole title is "#⃣ #uk #shortvideo #shorts #bahmut #artemovsk" — carries both spellings as SEO tags, so it records no spelling choice and no running text. _(pattern: `#artemovsk\b`)_
- `youtube` [https://youtube.com/watch?v=o2A1Mer-8Mo](https://youtube.com/watch?v=o2A1Mer-8Mo) — Fake-war-footage content farm: title "FRIDAY MAY 09! UKRAINE AND NATO APPROACH RUSSIAN FORCES IN ARTEMOVSK" but the description says "NOT the original footage, just a simulation of Arma 3 gameplay!" and keywords are arma 3 / military simulation — a fabricated event, not coverage of the real subject. _(pattern: `green\s+revonk|arma\s*3`)_

**dnipro-river** (1)

- `reddit` [https://reddit.com/r/germany/comments/1jkxu97](https://reddit.com/r/germany/comments/1jkxu97) — Travel-brochure ad copy ('a captivating destination... Why should you visit Kyiv?') pasted as a bare title into an unrelated subreddit; promotional boilerplate, not quotable running text. _(pattern: `captivating\s+destination|why\s+should\s+you\s+visit`)_

### frozen-brand — 234 drops

**odesa** (14)

- `news` [https://independentaustralia.net/politics/politics-display/russias-illusion-of-power-cracks-as-economy-and-war-effort-falter,20089](https://independentaustralia.net/politics/politics-display/russias-illusion-of-power-cracks-as-economy-and-war-effort-falter,20089) — Sole hit is a citation of the outlet name 'the Odessa Journal', not a spelling of the city. _(pattern: `the\s+odessa\s+journal`)_
- `news` [https://www.kyivpost.com/post/58891](https://www.kyivpost.com/post/58891) — Both hits are the masthead 'The Odessa Review'; the writer names a journal, never the city. _(pattern: `the\s+odessa\s+review`)_
- `news` [https://www.latimes.com/food/story/2025-08-21/for-valley-iranians-this-van-nuys-market-is-like-home](https://www.latimes.com/food/story/2025-08-21/for-valley-iranians-this-van-nuys-market-is-like-home) — Sole hit is 'Odessa Grocery', a Van Nuys, California market. _(pattern: `odessa\s+grocery`)_
- `openalex` [https://openalex.org/W7127616443](https://openalex.org/W7127616443) — 'M.V. Odessa' is a vessel name in a tour guide's travel notebook. _(pattern: `\bm\.?\s?v\.?\s+odessa\b`)_
- `reddit` [https://reddit.com/r/Wolfbros/comments/1hsaxes](https://reddit.com/r/Wolfbros/comments/1hsaxes) — Bob Weir concert setlist; 'Odessa' is a song title between 'Little Red Rooster' and 'Catfish John'. _(pattern: `setlist`)_
- `reddit` [https://reddit.com/r/fantanoforever/comments/1hrwlvw](https://reddit.com/r/fantanoforever/comments/1hrwlvw) — Favourite-songs list: '17. Odessa - Caribou' - a track title. _(pattern: `odessa\s*[-–]\s*caribou|caribou[^.]{0,12}odessa`)_
- `reddit` [https://reddit.com/r/pelletgrills/comments/1hvscon](https://reddit.com/r/pelletgrills/comments/1hvscon) — 'another one called the Odessa which is the 1250 model' - a pellet-grill product name. _(pattern: `odessa[^.]{0,30}\b(?:1250|model|grill|smoker|pellet)\b`)_
- `youtube` [https://youtube.com/watch?v=03ZEACWN6yE](https://youtube.com/watch?v=03ZEACWN6yE) — Hulu's film 'O'Dessa' (Sadie Sink); #ODessa is the title, not the city. _(pattern: `o['’]dessa|sadie\s+sink`)_
- `youtube` [https://youtube.com/watch?v=FaPixevAKlA](https://youtube.com/watch?v=FaPixevAKlA) — 'Neon Love by Odessa' - a recording artist on the Emily in Paris soundtrack. _(pattern: `neon\s+love|\bby\s+odessa\b`)_
- `youtube` [https://youtube.com/watch?v=Ps9FLtSXIN0](https://youtube.com/watch?v=Ps9FLtSXIN0) — 'AKH Odessa' - a Parov Stelar track (auto-generated Topic upload). _(pattern: `parov\s+stelar`)_
- `youtube` [https://youtube.com/watch?v=am-pdHMTSRo](https://youtube.com/watch?v=am-pdHMTSRo) — 'Band Odessa' (Банд Одесса), a Russian-language music act; tags entirely Cyrillic. _(pattern: `band\s+odessa`)_
- `youtube` [https://youtube.com/watch?v=ckhnnCLMoBc](https://youtube.com/watch?v=ckhnnCLMoBc) — 'ODESSA Mini Dress' - a clothing product name from a fashion store channel. _(pattern: `odessa\s+(?:mini\s+)?dress`)_
- `youtube` [https://youtube.com/watch?v=kjOg0lpoR1E](https://youtube.com/watch?v=kjOg0lpoR1E) — Parov Stelar's 2006 track 'Odessa' (auto-generated Topic upload). _(pattern: `parov\s+stelar`)_
- `youtube` [https://youtube.com/watch?v=zQ22AEdujj8](https://youtube.com/watch?v=zQ22AEdujj8) — 'Odessa Bulgar' - a klezmer tune title performed by Baba Yaga Vienna. _(pattern: `odessa\s+bulgar`)_

**kyiv** (21)

- `news` [https://gcaptain.com/category/incidents/ship-fires/](https://gcaptain.com/category/incidents/ship-fires/) — Sole match is "the former Soviet Kiev-class aircraft carrier Minsk" - ship class, not the city. _(pattern: `kiev[\s-]*class`)_
- `news` [https://idrw.org/indias-fading-carrier-edge-how-china-overtook-a-pioneer-in-maritime-air-power/](https://idrw.org/indias-fading-carrier-edge-how-china-overtook-a-pioneer-in-maritime-air-power/) — Sole match is "a modified Kiev-class Soviet-era platform" - carrier class designation. _(pattern: `kiev[\s-]*class`)_
- `news` [https://valleyadvocate.com/2025/12/24/a-toast-to-tranquility-supporting-farmers-winemakers-from-ukraine-with-spirits-from-state-street/](https://valleyadvocate.com/2025/12/24/a-toast-to-tranquility-supporting-farmers-winemakers-from-ukraine-with-spirits-from-state-street/) — Sole occurrence is the product name "Ghost of Kiev Ukrainian Freedom Vodka" on a bottle label. _(pattern: `ghost\s+of\s+kiev\s+ukrainian\s+freedom\s+vodka`)_
- `news` [https://variety.com/2025/shopping/news/best-corporate-gifts-gift-for-boss-coworkers-colleagues-1203081538/](https://variety.com/2025/shopping/news/best-corporate-gifts-gift-for-boss-coworkers-colleagues-1203081538/) — "Dr Martens Kiev Leather Backpack" - a product name in a gift guide. _(pattern: `martens\s+kiev|kiev\s+leather\s+backpack`)_
- `news` [https://www.firstpost.com/explainers/india-russia-defence-ties-explained-putin-visit-india-13954256.html](https://www.firstpost.com/explainers/india-russia-defence-ties-explained-putin-visit-india-13954256.html) — Sole match is "the Soviet-made Kiev-class vessel" (INS Vikramaditya) - a carrier class designation. _(pattern: `kiev[\s-]*class`)_
- `news` [https://www.loudersound.com/bands-artists/pelagic-records-have-announced-they-are-bringing-together-some-of-metals-most-beautiful-and-boundless-bands-for-the-2026-edition-of-pelagic-fest](https://www.loudersound.com/bands-artists/pelagic-records-have-announced-they-are-bringing-together-some-of-metals-most-beautiful-and-boundless-bands-for-the-2026-edition-of-pelagic-fest) — "Lost In Kiev" is a French post-rock band on a festival line-up. _(pattern: `lost\s+in\s+kiev`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1hr68ku](https://reddit.com/r/AnalogCommunity/comments/1hr68ku) — Kiev-88 CM medium-format camera (Arsenal factory product line); config homonym_filters already cover this class but the holdout list was built unfiltered. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1hrbxnj](https://reddit.com/r/AnalogCommunity/comments/1hrbxnj) — Kiev 88 camera light-leak troubleshooting. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1hrheoq](https://reddit.com/r/AnalogCommunity/comments/1hrheoq) — "Light leak galore, Kiev 88" - camera model. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1hry9dw](https://reddit.com/r/AnalogCommunity/comments/1hry9dw) — "my first medium format camera (Kiev 6c)" - camera model. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1hs442l](https://reddit.com/r/AnalogCommunity/comments/1hs442l) — "my Kiev 6C that's been rebuilt by ARAX" - camera model. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1httylx](https://reddit.com/r/AnalogCommunity/comments/1httylx) — "The Kiev60 ... I've owned 3 Kiev's" - camera models in a film-camera line-up. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1hum3g8](https://reddit.com/r/AnalogCommunity/comments/1hum3g8) — "Kiev 6c rough film advance" - camera fault thread. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1hx2jm2](https://reddit.com/r/AnalogCommunity/comments/1hx2jm2) — "Why are these my ISO options on my Kiev-19?" - camera model (already in config homonym_filters). _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/AnalogCommunity/comments/1hx69z8](https://reddit.com/r/AnalogCommunity/comments/1hx69z8) — "the battery compartment in my Kiev-19" - camera model. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/ImaginaryWarships/comments/1hwggwt](https://reddit.com/r/ImaginaryWarships/comments/1hwggwt) — "a Soviet Kiev-class aircraft carrier" - ship class designation. _(pattern: `kiev[\s-]*class`)_
- `reddit` [https://reddit.com/r/LineageOS/comments/1hr4opz](https://reddit.com/r/LineageOS/comments/1hr4opz) — "Motorola one 5g ace (Kiev)" - Kiev is the handset's internal device codename in an Android ROM thread. _(pattern: `one\s+5g\s+ace|codename\s+kiev`)_
- `reddit` [https://reddit.com/r/analog/comments/1hv8yk1](https://reddit.com/r/analog/comments/1hv8yk1) — "Cinestill 800T - Kiev 88 - Volna 3" - camera model in a photo caption. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/analog/comments/1hx07v1](https://reddit.com/r/analog/comments/1hx07v1) — "Cinestill 800T - Kiev 88 and Volna 3" - camera model in a photo caption. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `reddit` [https://reddit.com/r/mediumformat/comments/1ht9wp3](https://reddit.com/r/mediumformat/comments/1ht9wp3) — "Svema A2-Sh | Kiev 6C | CZJ Flektogon" - camera model in a gear caption. _(pattern: `kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b`)_
- `youtube` [https://youtube.com/watch?v=j4z_V-wN1YA](https://youtube.com/watch?v=j4z_V-wN1YA) — "Green Soul Kiev Orthopedic Boss Chair" - an office-chair model name in a product review. _(pattern: `green\s+soul\s+kiev|kiev\s+orthopedic`)_

**lviv** (15)

- `openalex` [https://openalex.org/W4408112815](https://openalex.org/W4408112815) — Witwicki motivation-theory paper; 'Lvov-Warsaw School' is the frozen English name of a Polish philosophical school. The paper is about psychology, not the city. _(pattern: `\blvov[\s–-]*warsaw\b`)_
- `openalex` [https://openalex.org/W4413196747](https://openalex.org/W4413196747) — 'The Historical Examples of the Lvov-Warsaw School and US Logical Empiricism' - title-only record; the frozen school name again. _(pattern: `\blvov[\s–-]*warsaw\b`)_
- `openalex` [https://openalex.org/W7115721741](https://openalex.org/W7115721741) — 'The Lvov-Warsaw School in clandestine education' - the school's wartime activity; the city is not the referent. _(pattern: `\blvov[\s–-]*warsaw\b`)_
- `openalex` [https://openalex.org/W7117894063](https://openalex.org/W7117894063) — 'Phenomenology from the Standpoint of the Lvov-Warsaw School (henceforth: LWS)' - the school as an abbreviated term of art, not the city. _(pattern: `\blvov[\s–-]*warsaw\b`)_
- `openalex` [https://openalex.org/W7124823350](https://openalex.org/W7124823350) — 'Towards the Lvov-Warsaw Generations Relay' - a derivative of the same frozen school name; the paper reconstructs Jerzy Pelc's metaphilosophy. _(pattern: `\blvov[\s–-]*warsaw\b`)_
- `openalex` [https://openalex.org/W7128778523](https://openalex.org/W7128778523) — Decisive: the abstract calls the city itself 'Lwow' ('Founded by Kazimierz Twardowski in Lwow') and uses 'Lvov' only inside the school's fixed name - direct evidence the compound is frozen. _(pattern: `\blvov[\s–-]*warsaw\b`)_
- `reddit` [https://reddit.com/r/SovietPhotosOfWW2/comments/1ka62q2](https://reddit.com/r/SovietPhotosOfWW2/comments/1ka62q2) — '61st Guards Tank Sverdlovsk-Lvov Red Banner Brigade' - a frozen 1944 Soviet unit honorific (paired with Sverdlovsk, itself a dead name); the caption is about the tank crew, not the city. _(pattern: `\bsverdlovsk[\s–-]*lvov\b`)_
- `youtube` [https://youtube.com/watch?v=CKZBtcdB7ug](https://youtube.com/watch?v=CKZBtcdB7ug) — 'Lvov-Warsaw School' is the frozen English name of Twardowski's philosophical school; the webinar is about philosophy education, not the city. _(pattern: `\blvov[\s–-]*warsaw\b`)_
- `youtube` [https://youtube.com/watch?v=NQ6mZ-YEtzQ](https://youtube.com/watch?v=NQ6mZ-YEtzQ) — 'Love Song for Lemberg Lvov' is the fixed title of a David Krakauer composition; every mention quotes the work's name rather than making a live spelling choice. _(pattern: `love\s+song\s+for\s+lemberg\s+lvov`)_
- `youtube` [https://youtube.com/watch?v=dSCO6U-_-Ls](https://youtube.com/watch?v=dSCO6U-_-Ls) — 'Slawowycz - 03 - Romance In Lvov' - fixed album track title. _(pattern: `\bromance\s+in\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=k7Xa7D8jMxs](https://youtube.com/watch?v=k7Xa7D8jMxs) — 'Romance In Lvov (Ash Remix)' by Slawowycz - a fixed track title from the album 'Karpat'. _(pattern: `\bromance\s+in\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=sRjrOppeDas](https://youtube.com/watch?v=sRjrOppeDas) — Auto-generated topic upload of 'Romance In Lvov (InnocentButGuilty Remix)'; song title. _(pattern: `\bromance\s+in\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=si2hTPomgoI](https://youtube.com/watch?v=si2hTPomgoI) — Auto-generated topic upload of 'Romance In Lvov (Ash Remix)'; song title. _(pattern: `\bromance\s+in\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=swR1YewEHg8](https://youtube.com/watch?v=swR1YewEHg8) — Auto-generated 'Slawowycz - Topic' upload of the track 'Romance In Lvov'; a song title. _(pattern: `\bromance\s+in\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=v7Qp5yF1NMk](https://youtube.com/watch?v=v7Qp5yF1NMk) — 'LVOV Vodka Review' - LVOV is a vodka brand; the video reviews the bottle, not the city. _(pattern: `\blvov\s+vodka\b`)_

**borscht** (13)

- `news` [http://www.israelnationalnews.com/news/406324](http://www.israelnationalnews.com/news/406324) — "tickets to comedy shows in the Borsch Belt in the Catskills" — the region, not the soup. _(pattern: `borsch\s+belt`)_
- `news` [https://chi.streetsblog.org/2025/09/30/yes-cta-needs-to-be-and-feel-safer-but-despite-what-the-u-s-transit-czar-implied-trump-shouldnt-use-that-as-an-excuse-to-send-in-national-guard](https://chi.streetsblog.org/2025/09/30/yes-cta-needs-to-be-and-feel-safer-but-despite-what-the-u-s-transit-czar-implied-trump-shouldnt-use-that-as-an-excuse-to-send-in-national-guard) — "he probably knows a lot more about the Borsch Belt than the Red Line" — the Catskills resort/comedy region, a frozen US toponym, not the soup. _(pattern: `borsch\s+belt`)_
- `news` [https://jazzjournal.co.uk/2025/08/14/reviewed-sam-sadigursky-nathan-koci-dino-saluzzi-jacob-young-jose-maria-saluzzi/](https://jazzjournal.co.uk/2025/08/14/reviewed-sam-sadigursky-nathan-koci-dino-saluzzi-jacob-young-jose-maria-saluzzi/) — "the ghostly silence of [Jewish] Borsch Belt ruins [in NY's Catskill Mountains]" — the Catskills region, not the dish. _(pattern: `borsch\s+belt`)_
- `news` [https://laughingsquid.com/david-johansen-punk-rock-catskills/](https://laughingsquid.com/david-johansen-punk-rock-catskills/) — "putting on an incredible Yiddish accent to tell a funny Borsch Belt joke" — the Catskills comedy idiom. _(pattern: `borsch\s+belt`)_
- `news` [https://www.chronogram.com/hudsonvalley/borscht-belt-comedy-club/Event?oid=23241328](https://www.chronogram.com/hudsonvalley/borscht-belt-comedy-club/Event?oid=23241328) — "the Borsch Belt Museum is hosting their second night of comedy" — an Ellenville institution named for the Catskills region. _(pattern: `borsch\s+belt`)_
- `reddit` [https://reddit.com/r/AllThingsNIO/comments/1ia1rkd](https://reddit.com/r/AllThingsNIO/comments/1ia1rkd) — EV stock thread: "It tries to be an upstream EV supplier like Borsch" — a misspelling of the Bosch auto-parts company. _(pattern: `(?:ev|auto\w*|parts|component)\s+supplier[^.]{0,25}\bborsch\b`)_
- `reddit` [https://reddit.com/r/AskBrits/comments/1lhl30d](https://reddit.com/r/AskBrits/comments/1lhl30d) — "I bought a fairly expensive lithium battery hoover by Borsch" — misspelling of the Bosch appliance brand. _(pattern: `\bborsch\b(?=[\s\S]{0,400}\b(?:hoover|vacuum|appliance|bosch)\b)`)_
- `reddit` [https://reddit.com/r/CinematicDiversions/comments/1l14yu1](https://reddit.com/r/CinematicDiversions/comments/1l14yu1) — Film review: "blame their mothers outside of a Borsch Belt comedy" — the Catskills idiom. _(pattern: `borsch\s+belt`)_
- `reddit` [https://reddit.com/r/IKEA/comments/1jcbgu3](https://reddit.com/r/IKEA/comments/1jcbgu3) — Dishwasher thread: "I was thinking of getting one from Borsch or Frigidaire instead" — misspelling of Bosch. _(pattern: `\bborsch\b(?=[\s\S]{0,400}\b(?:frigidaire|dishwasher|ikea|appliance)\b)`)_
- `reddit` [https://reddit.com/r/OakIslandDiscussion/comments/1ijduqj](https://reddit.com/r/OakIslandDiscussion/comments/1ijduqj) — Identical "Borsch House Restaurant" title crossposted into an unrelated Oak Island sub — same business proper name, and a duplicate of the r/halifax row. _(pattern: `borsch\s+house`)_
- `reddit` [https://reddit.com/r/TheDeprogram/comments/1kdanxh](https://reddit.com/r/TheDeprogram/comments/1kdanxh) — "Pokepreet Podcast - Borsch Bandit Discusses Travelling Former Soviet Union" — "Borsch Bandit" is the guest's channel/persona handle. _(pattern: `borsch\s+bandit`)_
- `reddit` [https://reddit.com/r/halifax/comments/1ijdmau](https://reddit.com/r/halifax/comments/1ijdmau) — Entire post is the business name "Borsch House Restaurant - Grand Opening Tonight!" — a proper name, no running prose about the dish. _(pattern: `borsch\s+house`)_
- `reddit` [https://reddit.com/r/refrigerator/comments/1ihhf9i](https://reddit.com/r/refrigerator/comments/1ihhf9i) — Post title is "Need help to fix my Bosch Refrigerator"; "called customer serive borsch" is a misspelling of the appliance brand. _(pattern: `\bborsch\b(?=[\s\S]{0,400}\b(?:bosch|refrigerator|fridge|freezer|dishwasher|compressor)\b)`)_

**donbas** (9)

- `news` [https://openthemagazine.com/columns/when-politics-hijacked-the-beautiful-game-in-dublin](https://openthemagazine.com/columns/when-politics-hijacked-the-beautiful-game-in-dublin) — openthemagazine.com: only match is "Shakhtar's old Donbass Arena" — the stadium's own fixed English name, not a spelling choice for the region. _(pattern: `donbass\s+arena`)_
- `news` [https://tass.com/politics/2008335](https://tass.com/politics/2008335) — tass.com: only match is 'the Donbass Dome electronic warfare system' — a Russian weapon-system product name, not the region. _(pattern: `donbass\s+dome`)_
- `news` [https://www.arabnews.com/node/2602499/sport](https://www.arabnews.com/node/2602499/sport) — arabnews.com: only match is 'the Donbass Arena in Donetsk', Shakhtar's stadium — a fixed venue name, not the region. _(pattern: `donbass\s+arena`)_
- `news` [https://www.gazetaexpress.com/en/the-networks-that-Russia-uses-to-recruit-its-citizens-for-war/](https://www.gazetaexpress.com/en/the-networks-that-Russia-uses-to-recruit-its-citizens-for-war/) — gazetaexpress.com: only match is the organisation name 'Donbass Volunteer Union' (Союз добровольцев Донбасса), whose own English form is fixed. _(pattern: `(?:union\s+of\s+donbass\s+volunteers|donbass\s+volunteers?\s+union)`)_
- `news` [https://www.helsinkitimes.fi/world-int/26857-russia-uses-influencers-to-promote-life-in-destroyed-mariupol.html](https://www.helsinkitimes.fi/world-int/26857-russia-uses-influencers-to-promote-life-in-destroyed-mariupol.html) — helsinkitimes.fi: only match is 'Donbass Media Centre (DMC)', the Russian-run organisation's own registered name. _(pattern: `donbass\s+media\s+cent(?:re|er)`)_
- `news` [https://www.kyivpost.com/post/47415](https://www.kyivpost.com/post/47415) — kyivpost.com: only match is 'the Donbass Case Telegram channel' — a channel name; Kyiv Post writes 'Donbas' for the region itself. _(pattern: `donbass\s+case`)_
- `news` [https://www.rferl.org/a/russia-recruitment-war-ukraine-mercenaries/33476077.html](https://www.rferl.org/a/russia-recruitment-war-ukraine-mercenaries/33476077.html) — rferl.org: all three matches are the organisation name 'Union of Donbass Volunteers'; RFE/RL's house style for the region itself is 'Donbas'. _(pattern: `(?:union\s+of\s+donbass\s+volunteers|donbass\s+volunteers?\s+union)`)_
- `reddit` [https://reddit.com/r/DividedDonbass/comments/1i7eklr](https://reddit.com/r/DividedDonbass/comments/1i7eklr) — r/DividedDonbass: both matches are the title of a Hearts of Iron IV mod, 'Divided Donbass'; the body describing the real 2014-15 crisis never spells the region itself. _(pattern: `divided\s+donbass`)_
- `youtube` [https://youtube.com/watch?v=oYOmda4Uk_c](https://youtube.com/watch?v=oYOmda4Uk_c) — Berita Viral: Indonesian short about the Donbass Arena stadium — a fixed venue brand, not the region, and not English. _(pattern: `donbass\s*arena`)_

**luhansk** (1)

- `reddit` [https://reddit.com/r/LabDiamondGemstoneBST/comments/1kfosp0](https://reddit.com/r/LabDiamondGemstoneBST/comments/1kfosp0) — Jewellery sale listing: 'Lugansk' is a bracelet model name ('Lugansk 4-prong Lab Diamond braclet – 3ct – 14K white gold') _(pattern: `lugansk\s+4-prong`)_

**chornobyl** (62)

- `news` [http://www.nintendoworldreport.com/game/73579/chernobyl-escape-from-pripyat-switch](http://www.nintendoworldreport.com/game/73579/chernobyl-escape-from-pripyat-switch) — Game-catalogue boilerplate; only the product title 'Chernobyl: Escape from Pripyat', no running text. _(pattern: `chernobyl:?\s*escape\s+from\s+pripyat`)_
- `news` [https://bleedingcool.com/tv/assassins-creed-chernobyl-director-renck-helming-netflix-series/](https://bleedingcool.com/tv/assassins-creed-chernobyl-director-renck-helming-netflix-series/) — All mentions are the HBO series title used as Johan Renck's credit; nothing about Ukraine. _(pattern: `renck\s*\(chernobyl\)|\(hbo'?s[^)]*chernobyl\)`)_
- `news` [https://collider.com/chernobyl-apple-tv-store-streaming-success-december-2025/](https://collider.com/chernobyl-apple-tv-store-streaming-success-december-2025/) — All 7 mentions are HBO's miniseries title in a streaming-charts story; the disaster is never named. _(pattern: `(?:hbo'?s|miniseries|mini[- ]series)\s+chernobyl`)_
- `news` [https://collider.com/exterior-night-series-chernobyl-replacement/](https://collider.com/exterior-night-series-chernobyl-replacement/) — Both mentions are the HBO series title, used as a benchmark in a review of 'Exterior Night'. _(pattern: `(?:hbo'?s|miniseries|mini[- ]series)\s+chernobyl`)_
- `news` [https://collider.com/netflix-assassins-creed-series-director-johan-renck-chernobyl/](https://collider.com/netflix-assassins-creed-series-director-johan-renck-chernobyl/) — Sole mention is 'miniseries Chernobyl' as a director credit in an Assassin's Creed casting story. _(pattern: `(?:hbo'?s|miniseries|mini[- ]series)\s+chernobyl`)_
- `news` [https://www.inverness-courier.co.uk/news/end-of-an-era-as-beloved-church-hosts-last-service-before-423087/](https://www.inverness-courier.co.uk/news/end-of-an-era-as-beloved-church-hosts-last-service-before-423087/) — Sole mention is the charity name 'Chernobyl Children Lifeline'; the disaster itself is never named. _(pattern: `chernobyl\s+children`)_
- `news` [https://www.leinsterexpress.ie/news/your-community/1963525/laois-rose-of-tralee-katelyn-cummins-to-hold-chernobyl-trip-fundraiser.html](https://www.leinsterexpress.ie/news/your-community/1963525/laois-rose-of-tralee-katelyn-cummins-to-hold-chernobyl-trip-fundraiser.html) — Both mentions are the 'Chernobyl Children International' fundraiser brand; the trip is to Poland. _(pattern: `chernobyl\s+children`)_
- `news` [https://www.limerickleader.ie/news/community/1964079/pensioner-to-repeat-charity-run-from-ballybunion-to-limerick-20-years-later.html](https://www.limerickleader.ie/news/community/1964079/pensioner-to-repeat-charity-run-from-ballybunion-to-limerick-20-years-later.html) — Both mentions are 'Chernobyl Children International'; the charity run is the subject, not the disaster. _(pattern: `chernobyl\s+children`)_
- `news` [https://www.newstalk.com/podcasts/highlights-from-the-hard-shoulder/reactions-to-zelenskyys-irish-visit](https://www.newstalk.com/podcasts/highlights-from-the-hard-shoulder/reactions-to-zelenskyys-irish-visit) — Sole mention is 'Chernobyl Children International' in a podcast guest credit. _(pattern: `chernobyl\s+children`)_
- `news` [https://www.theguardian.com/commentisfree/2025/dec/28/the-hill-i-will-die-on-faux-cyrillic](https://www.theguardian.com/commentisfree/2025/dec/28/the-hill-i-will-die-on-faux-cyrillic) — Only mention is the film title 'Chernobyl Diaries' in a column about faux-Cyrillic poster design. _(pattern: `chernobyl\s+diaries`)_
- `openalex` [https://openalex.org/W4406634430](https://openalex.org/W4406634430) — 'Chernobyl disaster optimization' is the CDO metaheuristic's fixed name, not the disaster. _(pattern: `chernobyl[\s-]*(?:disaster\s*)?optimi[sz](?:er|ation|ing)|chernobyl[\s-]inspired\s+optimi`)_
- `openalex` [https://openalex.org/W4406798844](https://openalex.org/W4406798844) — Uses the 'Chernobyl disaster optimizer (CDO)' metaheuristic to tune fuel cells. _(pattern: `chernobyl[\s-]*(?:disaster\s*)?optimi[sz](?:er|ation|ing)`)_
- `openalex` [https://openalex.org/W4409495706](https://openalex.org/W4409495706) — 'Chernobyl-Inspired Optimization' names the CDO metaheuristic, not the disaster. _(pattern: `chernobyl[\s-]inspired\s+optimi`)_
- `reddit` [https://reddit.com/r/BuyItForLife/comments/1hr5h94](https://reddit.com/r/BuyItForLife/comments/1hr5h94) — 'my original Chernobyl pack' — Chernobyl is a Cold Cold World backpack model. _(pattern: `chernobyl\s+pack`)_
- `reddit` [https://reddit.com/r/Craftmarijuana/comments/1hsc08k](https://reddit.com/r/Craftmarijuana/comments/1hsc08k) — 'Chernobyl' here is a cannabis strain crossed with 9lb Hammer and Skunk #1. _(pattern: `9\s*lb\s+hammer|skunk\s*#\s*1`)_
- `reddit` [https://reddit.com/r/DiscoElysium/comments/1hrljdv](https://reddit.com/r/DiscoElysium/comments/1hrljdv) — 'Was rewatching Chernobyl' — the HBO series, in a Disco Elysium sub. _(pattern: `(?:re)?watch(?:ing|ed)\s+chernobyl`)_
- `reddit` [https://reddit.com/r/GameTrade/comments/1hswgy6](https://reddit.com/r/GameTrade/comments/1hswgy6) — Game-key trade list containing 'Stalker shadow of Chernobyl'. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/ImmersiveSim/comments/1ht32tk](https://reddit.com/r/ImmersiveSim/comments/1ht32tk) — Games feedback list containing 'S.T.A.L.K.E.R.:Shadow of Chernobyl'. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/LetsChat/comments/1hr51y7](https://reddit.com/r/LetsChat/comments/1hr51y7) — Personal ad listing 'Chernobyl' among favourite TV shows. _(pattern: `chernobyl\s*(?:,|and)\s*(?:i\s+also|mindhunter|severance)`)_
- `reddit` [https://reddit.com/r/MakeFriendsUK/comments/1hr56du](https://reddit.com/r/MakeFriendsUK/comments/1hr56du) — Same personal ad crossposted; 'Chernobyl' is one of the favourite TV shows listed.
- `reddit` [https://reddit.com/r/MeetNewPeopleHere/comments/1hr55o5](https://reddit.com/r/MeetNewPeopleHere/comments/1hr55o5) — Same personal ad crossposted; 'Chernobyl' is one of the favourite TV shows listed.
- `reddit` [https://reddit.com/r/MovieRecommendations/comments/1hrizfz](https://reddit.com/r/MovieRecommendations/comments/1hrizfz) — Sole mention is 'the Chernobyl mini series' in a biopic recommendation thread. _(pattern: `chernobyl\s+mini[\s-]?series`)_
- `reddit` [https://reddit.com/r/Pampanga/comments/1hqzvv4](https://reddit.com/r/Pampanga/comments/1hqzvv4) — Sole mention is 'the mini series Chernobyl'; the post is about a Philippine gas-station sign. _(pattern: `mini[\s-]?series\s+chernobyl`)_
- `reddit` [https://reddit.com/r/PeepShowQuotes/comments/1hqtcsg](https://reddit.com/r/PeepShowQuotes/comments/1hqtcsg) — 'Rewatching Chernobyl' = the HBO miniseries, in a Peep Show quotes sub. _(pattern: `(?:re)?watch(?:ing|ed)\s+chernobyl`)_
- `reddit` [https://reddit.com/r/R4R30Plus/comments/1hr52xk](https://reddit.com/r/R4R30Plus/comments/1hr52xk) — Same personal ad crossposted; 'Chernobyl' is one of the favourite TV shows listed.
- `reddit` [https://reddit.com/r/YouTubeCreators/comments/1hspriz](https://reddit.com/r/YouTubeCreators/comments/1hspriz) — Game title 'STALKER 2: Heart of Chernobyl' in a playthrough promo. _(pattern: `heart\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/chernobyl/comments/1hra38w](https://reddit.com/r/chernobyl/comments/1hra38w) — Sole mention is 'watching Chernobyl again' — the HBO series. _(pattern: `(?:re)?watch(?:ing|ed)\s+chernobyl`)_
- `reddit` [https://reddit.com/r/chernobyl/comments/1hse60x](https://reddit.com/r/chernobyl/comments/1hse60x) — Both mentions are the book title 'Midnight in Chernobyl' by Adam Higginbotham. _(pattern: `midnight\s+in\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/chernobyl/comments/1hsy6mm](https://reddit.com/r/chernobyl/comments/1hsy6mm) — Sole mention is 'watching Chernobyl(2019)' — the HBO series. _(pattern: `chernobyl\s*\(?\s*2019\s*\)?`)_
- `reddit` [https://reddit.com/r/gamereviews/comments/1hr49er](https://reddit.com/r/gamereviews/comments/1hr49er) — Game title 'Stalker 2: Heart Of Chernobyl' (and a misspelling of the official Chornobyl). _(pattern: `heart\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/indiegameswap/comments/1hr5izo](https://reddit.com/r/indiegameswap/comments/1hr5izo) — Game-key sale: 'S.T.A.L.K.E.R 2 Heart of Chernobyl Ultimate Edition'. _(pattern: `heart\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/indiegameswap/comments/1hr5nb4](https://reddit.com/r/indiegameswap/comments/1hr5nb4) — Duplicate game-key sale listing for S.T.A.L.K.E.R 2 Heart of Chernobyl.
- `reddit` [https://reddit.com/r/indiegameswap/comments/1hr5oyk](https://reddit.com/r/indiegameswap/comments/1hr5oyk) — Duplicate game-key sale listing for S.T.A.L.K.E.R 2 Heart of Chernobyl.
- `reddit` [https://reddit.com/r/indiegameswap/comments/1hr5r8g](https://reddit.com/r/indiegameswap/comments/1hr5r8g) — Duplicate game-key sale listing for S.T.A.L.K.E.R 2 Heart of Chernobyl.
- `reddit` [https://reddit.com/r/indiegameswap/comments/1hr5xjf](https://reddit.com/r/indiegameswap/comments/1hr5xjf) — Duplicate game-key sale listing for S.T.A.L.K.E.R 2 Heart of Chernobyl.
- `reddit` [https://reddit.com/r/indiegameswap/comments/1hrbi57](https://reddit.com/r/indiegameswap/comments/1hrbi57) — Game-key sale: 'S.T.A.L.K.E.R 2 Heart of Chernobyl Ultimate Edition'. _(pattern: `heart\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/linux4noobs/comments/1hroijn](https://reddit.com/r/linux4noobs/comments/1hroijn) — Wine error log; every hit is the file path 'Stalker Shadow of Chernobyl\bin_x64'. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/moviecritic/comments/1hrbj3l](https://reddit.com/r/moviecritic/comments/1hrbj3l) — 'Chernobyl 2019 - best serial' — the HBO miniseries. _(pattern: `chernobyl\s*\(?\s*2019\s*\)?`)_
- `reddit` [https://reddit.com/r/movies/comments/1hr5rsg](https://reddit.com/r/movies/comments/1hr5rsg) — 'thought that Chernobyl was his first work' — Jared Harris and the HBO series.
- `reddit` [https://reddit.com/r/stalker/comments/1hqs9eu](https://reddit.com/r/stalker/comments/1hqs9eu) — Joke riff on the S.T.A.L.K.E.R. 2 title 'Heart of Chernobyl'; body is an in-game location. _(pattern: `heart\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hr2sap](https://reddit.com/r/stalker/comments/1hr2sap) — Game title 'S.T.A.L.K.E.R.: Shadow of Chernobyl' — a gameplay support question. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hr9tso](https://reddit.com/r/stalker/comments/1hr9tso) — Game title 'Shadow of Chernobyl' in a launch-crash support post. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hrkek5](https://reddit.com/r/stalker/comments/1hrkek5) — Sole mention is 'the mini series Chernobyl'. _(pattern: `mini[\s-]?series\s+chernobyl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hrsiwc](https://reddit.com/r/stalker/comments/1hrsiwc) — Steam release of the indie game 'Chernobyl 2 Exclusion Zone'. _(pattern: `chernobyl\s*2\s*exclu[sz]ion\s+zone`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hs2xp6](https://reddit.com/r/stalker/comments/1hs2xp6) — 'Shadow of Chernobyl playthrough' — the 2007 game. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hs40s1](https://reddit.com/r/stalker/comments/1hs40s1) — 'Freedom bug in Shadow of Chernobyl' — a gameplay bug report. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hs9leq](https://reddit.com/r/stalker/comments/1hs9leq) — 'Me rewatching Chernobyl … after playing Stalker 2' — the HBO series. _(pattern: `(?:re)?watch(?:ing|ed)\s+chernobyl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hsd8l8](https://reddit.com/r/stalker/comments/1hsd8l8) — Game title 'Chernobyl: Escape from Pripyat - Coming to PS5'. _(pattern: `chernobyl:?\s*escape\s+from\s+pripyat`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hsqk9d](https://reddit.com/r/stalker/comments/1hsqk9d) — Both mentions are the game 'Shadow of Chernobyl' in a series review. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hsqyab](https://reddit.com/r/stalker/comments/1hsqyab) — 'X16 lab in Shadow of Chernobyl' — an in-game armour question. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/stalker/comments/1hsyfrw](https://reddit.com/r/stalker/comments/1hsyfrw) — Title tag '[Shadow of Chernobyl - OGSR_Engine 2.1]' — the game. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `reddit` [https://reddit.com/r/television/comments/1ht29p8](https://reddit.com/r/television/comments/1ht29p8) — 'Chernobyl' is item 2 in a list of favourite TV season ones.
- `reddit` [https://reddit.com/r/televisionsuggestions/comments/1hs674x](https://reddit.com/r/televisionsuggestions/comments/1hs674x) — 'Chernobyl' appears only as an item in a list of TV shows.
- `reddit` [https://reddit.com/r/televisionsuggestions/comments/1hsm3ri](https://reddit.com/r/televisionsuggestions/comments/1hsm3ri) — 'Chernobyl' appears only in a list of series to binge.
- `reddit` [https://reddit.com/r/u_Kitty-Meowington/comments/1hreinn](https://reddit.com/r/u_Kitty-Meowington/comments/1hreinn) — Sole mention is 'the Chernobyl Mini Series on Prime TV'. _(pattern: `chernobyl\s+mini[\s-]?series`)_
- `reddit` [https://reddit.com/r/u_Wonderful-Ideal-6814/comments/1hsqqdh](https://reddit.com/r/u_Wonderful-Ideal-6814/comments/1hsqqdh) — S.T.A.L.K.E.R. fanfiction 'set in … Shadow of Chernobyl'. _(pattern: `shadow\s+of\s+cherno?byl`)_
- `youtube` [https://youtube.com/watch?v=9k6ukzg2DXA](https://youtube.com/watch?v=9k6ukzg2DXA) — All three mentions are HBO's series title (#Chernobyl #HBOseries); a clip-reaction short. _(pattern: `chernobyl\s*\(2019\)|#chernobyl\s*#hboseries`)_
- `youtube` [https://youtube.com/watch?v=WPBLid_j-P4](https://youtube.com/watch?v=WPBLid_j-P4) — Title is the HBO series; the description only ever calls it the miniseries ('Cherylobyl'). _(pattern: `chernobyl\s*\(2019\)`)_
- `youtube` [https://youtube.com/watch?v=XC3Z3rucUJ0](https://youtube.com/watch?v=XC3Z3rucUJ0) — Compares the HBO series with the anime Cyberpunk: Edgerunners; the title is the show. _(pattern: `chernobyl\s+vs\s+edgerunners`)_
- `youtube` [https://youtube.com/watch?v=YCZyHKi6dt4](https://youtube.com/watch?v=YCZyHKi6dt4) — Fan clip of the HBO series ('CHERNOBYL. Adam Nagaitis as Vasily Ignatenko'); no other use. _(pattern: `chernobyl\.\s*adam\s+nagaitis`)_
- `youtube` [https://youtube.com/watch?v=uC3iYYtnf4k](https://youtube.com/watch?v=uC3iYYtnf4k) — Song title 'CHERNOBYL (76 STICKS)' by the band hooligan chase — an auto-generated Topic upload. _(pattern: `chernobyl\s*\(?\s*76\s*sticks\)?|hooligan\s+chase`)_
- `youtube` [https://youtube.com/watch?v=vkyj0gqkjbE](https://youtube.com/watch?v=vkyj0gqkjbE) — Lyric video for the hooligan chase song 'CHERNOBYL (76 STICKS)'. _(pattern: `chernobyl\s*\(?\s*76\s*sticks\)?|hooligan\s+chase`)_

**chicken-kyiv** (13)

- `reddit` [https://reddit.com/r/SFM/comments/1jyf02v](https://reddit.com/r/SFM/comments/1jyf02v) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - r/SFM (Source Filmmaker) skit: 'his prized Chicken Kiev escapes captivity', i.e. the TF2 bird cosmetic, not food. _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `reddit` [https://reddit.com/r/Seaofthieves/comments/1hstg1f](https://reddit.com/r/Seaofthieves/comments/1hstg1f) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - body reads 'think something akin to chicken kiev from tf2'. _(pattern: `\b(?:tf2|team fortress 2|pootis)\b`)_
- `reddit` [https://reddit.com/r/TF2fashionadvice/comments/1jdeddd](https://reddit.com/r/TF2fashionadvice/comments/1jdeddd) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - title 'Chicken Kiev Cosmetic Combos' in a TF2 loadout subreddit. _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `reddit` [https://reddit.com/r/TF2fashionadvice/comments/1jdedzf](https://reddit.com/r/TF2fashionadvice/comments/1jdedzf) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - body: 'damn chicken Kiev looks so good and it is prob my favourite cosmetic'. _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `reddit` [https://reddit.com/r/minecraftskins/comments/1iajx99](https://reddit.com/r/minecraftskins/comments/1iajx99) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - body lists 'Pootis/Chicken Kiev' as TF2 cosmetics being copied into a Minecraft skin. _(pattern: `\b(?:tf2|team fortress 2|pootis)\b`)_
- `reddit` [https://reddit.com/r/tf2/comments/1iqdeg3](https://reddit.com/r/tf2/comments/1iqdeg3) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - the title is literally 'The cosmetic "Chicken Kiev" is misspelled in the wiki'. _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `reddit` [https://reddit.com/r/tf2/comments/1jde32t](https://reddit.com/r/tf2/comments/1jde32t) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - body: 'chicken Kiev ... it's prob my favourite cosmetic ... worth more than my whole inv'. _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `reddit` [https://reddit.com/r/tf2/comments/1k81n8k](https://reddit.com/r/tf2/comments/1k81n8k) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - body: 'I really need a pootis bird/chicken kiev hat ... should I just buy the Chicken Kiev now'. _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `reddit` [https://reddit.com/r/tf2/comments/1lo45l7](https://reddit.com/r/tf2/comments/1lo45l7) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - body: 'one is chicken kiev heavy when it's not Halloween' (the cosmetic on the Heavy class). _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `reddit` [https://reddit.com/r/tf2shitposterclub/comments/1lw31p9](https://reddit.com/r/tf2shitposterclub/comments/1lw31p9) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - r/tf2shitposterclub: 'The Chicken Kiev Is a Lie!' _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `reddit` [https://reddit.com/r/truetf2/comments/1ihviop](https://reddit.com/r/truetf2/comments/1ihviop) — Team Fortress 2 cosmetic named 'Chicken Kiev', not the dish - body: 'it is disappointing that I can't have my chicken Kiev on all the time' (equipping the cosmetic). _(pattern: `r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b`)_
- `youtube` [https://youtube.com/watch?v=8IF2fhgKmkU](https://youtube.com/watch?v=8IF2fhgKmkU) — TF2/SFM animation tagged #tf2 #sfm; '(Chicken Kiev)' names the Team Fortress 2 bird cosmetic, not the dish. _(pattern: `\b(?:tf2|team fortress 2|pootis)\b`)_
- `youtube` [https://youtube.com/watch?v=exeP2-rvbl4](https://youtube.com/watch?v=exeP2-rvbl4) — Team Fortress 2 / Source Filmmaker video; description reads 'Pootis Bird / Chicken Kiev and General Freedom Feathers' - the TF2 cosmetic, not the dish. _(pattern: `\b(?:tf2|team fortress 2|pootis)\b`)_

**kharkiv** (24)

- `reddit` [https://reddit.com/r/AskHistorians/comments/1i0n6ye](https://reddit.com/r/AskHistorians/comments/1i0n6ye) — Only mention is a citation of Jean Lopez's book title 'Kharkov 1942'; the poster's own prose never names the city. _(pattern: `jean\s+lopez`)_
- `reddit` [https://reddit.com/r/GatesOfHellOstfront/comments/1htbotn](https://reddit.com/r/GatesOfHellOstfront/comments/1htbotn) — Gates of Hell: Ostfront mission list; 'Kharkov' is a shipped skirmish-mission name. _(pattern: `r/gatesofhellostfront`)_
- `reddit` [https://reddit.com/r/HellLetLoose/comments/1hw0n0w](https://reddit.com/r/HellLetLoose/comments/1hw0n0w) — Hell Let Loose achievement bug report; 'Offense match on Kharkov' is the game map. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/HellLetLoose/comments/1hwvju8](https://reddit.com/r/HellLetLoose/comments/1hwvju8) — Hell Let Loose map bug: 'Arty in middle soviet HQ in Kharkov'. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/HellLetLoose/comments/1i2l5c9](https://reddit.com/r/HellLetLoose/comments/1i2l5c9) — Hell Let Loose rant: 'on a Kharkov match' - the game map. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/HellLetLoose/comments/1i5qcyp](https://reddit.com/r/HellLetLoose/comments/1i5qcyp) — Hell Let Loose map preferences: 'I like only Kharkov, Kursk, Stalingrad'. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/HellLetLoose/comments/1i6mob5](https://reddit.com/r/HellLetLoose/comments/1i6mob5) — Hell Let Loose prop-hitbox bug report '(Kharkov, Kursk)' - map names. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/HellLetLoose/comments/1iaevns](https://reddit.com/r/HellLetLoose/comments/1iaevns) — Hell Let Loose map-geometry complaint: 'the German spawn on Kharkov'. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/HellLetLoose/comments/1iantv9](https://reddit.com/r/HellLetLoose/comments/1iantv9) — Hell Let Loose map list: 'Kharkov taught me tanks or lose'. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/HellLetLoose/comments/1iggk9h](https://reddit.com/r/HellLetLoose/comments/1iggk9h) — Hell Let Loose story: 'playing on Warfare Kharkov' - the game map. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/HellLetLooseConsole/comments/1hxvgiv](https://reddit.com/r/HellLetLooseConsole/comments/1hxvgiv) — Hell Let Loose server map rotation: 'Stalingrad, Kharkov, Kursk'. _(pattern: `r/hellletloose|hell\s*let\s*loose`)_
- `reddit` [https://reddit.com/r/boardgames/comments/1ilc2g2](https://reddit.com/r/boardgames/comments/1ilc2g2) — Automated board-game crowdfunding roundup; mentions are the Kickstarter title 'Spring Prelude: Second Kharkov, May 1942'. _(pattern: `spring\s+prelude`)_
- `reddit` [https://reddit.com/r/hexandcounter/comments/1hv9b1g](https://reddit.com/r/hexandcounter/comments/1hv9b1g) — Same post crossposted; the mention is the board wargame title 'Kharkov: Battles Before and After Fall Blau'. _(pattern: `kharkov:?\s*battles\s+before\s+and\s+after`)_
- `reddit` [https://reddit.com/r/soloboardgaming/comments/1hv98xw](https://reddit.com/r/soloboardgaming/comments/1hv98xw) — Board wargame title 'Kharkov: Battles Before and After Fall Blau' (Compass Games), not the city. _(pattern: `kharkov:?\s*battles\s+before\s+and\s+after`)_
- `reddit` [https://reddit.com/r/victoria3/comments/1hzk1qq](https://reddit.com/r/victoria3/comments/1hzk1qq) — Victoria 3 gameplay; 'the states of Kiev, Kursk and Kharkov' are in-game state names. _(pattern: `r/victoria3`)_
- `youtube` [https://youtube.com/watch?v=1sv5y1WjWIE](https://youtube.com/watch?v=1sv5y1WjWIE) — World Conqueror 4 wargame playthrough; the name is the in-game scenario label, not the creator's own reference to the city.
- `youtube` [https://youtube.com/watch?v=HHauABBtvLc](https://youtube.com/watch?v=HHauABBtvLc) — Scale-model unboxing; both mentions quote the product name 'White Stork set F72055 Kharkov 1943 Part I'. _(pattern: `white\s+stork`)_
- `youtube` [https://youtube.com/watch?v=IVr27rmOsQg](https://youtube.com/watch?v=IVr27rmOsQg) — World of Tanks 'Kharkov' map (a deleted game map), not the city. _(pattern: `world\s+of\s+tanks`)_
- `youtube` [https://youtube.com/watch?v=PqR_M7bULw4](https://youtube.com/watch?v=PqR_M7bULw4) — Hell Let Loose gameplay; 'Kharkov' is the shipped game map ('the frozen hills of Kharkov... hold the line against the Germans'). _(pattern: `hell\s*let\s*loose|\bhll\b`)_
- `youtube` [https://youtube.com/watch?v=lt1dMrPNlwo](https://youtube.com/watch?v=lt1dMrPNlwo) — Auto-generated music release; 'Kharkov Never Sleeps' is a track title by SFA, not a reference to the city. _(pattern: `kharkov never sleeps`)_
- `youtube` [https://youtube.com/watch?v=q-s1i2Ya--0](https://youtube.com/watch?v=q-s1i2Ya--0) — Call of Duty: United Offensive walkthrough; 'Battle Of Kharkov' is the game's mission name. _(pattern: `call\s+of\s+duty`)_
- `youtube` [https://youtube.com/watch?v=uF3flIVl6fA](https://youtube.com/watch?v=uF3flIVl6fA) — Hell Let Loose commander gameplay; 'Kharkov' is the shipped game map. _(pattern: `hell\s*let\s*loose|\bhll\b`)_
- `youtube` [https://youtube.com/watch?v=uvZ7EiWFahA](https://youtube.com/watch?v=uvZ7EiWFahA) — Auto-generated music release: track 'Kharkov 8' by Russian artist Tsifey. _(pattern: `\bkharkov\s+8\b`)_
- `youtube` [https://youtube.com/watch?v=xXjVKJSIYWI](https://youtube.com/watch?v=xXjVKJSIYWI) — Music track from the World of Tanks soundtrack, named after the game's 'Kharkov' map. _(pattern: `world\s+of\s+tanks`)_

**ihor-sikorsky** (25)

- `news` [http://baystreet.ca/stockstowatch/20470/Safe-Pro-Sinks-on-Full-Year-Figures](http://baystreet.ca/stockstowatch/20470/Safe-Pro-Sinks-on-Full-Year-Figures) — Stock note listing 'the Igor Sikorsky Kyiv Polytechnic Institute' among Safe Pro partners. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://en.interfax.com.ua/news/economic/1095276.html](https://en.interfax.com.ua/news/economic/1095276.html) — Corporate partner list naming 'the Igor Sikorsky Kyiv Polytechnic Institute' — institution, not the man. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://en.interfax.com.ua/news/general/1107550.html](https://en.interfax.com.ua/news/general/1107550.html) — All three hits are the official name 'Igor Sikorsky Kyiv Polytechnic Institute' (Ajax lab opening) — the university, not the person. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://en.interfax.com.ua/news/press-conference/1121810.html](https://en.interfax.com.ua/news/press-conference/1121810.html) — 'Associate Professor of Igor Sikorsky KPI' — frozen official university name, not a reference to the man. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://fiia.fi/en/tapahtumat/a-presidential-election-in-belarus-the-crisis-continues-to-deepen](https://fiia.fi/en/tapahtumat/a-presidential-election-in-belarus-the-crisis-continues-to-deepen) — Event speaker affiliation: 'Associate Professor at Igor Sikorsky Kyiv Polytechnic Institute'. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://www.cjme.com/2025/08/15/the-evan-bray-show-segments-august-15-2025/](https://www.cjme.com/2025/08/15/the-evan-bray-show-segments-august-15-2025/) — 'visiting professor of sociology at Igor Sikorsky Kyiv Polytechnic Institute' — affiliation line naming the university. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://www.insidermonkey.com/blog/top-10-trending-ai-news-updates-that-investors-likely-missed-1463798/](https://www.insidermonkey.com/blog/top-10-trending-ai-news-updates-that-investors-likely-missed-1463798/) — 'MoU with the Igor Sikorsky Kyiv Polytechnic Institute (KPI)' — institutional counterparty name. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://www.marketscreener.com/quote/stock/SAFE-PRO-GROUP-INC-174322226/news/Safe-Pro-Group-Provides-First-Quarter-2025-Results-and-Corporate-Update-49970615/](https://www.marketscreener.com/quote/stock/SAFE-PRO-GROUP-INC-174322226/news/Safe-Pro-Group-Provides-First-Quarter-2025-Results-and-Corporate-Update-49970615/) — Safe Pro investor release listing 'the Igor Sikorsky Kyiv Polytechnic Institute' as a partner university. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://www.tuftsdaily.com/article/2025/03/ukraine-at-war-how-private-initiatives-help-ukrainian-universities-adjust-to-the-war](https://www.tuftsdaily.com/article/2025/03/ukraine-at-war-how-private-initiatives-help-ukrainian-universities-adjust-to-the-war) — 'located in the Igor Sikorsky Kyiv Polytechnic Institute Library' — building of the university, not the person. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `news` [https://www.universityworldnews.com/post.php?story=20251204120717166](https://www.universityworldnews.com/post.php?story=20251204120717166) — Only mention is the official institutional name 'Igor Sikorsky Kyiv Polytechnic Institute' (where a sociologist taught) — the university, not the aviation pioneer. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `reddit` [https://reddit.com/r/AerospaceEngineering/comments/1o7bqpq](https://reddit.com/r/AerospaceEngineering/comments/1o7bqpq) — 'graduated this year from the National Technical University of Ukraine Igor Sikorsky Kyiv Polytechnic Institute' — affiliation naming the university. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `reddit` [https://reddit.com/r/AzovUkraine/comments/1hvnlfa](https://reddit.com/r/AzovUkraine/comments/1hvnlfa) — Match is 'engineers who graduated from the Igor Sikorsky Kyiv Polytechnic Institute' — the university, not the man. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `reddit` [https://reddit.com/r/ProRussia_news/comments/1li5taz](https://reddit.com/r/ProRussia_news/comments/1li5taz) — 'the Igor Sikorsky Kyiv Polytechnic Institute building is on fire' — names the university, not the man. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `reddit` [https://reddit.com/r/UkraineRussiaReport/comments/1ldpvtc](https://reddit.com/r/UkraineRussiaReport/comments/1ldpvtc) — 'industrial zone adjacent to the Igor Sikorsky Kyiv International Airport' — the Zhuliany airport, not the man. _(pattern: `igor\s+sikorsky\s+(kyiv|kiev)\s+international\s+airport`)_
- `reddit` [https://reddit.com/r/UkraineRussiaReport/comments/1li4oyb](https://reddit.com/r/UkraineRussiaReport/comments/1li4oyb) — 'Igor Sikorsky Kyiv Polytechnic Institute burning after tonight's strikes' — genuine war coverage, but the surface form names the university, not the man. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `reddit` [https://reddit.com/r/UkraineRussiaReport/comments/1nonw37](https://reddit.com/r/UkraineRussiaReport/comments/1nonw37) — 'the parking lot at 478 Igor Sikorsky Street' in Zaporizhzhia — a street address, not the man. _(pattern: `igor\s+sikorsky\s+street`)_
- `youtube` [https://youtube.com/watch?v=3p0H1yLxDZg](https://youtube.com/watch?v=3p0H1yLxDZg) — Name appears only in the source credits as 'Igor Sikorsky Historical Archives' — the Stratford archive institution, not the man. _(pattern: `igor\s+sikorsky\s+historical\s+archives`)_
- `youtube` [https://youtube.com/watch?v=E9_1XhDZJnc](https://youtube.com/watch?v=E9_1XhDZJnc) — Highway junction list naming the 'Igor Sikorsky Bridge' on the Merritt Parkway — a structure, not the man. _(pattern: `igor\s+sikorsky\s+bridge`)_
- `youtube` [https://youtube.com/watch?v=ETXDcuRUne8](https://youtube.com/watch?v=ETXDcuRUne8) — Title is the official university name 'Igor Sikorsky Kiev Polytechnic Institute'. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `youtube` [https://youtube.com/watch?v=PJjWU8w1v48](https://youtube.com/watch?v=PJjWU8w1v48) — Roger Penrose honours list: 'Honorary Doctorate - Igor Sikorsky Kyiv Polytechnic Institute (Ukraine) - 2012' — the university. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `youtube` [https://youtube.com/watch?v=TBFSSJ6xhAg](https://youtube.com/watch?v=TBFSSJ6xhAg) — Same road-video junction list, 'Igor Sikorsky Bridge' — a structure, not the man. _(pattern: `igor\s+sikorsky\s+bridge`)_
- `youtube` [https://youtube.com/watch?v=U8kRJrPC5vY](https://youtube.com/watch?v=U8kRJrPC5vY) — Airport list: 'Igor Sikorsky Memorial (KBDR) in Bridgeport' — the Connecticut airport, not the man. _(pattern: `igor\s+sikorsky\s+memorial`)_
- `youtube` [https://youtube.com/watch?v=cYuN_Qfwqks](https://youtube.com/watch?v=cYuN_Qfwqks) — Wikipedia-copied blurb about 'The Igor Sikorsky Kyiv International Airport (Zhuliany)' — the airport, not the man. _(pattern: `igor\s+sikorsky\s+(kyiv|kiev)\s+international\s+airport`)_
- `youtube` [https://youtube.com/watch?v=mSciJOKY8sw](https://youtube.com/watch?v=mSciJOKY8sw) — Title is the official university name 'National Technical University of Ukraine Igor Sikorsky Kyiv Polytechnic Institute'. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_
- `youtube` [https://youtube.com/watch?v=u82slgrUwsc](https://youtube.com/watch?v=u82slgrUwsc) — Title is nothing but the official university name 'NATIONAL TECHNICAL UNIVERSITY OF UKRAINE IGOR SIKORSKY KYIV POLYTECHNIC INSTITUTE'. _(pattern: `igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)`)_

**kyivan-rus** (1)

- `youtube` [https://youtube.com/watch?v=O5YI0KWiWjQ](https://youtube.com/watch?v=O5YI0KWiWjQ) — All 4 hits are the Oxiwyle mobile game's product name and its Google Play / App Store URLs; the blurb never discusses the medieval state. _(pattern: `oxiwyle|com\.oxiwyle\.kievanrus|apps\.apple\.com/[a-z]{2}/app/kievan-rus`)_

**babyn-yar** (22)

- `news` [https://nuclear-news.net/2025/04/24/1-b1-drawing-inspiration-from-vaclav-havel/](https://nuclear-news.net/2025/04/24/1-b1-drawing-inspiration-from-vaclav-havel/) — Only hit is 'the Babi Yar symphony' — Shostakovich's work title; the ravine is referred to only as 'the worst pogrom'. _(pattern: `babi yar\s+symphony`)_
- `news` [https://slippedisc.com/2025/11/the-first-babi-yar-symphony-receuves-its-us-premiere/](https://slippedisc.com/2025/11/the-first-babi-yar-symphony-receuves-its-us-premiere/) — Only hit is the work nickname 'Babi Yar symphony'; the ravine itself is never named in the 665-char stub. _(pattern: `babi yar\s+symphony`)_
- `news` [https://www.gw2ru.com/arts/226082-100-main-russian-books](https://www.gw2ru.com/arts/226082-100-main-russian-books) — Only hit is the poem title in a '100 main Russian books' list: 'His poems Babi Yar and The Bratsk Station'. _(pattern: `poems?[^\n]{0,25}babi yar`)_
- `news` [https://www.islands.com/2055857/four-square-mile-tucked-between-downtown-denver-aurora-colorado-neighborhood-green-spaces-cozy-living/](https://www.islands.com/2055857/four-square-mile-tucked-between-downtown-denver-aurora-colorado-neighborhood-green-spaces-cozy-living/) — Only hit is Denver's 'Babi Yar Memorial Park' in a Colorado neighbourhood travel guide — a frozen 1970 US park name, not a live choice about the Kyiv site. _(pattern: `babi yar (?:memorial )?park`)_
- `news` [https://www.rri.ro/en/features-and-reports/world-of-culture/bucharest-international-film-festival-id937355.html](https://www.rri.ro/en/features-and-reports/world-of-culture/bucharest-international-film-festival-id937355.html) — Only hit is the title of Loznitsa's documentary listed alongside 'State Funeral' in a film-festival roundup.
- `news` [https://www.turlockjournal.com/news/local/5-things-to-do-this-weekend-nov-14-16-2025/](https://www.turlockjournal.com/news/local/5-things-to-do-this-weekend-nov-14-16-2025/) — Only hit is the fixed work title of Klebanov's Symphony No. 1 'In Memoriam Babi Yar' in a weekend events listing. _(pattern: `in memoriam[^\n]{0,15}babi yar`)_
- `news` [https://www.westword.com/arts-culture/fall-2025-concerts-events-markets-more-25192591/](https://www.westword.com/arts-culture/fall-2025-concerts-events-markets-more-25192591/) — Only hit is the proper name of Denver's annual 'Babi Yar Commemoration' event at Babi Yar Park, Colorado — a frozen US institution name. _(pattern: `babi yar commemoration`)_
- `reddit` [https://reddit.com/r/Indianbooks/comments/1igtjqp](https://reddit.com/r/Indianbooks/comments/1igtjqp) — Only hit is a filename in a dumped PDF catalogue of an Indian-books thread: '>Babi Yar Symphony.pdf'. _(pattern: `babi yar symphony`)_
- `reddit` [https://reddit.com/r/RussianLiterature/comments/1ithgft](https://reddit.com/r/RussianLiterature/comments/1ithgft) — Only hit is the poem title in a Russian-poetry anthology post: 'In *Babi Yar*, he condemned Soviet attempts to erase Jewish suffering'.
- `reddit` [https://reddit.com/r/VerdunGame/comments/1jwliw1](https://reddit.com/r/VerdunGame/comments/1jwliw1) — Only hit is the work title in a gameplay-video credit: 'I used Shostakovich No. 13 Op. 113 Babi Yar'. _(pattern: `op\.?\s*113[^\n]{0,25}babi yar`)_
- `reddit` [https://reddit.com/r/classicalmusic/comments/1keqyxy](https://reddit.com/r/classicalmusic/comments/1keqyxy) — Whole post is about the recording of Shostakovich's Symphony No 13 Babi Yar; both hits are the work title. _(pattern: `symphony no\.?\s*13[^\n]{0,25}babi yar`)_
- `reddit` [https://reddit.com/r/theIrishleft/comments/1o5ko41](https://reddit.com/r/theIrishleft/comments/1o5ko41) — Only hit is a linked article headline in a newspaper table of contents: "Shostakovich's 13th Symphony 'Babi Yar'". _(pattern: `13th symphony[^\n]{0,20}babi yar`)_
- `youtube` [https://youtube.com/watch?v=2R2Rx3AzzRI](https://youtube.com/watch?v=2R2Rx3AzzRI) — Only hit is the work title in a repertoire list: 'Shostakovich: Symphony No. 13 Babi Yar'. _(pattern: `symphony no\.?\s*13[^\n]{0,25}babi yar`)_
- `youtube` [https://youtube.com/watch?v=6txmZvngn0g](https://youtube.com/watch?v=6txmZvngn0g) — Only hit is the work title in a choral-repertoire list: 'Shostakovich: Symphony No. 13 Babi Yar'. _(pattern: `symphony no\.?\s*13[^\n]{0,25}babi yar`)_
- `youtube` [https://youtube.com/watch?v=8X-Ojri39MY](https://youtube.com/watch?v=8X-Ojri39MY) — French concert upload; all eight hits are the printed work title 'Symphonie n°13 Babi Yar' and its movement headings. _(pattern: `op\.?\s*113[^\n]{0,25}babi yar`)_
- `youtube` [https://youtube.com/watch?v=DfT03fiuw70](https://youtube.com/watch?v=DfT03fiuw70) — Only hit is a chapter marker for the work: '11:12- 11:54 Shostakovich Babi Yar Symphony'. _(pattern: `babi yar.{0,2}\s*symphony`)_
- `youtube` [https://youtube.com/watch?v=MxRoLxw2IgQ](https://youtube.com/watch?v=MxRoLxw2IgQ) — Duplicate vlog of Denver's 'Babi Yar Park! Denver, Colorado' — frozen US park name. _(pattern: `babi yar park`)_
- `youtube` [https://youtube.com/watch?v=Nit_ZbIp2iw](https://youtube.com/watch?v=Nit_ZbIp2iw) — Concert upload named for the Shostakovich work ('Babi Yar Concert - 2025'); the description is a Christmas-concert blurb with no reference to the ravine. _(pattern: `babi yar concert`)_
- `youtube` [https://youtube.com/watch?v=QD_2RrN1Sew](https://youtube.com/watch?v=QD_2RrN1Sew) — Auto-generated 'Topic' channel track metadata: 'Symphony No. 13 in B-Flat Minor, Op. 113 Babi Yar: V. Career' — record catalogue title. _(pattern: `op\.?\s*113[^\n]{0,25}babi yar`)_
- `youtube` [https://youtube.com/watch?v=Xet0jPmu1sY](https://youtube.com/watch?v=Xet0jPmu1sY) — Only hit is a chapter marker for the music, '4:20 Music: Babi Yar on Location' — Shostakovich's 13th, not the ravine.
- `youtube` [https://youtube.com/watch?v=eqTyI1j9Zzg](https://youtube.com/watch?v=eqTyI1j9Zzg) — Only hit is the work title in a decade-by-decade symphony list: 'Shostakovich: Symphony No. 13 Babi Yar (1962)'. _(pattern: `symphony no\.?\s*13[^\n]{0,25}babi yar`)_
- `youtube` [https://youtube.com/watch?v=zLmCBi9ZamA](https://youtube.com/watch?v=zLmCBi9ZamA) — Vlog about Denver's 'Babi Yar Park!! Denver, Colorado' (10451 E Yale Ave) — frozen US park name, not the Kyiv site. _(pattern: `babi yar park`)_

**oleksandr-usyk** (1)

- `news` [https://www.hotpress.com/music/hardwicke-circuss-ukraine-tour-diary-part-4-heroes-of-the-revolution-23096793](https://www.hotpress.com/music/hardwicke-circuss-ukraine-tour-diary-part-4-heroes-of-the-revolution-23096793) — The article's ONLY Usyk mention is a song title: "We dedicated 'Ballad Of Alexander Usyk' to Budgie" — Hardwicke Circus's own track name in a music tour diary, a frozen work title whose spelling no journalist chose. _(pattern: `ballad\s+of\s+alexander\s+usyk`)_

**kazymyr-malevych** (3)

- `news` [https://www.grandforksherald.com/lifestyle/arts-and-entertainment/north-dakota-museum-of-art-to-host-gallery-talk-film-screenings-to-complement-ukraine-exhibition](https://www.grandforksherald.com/lifestyle/arts-and-entertainment/north-dakota-museum-of-art-to-host-gallery-talk-film-screenings-to-complement-ukraine-exhibition) — Sole occurrence is the fixed award title 'Kazimir Malevich Artist Prize' in a bio of filmmaker Kakhidze; the article is not about the painter and the spelling is locked by the award's official name. _(pattern: `kazimir malevich artist (?:prize|award)`)_
- `youtube` [https://youtube.com/watch?v=GZwAcrwZ04U](https://youtube.com/watch?v=GZwAcrwZ04U) — The match is a track title in a sample-credit list ('Patricia Taxxon - Kazimir Malevich'), i.e. the name of a song, not a reference to the painter. _(pattern: `patricia taxxon\s*[-–]\s*kazimir malevich`)_
- `youtube` [https://youtube.com/watch?v=S_vHhH2BN5o](https://youtube.com/watch?v=S_vHhH2BN5o) — Only occurrence is the fixed award title 'Kazimir Malevich Artist Award established by the Polish Institute in Ukraine' inside a guest bio; not a reference to the painter himself. _(pattern: `kazimir malevich artist (?:prize|award)`)_

**ternopil** (3)

- `youtube` [https://youtube.com/watch?v=GA3i82daWEg](https://youtube.com/watch?v=GA3i82daWEg) — Same Istanbul leather brand: "Denim Spirit – Structured Shearling Jacket by Ternopol" — brand name, not the city. _(pattern: `ternopol\s*leather`)_
- `youtube` [https://youtube.com/watch?v=GKQHDlzDOzE](https://youtube.com/watch?v=GKQHDlzDOzE) — Channel "Ternopol Leather" is an Istanbul shearling-jacket brand (ternopolleather.com, "handcrafted in Istanbul") — a product name, not the Ukrainian city. _(pattern: `ternopol\s*leather`)_
- `youtube` [https://youtube.com/watch?v=J3x8aSIcdEA](https://youtube.com/watch?v=J3x8aSIcdEA) — Same Istanbul leather brand ("Black Shearling Coat … | Ternopol Leather"); not in youtube_titles.json but the stored channel+title identify it unambiguously. _(pattern: `ternopol\s*leather`)_

**volodymyr-the-great** (2)

- `openalex` [https://openalex.org/W4411695148](https://openalex.org/W4411695148) — The matched string is the fixed title of F. P. Klyucharev's tragedy (rendered from the Russian «Владимир Великий»): 'compare the tragedies Vladimir the Great by F. P. Klyucharev and Idylo-Worshippers, or Gorislava'; the prince himself is called 'Prince Vladimir'/'Vladimir' throughout the same abstract, so the epithet is never used to name him. _(pattern: `klyucharev`)_
- `youtube` [https://youtube.com/watch?v=tBgvGzfNfOw](https://youtube.com/watch?v=tBgvGzfNfOw) — Polish military-history wojak video; the phrase occurs only in the music-credits list as a song title ('Old East Slavic Song - Vladimir the Great'), the prince is never discussed.

**bakhmut** (4)

- `reddit` [https://reddit.com/r/Blooddebt/comments/1og026p](https://reddit.com/r/Blooddebt/comments/1og026p) — Video-game map name, not the city: "where can I get new maps like Kovalevsky's dinner, Artemovsk overpass, cafe pages" in r/Blooddebt (the game Blood Debt). _(pattern: `artemovsk\s+overpass`)_
- `youtube` [https://youtube.com/watch?v=2bMWgo0yXvs](https://youtube.com/watch?v=2bMWgo0yXvs) — Game-map name, not the city: "Artemovsk Overpass - teambattle random gameplay [Blood debt]", description "#blooddebt nothing to see here". _(pattern: `artemovsk\s+overpass`)_
- `youtube` [https://youtube.com/watch?v=7tO24iKJfl0](https://youtube.com/watch?v=7tO24iKJfl0) — Auto-generated YouTube Music "Topic" upload: track/album titled "Artemovsk" by Iz_Rem_Records, label PEREGRINO, "Auto-generated by YouTube" — a music release name, not running text about the city. _(pattern: `\s-\s*topic:`)_
- `youtube` [https://youtube.com/watch?v=G6DfBJa6XY8](https://youtube.com/watch?v=G6DfBJa6XY8) — Auto-generated YouTube Music "Topic" upload; the track is "Alena" and "Artemovsk" is only the release/album title in the boilerplate ("Provided to YouTube by Label Worx Limited... Alena · Iz_Rem_Records / Artemovsk... Auto-generated by YouTube") — a product name, not running text. _(pattern: `\s-\s*topic:`)_

**dnipro-river** (1)

- `reddit` [https://reddit.com/r/23andme/comments/1ki31wf](https://reddit.com/r/23andme/comments/1ki31wf) — 'Belarusian Dnieper River' is 23andMe's fixed ancestry-region label; the poster is quoting a product region name, not choosing a spelling in running text. _(pattern: `belarusian\s+dnieper\s+river`)_

### wrong-referent — 158 drops

**odesa** (13)

- `news` [https://amimagazine.org/2025/10/02/nazis-swiss-banks-the-jewish-money-that-vanished-2/](https://amimagazine.org/2025/10/02/nazis-swiss-banks-the-jewish-money-that-vanished-2/) — All 4 hits are 'the Odessa Group', the postwar Nazi ODESSA escape network, not the city. _(pattern: `odessa\s+(?:group|network|organi[sz]ation)`)_
- `news` [https://screenrant.com/gen-v-season-2-cipher-angelina-jolie-reference-explained/](https://screenrant.com/gen-v-season-2-cipher-angelina-jolie-reference-explained/) — Both hits are 'Project Odessa', a fictional Vought program in the TV series Gen V. _(pattern: `project\s+odessa`)_
- `openalex` [https://openalex.org/W7121614562](https://openalex.org/W7121614562) — 'ODESSA Prometheus 25' is an acronym for an LLM harmful-code evaluation protocol, not the city. _(pattern: `odessa\s+(?:v\d|prometheus)`)_
- `youtube` [https://youtube.com/watch?v=0kHM3a1ebAY](https://youtube.com/watch?v=0kHM3a1ebAY) — 'Gen V Project Odessa EXPLAINED' - the fictional Vought program. _(pattern: `project\s+odessa|odessa\s+baby`)_
- `youtube` [https://youtube.com/watch?v=1nZ96unO4Mc](https://youtube.com/watch?v=1nZ96unO4Mc) — 'Project Odessa' is a fictional Vought program in Gen V, not the city. _(pattern: `project\s+odessa|odessa\s+baby`)_
- `youtube` [https://youtube.com/watch?v=44Dy1X6znRk](https://youtube.com/watch?v=44Dy1X6znRk) — 'Marie and Homelander Survived Project Odessa' - Gen V fiction. _(pattern: `project\s+odessa|odessa\s+baby`)_
- `youtube` [https://youtube.com/watch?v=AnT0ljmRFrU](https://youtube.com/watch?v=AnT0ljmRFrU) — 'Who is the stronger Odessa Baby?' - Project Odessa from Gen V / The Boys. _(pattern: `project\s+odessa|odessa\s+baby`)_
- `youtube` [https://youtube.com/watch?v=DMswVaemfDU](https://youtube.com/watch?v=DMswVaemfDU) — 'Gen V - Project Odessa EXPLAINED' - the fictional Vought program. _(pattern: `project\s+odessa|odessa\s+baby`)_
- `youtube` [https://youtube.com/watch?v=lQoEj9RbAkg](https://youtube.com/watch?v=lQoEj9RbAkg) — Spanish Gen V Short; #odessa is the fictional Project Odessa. _(pattern: `project\s+odessa|odessa\s+baby`)_
- `youtube` [https://youtube.com/watch?v=nS09QuYr2OQ](https://youtube.com/watch?v=nS09QuYr2OQ) — 'The network known as ODESSA' - the SS escape organisation, not the city. _(pattern: `odessa\s+(?:network|group|organi[sz]ation)`)_
- `youtube` [https://youtube.com/watch?v=oNaucnVJ4U0](https://youtube.com/watch?v=oNaucnVJ4U0) — Percy Jackson / EPIC the Musical fanart; #odessa is unrelated hashtag stuffing.
- `youtube` [https://youtube.com/watch?v=r5XJlDIsMyQ](https://youtube.com/watch?v=r5XJlDIsMyQ) — 'Glenda & Odessa | Garfield' - a children's cartoon upload, not the city.
- `youtube` [https://youtube.com/watch?v=reIU_b2cSeQ](https://youtube.com/watch?v=reIU_b2cSeQ) — 'Odessa-Doran Loop' - an off-road trail at Calico, California. _(pattern: `odessa[\s-]*doran|odessa\s+canyon`)_

**kyiv** (12)

- `news` [https://wordswithoutborders.org/read/article/2014-09/the-stone-guest/](https://wordswithoutborders.org/read/article/2014-09/the-stone-guest/) — All 3 matches are "Kiev Station", Moscow's Kiyevsky rail terminal, in a translated Uzbek short story set in Moscow. _(pattern: `kiev\s+station`)_
- `news` [https://www.birminghammail.co.uk/whats-on/food-drink-news/full-menu-prices-glynn-purnell-33031540](https://www.birminghammail.co.uk/whats-on/food-drink-news/full-menu-prices-glynn-purnell-33031540) — "Pheasant Kiev for £25" on a restaurant menu - the chicken-Kiev dish (chicken-kyiv pair), not the city. _(pattern: `\b(?:chicken|pheasant|frog\s+legs?|mushroom|garlic|veggie|vegan|salmon|turkey|beef|cod)\s+(?:a\s*la\s+|ala\s+)?kiev\b`)_
- `news` [https://www.grubstreet.com/article/first-taste-wild-cherry-in-the-west-village.html](https://www.grubstreet.com/article/first-taste-wild-cherry-in-the-west-village.html) — Sole match is "Frog Legs Kiev", a restaurant dish riffing on chicken Kiev. _(pattern: `\b(?:chicken|pheasant|frog\s+legs?|mushroom|garlic|veggie|vegan|salmon|turkey|beef|cod)\s+(?:a\s*la\s+|ala\s+)?kiev\b`)_
- `news` [https://www.news18.com/lifestyle/food/thanksgiving-2025-the-ultimate-guide-to-festive-dining-across-india-9732038.html](https://www.news18.com/lifestyle/food/thanksgiving-2025-the-ultimate-guide-to-festive-dining-across-india-9732038.html) — Sole match is "Chicken Ala Kiev" on a Thanksgiving menu - the dish. _(pattern: `\b(?:chicken|pheasant|frog\s+legs?|mushroom|garlic|veggie|vegan|salmon|turkey|beef|cod)\s+(?:a\s*la\s+|ala\s+)?kiev\b`)_
- `reddit` [https://reddit.com/r/AzureLane/comments/1hsxy77](https://reddit.com/r/AzureLane/comments/1hsxy77) — "Kiev and Kursk usually sleep with their eyes wide open" - Azur Lane shipgirl characters (anthropomorphised Soviet warships). _(pattern: `azur\s*lane|azurlane`)_
- `reddit` [https://reddit.com/r/Azurlanefanfics/comments/1htyoa7](https://reddit.com/r/Azurlanefanfics/comments/1htyoa7) — "it resembles Kiev's rigging" - Azur Lane shipgirl character, not the city. _(pattern: `azur\s*lane|azurlane`)_
- `reddit` [https://reddit.com/r/Seaofthieves/comments/1hstg1f](https://reddit.com/r/Seaofthieves/comments/1hstg1f) — "something akin to chicken kiev from tf2" - the dish / a Team Fortress 2 cosmetic. _(pattern: `\b(?:chicken|pheasant|frog\s+legs?|mushroom|garlic|veggie|vegan|salmon|turkey|beef|cod)\s+(?:a\s*la\s+|ala\s+)?kiev\b`)_
- `reddit` [https://reddit.com/r/WoWs_Legends/comments/1hv1xbq](https://reddit.com/r/WoWs_Legends/comments/1hv1xbq) — "Tier VI Destroyer Kiev" in a World of Warships ship list - the warship, not the city. _(pattern: `kiev.{0,40}\bdestroyer\b`)_
- `youtube` [https://youtube.com/watch?v=8Lyaodks9aU](https://youtube.com/watch?v=8Lyaodks9aU) — "Best chicken a la Kiev in Bombay" - the dish. _(pattern: `\b(?:chicken|pheasant|frog\s+legs?|mushroom|garlic|veggie|vegan|salmon|turkey|beef|cod)\s+(?:a\s*la\s+|ala\s+)?kiev\b`)_
- `youtube` [https://youtube.com/watch?v=S3DjQ32s6qg](https://youtube.com/watch?v=S3DjQ32s6qg) — "Kiev - The Destroyer That Made Me Known" is the World of Warships Tier-VI destroyer, not the city. _(pattern: `kiev.{0,40}\bdestroyer\b`)_
- `youtube` [https://youtube.com/watch?v=YbzSZsxGmrI](https://youtube.com/watch?v=YbzSZsxGmrI) — "Make Delicious Chicken Ala Kiev at Home" - a recipe for the dish. _(pattern: `\b(?:chicken|pheasant|frog\s+legs?|mushroom|garlic|veggie|vegan|salmon|turkey|beef|cod)\s+(?:a\s*la\s+|ala\s+)?kiev\b`)_
- `youtube` [https://youtube.com/watch?v=iCuBqbD-GsE](https://youtube.com/watch?v=iCuBqbD-GsE) — Review of "the chicken a la Kiev at Mocambo in Kolkata" - the dish, not the city. _(pattern: `\b(?:chicken|pheasant|frog\s+legs?|mushroom|garlic|veggie|vegan|salmon|turkey|beef|cod)\s+(?:a\s*la\s+|ala\s+)?kiev\b`)_

**lviv** (1)

- `reddit` [https://reddit.com/r/RHOBH/comments/1intigh](https://reddit.com/r/RHOBH/comments/1intigh) — r/RHOBH: 'Number one being Lvov's and Giggy!' - a garbling of 'LVP's' (Lisa Vanderpump, Giggy's owner); a Real Housewives nickname, not the city. _(pattern: `\blvov(?:'|’)?s\b[^.]{0,40}\bgiggy\b`)_

**mykola-hohol** (97)

- `reddit` [https://reddit.com/r/BlueLock/comments/1jp9k86](https://reddit.com/r/BlueLock/comments/1jp9k86) — r/BlueLock: 'included Nikolai Gogol from BSD ... their birthdays also fall on april first'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/BungouSimpBattles/comments/1hrhshl](https://reddit.com/r/BungouSimpBattles/comments/1hrhshl) — r/BungouSimpBattles character-of-the-day post: 'Art credits: Screenshot from the official anime'.
- `reddit` [https://reddit.com/r/BungouStrayDogs/comments/1hqyj1d](https://reddit.com/r/BungouStrayDogs/comments/1hqyj1d) — r/BungouStrayDogs poll results: 'Nikolai Gogol - 32 votes' - the anime character.
- `reddit` [https://reddit.com/r/BungouStrayDogs/comments/1iwdaji](https://reddit.com/r/BungouStrayDogs/comments/1iwdaji) — r/BungouStrayDogs ship list: 'Sigma/Nikolai Gogol ... Nikolai Gogol/Dazai Osamu'.
- `reddit` [https://reddit.com/r/BungouStrayDogs/comments/1ix3tx0](https://reddit.com/r/BungouStrayDogs/comments/1ix3tx0) — r/BungouStrayDogs fan art titled 'Nikolai Gogol'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/BungouStrayDogs/comments/1ix46qs](https://reddit.com/r/BungouStrayDogs/comments/1ix46qs) — r/BungouStrayDogs fan art ('This art belongs to me'). _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/BungouStrayDogs/comments/1iziph2](https://reddit.com/r/BungouStrayDogs/comments/1iziph2) — r/BungouStrayDogs fan art titled 'Nikolai Gogol'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/BungouStrayDogs/comments/1jp6sre](https://reddit.com/r/BungouStrayDogs/comments/1jp6sre) — r/BungouStrayDogs: 'Happy birthday to Nikolai Gogol (the silly!!)' - the character's 1 April birthday. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/BungouStrayDogs/comments/1jpdl8z](https://reddit.com/r/BungouStrayDogs/comments/1jpdl8z) — r/BungouStrayDogs: "the most sane character in all of BSD Nikolai Gogol's Birthday". _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/BungouStrayDogs/comments/1js885b](https://reddit.com/r/BungouStrayDogs/comments/1js885b) — r/BungouStrayDogs: 'the japanese va of Nikolai Gogol' - the anime character's voice actor.
- `reddit` [https://reddit.com/r/CharacterAI/comments/1k8z0ex](https://reddit.com/r/CharacterAI/comments/1k8z0ex) — r/CharacterAI: 'Im doing a bsd Nikolai Gogol royal au'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1hxibdd](https://reddit.com/r/DeathBattleMatchups/comments/1hxibdd) — r/DeathBattleMatchups 'Madcap VS Nikolai Gogol (Marvel VS Bungo Stray Dogs)' - the anime character. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1i4yyfk](https://reddit.com/r/DeathBattleMatchups/comments/1i4yyfk) — r/DeathBattleMatchups Marvel-vs-BSD matchup. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1i4yz2o](https://reddit.com/r/DeathBattleMatchups/comments/1i4yz2o) — r/DeathBattleMatchups Marvel-vs-BSD matchup (duplicate submission). _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1icuj3h](https://reddit.com/r/DeathBattleMatchups/comments/1icuj3h) — r/DeathBattleMatchups Marvel-vs-BSD matchup. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1jmkh26](https://reddit.com/r/DeathBattleMatchups/comments/1jmkh26) — r/DeathBattleMatchups 'Nikolai Gogol VS Madcap (Bungo Stray Dogs VS Marvel)'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1jufhze](https://reddit.com/r/DeathBattleMatchups/comments/1jufhze) — r/DeathBattleMatchups Bungo-Stray-Dogs-vs-Marvel matchup. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1k3h3ae](https://reddit.com/r/DeathBattleMatchups/comments/1k3h3ae) — r/DeathBattleMatchups Bungo-Stray-Dogs-vs-Marvel matchup. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1kc2q09](https://reddit.com/r/DeathBattleMatchups/comments/1kc2q09) — r/DeathBattleMatchups Bungo-Stray-Dogs-vs-Marvel matchup. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/GachaLife2/comments/1jrhquk](https://reddit.com/r/GachaLife2/comments/1jrhquk) — r/GachaLife2 voice claims: 'Nikolai Gogol (Bungou Stray Dogs)'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/GachaLife2/comments/1jrhqyb](https://reddit.com/r/GachaLife2/comments/1jrhqyb) — r/GachaLife2 duplicate voice-claim post naming the anime character. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/Ratorix/comments/1j8xtgf](https://reddit.com/r/Ratorix/comments/1j8xtgf) — Italian-language post: 'coi vestiti di Nikolai Gogol (bungo stray dogs se qlcn e interessato)'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/autism/comments/1k9rbne](https://reddit.com/r/autism/comments/1k9rbne) — r/autism: 'Nikolai Gogol from Bungou Stray Dogs' among anime characters. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/fictionkin/comments/1jm86nr](https://reddit.com/r/fictionkin/comments/1jm86nr) — r/fictionkin kinlist entry 'nikolai gogol (bungo stray dogs)'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/fictionkin/comments/1juwl2a](https://reddit.com/r/fictionkin/comments/1juwl2a) — r/fictionkin: 'Nikolai Gogol from Bungou Stray Dogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/fictkin/comments/1jzfv2r](https://reddit.com/r/fictkin/comments/1jzfv2r) — r/fictkin: 'Nikolai Gogol, Bungou Stray Dogs, high kin'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/teenagers/comments/1kv7o75](https://reddit.com/r/teenagers/comments/1kv7o75) — r/teenagers: 'Cosplay: Nikolai Gogol from bungo stray dogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `reddit` [https://reddit.com/r/teenarazzi/comments/1ktxt9g](https://reddit.com/r/teenarazzi/comments/1ktxt9g) — r/teenarazzi: 'Nekolai Google ahh (Nikolai Gogol from Bungo Stray Dogs)'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=-89cZwj2n-w](https://youtube.com/watch?v=-89cZwj2n-w) — 'Nikolai Gogol doll #bsd #bungoustraydogs' papercraft of the anime character. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=-Kvw2OPXsh8](https://youtube.com/watch?v=-Kvw2OPXsh8) — BSD character post, '#anime #bungoustraydogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=-kYnhqdD55Q](https://youtube.com/watch?v=-kYnhqdD55Q) — 'HALLOWEEN SPECIAL NIKOLAI GOGOL FROM BSD FANART'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=0IQKHHqStiw](https://youtube.com/watch?v=0IQKHHqStiw) — 'Nikolai Gogol sketch' tagged #animeart - BSD character fan art. _(pattern: `#animeart|animeart`)_
- `youtube` [https://youtube.com/watch?v=0WfSVfOKSbA](https://youtube.com/watch?v=0WfSVfOKSbA) — BSD character edit, '#bsdedits #nikolaigogol'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=0yUYTRFnG_E](https://youtube.com/watch?v=0yUYTRFnG_E) — 'Propose.//Nikolai Gogol|| BSD!!'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=10GkGGjC57c](https://youtube.com/watch?v=10GkGGjC57c) — Anime-character personality list including Nikolai Gogol (Bungo Stray Dogs).
- `youtube` [https://youtube.com/watch?v=1C1Y5RvMwgQ](https://youtube.com/watch?v=1C1Y5RvMwgQ) — 'humor/bsd/Nikolai Gogol'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=1KIFfCtINfk](https://youtube.com/watch?v=1KIFfCtINfk) — 'Character: Nikolai Gogol | Show: Bungo Stray Dogs (BSD)'.
- `youtube` [https://youtube.com/watch?v=2-ymBZUEkp0](https://youtube.com/watch?v=2-ymBZUEkp0) — 'me nikolai gogol #nikolaigogol #bungostraydogs' - anime fan channel. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=5UVNuiS0ryo](https://youtube.com/watch?v=5UVNuiS0ryo) — 'My top Bsd characters' listing Nikolai Gogol; 'Anime/Show: Bungou Stray Dogs'.
- `youtube` [https://youtube.com/watch?v=5itQYhg7KfI](https://youtube.com/watch?v=5itQYhg7KfI) — 'ft: nikolai gogol and fyodor' BSD edit. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=5tMUwd1_abk](https://youtube.com/watch?v=5tMUwd1_abk) — BSD character edit, '#bsd #nikolaigogol #bungoustraydogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=65se2ARU9Vc](https://youtube.com/watch?v=65se2ARU9Vc) — BSD ship edit (Fyodor x Nikolai) tagged #bungoustraydogs. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=8p23Igc0DGc](https://youtube.com/watch?v=8p23Igc0DGc) — BSD lyric edit; character list includes Nikolai Gogol alongside Sigma and Akutagawa.
- `youtube` [https://youtube.com/watch?v=B6mhnSwSb_E](https://youtube.com/watch?v=B6mhnSwSb_E) — 'Characters: Sigma, Nikolai Gogol, Fyodor Dostoevsky | Fandom: Bungo Stray Dogs'.
- `youtube` [https://youtube.com/watch?v=DP0IdKGufCc](https://youtube.com/watch?v=DP0IdKGufCc) — 'Bungo Stray Dogs || Nikolai Gogol BSD edit' (Portuguese-language fan edit). _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=JUfY64o5RVo](https://youtube.com/watch?v=JUfY64o5RVo) — BSD crack video; 'Nikolai gogol' in a cast list of anime characters.
- `youtube` [https://youtube.com/watch?v=K415baoNiF4](https://youtube.com/watch?v=K415baoNiF4) — BSD character edit, '#bungoustraydogs #bsd'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=KS5Xjnw82VA](https://youtube.com/watch?v=KS5Xjnw82VA) — BSD character edit, '#bungoustraydogs #bsd' (Portuguese song title). _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=KycjBY8jbVU](https://youtube.com/watch?v=KycjBY8jbVU) — BSD character edit, '#bungostraydogs #bsd #anime'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=L76IUODTiTs](https://youtube.com/watch?v=L76IUODTiTs) — 'Character: Nikolai Gogol | Anime: Bungou Stray Dogs' crossover with Laughing Jack. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=Lz0R4aGiigU](https://youtube.com/watch?v=Lz0R4aGiigU) — BSD character edit, '#bsd #animeedit'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=MEcubMUwCIY](https://youtube.com/watch?v=MEcubMUwCIY) — 'humor/Bungou stray dogs/Nikolai Gogol'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=MtoBTaYP0kc](https://youtube.com/watch?v=MtoBTaYP0kc) — Fan art titled 'THE OVERCOAT - NIKOLAI GOGOL' but tagged #bsd #anime - the character's ability, not the story. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=NPXBFsuXFfk](https://youtube.com/watch?v=NPXBFsuXFfk) — Cosplay compilation crediting 'Nikolai Gogol (BSD)'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=OQuRoZ0NtUY](https://youtube.com/watch?v=OQuRoZ0NtUY) — 'Nikolai Gogol edit // Bungo Stray Dogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=OUZ3JG1gfj8](https://youtube.com/watch?v=OUZ3JG1gfj8) — Creepypasta-reacts fan video 'M!Y/N as NIKOLAI GOGOL' - the anime character in a roleplay format. _(pattern: `creepypasta reacts|m!y/n`)_
- `youtube` [https://youtube.com/watch?v=P-opbioRjnw](https://youtube.com/watch?v=P-opbioRjnw) — 'Nikolai gogol edit #nikolaigogol #fypシ #semogafyp' - fan-edit format, no literary content. _(pattern: `nikolai\s*gogol\s*edit`)_
- `youtube` [https://youtube.com/watch?v=P4A7aqxbaoY](https://youtube.com/watch?v=P4A7aqxbaoY) — Bungou Stray Dogs gacha ship post; nikolai gogol in the anime cast list. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=Sq9-_HYAsvo](https://youtube.com/watch?v=Sq9-_HYAsvo) — BSD character edit, '#bsd #bungoustraydogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=UIhmxF-xJ7s](https://youtube.com/watch?v=UIhmxF-xJ7s) — AMV about an anime villain: 'his jester-like design and his ability' - the BSD character. _(pattern: `\(amv\)|jester-like design`)_
- `youtube` [https://youtube.com/watch?v=UdSTxAE2WEY](https://youtube.com/watch?v=UdSTxAE2WEY) — 'me nikolai gogol #nikolaigogol #bungostraydogs' - cosplay/fan channel for the anime character. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=Uu_i9i-IEOo](https://youtube.com/watch?v=Uu_i9i-IEOo) — 'Fyodor x Nikolai Edit ... Bungo stray dogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=V9ds95x9tZA](https://youtube.com/watch?v=V9ds95x9tZA) — BSD character edit, '#bungoustraydogs #decayofangels'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=WM4XwqKufo0](https://youtube.com/watch?v=WM4XwqKufo0) — Bare title 'Nikolai Gogol', no description or tags, anime-fandom channel handle; no evidence it is the writer rather than the BSD character.
- `youtube` [https://youtube.com/watch?v=XQamr1oe3s0](https://youtube.com/watch?v=XQamr1oe3s0) — 'Bsd Fan art' character list including Nikolai Gogol.
- `youtube` [https://youtube.com/watch?v=YOJF76bL1Ok](https://youtube.com/watch?v=YOJF76bL1Ok) — BSD character edit tagged #bsd #bungosd #nikolaigogol. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=YXGE5RDVqdk](https://youtube.com/watch?v=YXGE5RDVqdk) — BSD character edit: '#anime #bsd #bungoustraydogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=ZIXN2aatOjg](https://youtube.com/watch?v=ZIXN2aatOjg) — 'Nikolai Gogol Edit #anime #bsd #bungoustraydogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=Zv81nnbQYoE](https://youtube.com/watch?v=Zv81nnbQYoE) — BSD character edit: 'Nikolai Gogol isn't insane - he's just painfully relatable' about the anime character. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=aOrVNexxyyE](https://youtube.com/watch?v=aOrVNexxyyE) — 'Nikolai Gogol- Bungo Stray Dogs' in an anime character list. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=aawIwiLdD_g](https://youtube.com/watch?v=aawIwiLdD_g) — 'Nikolai and Sigma edit from Bungo Stray Dogs'.
- `youtube` [https://youtube.com/watch?v=buzaMWz7Xiw](https://youtube.com/watch?v=buzaMWz7Xiw) — 'Nikolai Gogol from BSD' fan art. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=cUAU22GcSXQ](https://youtube.com/watch?v=cUAU22GcSXQ) — 'Nikolai Gogol and Ranpo Edogawa' - two BSD characters. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=csxuxqZaxME](https://youtube.com/watch?v=csxuxqZaxME) — Bungou Stray Dogs fan skit ('react to M!y/n as Nikolai Gogol') - the anime character, not the writer.
- `youtube` [https://youtube.com/watch?v=dFt3LyKtSrs](https://youtube.com/watch?v=dFt3LyKtSrs) — 'NIKOLAI GOGOL//Nikolai BSD//#bungoustraydogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=dVZ1KJniy2Y](https://youtube.com/watch?v=dVZ1KJniy2Y) — 'BSD CAPCUT EDIT -Nikolai Gogol-'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=dioJ-h-S8T0](https://youtube.com/watch?v=dioJ-h-S8T0) — BSD character edit set to Malice Mizer. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=e1GFg7I_8kY](https://youtube.com/watch?v=e1GFg7I_8kY) — BSD character edit, '#anime #bsd #nikolaigogol'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=eZ9AIs8JxFI](https://youtube.com/watch?v=eZ9AIs8JxFI) — 'Nikolai Gogol edit // Bungo Stray Dogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=eijtOf1lCSk](https://youtube.com/watch?v=eijtOf1lCSk) — BSD character edit, '#bsd #anime #bungoustraydogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=iN5j1ZHGLOM](https://youtube.com/watch?v=iN5j1ZHGLOM) — BSD ship edit; 'Source: Bungo Stray Dogs (anime ver)'.
- `youtube` [https://youtube.com/watch?v=iOg56MptVDg](https://youtube.com/watch?v=iOg56MptVDg) — BSD character edit (Spanish tags), '#bsd #anime'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=j0v3gOIldyA](https://youtube.com/watch?v=j0v3gOIldyA) — 'Nikolai Gogol edit // Bungo Stray Dogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=lqNKfmoVwsI](https://youtube.com/watch?v=lqNKfmoVwsI) — 'Nikolai gogol edit' tagged #anime - BSD fan edit, no literary content. _(pattern: `nikolai\s*gogol\s*edit`)_
- `youtube` [https://youtube.com/watch?v=ofsTxsAvYGc](https://youtube.com/watch?v=ofsTxsAvYGc) — BSD crack skit; Nikolai gogol listed as an anime character.
- `youtube` [https://youtube.com/watch?v=qqmJZhozbeY](https://youtube.com/watch?v=qqmJZhozbeY) — BSD reaction video; keyword dump 'FYODOR DOSTOEVSKY NIKOLAI GOGOL SIGMA'.
- `youtube` [https://youtube.com/watch?v=rdLGC5acRRI](https://youtube.com/watch?v=rdLGC5acRRI) — BSD character AMV set to Deftones; tagged #bsd #nikolaigogol. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=sD3OJQI1KbM](https://youtube.com/watch?v=sD3OJQI1KbM) — 'Asking chatgpt to describe BSD characters as nature ... Nikolai Gogol'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=sEZgm_DQVzk](https://youtube.com/watch?v=sEZgm_DQVzk) — 'nikolai Gogol edit!!! #bsd #bungoustraydogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=tyqEVhl_KwI](https://youtube.com/watch?v=tyqEVhl_KwI) — BSD character post, '#bungosd'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=uJC-8CXLbA4](https://youtube.com/watch?v=uJC-8CXLbA4) — 'Nikolai gogol edit|bsd|'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=uaMviulIxdA](https://youtube.com/watch?v=uaMviulIxdA) — 'Nikolai Gogol de bungo stray dogs'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=vBuSaw3gV4E](https://youtube.com/watch?v=vBuSaw3gV4E) — BSD reaction video; Nikolai Gogol listed among anime characters (Dazai, Chuuya, Akutagawa).
- `youtube` [https://youtube.com/watch?v=wKDRUf2fUm0](https://youtube.com/watch?v=wKDRUf2fUm0) — 'Nikolai Gogol edit' - BSD fan-edit format, no literary content. _(pattern: `nikolai\s*gogol\s*edit`)_
- `youtube` [https://youtube.com/watch?v=xzMAQMMiXuc](https://youtube.com/watch?v=xzMAQMMiXuc) — BSD character edit, '#Nikolai #Bsd #Angst'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_
- `youtube` [https://youtube.com/watch?v=yUZaDoVycYw](https://youtube.com/watch?v=yUZaDoVycYw) — 'Nikolai gogol art} drawing anime' with #animeart #manga tags - BSD fan art. _(pattern: `#animeart|animedrawing`)_
- `youtube` [https://youtube.com/watch?v=z3w9_JWk5JQ](https://youtube.com/watch?v=z3w9_JWk5JQ) — BSD character edit, '#bsd #burgostraydogs #nikolaigogol'. _(pattern: `nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol`)_

**borscht** (8)

- `reddit` [https://reddit.com/r/JujutsuShenanigans/comments/1l9swo9](https://reddit.com/r/JujutsuShenanigans/comments/1l9swo9) — "Ngl I cooked (only the real ones know who borsch man is)" — "borsch man" is a character meme in that game community, not the soup. _(pattern: `borsch\s+man`)_
- `reddit` [https://reddit.com/r/JujutsuShenanigans/comments/1l9tv0w](https://reddit.com/r/JujutsuShenanigans/comments/1l9tv0w) — Same "borsch man" game meme, reposted ("Did I Cook?"). _(pattern: `borsch\s+man`)_
- `reddit` [https://reddit.com/r/JujutsuShenanigans/comments/1l9ty13](https://reddit.com/r/JujutsuShenanigans/comments/1l9ty13) — Same "borsch man" game meme, reposted. _(pattern: `borsch\s+man`)_
- `reddit` [https://reddit.com/r/JujutsuShenanigans/comments/1l9u9d6](https://reddit.com/r/JujutsuShenanigans/comments/1l9u9d6) — Same "borsch man" game meme, reposted. _(pattern: `borsch\s+man`)_
- `reddit` [https://reddit.com/r/JujutsuShenanigans/comments/1l9ueuj](https://reddit.com/r/JujutsuShenanigans/comments/1l9ueuj) — Same "borsch man" game meme, reposted. _(pattern: `borsch\s+man`)_
- `reddit` [https://reddit.com/r/JujutsuShenanigans/comments/1lbwt9s](https://reddit.com/r/JujutsuShenanigans/comments/1lbwt9s) — Same "borsch man" game meme, reposted after a title fix. _(pattern: `borsch\s+man`)_
- `reddit` [https://reddit.com/r/albania/comments/1lb6sa1](https://reddit.com/r/albania/comments/1lb6sa1) — Albanian Riviera trip planning: "some nice towns… Himare, Dhermi, Borsch" — the coastal village of Borsh, not the soup. _(pattern: `\b(?:himar[eë]|dh[eë]rmi|ksamil|albanian?\s+riviera)\b`)_
- `reddit` [https://reddit.com/r/kumocrew/comments/1jqjb98](https://reddit.com/r/kumocrew/comments/1jqjb98) — Elite Dangerous player-group newsletter: "'*Operation Borsch*' was a success!" — an in-game operation codename. _(pattern: `operation\s+borsch`)_

**donbas** (2)

- `news` [https://en.interfax.com.ua/news/general/1085584.html](https://en.interfax.com.ua/news/general/1085584.html) — interfax.com.ua: sole match is 'a house was damaged in the New Donbass of the Dobropilska community' — the village Novyi Donbas, not the region. _(pattern: `\b(?:novyi?|new)\s+donbass\b(?!\s+line)`)_
- `wikipedia` [https://en.wikipedia.org/wiki/Donbas](https://en.wikipedia.org/wiki/Donbas) — The listed page title is 'Donbas' — the Ukrainian form, so it attests nothing about Russian spelling. Ingest followed the Donbass→Donbas redirect and filed the target under variant=russian (identical pageviews appear under both variants in wikipedia_processed.parquet), then the redirect probe tested 'Donbas' and returned redirects:false. _(pattern: `^donbas$`)_

**luhansk** (2)

- `reddit` [https://reddit.com/r/urbanhellcirclejerk/comments/1kh13xe](https://reddit.com/r/urbanhellcirclejerk/comments/1kh13xe) — Circlejerk parody: 'Hideous place at berlinskorpol, lugansk, Japanussia' — invented mashup placenames, not the city _(pattern: `urbanhellcirclejerk`)_
- `reddit` [https://reddit.com/r/urbanhellcirclejerk/comments/1l6yyew](https://reddit.com/r/urbanhellcirclejerk/comments/1l6yyew) — Circlejerk joke title 'Lugansk, Russia / Lugakawa, Japan'; the body reveals the photo is 'actually Lugano, Switzerland' _(pattern: `urbanhellcirclejerk`)_

**chornobyl** (1)

- `reddit` [https://reddit.com/r/CFB/comments/1hr279y](https://reddit.com/r/CFB/comments/1hr279y) — Satirical year-2070 college-football post in which 'Chernobyl' is a fictional team name.

**dynamo-kyiv** (2)

- `youtube` [https://youtube.com/watch?v=0nIWnmC8fcE](https://youtube.com/watch?v=0nIWnmC8fcE) — English description says 'GNK Dynamo Kiev' - GNK is Dinamo Zagreb's prefix (Građanski nogometni klub) and its EA FC club name; the career-mode opponent is Dinamo Zagreb, not the Kyiv club. _(pattern: `\bgnk\s+d[iy]namo\b`)_
- `youtube` [https://youtube.com/watch?v=qhp06gop35g](https://youtube.com/watch?v=qhp06gop35g) — 'Dynamo Gaming' is an Indian BGMI streamer, not the football club; the video matched on 'Dynamo' through search relevance only. _(pattern: `\b(bgmi|dynamo gaming|dynamogaming)\b`)_

**chicken-kyiv** (6)

- `news` [https://www.americanthinker.com/articles/2025/08/trump_s_peace_train_rolls_into_anchorage.html](https://www.americanthinker.com/articles/2025/08/trump_s_peace_train_rolls_into_anchorage.html) — 'Nixon speechwriter William Safire dubbed it the Chicken Kiev speech' - the referent is Bush's 1991 Kyiv address, a frozen political proper noun, not the dish. _(pattern: `chicken[\s-]*kiev['’"”]?\s+(?:speech|address)`)_
- `news` [https://www.kyivpost.com/analysis/52308](https://www.kyivpost.com/analysis/52308) — Kyiv Post quotes Vox Ukraine: 'Remember Bush-senior's Chicken Kiev speech' - Bush's 1991 address, not the dish. _(pattern: `chicken[\s-]*kiev['’"”]?\s+(?:speech|address)`)_
- `news` [https://www.nakedcapitalism.com/2025/03/links-3-1-2025.html](https://www.nakedcapitalism.com/2025/03/links-3-1-2025.html) — Naked Capitalism links roundup: 'Why did the Chicken Kiev cross the road? ...to get away from the White House' - a pun standing for Zelensky/Ukraine after the Oval Office row, not the dish, and it appears in the reader-comment block rather than the post body.
- `reddit` [https://reddit.com/r/2ndStoicSchool/comments/1iu2g55](https://reddit.com/r/2ndStoicSchool/comments/1iu2g55) — r/2ndStoicSchool doggerel about impaling Zelensky, signed off with the standalone line 'Chicken Kiev.' - a pun on Zelensky/Kyiv, not the dish; also violent political content that should not be a public exhibit.
- `reddit` [https://reddit.com/r/playrust/comments/1llq4er](https://reddit.com/r/playrust/comments/1llq4er) — r/playrust image post titled 'Chicken Kiev Large Box Placement' with no body text - a Rust base/loot-box meme label; nothing in the record attests the dish. _(pattern: `r/playrust\b`)_
- `youtube` [https://youtube.com/watch?v=UBaR742wmBw](https://youtube.com/watch?v=UBaR742wmBw) — Description: 'From the Chicken Kiev Speech to the Black Budget' - Bush's 1991 Kyiv address, not the dish. _(pattern: `chicken[\s-]*kiev['’"”]?\s+(?:speech|address)`)_

**kharkiv** (1)

- `reddit` [https://reddit.com/r/NatureofPredators/comments/1i3pwo8](https://reddit.com/r/NatureofPredators/comments/1i3pwo8) — Sci-fi fan fiction; 'Kharkov, for the motherland!' is the name of a fictional destroyer, not the city. _(pattern: `r/natureofpredators`)_

**volodymyr-zelenskyy** (3)

- `youtube` [https://youtube.com/watch?v=-zWen4q_OIE](https://youtube.com/watch?v=-zWen4q_OIE) — NEWS HUB (copy of the Firstpost blurb) writes “Russian President Vladimir Zelensky is likely to retaliate” — a Putin/Zelensky name blend; the intended referent is Putin. _(pattern: `(?:russian president|president of russia)\s+vladimir zelensk`)_
- `youtube` [https://youtube.com/watch?v=1wEX2WpV05s](https://youtube.com/watch?v=1wEX2WpV05s) — CNN description writes “Trump met with Russian President Vladimir Zelensky” — a Putin/Zelensky name blend; the intended referent is Putin, so it is not evidence of a Russian spelling of Zelenskyy. _(pattern: `(?:russian president|president of russia)\s+vladimir zelensk`)_
- `youtube` [https://youtube.com/watch?v=PiEXRYD86vo](https://youtube.com/watch?v=PiEXRYD86vo) — Firstpost description writes “Russian President Vladimir Zelensky is likely to retaliate” — a Putin/Zelensky name blend; the intended referent is Putin. _(pattern: `(?:russian president|president of russia)\s+vladimir zelensk`)_

**ihor-sikorsky** (1)

- `reddit` [https://reddit.com/r/RATS/comments/1hzepir](https://reddit.com/r/RATS/comments/1hzepir) — r/RATS pet photo: 'My girls Iggy, short for Igor Sikorsky' — the referent is a pet rat named after him.

**oleksandr-usyk** (1)

- `youtube` [https://youtube.com/watch?v=zp4rZCV-APU](https://youtube.com/watch?v=zp4rZCV-APU) — Indonesian title 'kucing meong namanya Alexander Usyk' = a pet CAT named Alexander Usyk; the string does not denote the boxer, and the title is not English. _(pattern: `\bkucing\b|\bnamanya\b`)_

**ternopil** (1)

- `youtube` [https://youtube.com/watch?v=9yohM-lPnA0](https://youtube.com/watch?v=9yohM-lPnA0) — Spanish craft video "haciendo capibara de ternopol" — a styrofoam capybara (misrendered tecnopor/termopol), no connection to the Ukrainian city.

**volodymyr-the-great** (4)

- `reddit` [https://reddit.com/r/Kaiserreich/comments/1pj3etd](https://reddit.com/r/Kaiserreich/comments/1pj3etd) — r/Kaiserreich (HOI4 alt-history mod) image post with no body: 'Vladimir the Great' is the mod's restored Tsar Vladimir III (Vladimir Kirillovich Romanov, son of Kirill) in a 1930s-40s 'People's monarchy / white empire' Russia, not the Kyivan Rus prince. _(pattern: `^r/kaiserreich`)_
- `reddit` [https://reddit.com/r/Kaiserreich/comments/1pj3f2u](https://reddit.com/r/Kaiserreich/comments/1pj3f2u) — Same-author duplicate of 1pj3etd posted the same day; identical Kaiserreich Tsar Vladimir III referent, not the Kyivan Rus prince. _(pattern: `^r/kaiserreich`)_
- `youtube` [https://youtube.com/watch?v=Sq67XuXxNuY](https://youtube.com/watch?v=Sq67XuXxNuY) — Joke short 'Vladimir the Dirty IRL'; the epithet appears only as a parenthetical gloss on the pun ('No credit to Vladimir the dirty (Vladimir the great)'), the video's subject is someone else. _(pattern: `vladimir the dirty`)_
- `youtube` [https://youtube.com/watch?v=njKMDLd7lQU](https://youtube.com/watch?v=njKMDLd7lQU) — NewsNation/The Hill clip: the phrase names Vladimir Putin, whose 'goal in life is to exit the earth as Vladimir The Great' — not the Kyivan prince.

**bakhmut** (1)

- `wikipedia` [https://en.wikipedia.org/wiki/Artemovsk](https://en.wikipedia.org/wiki/Artemovsk) — Live API check: en.wikipedia.org/wiki/Artemovsk redirects to "Artemivsk (disambiguation)" (NOT to Bakhmut), which also lists Kypuche in Luhansk Oblast, a retired Soviet amphibious ship, and Artyomovsk in Krasnoyarsk Krai, Russia — so the 770 pageviews cannot be attributed to this pair's subject.

**dnipro-river** (2)

- `news` [https://tass.com/world/2045101](https://tass.com/world/2045101) — TASS on Moldova: Russia's peacekeeping operation is on the banks of the DNIESTER in Transnistria; the English service rendered it 'Dnieper River'. Different river. _(pattern: `(?:transnistria\w*|tiraspol|bender|gagauz|moldova\w*)[^.]{0,150}dnieper|dnieper[^.]{0,150}(?:transnistria\w*|tiraspol|bender|gagauz)`)_
- `reddit` [https://reddit.com/r/Transnistria/comments/1l92vfu](https://reddit.com/r/Transnistria/comments/1l92vfu) — Trip report from Bender, Transnistria: the bridge described is over the DNIESTER, not the Dnieper. Wrong river. _(pattern: `(?:bender|tiraspol|transnistria\w*|chisinau|moldova\w*)[^.]{0,150}dnieper|dnieper[^.]{0,150}(?:bender|tiraspol|transnistria\w*)`)_

### person-name — 154 drops

**odesa** (25)

- `reddit` [https://reddit.com/r/DeathBattleMatchups/comments/1htqdnr](https://reddit.com/r/DeathBattleMatchups/comments/1htqdnr) — 'the death of the original leader, Odessa Silverberg' - a Suikoden character. _(pattern: `odessa\s+silverberg`)_
- `reddit` [https://reddit.com/r/horror/comments/1hsbva3](https://reddit.com/r/horror/comments/1hsbva3) — 'the cast was lead by Odessa Young' - the actress, not the city. _(pattern: `odessa\s+young`)_
- `reddit` [https://reddit.com/r/innervoice/comments/1ht4ks7](https://reddit.com/r/innervoice/comments/1ht4ks7) — 'have my triton blood hunter, Odessa' - a tabletop RPG character name.
- `reddit` [https://reddit.com/r/movies/comments/1hrvm4a](https://reddit.com/r/movies/comments/1hrvm4a) — 2025 wide-release listing crediting actress 'Odessa A'zion' twice. _(pattern: `odessa\s+a['’]?zion`)_
- `reddit` [https://reddit.com/r/movies/comments/1hsj591](https://reddit.com/r/movies/comments/1hsj591) — Cast list: 'Odessa Young, Rory McCann, Joe Cole'. _(pattern: `odessa\s+young`)_
- `reddit` [https://reddit.com/r/namenerds/comments/1htrnl3](https://reddit.com/r/namenerds/comments/1htrnl3) — Baby-name shortlist; 'Odessa' listed among girls' given names. _(pattern: `^r/namenerds\b`)_
- `reddit` [https://reddit.com/r/screenunseen/comments/1hvbqlk](https://reddit.com/r/screenunseen/comments/1hvbqlk) — 'a new film starring Joe Cole and Odessa Young' - the actress. _(pattern: `odessa\s+young`)_
- `youtube` [https://youtube.com/watch?v=3amYjBjJ81I](https://youtube.com/watch?v=3amYjBjJ81I) — 'Best of Odessa Oliveira' - an AI-music artist's name. _(pattern: `odessa\s+oliveira`)_
- `youtube` [https://youtube.com/watch?v=54u0Buguq0E](https://youtube.com/watch?v=54u0Buguq0E) — 'Vampirate Odessa' / 'Captain, Odessa Delico' - a VTuber persona. _(pattern: `vampirate\s+odessa|odessa\s+delico`)_
- `youtube` [https://youtube.com/watch?v=BM_MDkFrp1M](https://youtube.com/watch?v=BM_MDkFrp1M) — theSkimm clip about actress Odessa A'zion. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=FTnQoD1DBMo](https://youtube.com/watch?v=FTnQoD1DBMo) — Good Morning America interview with actress Odessa A'zion. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=Iv6fpnaSoUI](https://youtube.com/watch?v=Iv6fpnaSoUI) — Actress Odessa A'zion on Late Night, promoting 'Marty Supreme'. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=LVKab1-kQ7E](https://youtube.com/watch?v=LVKab1-kQ7E) — 'Trying Odessa's curly hair routine' - an influencer named Odessa.
- `youtube` [https://youtube.com/watch?v=NpVwSLCGsFM](https://youtube.com/watch?v=NpVwSLCGsFM) — 'Timothée Chalamet & Odessa A'zion at the New York premiere'. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=RBmHM4QVXxU](https://youtube.com/watch?v=RBmHM4QVXxU) — Oscars channel on how Odessa A'zion was cast in 'Marty Supreme'. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=S4Enrdl75Ks](https://youtube.com/watch?v=S4Enrdl75Ks) — Indonesian Mobile Legends video on the top-global player 'Kagura Odessa'. _(pattern: `kagura\s+odessa|global\s+kagura`)_
- `youtube` [https://youtube.com/watch?v=SShS6HFli9I](https://youtube.com/watch?v=SShS6HFli9I) — 'Kylie Jenner vs Odessa A'zion' - celebrity gossip. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=Tlo95noJdkQ](https://youtube.com/watch?v=Tlo95noJdkQ) — Outer Banks (#obx5) fandom clip about a person named Odessa; no city reference.
- `youtube` [https://youtube.com/watch?v=dnSNF-0v_f4](https://youtube.com/watch?v=dnSNF-0v_f4) — #billieeilish #odessa EILISH - Odessa A'zion, not the city. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=eVJMdwhqi2E](https://youtube.com/watch?v=eVJMdwhqi2E) — 'Odessa A'zion's changes over the years' - the actress. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=fhd90UOnANc](https://youtube.com/watch?v=fhd90UOnANc) — 'Top 1 Global Kagura by Odessa' - a Mobile Legends player handle. _(pattern: `kagura\s+odessa|global\s+kagura`)_
- `youtube` [https://youtube.com/watch?v=gPB3rNqpxLs](https://youtube.com/watch?v=gPB3rNqpxLs) — Gossip video about Odessa A'zion and Timothée Chalamet. _(pattern: `odessa\s+a['’]?zion`)_
- `youtube` [https://youtube.com/watch?v=iSrUFWXjEA4](https://youtube.com/watch?v=iSrUFWXjEA4) — 'Odessa Cubbage' - a Half-Life 2 character, not the city. _(pattern: `odessa\s+cubbage`)_
- `youtube` [https://youtube.com/watch?v=qA8hr_MucJ0](https://youtube.com/watch?v=qA8hr_MucJ0) — 'Reasonings With Odessa' - a Jamaican podcast host's name. _(pattern: `reasonings\s+with\s+odessa`)_
- `youtube` [https://youtube.com/watch?v=vaH_E49-h9E](https://youtube.com/watch?v=vaH_E49-h9E) — Red-carpet clip of Odessa A'zion at the Marty Supreme premiere. _(pattern: `odessa\s+a['’]?zion`)_

**kyiv** (8)

- `news` [https://www.ncnewsonline.com/news/man-facing-new-charges-in-reported-teen-sexual-assault/article_3bc9349b-2cde-4c89-8df9-4279dbdda781.html](https://www.ncnewsonline.com/news/man-facing-new-charges-in-reported-teen-sexual-assault/article_3bc9349b-2cde-4c89-8df9-4279dbdda781.html) — "Kiev Kwentel Otey" is a US criminal defendant's given name; local crime story, no city referent. _(pattern: `kiev\s+kwentel`)_
- `reddit` [https://reddit.com/r/Destiny/comments/1hr3e8l](https://reddit.com/r/Destiny/comments/1hr3e8l) — Sam "Ghost of Kiev" Hyde - the phrase functions as a US comedian's ironic nickname, not a reference to the city. _(pattern: `sam\s+hyde`)_
- `reddit` [https://reddit.com/r/airsoftloadouts/comments/1htjzqm](https://reddit.com/r/airsoftloadouts/comments/1htjzqm) — "Ghost of Kiev - sam hyde kit" - an airsoft loadout named after the Sam Hyde meme persona. _(pattern: `sam\s+hyde`)_
- `youtube` [https://youtube.com/watch?v=3j4q9_RyDZc](https://youtube.com/watch?v=3j4q9_RyDZc) — "@Kiev Beats" - producer alias credited for mixing and mastering. _(pattern: `kiev\s*beats`)_
- `youtube` [https://youtube.com/watch?v=7HLhoUHjkO4](https://youtube.com/watch?v=7HLhoUHjkO4) — "Langafinga X Kiev" - Kiev is the featured artist on a Sranan-Tongo track. _(pattern: `langa\s*finga|kappalani`)_
- `youtube` [https://youtube.com/watch?v=8Kv48S_hFFE](https://youtube.com/watch?v=8Kv48S_hFFE) — "Ft. Kiev, Kappalani & Mambolo" - Kiev is a featured recording artist. _(pattern: `langa\s*finga|kappalani`)_
- `youtube` [https://youtube.com/watch?v=MK9ltWPAKag](https://youtube.com/watch?v=MK9ltWPAKag) — "Prod. by Kiev beats" - a music producer's alias (Instagram @kievbeats). _(pattern: `kiev\s*beats`)_
- `youtube` [https://youtube.com/watch?v=eRQn0q5-YZg](https://youtube.com/watch?v=eRQn0q5-YZg) — "Langafinga - Kiev - Kappalani": Kiev is a Surinamese recording artist on an Eli F Records track. _(pattern: `langa\s*finga|kappalani`)_

**lviv** (83)

- `news` [https://en.interfax.com.ua/news/general/1113805.html](https://en.interfax.com.ua/news/general/1113805.html) — All 3 body hits are Bohdan Lvov, the dismissed Ukrainian Supreme Court judge (bail, custody, Russian citizenship) - a surname, not the city. _(pattern: `bohdan\s+lvov`)_
- `news` [https://larouchepub.com/other/2025/5236-larouche_s_fifty_year_fight_fo.html](https://larouchepub.com/other/2025/5236-larouche_s_fifty_year_fight_fo.html) — All 3 hits are Academician Dmitri Lvov, Russian economist at CEMI, in a LaRouche/EIR retrospective; the city never appears in the 110KB body. _(pattern: `dmitri\s+lvov`)_
- `news` [https://www.communityadvocate.com/police-fire/southborough-police-log-march-21-edition/article_8e6e7614-0360-11f0-ab98-b7ac7b83b14f.html](https://www.communityadvocate.com/police-fire/southborough-police-log-march-21-edition/article_8e6e7614-0360-11f0-ab98-b7ac7b83b14f.html) — Single hit: 'Arrested, David Lvov, of Woodward St., Framingham' in a Southborough MA police log. _(pattern: `david\s+lvov`)_
- `news` [https://www.pr.com/press-release/939353](https://www.pr.com/press-release/939353) — Both hits are photographer Arkady Lvov in an IPC Hall of Fame press-release contributor list. _(pattern: `arkady\s+lvov`)_
- `openalex` [https://openalex.org/W4413285014](https://openalex.org/W4413285014) — Abstract: 'the life and creative legacy of Nikolai Alexandrovich Lvov ... his close friendship with Gavriil Romanovich Derzhavin' - the Russian architect, a surname. _(pattern: `\bnikola[iy]\s+(?:\w+\s+)?lvov\b`)_
- `reddit` [https://reddit.com/r/100yearsago/comments/1j5tg61](https://reddit.com/r/100yearsago/comments/1j5tg61) — 'Georgy Lvov, the first Prime Minister of the Russian Republic ... died in exile in Paris' - the 1917 premier. Note the existing pairs.yaml filter requires the word 'prince', which this title lacks, so it slips through today. _(pattern: `\bgeorg\w*\s+lvov\b`)_
- `reddit` [https://reddit.com/r/ArtNude/comments/1k3946d](https://reddit.com/r/ArtNude/comments/1k3946d) — Photo credit 'Blood & Wine by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/BabeModels/comments/1kbv7h8](https://reddit.com/r/BabeModels/comments/1kbv7h8) — Photo credit 'Mira by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/BlackWhite_Nude_Photo/comments/1jwsnya](https://reddit.com/r/BlackWhite_Nude_Photo/comments/1jwsnya) — Photo credit 'Lika by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/EliteFemale/comments/1jgvq5r](https://reddit.com/r/EliteFemale/comments/1jgvq5r) — Photo credit 'Kalina by Dmitrii Lvov' - boudoir photographer's surname; part of the 62-post cross-posting cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/EliteFemale/comments/1k6e4to](https://reddit.com/r/EliteFemale/comments/1k6e4to) — Photo credit 'Liza by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1jlig6n](https://reddit.com/r/LaBeauteFeminine/comments/1jlig6n) — Photo credit 'Belka by Dmitriy Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1jvk2jt](https://reddit.com/r/LaBeauteFeminine/comments/1jvk2jt) — Photo credit 'Little Di by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1jwd1dt](https://reddit.com/r/LaBeauteFeminine/comments/1jwd1dt) — Photo credit 'Toma by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1k05s9i](https://reddit.com/r/LaBeauteFeminine/comments/1k05s9i) — Photo credit 'Nastya by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1k0xpek](https://reddit.com/r/LaBeauteFeminine/comments/1k0xpek) — Photo credit 'Marfa by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1k74v0f](https://reddit.com/r/LaBeauteFeminine/comments/1k74v0f) — Photo credit 'Nika by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1k819md](https://reddit.com/r/LaBeauteFeminine/comments/1k819md) — Photo credit 'Alyona by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1kb1p7x](https://reddit.com/r/LaBeauteFeminine/comments/1kb1p7x) — Photo credit 'Nastya by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1kfohqg](https://reddit.com/r/LaBeauteFeminine/comments/1kfohqg) — Photo credit 'Kalina by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/LaBeauteFeminine/comments/1ki23v0](https://reddit.com/r/LaBeauteFeminine/comments/1ki23v0) — Photo credit 'Kris by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/MarxistCulture/comments/1ixuepf](https://reddit.com/r/MarxistCulture/comments/1ixuepf) — 'Monument to Lenin in Hanoi, Vietnam - photo by Pavel Lvov' - RIA Novosti photographer's surname. _(pattern: `\bpavel\s+lvov\b`)_
- `reddit` [https://reddit.com/r/ModernSocialist/comments/1ixx01b](https://reddit.com/r/ModernSocialist/comments/1ixx01b) — Same Pavel Lvov photo credit cross-posted to r/ModernSocialist. _(pattern: `\bpavel\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Poetry/comments/1k2wi8f](https://reddit.com/r/Poetry/comments/1k2wi8f) — '[POEM] Let's honor those great patriotic years by Mikhail Lvov' - the Soviet poet's surname. _(pattern: `\bmikhail\s+lvov\b`)_
- `reddit` [https://reddit.com/r/PropagandaPosters/comments/1kbaaid](https://reddit.com/r/PropagandaPosters/comments/1kbaaid) — 'Soviet artist Evgeny Alexandrovich Lvov (1892-1983) next to a German military command order in Berlin' - a surname. _(pattern: `\bevgeny\s+\w+\s+lvov\b`)_
- `reddit` [https://reddit.com/r/RisquePhoto/comments/1k8182c](https://reddit.com/r/RisquePhoto/comments/1k8182c) — Photo credit 'Alyona | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/RisquePhoto/comments/1kb1koq](https://reddit.com/r/RisquePhoto/comments/1kb1koq) — Photo credit 'Nastya | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/RisquePhoto/comments/1kbv5jz](https://reddit.com/r/RisquePhoto/comments/1kbv5jz) — Photo credit 'Mira | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/RisquePhoto/comments/1kcnr7a](https://reddit.com/r/RisquePhoto/comments/1kcnr7a) — Photo credit 'Belka | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/RisquePhoto/comments/1kfoecs](https://reddit.com/r/RisquePhoto/comments/1kfoecs) — Photo credit 'Kalina | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/RisquePhoto/comments/1kgi0ja](https://reddit.com/r/RisquePhoto/comments/1kgi0ja) — Photo credit 'Dark Mickey | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/RisquePhoto/comments/1kgi145](https://reddit.com/r/RisquePhoto/comments/1kgi145) — Photo credit 'Dark Mickey | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/RisquePhoto/comments/1ki2382](https://reddit.com/r/RisquePhoto/comments/1ki2382) — Photo credit 'Kris | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1jgvse9](https://reddit.com/r/Sensual_photography/comments/1jgvse9) — Photo credit 'Kalina by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1jifgix](https://reddit.com/r/Sensual_photography/comments/1jifgix) — Photo credit 'Toma by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1jpbkgt](https://reddit.com/r/Sensual_photography/comments/1jpbkgt) — Photo credit 'Jopulya by Dmitriy Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1jq3nr8](https://reddit.com/r/Sensual_photography/comments/1jq3nr8) — Photo credit 'Sasha by Dmitriy Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1jwcyuu](https://reddit.com/r/Sensual_photography/comments/1jwcyuu) — Photo credit 'Toma by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1k392h3](https://reddit.com/r/Sensual_photography/comments/1k392h3) — Photo credit 'Blood & Wine by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1k74wlm](https://reddit.com/r/Sensual_photography/comments/1k74wlm) — Photo credit 'Nika by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1k8165b](https://reddit.com/r/Sensual_photography/comments/1k8165b) — Photo credit 'Alyona by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1kb1omz](https://reddit.com/r/Sensual_photography/comments/1kb1omz) — Photo credit 'Nastya by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1kbv6w6](https://reddit.com/r/Sensual_photography/comments/1kbv6w6) — Photo credit 'Mira by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Sensual_photography/comments/1kfoama](https://reddit.com/r/Sensual_photography/comments/1kfoama) — Photo credit 'Kalina by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/SovietPhotosOfWW2/comments/1jup0lm](https://reddit.com/r/SovietPhotosOfWW2/comments/1jup0lm) — 'Holders of the Order of Saint Georg Crosses (all 4 classes) V. Lvov and Hero of the Soviet Union I. G. Stepanov - residents of the city of Kansk' - a soldier's surname. _(pattern: `\bv\.\s*lvov\b`)_
- `reddit` [https://reddit.com/r/SovietPhotosOfWW2/comments/1jup19b](https://reddit.com/r/SovietPhotosOfWW2/comments/1jup19b) — Duplicate of the same 'V. Lvov' Siberians caption. _(pattern: `\bv\.\s*lvov\b`)_
- `reddit` [https://reddit.com/r/WarshipPorn/comments/1k2rinj](https://reddit.com/r/WarshipPorn/comments/1k2rinj) — Same Pavel Lvov/RIA Novosti submarine credit cross-posted to r/WarshipPorn. _(pattern: `\bpavel\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1jwcxfn](https://reddit.com/r/Wildmodels/comments/1jwcxfn) — Photo credit 'Toma by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1k0xf5d](https://reddit.com/r/Wildmodels/comments/1k0xf5d) — Photo credit 'Marfa by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1k391ei](https://reddit.com/r/Wildmodels/comments/1k391ei) — Photo credit 'Blood & Wine by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1k6eaga](https://reddit.com/r/Wildmodels/comments/1k6eaga) — Photo credit 'Linda by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1k74tkm](https://reddit.com/r/Wildmodels/comments/1k74tkm) — Photo credit 'Nika by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1k815kr](https://reddit.com/r/Wildmodels/comments/1k815kr) — Photo credit 'Alyona by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1kb1mtk](https://reddit.com/r/Wildmodels/comments/1kb1mtk) — Photo credit 'Nastya by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1kb1nem](https://reddit.com/r/Wildmodels/comments/1kb1nem) — Photo credit 'Nastya by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1kbv53u](https://reddit.com/r/Wildmodels/comments/1kbv53u) — Photo credit 'Mira by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1kcnust](https://reddit.com/r/Wildmodels/comments/1kcnust) — Photo credit 'Belka by Dmitrii Lvov' - a boudoir photographer's surname; one account cross-posts these across NSFW subs. 62 of the 100 reddit holdouts are this single cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1kfo9op](https://reddit.com/r/Wildmodels/comments/1kfo9op) — Photo credit 'Kalina by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/Wildmodels/comments/1kghz7h](https://reddit.com/r/Wildmodels/comments/1kghz7h) — Photo credit 'Dark Mickey by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1jiffo6](https://reddit.com/r/boudoir_community/comments/1jiffo6) — Photo credit 'Toma by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1jlidnn](https://reddit.com/r/boudoir_community/comments/1jlidnn) — Photo credit 'Belka by Dmitriy Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1jn38oq](https://reddit.com/r/boudoir_community/comments/1jn38oq) — Photo credit 'Sasha by Dmitriy Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1jojasm](https://reddit.com/r/boudoir_community/comments/1jojasm) — Photo credit 'Liza by Dmitriy Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1jsi7e3](https://reddit.com/r/boudoir_community/comments/1jsi7e3) — Photo credit 'Kalina by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1jut093](https://reddit.com/r/boudoir_community/comments/1jut093) — Photo credit 'Lika by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1jwd0j0](https://reddit.com/r/boudoir_community/comments/1jwd0j0) — Photo credit 'Toma by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1k0xpyo](https://reddit.com/r/boudoir_community/comments/1k0xpyo) — Photo credit 'Marfa by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1k39369](https://reddit.com/r/boudoir_community/comments/1k39369) — Photo credit 'Blood & Wine by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1k816f2](https://reddit.com/r/boudoir_community/comments/1k816f2) — Photo credit 'Alyona by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1kb1o1o](https://reddit.com/r/boudoir_community/comments/1kb1o1o) — Photo credit 'Nastya by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1kbv6b4](https://reddit.com/r/boudoir_community/comments/1kbv6b4) — Photo credit 'Mira by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1kfog3o](https://reddit.com/r/boudoir_community/comments/1kfog3o) — Photo credit 'Kalina | ph Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/boudoir_community/comments/1kgi1nb](https://reddit.com/r/boudoir_community/comments/1kgi1nb) — Photo credit 'Dark Mickey by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/metalgirls/comments/1kmjmhm](https://reddit.com/r/metalgirls/comments/1kmjmhm) — Photo credit 'Girl by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/redlingerie/comments/1k74sj6](https://reddit.com/r/redlingerie/comments/1k74sj6) — Photo credit 'Nika by Dmitrii Lvov'; same photographer cluster. _(pattern: `\bdmitri[iy]\s+lvov\b`)_
- `reddit` [https://reddit.com/r/submarines/comments/1k2rimn](https://reddit.com/r/submarines/comments/1k2rimn) — 'Photo by Pavel Lvov/RIA NOVOSTI' on a Barents Sea Kilo-class submarine photo; photographer credit. _(pattern: `\bpavel\s+lvov\b`)_
- `reddit` [https://reddit.com/r/yearofannakarenina/comments/1hwc01n](https://reddit.com/r/yearofannakarenina/comments/1hwc01n) — Anna Karenina Ch.6 character list: 'Prince Lvov, diplomat, Nataly's husband' - a Tolstoy character. _(pattern: `\bprince\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=-EnTx_208-0](https://youtube.com/watch?v=-EnTx_208-0) — 'Alexey Lvov - Coherent sheaves on maximally singular varieties' - a mathematician's surname (channel 'Alex Lvov'). _(pattern: `\balexe[yi]\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=7RJXcdpqlwY](https://youtube.com/watch?v=7RJXcdpqlwY) — 'Improvisation #149 (Gene Lvov)' - same pianist's surname. _(pattern: `\bgene\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=OhneQM3OPzs](https://youtube.com/watch?v=OhneQM3OPzs) — 'een getuigenis van Christine Lvov' - a performer's surname (and the entry is Dutch). _(pattern: `\bchristine\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=Pd3RPAWNg34](https://youtube.com/watch?v=Pd3RPAWNg34) — 'Blues 125 (Gene Lvov)' - same pianist's surname. _(pattern: `\bgene\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=V7yYnprZx6A](https://youtube.com/watch?v=V7yYnprZx6A) — 'Improvisation #105 (Gene Lvov)' - jazz pianist Gene Lvov, a surname; description is 'Jazz piano improvisation'. _(pattern: `\bgene\s+lvov\b`)_
- `youtube` [https://youtube.com/watch?v=VDFuM-vZ0oU](https://youtube.com/watch?v=VDFuM-vZ0oU) — 'A Merry Song (D. Lvov-Kompaneets)' - composer Dmitri Lvov-Kompaneets; a surname. _(pattern: `\blvov-kompaneets\b`)_

**borscht** (23)

- `news` [http://northcountrynow.com/stories/potsdam-man-shareschristmas-eve-story,340690](http://northcountrynow.com/stories/potsdam-man-shareschristmas-eve-story,340690) — Letter to the editor: "Thanks to Don Borsch for his letter in North Country This Week" — a local surname. _(pattern: `\bdon(?:ald)?\s+borsch\b`)_
- `news` [https://arizonadailyindependent.com/2025/07/23/3-sentenced-to-prison-for-trafficking-methamphetamine/](https://arizonadailyindependent.com/2025/07/23/3-sentenced-to-prison-for-trafficking-methamphetamine/) — Meth-trafficking sentencing: defendant "Jerome Francisco Borsch" — surname. _(pattern: `jerome(?:\s+francisco)?\s+borsch`)_
- `news` [https://collider.com/vampires-animation-best-ranked/](https://collider.com/vampires-animation-best-ranked/) — Voice-actor credit "Kazuyuki Okitsu and Johnny Yong Borsch" (Johnny Yong Bosch) — a personal name. _(pattern: `johnny\s+yong\s+borsch`)_
- `news` [https://divernet.com/photography/mustard-too-hot-in-switzerland/](https://divernet.com/photography/mustard-too-hot-in-switzerland/) — Photo-contest winner "Germany's Peter Borsch, for his memorable portrait" — surname. _(pattern: `peter\s+borsch`)_
- `news` [https://eurovoix.com/2025/03/06/iolanda-joao-borsch-festival-da-cancao-2025-final/](https://eurovoix.com/2025/03/06/iolanda-joao-borsch-festival-da-cancao-2025-final/) — "Joining her will be João Borsch, the televote winner" — a Portuguese singer's surname. _(pattern: `jo[ãa]o\s+borsch`)_
- `news` [https://ktar.com/arizona-news/meth-trafficking-sentences/5730963/](https://ktar.com/arizona-news/meth-trafficking-sentences/5730963/) — Same Arizona case, "Michael Koontz, Jerome Borsch and Holly Dagnino" — surname. _(pattern: `jerome(?:\s+francisco)?\s+borsch`)_
- `news` [https://redstate.com/wardclark/2025/08/18/preposterous-drag-shows-for-kids-paid-for-by-german-taxpayers-n2192945](https://redstate.com/wardclark/2025/08/18/preposterous-drag-shows-for-kids-paid-for-by-german-taxpayers-n2192945) — "The director of the Botanical Garden, Thomas Borsch, emphasised…" — surname. _(pattern: `thomas\s+borsch`)_
- `news` [https://www.boredpanda.com/stunning-aerial-photography-borsch/](https://www.boredpanda.com/stunning-aerial-photography-borsch/) — "through the lens of Yura Borschev, also known as Borsch" — an aerial photographer's personal handle. _(pattern: `borschev`)_
- `news` [https://www.cantechletter.com/newswires/tatiana-borsch-releases-complete-horoscope-2026-astrological-predictions-2026-for-every-zodiac-sign/](https://www.cantechletter.com/newswires/tatiana-borsch-releases-complete-horoscope-2026-astrological-predictions-2026-for-every-zodiac-sign/) — Same Tatiana Borsch horoscope wire copy under a second masthead; surname, not the dish. _(pattern: `tatiana\s+borsch`)_
- `news` [https://www.dailyprincetonian.com/article/2025/01/princeton-opguest-opening-exercises-spirituality-religion-orientation](https://www.dailyprincetonian.com/article/2025/01/princeton-opguest-opening-exercises-spirituality-religion-orientation) — "Dean of the Chapel Frederick Borsch '57 'de-Christianized' Opening Exercises" — surname. _(pattern: `frederick\s+borsch`)_
- `news` [https://www.finanznachrichten.de/nachrichten-2025-12/67325767-actuls-tatiana-borsch-releases-complete-horoscope-2026-astrological-predictions-2026-for-every-zodiac-sign-296.htm](https://www.finanznachrichten.de/nachrichten-2025-12/67325767-actuls-tatiana-borsch-releases-complete-horoscope-2026-astrological-predictions-2026-for-every-zodiac-sign-296.htm) — "Renowned astrologer Tatiana Borsch has announced the release of 'Complete Horoscope 2026'" — a surname in a syndicated horoscope press release, not the dish. _(pattern: `tatiana\s+borsch`)_
- `news` [https://www.thisisanfield.com/2025/01/psv-eindhoven-vs-liverpool-10-key-things-to-know-ahead-of-final-group-match/](https://www.thisisanfield.com/2025/01/psv-eindhoven-vs-liverpool-10-key-things-to-know-ahead-of-final-group-match/) — "The assistant referees for this one are Christian Gittelmann and Mark Borsch" — a German referee's surname. _(pattern: `mark\s+borsch`)_
- `news` [https://www.timesrepublican.com/news/daily-record/2025/07/daily-record-july-5-2025/](https://www.timesrepublican.com/news/daily-record/2025/07/daily-record-july-5-2025/) — Same blotter series: "Anistyn Grace Borsch made an initial appearance" — surname. _(pattern: `anistyn(?:\s+grace)?\s+borsch`)_
- `news` [https://www.timesrepublican.com/news/daily-record/2025/07/daily-record-july-7-2025/](https://www.timesrepublican.com/news/daily-record/2025/07/daily-record-july-7-2025/) — Police blotter: "Anistyn Borsch, 19, was arrested for operating while under the influence" — surname. _(pattern: `anistyn(?:\s+grace)?\s+borsch`)_
- `news` [https://www.wdrb.com/news/business/owners-of-old-louisville-tavern-to-open-takeout-only-location-this-march/article_aff0f79e-e277-11ef-9f53-dbee743a340c.html](https://www.wdrb.com/news/business/owners-of-old-louisville-tavern-to-open-takeout-only-location-this-march/article_aff0f79e-e277-11ef-9f53-dbee743a340c.html) — All four hits are tavern co-owner "Dan Borsch" — surname. _(pattern: `dan\s+borsch`)_
- `news` [https://www.wfsb.com/2025/02/20/ukrainian-community-connecticut-concerned-after-trump-lashes-out-zelensky/](https://www.wfsb.com/2025/02/20/ukrainian-community-connecticut-concerned-after-trump-lashes-out-zelensky/) — Genuine Ukraine-war story, but all three hits are the surname of interviewee "Olga Borsch, two Ukrainian nationals… said Borsch" — a person, not the dish. _(pattern: `olga\s+borsch`)_
- `reddit` [https://reddit.com/r/Horoscope/comments/1intmtk](https://reddit.com/r/Horoscope/comments/1intmtk) — "Astrologer Tatiana Borsch, who accurately predicted the crises of 2020…" — surname (same promo as the news rows). _(pattern: `tatiana\s+borsch`)_
- `reddit` [https://reddit.com/r/InfluencerNSFW/comments/1jpn72j](https://reddit.com/r/InfluencerNSFW/comments/1jpn72j) — Whole post (title and body) is "Katarina borsch" in an NSFW influencer sub — a performer's name, no usage. _(pattern: `katarina\s+borsch`)_
- `reddit` [https://reddit.com/r/ScottishFootball/comments/1jfxbrz](https://reddit.com/r/ScottishFootball/comments/1jfxbrz) — Match thread referee list: "Tobias Stieler, Christian Gittelmann, Mark Borsch" — surname. _(pattern: `mark\s+borsch`)_
- `reddit` [https://reddit.com/r/astrology/comments/1intqf5](https://reddit.com/r/astrology/comments/1intqf5) — Same Tatiana Borsch horoscope promo crossposted to r/astrology — surname. _(pattern: `tatiana\s+borsch`)_
- `reddit` [https://reddit.com/r/eurovision/comments/1i8fnnr](https://reddit.com/r/eurovision/comments/1i8fnnr) — "João Borsch – …Pelas Costuras | Festival da Canção 2024" — a Portuguese singer's surname. _(pattern: `jo[ãa]o\s+borsch`)_
- `reddit` [https://reddit.com/r/eurovision/comments/1irhq94](https://reddit.com/r/eurovision/comments/1irhq94) — Festival da Canção line-up: "**João Borsch** invites… **Maria João**" — surname. _(pattern: `jo[ãa]o\s+borsch`)_
- `reddit` [https://reddit.com/r/eurovision/comments/1j6xdzk](https://reddit.com/r/eurovision/comments/1j6xdzk) — "João Borsch, Maria João e HUCA - As Vitórias da Suíça | Festival da Canção 2025" — surname (and Portuguese). _(pattern: `jo[ãa]o\s+borsch`)_

**luhansk** (1)

- `reddit` [https://reddit.com/r/AdamCarolla/comments/1k1tg69](https://reddit.com/r/AdamCarolla/comments/1k1tg69) — Comedy 'Carollisms' list uses Lugansk as an invented person's name: 'Lugansk - That gay diver who hit his head...' _(pattern: `carollism`)_

**kharkiv** (2)

- `youtube` [https://youtube.com/watch?v=4zYnEYTFbms](https://youtube.com/watch?v=4zYnEYTFbms) — 'Sergei Kharkov URS' is the Soviet gymnast's surname ('Muscovite Sergei Kharkov'), not the city. _(pattern: `serge[iy]\s+kharkov`)_
- `youtube` [https://youtube.com/watch?v=siZOB_z_CcQ](https://youtube.com/watch?v=siZOB_z_CcQ) — 'Simona Kharkov (ISR)' is an Israeli dancesport competitor's surname, not the city. _(pattern: `simona\s+kharkov`)_

**serhii-korolyov** (6)

- `news` [http://jamestown.org/putins-elites-could-become-proponents-of-peace/](http://jamestown.org/putins-elites-could-become-proponents-of-peace/) — Match is 'Federal Security Service (FSB) Deputy Director Sergei Korolev' — a Russian security-service official, not the rocket designer. _(pattern: `(fsb|federal security service)[^.]{0,120}sergei korolev`)_
- `news` [https://www.pr-inside.com/yamaha-motor-finance-canada-leverages-inovatec-s-propel-portal-to-streamline-r5094219.htm](https://www.pr-inside.com/yamaha-motor-finance-canada-leverages-inovatec-s-propel-portal-to-streamline-r5094219.htm) — Press release quoting 'Sergei Korolev, general manager, Propel' at Inovatec Systems — a Canadian fintech executive namesake. _(pattern: `sergei korolev,\s*general manager`)_
- `news` [https://www.sb.by/en/three-more-death-pits-from-great-patriotic-war-years-discovered-near-mogilev.html](https://www.sb.by/en/three-more-death-pits-from-great-patriotic-war-years-discovered-near-mogilev.html) — Match is 'the commander of the Pobeda search group, Sergei Korolev' exhuming WWII graves near Mogilev — a living Belarusian namesake, not the designer. _(pattern: `pobeda search group,\s*sergei korolev`)_
- `reddit` [https://reddit.com/r/CharacterRant/comments/1jn3ioe](https://reddit.com/r/CharacterRant/comments/1jn3ioe) — 'Sergei Korolev is a boss in the Russian mafia' — listed among Putin-era criminals/siloviki; a namesake, not the designer. _(pattern: `sergei korolev[^.]{0,60}(russian mafia|organi[sz]ed crime)`)_
- `reddit` [https://reddit.com/r/CredibleDiplomacy/comments/1o4ui9b](https://reddit.com/r/CredibleDiplomacy/comments/1o4ui9b) — FSB-succession podcast post: 'crown prince Sergei Korolev... perhaps the next head of the FSB' — the security official namesake. _(pattern: `(fsb|federal security service)[^.]{0,120}sergei korolev`)_
- `reddit` [https://reddit.com/r/somethingiswrong2024/comments/1pdotvj](https://reddit.com/r/somethingiswrong2024/comments/1pdotvj) — Security-services roster: 'Sergei Korolev, First Deputy Director, Federal Security Service' — the FSB namesake. _(pattern: `sergei korolev,\s*first deputy director`)_

**feodosiia** (6)

- `reddit` [https://reddit.com/r/OrthodoxChristianity/comments/1lyt39a](https://reddit.com/r/OrthodoxChristianity/comments/1lyt39a) — All 4 hits in the 11KB hagiography are 'Feodosiya Lapschina', Alexander Schmorell's Russian nanny; the city never appears. _(pattern: `feodosiya\s+lapschina`)_
- `reddit` [https://reddit.com/r/OrthodoxGreece/comments/1lyt7s5](https://reddit.com/r/OrthodoxGreece/comments/1lyt7s5) — Crosspost of the identical Schmorell text; all 4 hits are the nanny 'Feodosiya Lapschina', not the city. _(pattern: `feodosiya\s+lapschina`)_
- `youtube` [https://youtube.com/watch?v=79_o7XAVmlQ](https://youtube.com/watch?v=79_o7XAVmlQ) — Uzbek local-news clip whose whole title is the personal name 'Feodosiya To'qliyeva'; no city referent anywhere. _(pattern: `feodosiya\s+to(?:&#39;|['’`])?qliyeva`)_
- `youtube` [https://youtube.com/watch?v=I2Lhwfhpwh8](https://youtube.com/watch?v=I2Lhwfhpwh8) — Auto-generated Melodiya credit; both hits are the pianist 'Feodosiya Mironova' in the performer line and tags. _(pattern: `feodosiya\s+mironova`)_
- `youtube` [https://youtube.com/watch?v=KlZpZAZRxAk](https://youtube.com/watch?v=KlZpZAZRxAk) — Third Melodiya auto-upload; 'Feodosiya Mironova' appears only in the performer credits and tags. _(pattern: `feodosiya\s+mironova`)_
- `youtube` [https://youtube.com/watch?v=th11QHOGjgw](https://youtube.com/watch?v=th11QHOGjgw) — Auto-generated Melodiya credit list; 'Feodosiya Mironova' is a performer name, not the city. _(pattern: `feodosiya\s+mironova`)_

### other — 96 drops

**odesa** (10)

- `reddit` [https://reddit.com/r/UFOs/comments/1hvprte](https://reddit.com/r/UFOs/comments/1hvprte) — Bare title 'Odessa 2019 ID Request', no body; referent unverifiable in a US-dominated sub.
- `reddit` [https://reddit.com/r/UFOs_Archives/comments/1hvpt7c](https://reddit.com/r/UFOs_Archives/comments/1hvpt7c) — Crosspost of the same bare 'Odessa 2019 ID Request'; referent unverifiable.
- `reddit` [https://reddit.com/r/UFOs_Archives/comments/1hvpvf3](https://reddit.com/r/UFOs_Archives/comments/1hvpvf3) — Second crosspost of the same bare 'Odessa 2019 ID Request'; referent unverifiable.
- `youtube` [https://youtube.com/watch?v=-8o4tw2XxgI](https://youtube.com/watch?v=-8o4tw2XxgI) — Hashtag-only phone-sale Short (#odessa #ua); a seller geotag, not running text.
- `youtube` [https://youtube.com/watch?v=AJOvBZpP3_o](https://youtube.com/watch?v=AJOvBZpP3_o) — Hashtag-only Short (#adventure #warzone #odessa #history); no running text.
- `youtube` [https://youtube.com/watch?v=ATM-GcqWhhw](https://youtube.com/watch?v=ATM-GcqWhhw) — Emoji-and-hashtag-only Short (#odessa #bicycle); no running text.
- `youtube` [https://youtube.com/watch?v=cjVPUmwuGak](https://youtube.com/watch?v=cjVPUmwuGak) — Hashtag-only Short (#odessa #blacksea #october); no running text.
- `youtube` [https://youtube.com/watch?v=duLuHNpHLuk](https://youtube.com/watch?v=duLuHNpHLuk) — Hashtag-only skatepark Short; no prose and the Odessa referent is unverifiable.
- `youtube` [https://youtube.com/watch?v=vqWFainqZ78](https://youtube.com/watch?v=vqWFainqZ78) — Hashtag-only phone-sale Short (#odessa #ua); seller geotag, not running text.
- `youtube` [https://youtube.com/watch?v=zLUu1zMfTjM](https://youtube.com/watch?v=zLUu1zMfTjM) — Hashtag-only yoga Short; #odessa is a geotag, not running text.

**kyiv** (1)

- `youtube` [https://youtube.com/watch?v=qpIzu1MGq-4](https://youtube.com/watch?v=qpIzu1MGq-4) — Hashtag-only title ("#travel #ukraine #odessa #kiev #lviv...") with empty description; no running text to quote.

**lviv** (1)

- `reddit` [https://reddit.com/r/DnB/comments/1kf3z7v](https://reddit.com/r/DnB/comments/1kf3z7v) — r/DnB run-on tracklist; 'Japanese Horror Gred Lvov/Kybel' is an artist/track credit inside a DJ set listing - not the city and not running text.

**mykola-hohol** (2)

- `reddit` [https://reddit.com/r/GenZHumor/comments/1iframx](https://reddit.com/r/GenZHumor/comments/1iframx) — r/GenZHumor image macro 'nikolai gogol wanna be carti' with no body text; cannot tell whether it is the writer's portrait or the BSD character.
- `youtube` [https://youtube.com/watch?v=zLclc9eapGc](https://youtube.com/watch?v=zLclc9eapGc) — Bare title 'Nikolai gogol', no description, tags or channel context; referent unverifiable.

**borscht** (2)

- `reddit` [https://reddit.com/r/u_SECXREWT/comments/1l0yx3i](https://reddit.com/r/u_SECXREWT/comments/1l0yx3i) — Deliberately garbled profile "diary" ("borsby… borsch eat,, interrupted it… the borsny is not even thgat good") — not quotable prose in any register.
- `wikipedia` [https://en.wikipedia.org/wiki/Borscht](https://en.wikipedia.org/wiki/Borscht) — Substring false positive: the page IS "Borscht", the Ukrainian/UNESCO spelling. "borsch" only matches inside "borscht", so raw_wikipedia records the identical 590,057 views under BOTH variants (6.42M lifetime each), and the Ukrainian-titled page is exhibited as a Russian-spelling holdout. The single Wikipedia row for this pair is 100% artefact. _(pattern: `^borscht$`)_

**donbas** (14)

- `news` [https://www.moddb.com/mods/new-era-mod/downloads/new-era-mod-version-204](https://www.moddb.com/mods/new-era-mod/downloads/new-era-mod-version-204) — moddb.com: not a news article — a ModDB mod-download page; the sole match is a changelog bullet '-Added Ukrainian Donbass as a separate region'. _(pattern: `moddb\.com`)_
- `reddit` [https://reddit.com/r/shitposting/comments/1i5wmw6](https://reddit.com/r/shitposting/comments/1i5wmw6) — r/shitposting: 56-character word-salad title "volodymyr zelendroid ukrainoid donbass cyborg technology" — no syntax, no quotable referent.
- `youtube` [https://youtube.com/watch?v=4bkjqdeNPXw](https://youtube.com/watch?v=4bkjqdeNPXw) — Dhyeya TV: sole match is the title hashtag #donbass; description is a playlist-link dump — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=AYgczH7etK8](https://youtube.com/watch?v=AYgczH7etK8) — Brandon Mitchell: sole match is a trailing hashtag on a Lviv memorial video — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=CguOQM0BDjI](https://youtube.com/watch?v=CguOQM0BDjI) — Brennpunkt: title is a bare German hashtag string, description empty — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=Ie6mDCxvves](https://youtube.com/watch?v=Ie6mDCxvves) — Videos from Mariupol: sole match is a trailing hashtag (#donbass) on a Mariupol clip — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=PRV1jO5q7uY](https://youtube.com/watch?v=PRV1jO5q7uY) — WarArchive: sole match is a trailing hashtag (#donbass) after the caption — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=Z-iickEOYv4](https://youtube.com/watch?v=Z-iickEOYv4) — Spain Mapping: sole match sits in a pure hashtag list in the description — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=_Kdc6_fBA2k](https://youtube.com/watch?v=_Kdc6_fBA2k) — Brennpunkt: title is a bare German hashtag string, description empty — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=h1Ocov8bWjA](https://youtube.com/watch?v=h1Ocov8bWjA) — Patrick Lancaster: sole match sits inside the description's "Hashtags:" block — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=nV339EU7LCE](https://youtube.com/watch?v=nV339EU7LCE) — Quick Geo Facts: sole match sits in a pure hashtag list in the description — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=o7-yk0_LqPc](https://youtube.com/watch?v=o7-yk0_LqPc) — Oneindia News: sole match sits inside the description's hashtag block — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=q9BXlLC_Qec](https://youtube.com/watch?v=q9BXlLC_Qec) — jaySTARR: title is a bare hashtag string ("chas #ukrainewar #donbass …") — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_
- `youtube` [https://youtube.com/watch?v=tL4kvMAeVEY](https://youtube.com/watch?v=tL4kvMAeVEY) — Brennpunkt: title is a bare German hashtag string, description empty — a tag, not running text. _(pattern: `(?:^|\s)#\w*donbass\w*`)_

**luhansk** (5)

- `reddit` [https://reddit.com/r/bandcamp_discovery/comments/1jjhw8y](https://reddit.com/r/bandcamp_discovery/comments/1jjhw8y) — Automated weekly Bandcamp promo; 'lugansk' is only a getmusic.fm tag link, not prose _(pattern: `getmusic\.fm/tags/lugansk`)_
- `youtube` [https://youtube.com/watch?v=-6SD_ImMS3c](https://youtube.com/watch?v=-6SD_ImMS3c) — DawnNews English: 'lugansk' appears only in the trailing hashtag block; the English body never names it _(pattern: `#lugansk\b`)_
- `youtube` [https://youtube.com/watch?v=C5N9KTbSEgA](https://youtube.com/watch?v=C5N9KTbSEgA) — History Facts: the only occurrence is the image-source URL 'http://gorod.lugansk.ua/index.php?newsid=2460' in a credits list _(pattern: `lugansk\.ua`)_
- `youtube` [https://youtube.com/watch?v=hVl4paLvLzw](https://youtube.com/watch?v=hVl4paLvLzw) — Mentour Pilot: the only occurrences are in the image-credit block — a Wikimedia file caption and filename ('File:Valery_Bolotov_proclaims_Lugansk_People%27s_Republic...'), never the channel's own prose _(pattern: `wikimedia\.org/wiki/file:\S*lugansk`)_
- `youtube` [https://youtube.com/watch?v=r8hycg39hz8](https://youtube.com/watch?v=r8hycg39hz8) — UA-ball countryballs short: 'Lugansk' occurs only as a hashtag in the title, with an empty description — no running text _(pattern: `#lugansk\b`)_

**chornobyl** (1)

- `youtube` [https://youtube.com/watch?v=hlT8Tr4p9EA](https://youtube.com/watch?v=hlT8Tr4p9EA) — Two-word nonsense title 'chernobyl indecent' from a keyword-farm channel, no description; referent unverifiable.

**dynamo-kyiv** (20)

- `wikipedia` [https://en.wikipedia.org/wiki/FC_Dynamo_Kyiv](https://en.wikipedia.org/wiki/FC_Dynamo_Kyiv) — The entry is the UKRAINIAN-form title 'FC_Dynamo_Kyiv' (redirects=false), displayed under a panel captioned 'Wikipedia pages titled with the Russian spelling'. Every other pair uses the Russian-form page (Kiev, Odessa, Kharkov, Artemovsk, Chernobyl_disaster); here the Russian-form redirect 'Dynamo_Kiev' was replaced by its target, so 463,427 article pageviews are credited to the Russian spelling. _(pattern: `^fc_dynamo_kyiv$`)_
- `youtube` [https://youtube.com/watch?v=-yDHFpfOdmo](https://youtube.com/watch?v=-yDHFpfOdmo) — Phrase absent: Turkish Samsunspor opponents video ('Uefa Konferans Ligi Rakipleri'); no 'Dynamo Kiev' on record. _(pattern: `\b(konferans ligi|rakipleri|rakibi)\b`)_
- `youtube` [https://youtube.com/watch?v=1zDBviAkzic](https://youtube.com/watch?v=1zDBviAkzic) — Phrase absent: French Saint-Etienne retrospective ('l exploit incroyable qui a marqué l histoire'); no 'Dynamo Kiev' on record. _(pattern: `\b(exploit incroyable|qui a marqué|l histoire)\b`)_
- `youtube` [https://youtube.com/watch?v=2NgIKVA6ypc](https://youtube.com/watch?v=2NgIKVA6ypc) — Phrase absent: Newcastle United v Athletic Club highlights; no 'Dynamo Kiev' on record - search-relevance noise.
- `youtube` [https://youtube.com/watch?v=3mLGHVA4Weg](https://youtube.com/watch?v=3mLGHVA4Weg) — Phrase absent: Uzbek/English short about Eldor Shomurodov at Roma; nothing to do with the club and no description on record.
- `youtube` [https://youtube.com/watch?v=A_O_Bqvm-Mg](https://youtube.com/watch?v=A_O_Bqvm-Mg) — Phrase absent from the captured title (Roy Keane / Man Utd retro podcast) and no description on record - spelling unverifiable.
- `youtube` [https://youtube.com/watch?v=EKNb5eGhDNU](https://youtube.com/watch?v=EKNb5eGhDNU) — Phrase absent: Newcastle United v Benfica highlights; no 'Dynamo Kiev' on record - search-relevance noise.
- `youtube` [https://youtube.com/watch?v=NOWJF9mqSdc](https://youtube.com/watch?v=NOWJF9mqSdc) — Phrase absent from the captured title ('Want to KNOW the TRUTH About Champions League Bribery?') and no description on record - spelling unverifiable.
- `youtube` [https://youtube.com/watch?v=PtIOsBe0Z9g](https://youtube.com/watch?v=PtIOsBe0Z9g) — Phrase absent from the captured title ('Most POWERFUL Football Teams From The USSR Era') and no description on record - spelling unverifiable.
- `youtube` [https://youtube.com/watch?v=QtopdzXSHLY](https://youtube.com/watch?v=QtopdzXSHLY) — Phrase absent: Portuguese betting-tips video ('PALPITES DA QUINTA!'); no 'Dynamo Kiev' on record. _(pattern: `\b(palpites|de quinta|da quinta)\b`)_
- `youtube` [https://youtube.com/watch?v=WX3mWb8BOEw](https://youtube.com/watch?v=WX3mWb8BOEw) — Phrase absent: Turkish Galatasaray-Adana Demirspor VAR video; no 'Dynamo Kiev' on record - search-relevance noise.
- `youtube` [https://youtube.com/watch?v=YHAG3_bffCQ](https://youtube.com/watch?v=YHAG3_bffCQ) — Phrase absent: Lamine Yamal short; no 'Dynamo Kiev' anywhere on record - search-relevance noise.
- `youtube` [https://youtube.com/watch?v=Zr80V5hvuaE](https://youtube.com/watch?v=Zr80V5hvuaE) — Phrase absent from the captured title (generic Ronaldo edit short) and no description on record - spelling unverifiable.
- `youtube` [https://youtube.com/watch?v=_lZKIriUC_E](https://youtube.com/watch?v=_lZKIriUC_E) — Phrase absent: Turkish video about Galatasaray-Adana Demirspor VAR audio; no 'Dynamo Kiev' in the captured title and no description on record - pure YouTube search-relevance noise.
- `youtube` [https://youtube.com/watch?v=brsYkp0zQrw](https://youtube.com/watch?v=brsYkp0zQrw) — Phrase absent from the captured title ('UEFA Conference League Predictions - Betting Weekly LIVE') and no description on record - spelling unverifiable.
- `youtube` [https://youtube.com/watch?v=bvji2lbnRb4](https://youtube.com/watch?v=bvji2lbnRb4) — Phrase absent from the captured title ('My Europa League Matchday 8 Predictions') and no description on record - spelling unverifiable.
- `youtube` [https://youtube.com/watch?v=eH9AENboGck](https://youtube.com/watch?v=eH9AENboGck) — Phrase absent: French Shevchenko documentary ('LE ROI OUBLIÉ DU FOOTBALL'); no 'Dynamo Kiev' on record. _(pattern: `\b(le roi oublié|du football)\b`)_
- `youtube` [https://youtube.com/watch?v=lYtVA2wpJEA](https://youtube.com/watch?v=lYtVA2wpJEA) — Phrase absent: Portuguese betting-tips video ('PALPITES DE QUINTA!'); no 'Dynamo Kiev' on record and no description captured. _(pattern: `\b(palpites|de quinta|da quinta)\b`)_
- `youtube` [https://youtube.com/watch?v=nmeDal580UU](https://youtube.com/watch?v=nmeDal580UU) — Phrase absent: Shomurodov highlight reel; no 'Dynamo Kiev' on record - search-relevance noise.
- `youtube` [https://youtube.com/watch?v=tl8I0ewDZec](https://youtube.com/watch?v=tl8I0ewDZec) — Phrase absent: French AS Roma v Eintracht Frankfurt video; no 'Dynamo Kiev' on record - search-relevance noise. _(pattern: `\b(victoire importante|pour continuer|l aventure europeenne)\b`)_

**chicken-kyiv** (8)

- `news` [https://www.nzherald.co.nz/viva/food-drink/jesse-mulligan-restaurant-review-honoka-opens-in-bar-celestes-former-k-road-site-is-it-as-good/ITIYFN4HY5ES5MDKDJ6PXUBTGQ/](https://www.nzherald.co.nz/viva/food-drink/jesse-mulligan-restaurant-review-honoka-opens-in-bar-celestes-former-k-road-site-is-it-as-good/ITIYFN4HY5ES5MDKDJ6PXUBTGQ/) — Article is a yakitori restaurant review (Honoka, Auckland); both matches come from an embedded 'Viva Food' recommended-articles teaser for the Angela Casley recipe that is already counted as its own holdout entry - site furniture, not this article's prose, and a double count. _(pattern: `viva food:\s*[^.]{0,60}chicken kiev recipe`)_
- `reddit` [https://reddit.com/r/52weeksofcooking/comments/1lds0zy](https://reddit.com/r/52weeksofcooking/comments/1lds0zy) — Duplicate submission of the r/52weeksofcooking entry above: same author (u/didiwritesomething), same title and day, ids 1lds0rg/1lds0zy.
- `reddit` [https://reddit.com/r/BDSMpersonals/comments/1lupfha](https://reddit.com/r/BDSMpersonals/comments/1lupfha) — r/BDSMpersonals ad: the dish referent is real ('going to Kiev to have Chicken Kiev') but a personals post is not quotable prose and is a PII/reputation risk as a public holdout exhibit. _(pattern: `r/[A-Za-z]*personals\b`)_
- `reddit` [https://reddit.com/r/FoodPorn/comments/1jyft63](https://reddit.com/r/FoodPorn/comments/1jyft63) — Crosspost duplicate of the r/Baking entry: same author (u/Late_Volume_6404), same dish and day, reposted to r/FoodPorn.
- `reddit` [https://reddit.com/r/RateMyPlate/comments/1jyfwis](https://reddit.com/r/RateMyPlate/comments/1jyfwis) — Crosspost duplicate of the r/Baking entry: identical title and day, reposted to r/RateMyPlate.
- `reddit` [https://reddit.com/r/UK_Food/comments/1jgfibm](https://reddit.com/r/UK_Food/comments/1jgfibm) — Crosspost duplicate: same author (u/lemonsarethekey) posted the same plate to r/decentfoodporn and r/UK_Food the same day - one utterance counted twice.
- `reddit` [https://reddit.com/r/burlington/comments/1ifiibj](https://reddit.com/r/burlington/comments/1ifiibj) — Exact duplicate of the r/burlington entry above: same author (u/jinside), same title and body, sequential ids 1ifiibi/1ifiibj - one utterance counted twice.
- `reddit` [https://reddit.com/r/medical_advice/comments/1ii7ew6](https://reddit.com/r/medical_advice/comments/1ii7ew6) — Duplicate repost of the r/medical_advice entry above: same author (u/Deep_Cup_4688), same subreddit and near-identical body, ids 1ihwlsc/1ii7ew6 - one utterance counted twice.

**kharkiv** (6)

- `reddit` [https://reddit.com/r/IBMi/comments/1inom5h](https://reddit.com/r/IBMi/comments/1inom5h) — OpenSSL CSR example copied from a cheat sheet: '/C=UA/ST=Kharkov/L=Kharkov/O=Super Secure Company' - boilerplate placeholder, not usage. _(pattern: `st=kharkov|/l=kharkov`)_
- `reddit` [https://reddit.com/r/TheFireRisesMod/comments/1i1zplx](https://reddit.com/r/TheFireRisesMod/comments/1i1zplx) — Fan fiction posted to a Hearts of Iron IV mod sub; a fictional war narrative translated from Russian, not real-world reference. _(pattern: `r/thefirerisesmod`)_
- `reddit` [https://reddit.com/r/TheFireRisesMod/comments/1i21f1w](https://reddit.com/r/TheFireRisesMod/comments/1i21f1w) — Same fan fiction, redacted repost; fictional narrative on a HOI4 mod sub. _(pattern: `r/thefirerisesmod`)_
- `reddit` [https://reddit.com/r/rpmap2/comments/1hx032g](https://reddit.com/r/rpmap2/comments/1hx032g) — Roleplay-map JSON dump; 'Kharkov_1-11' is a map-region path token, not running text. _(pattern: `kharkov\\?_\d+-\d+`)_
- `youtube` [https://youtube.com/watch?v=V6CuuZzUY5Y](https://youtube.com/watch?v=V6CuuZzUY5Y) — Bare location hashtag '#Kharkov' on a ballroom-dance competition clip; no running text, uploader is a Ukrainian/Polish dance channel. _(pattern: `balroomdancing|ballroomdancing`)_
- `youtube` [https://youtube.com/watch?v=yX_LmvrIp8E](https://youtube.com/watch?v=yX_LmvrIp8E) — Bare location hashtag '#Kharkov' on a ballroom-dance competition clip; no running text. _(pattern: `balroomdancing|ballroomdancing`)_

**volodymyr-zelenskyy** (4)

- `reddit` [https://reddit.com/r/BeAmazed/comments/1j0irb3](https://reddit.com/r/BeAmazed/comments/1j0irb3) — r/BeAmazed: title and body are the bare string “Vladimir Zelensky” on an image post — no running text at all. _(pattern: `^vladimir zelensky$`)_
- `wikipedia` [https://en.wikipedia.org/wiki/Volodymyr_Zelenskyy](https://en.wikipedia.org/wiki/Volodymyr_Zelenskyy) — The only Wikipedia holdout row is titled 'Volodymyr Zelenskyy' — the UKRAINIAN form. dataset/raw_wikipedia.parquet carries this single title under BOTH variants with identical pageviews (25,031,591 each) because the pageviews API resolves the 'Vladimir Zelensky' redirect to the canonical title, so the exhibit asserts a Russian-titled article that does not exist. _(pattern: `^volodymyr zelenskyy$`)_
- `youtube` [https://youtube.com/watch?v=8zb_uqiinAc](https://youtube.com/watch?v=8zb_uqiinAc) — FH production: the name appears only in an undelimited cast-name dump (“Starring AI models: … Vladimir Putin Joe Biden Vladimir Zelensky …”) for an AI deepfake short — not running text. _(pattern: `starring ai models:`)_
- `youtube` [https://youtube.com/watch?v=GeckZmZsJc8](https://youtube.com/watch?v=GeckZmZsJc8) — Maks PRO Media: the name appears only as a bullet in a leader-name list (“• Vladimir Zelensky (Ukraine)”) in an AI-art description — not running text. _(pattern: `•\s*vladimir zelensky \(ukraine\)`)_

**kyivan-rus** (2)

- `reddit` [https://reddit.com/r/ClinicalGenetics/comments/1ibghnm](https://reddit.com/r/ClinicalGenetics/comments/1ibghnm) — Sole match is a MyTrueAncestry ancient-DNA sample label ('# Kievan Rus 1130 AD #') inside a pasted SNP results table, not the poster's own prose. _(pattern: `kievan rus\s+\d{3,4}\s*ad`)_
- `reddit` [https://reddit.com/r/Genealogy/comments/1iq5lvl](https://reddit.com/r/Genealogy/comments/1iq5lvl) — Body is verbatim ChatGPT output ('Here is a Report it created for me'); all 25 hits are model-generated text, not a writer's spelling choice. _(pattern: `(?:analysis|report)\s+from\s+chatgpt`)_

**varenyky** (8)

- `youtube` [https://youtube.com/watch?v=ASlQJdKwd_U](https://youtube.com/watch?v=ASlQJdKwd_U) — Bare one-word title 'vareniki', empty description, no tags, Slavic-named personal channel; no English context.
- `youtube` [https://youtube.com/watch?v=As3vkEcmXXM](https://youtube.com/watch?v=As3vkEcmXXM) — Bare one-word title 'vareniki' with an empty description and no tags; nothing in the record is English or identifies anything beyond the term itself.
- `youtube` [https://youtube.com/watch?v=H_qocGJzMsY](https://youtube.com/watch?v=H_qocGJzMsY) — Title is the bare term plus a generic hashtag run ('Vareniki#asmr#cooking#food#thebakefeed#breakfast#food#shorts'), empty description, no tags; no descriptive text in any language.
- `youtube` [https://youtube.com/watch?v=NoG8Sgoq4SU](https://youtube.com/watch?v=NoG8Sgoq4SU) — Title is a bare hashtag string mixing German and English tags ('#vareniki #ukraine #ukrainische #ukrainianfood'), empty description, Cyrillic-named channel; no descriptive text in any language. _(pattern: `^(?:\s*#\w+)+\s*$`)_
- `youtube` [https://youtube.com/watch?v=xrrpnO2yJZw](https://youtube.com/watch?v=xrrpnO2yJZw) — Title is the bare term 'Vareniki' with an empty description and no tags on the Uzbek-named channel 'Bekajon'; no English context.
- `youtube` [https://youtube.com/watch?v=yvjjINIEVJc](https://youtube.com/watch?v=yvjjINIEVJc) — Hashtag-only title ('#ukraine #ukrainian #food #foodlover #ukrainianfood #vareniki #christian #jesus #fall #foodie #art') with an empty description; no descriptive text, and half the tags are off-topic reach tags. _(pattern: `^(?:\s*#\w+)+\s*$`)_
- `youtube` [https://youtube.com/watch?v=zQF-2-3uZis](https://youtube.com/watch?v=zQF-2-3uZis) — Title is two generic hashtags plus the emoji-wrapped bare term ('#food #recipe 🥟🥟Vareniki🥟🥟'), empty description, no tags; no descriptive text.
- `youtube` [https://youtube.com/watch?v=zR1B2fCISvk](https://youtube.com/watch?v=zR1B2fCISvk) — Title is the bare term plus a Russian @handle ('VARENIKI @yuraivanov9510'), empty description, no tags, Tibetan-script channel name; no English context. _(pattern: `^\s*vareniki\s*@`)_

**oleksandr-usyk** (3)

- `reddit` [https://reddit.com/r/boxingcirclejerk/comments/1kb0hbr](https://reddit.com/r/boxingcirclejerk/comments/1kb0hbr) — Dead exhibit and a duplicate: data/audit/reddit_liveness.json records status='removed' (the shipped row itself carries live:false), and its text is identical to the live profile mirror 1kbii6g already in this table.
- `wikipedia` [https://en.wikipedia.org/wiki/Oleksandr_Usyk](https://en.wikipedia.org/wiki/Oleksandr_Usyk) — The page title is the UKRAINIAN spelling. dataset/raw_wikipedia.parquet holds one page, 'Oleksandr Usyk', with identical pageviews duplicated under variant='russian' AND variant='ukrainian' (17,684,959 each), so the russian-variant filter emitted a Ukrainian-titled article. Live API check: 'Alexander Usyk' is 'missing' on en.wikipedia — not even a redirect — so the correct wikipedia holdout list for this pair is empty. _(pattern: `^oleksandr[ _]usyk$`)_
- `youtube` [https://youtube.com/watch?v=FB7pEb-PWYM](https://youtube.com/watch?v=FB7pEb-PWYM) — Title displayed on the site reads 'Oleksander Usyk' (a Ukrainian-form misspelling); only the auto-written description carries 'Alexander Usyk', so the row shows the reader the opposite spelling to the header it sits under. _(pattern: `\boleksander\b`)_

**kazymyr-malevych** (2)

- `news` [https://pjmedia.com/stephen-kruiser/2025/10/17/the-morning-briefing-republican-women-are-not-pulling-any-punches-when-bashing-dems-n4944958](https://pjmedia.com/stephen-kruiser/2025/10/17/the-morning-briefing-republican-women-are-not-pulling-any-punches-when-bashing-dems-n4944958) — Sole occurrence is the byline of an embedded tweet from the @artistmalevich art-bot ('- Kazimir Malevich (@artistmalevich) October 4, 2025') inside a US political morning briefing; not prose about the painter. _(pattern: `\(@artistmalevich\)`)_
- `youtube` [https://youtube.com/watch?v=UFxCNLQ8Z9Q](https://youtube.com/watch?v=UFxCNLQ8Z9Q) — Row is stored as variant=ukrainian with matched_term 'Kazymyr Malevych', but neither title nor description contains any Ukrainian form - only 'Kazimir Malevich'; it is a false Ukrainian attestation (and a duplicate re-upload of mOFe2M39mWU).

**ternopil** (4)

- `reddit` [https://reddit.com/r/AutoTransport/comments/1p5atbv](https://reddit.com/r/AutoTransport/comments/1p5atbv) — Auto-transport review-fraud dispute; both matches are inside a Ukrainian classifieds URL slug (automoto.ua/uk/Porsche-Macan-2023-Ternopol-…), not running text. _(pattern: `https?://[^\s)\]]*ternopol`)_
- `reddit` [https://reddit.com/r/AutoTransportReviews/comments/1p6tgbd](https://reddit.com/r/AutoTransportReviews/comments/1p6tgbd) — Same author, same dispute, same defect: the only occurrences are the automoto.ua Porsche-Macan URL slug pasted twice — no prose usage. _(pattern: `https?://[^\s)\]]*ternopol`)_
- `reddit` [https://reddit.com/r/ukraineforeignlegion/comments/1ivc4yk](https://reddit.com/r/ukraineforeignlegion/comments/1ivc4yk) — Content is genuine English usage for the real city ("getting to Ternopol from Warsaw"), but the post is removed — data/audit/reddit_liveness.json says status=removed, author=[deleted], and the holdout itself carries live:false, so the link is dead and nothing is verifiable to a reader.
- `youtube` [https://youtube.com/watch?v=T7R4VyZrjn0](https://youtube.com/watch?v=T7R4VyZrjn0) — Title is Ukrainian ("Водоспад. Waterfall.") and contains no variant; the only occurrence is an SEO hashtag in the description ("#Mykulinsk #ternopol region"). _(pattern: `#\s?ternopol`)_

**feodosiia** (2)

- `youtube` [https://youtube.com/watch?v=LfMieNWYuKI](https://youtube.com/watch?v=LfMieNWYuKI) — Second DX log from the same channel/date ('Nashe Radio [RUS] (Feodosiya/mis Illi) ~ 1038KM'); transmitter-site field, not running text. _(pattern: `\(es\)\s*\d{2}\.\d{2}\.\d{4}`)_
- `youtube` [https://youtube.com/watch?v=_Od4Kq8DljA](https://youtube.com/watch?v=_Od4Kq8DljA) — Sporadic-E radio reception log '(ES) 11.06.2025 Marusya FM [RUS] (Feodosiya/mis Illi) ~ 1038KM'; the name is a transmitter-site field in a DX log, not running text. _(pattern: `\(es\)\s*\d{2}\.\d{2}\.\d{4}`)_

**volodymyr-the-great** (1)

- `youtube` [https://youtube.com/watch?v=KoS3xQOz1GY](https://youtube.com/watch?v=KoS3xQOz1GY) — Meme repost whose entire text is the title plus 'credits to dumbcone hahahaha'; nothing establishes whether the referent is the prince or the Putin nickname.

### city-usage — 23 drops

**odesa** (22)

- `news` [https://929zzu.com/2025/09/18/enjoy-traditional-german-food-and-music-at-this-years-deutschesfest-in-odessa/](https://929zzu.com/2025/09/18/enjoy-traditional-german-food-and-music-at-this-years-deutschesfest-in-odessa/) — Syndicated copy of the same Odessa, Washington Deutschesfest item. _(pattern: `odessa,?\s*wash(?:ington)?\b|deutschesfest`)_
- `news` [https://chescotimes.com/?p=42789](https://chescotimes.com/?p=42789) — All 11 hits are Odessa, Delaware (Historic Odessa Foundation, 'Christmas in Odessa'); no Ukrainian content. _(pattern: `historic\s+odessa|odessa,?\s*delaware|women'?s\s+club\s+of\s+odessa`)_
- `news` [https://dailyyonder.com/a-jewish-farming-legacy-in-south-jersey/2025/08/06/](https://dailyyonder.com/a-jewish-farming-legacy-in-south-jersey/2025/08/06/) — Sole hit is 'New Odessa, Oregon', a 19th-c. Am Olam colony in the US. _(pattern: `new\s+odessa`)_
- `news` [https://familydestinationsguide.com/budget-town-delaware-live/](https://familydestinationsguide.com/budget-town-delaware-live/) — 33 hits, all a travel feature on Odessa, Delaware; the Ukrainian city appears only as the namesake. _(pattern: `odessa,?\s+(?:de|delaware)\b|odessa,\s*de\s+197`)_
- `news` [https://www.kxly.com/news/enjoy-traditional-german-food-and-music-at-this-years-deutschesfest-in-odessa/article_a79b3b9b-1dfd-4b38-8d04-10fde85308ba.html](https://www.kxly.com/news/enjoy-traditional-german-food-and-music-at-this-years-deutschesfest-in-odessa/article_a79b3b9b-1dfd-4b38-8d04-10fde85308ba.html) — Dateline 'ODESSA, Wash.' - Deutschesfest in Odessa, Washington. _(pattern: `odessa,?\s*wash(?:ington)?\b|deutschesfest`)_
- `openalex` [https://openalex.org/W7130557026](https://openalex.org/W7130557026) — 'Nova Odessa' is a municipality in São Paulo state, Brazil. _(pattern: `nova\s+odessa`)_
- `reddit` [https://reddit.com/r/Breastaurantlove/comments/1hw2vqf](https://reddit.com/r/Breastaurantlove/comments/1hw2vqf) — Second crosspost of 'Twin Peaks Odessa', the Texas location. _(pattern: `twin\s+peaks\s+odessa`)_
- `reddit` [https://reddit.com/r/Brockville/comments/1ht4wla](https://reddit.com/r/Brockville/comments/1ht4wla) — 'fast food junk at Mallorytown or Odessa' - the Odessa, Ontario ONroute on Highway 401. _(pattern: `onroute|mallorytown`)_
- `reddit` [https://reddit.com/r/Delaware/comments/1hrfq76](https://reddit.com/r/Delaware/comments/1hrfq76) — r/Delaware drone sighting: 'what was that white line above the odessa area???' - Odessa, Delaware. _(pattern: `^r/delaware\b`)_
- `reddit` [https://reddit.com/r/LandmanSeries/comments/1hr4rvz](https://reddit.com/r/LandmanSeries/comments/1hr4rvz) — Permian oilfield thread: 'an inspection of multiple structures... in Odessa' - Odessa, Texas. _(pattern: `\b(?:permian|midland|landman|west\s+texas|oil\s+jobs)\b`)_
- `reddit` [https://reddit.com/r/Midessa/comments/1hv9kce](https://reddit.com/r/Midessa/comments/1hv9kce) — 'Odessa traffic is bad' - driving test in Odessa, Texas (r/Midessa = Midland-Odessa). _(pattern: `^r/midessa\b`)_
- `reddit` [https://reddit.com/r/hooters/comments/1hw2vdd](https://reddit.com/r/hooters/comments/1hw2vdd) — 'Twin Peaks Odessa' - the restaurant chain's Odessa, Texas location. _(pattern: `twin\s+peaks\s+odessa`)_
- `reddit` [https://reddit.com/r/oilandgasworkers/comments/1hukxpj](https://reddit.com/r/oilandgasworkers/comments/1hukxpj) — 'ready to drive out to Odessa or Williston' - Permian vs Bakken oilfields, Odessa, Texas. _(pattern: `williston|bakken|permian`)_
- `reddit` [https://reddit.com/r/trainwrecks/comments/1hvbckz](https://reddit.com/r/trainwrecks/comments/1hvbckz) — US local-news headline 'train colliding with stalled semi in Odessa; no injuries reported'; no Ukraine context.
- `reddit` [https://reddit.com/r/twinpeakshotties/comments/1hw2vj0](https://reddit.com/r/twinpeakshotties/comments/1hw2vj0) — Crosspost of 'Twin Peaks Odessa', the Texas location. _(pattern: `twin\s+peaks\s+odessa`)_
- `youtube` [https://youtube.com/watch?v=1EwxlG5uOio](https://youtube.com/watch?v=1EwxlG5uOio) — KCEN (Texas): 'Odessa teen accused of shooting'; tagged texas-news.
- `youtube` [https://youtube.com/watch?v=1e7YHBP70N8](https://youtube.com/watch?v=1e7YHBP70N8) — 'Odessa Priority Emergency Room' - an Odessa, Texas ER advertisement. _(pattern: `odessa\s+priority\s+emergency\s+room`)_
- `youtube` [https://youtube.com/watch?v=2RmiwjxbYb4](https://youtube.com/watch?v=2RmiwjxbYb4) — KTSM 9 (El Paso): 'Odessa triple homicide'; 'The Odessa Police Department'.
- `youtube` [https://youtube.com/watch?v=2a63-GV4yns](https://youtube.com/watch?v=2a63-GV4yns) — KIII 3 News (Corpus Christi): 'Triple homicide in Odessa' - Odessa, Texas.
- `youtube` [https://youtube.com/watch?v=VwO9EJa4qow](https://youtube.com/watch?v=VwO9EJa4qow) — KCEN (Texas): 'Odessa teen charged with capital murder'.
- `youtube` [https://youtube.com/watch?v=YZ5SrUPTyCo](https://youtube.com/watch?v=YZ5SrUPTyCo) — NewsWest 9 (Odessa, TX): shooting 'at a North Odessa apartment'.
- `youtube` [https://youtube.com/watch?v=m8ja8a5T2Ew](https://youtube.com/watch?v=m8ja8a5T2Ew) — Barber Short tagged #barber #florida #tampa #odessa - Odessa, Florida. _(pattern: `odessa[^a-z]{0,5}(?:fl|florida)\b|tampa[^.]{0,20}odessa`)_

**dnipro-river** (1)

- `news` [https://charter97.org/en/news/2025/12/1/665031/](https://charter97.org/en/news/2025/12/1/665031/) — Headline calls the CITY Dnipro 'the Dnieper River'; the body is a missile strike on Dnipro city, quoting the Dnipropetrovsk OVA head. Referent is pair 7's city, not the river. _(pattern: `(?:hit|struck|attack(?:ed|s)?|shell(?:ed|s)?)\s+the\s+dnieper\s+river\b`)_

### metalinguistic — 7 drops

**lviv** (3)

- `reddit` [https://reddit.com/r/omoshiroi_eigo_bunpou/comments/1it2s1o](https://reddit.com/r/omoshiroi_eigo_bunpou/comments/1it2s1o) — Japanese auto-posting dictionary bot: '「英単語解説」lvovの意味について' - a Japanese post about the word 'lvov', not a usage of it. _(pattern: `lvovの意味`)_
- `reddit` [https://reddit.com/r/omoshiroi_eigo_bunpou/comments/1j1do47](https://reddit.com/r/omoshiroi_eigo_bunpou/comments/1j1do47) — Duplicate Japanese dictionary-bot post about the word 'lvov'. _(pattern: `lvovの意味`)_
- `reddit` [https://reddit.com/r/omoshiroi_eigo_bunpou/comments/1j1zs5p](https://reddit.com/r/omoshiroi_eigo_bunpou/comments/1j1zs5p) — Third duplicate of the same Japanese dictionary-bot post. _(pattern: `lvovの意味`)_

**luhansk** (1)

- `reddit` [https://reddit.com/r/omoshiroi_eigo_bunpou/comments/1kmt0dy](https://reddit.com/r/omoshiroi_eigo_bunpou/comments/1kmt0dy) — Japanese SEO vocabulary post explaining the English word: '「英単語解説」luganskの意味について' — the word is mentioned, not used

**chicken-kyiv** (1)

- `reddit` [https://reddit.com/r/onionheadlines/comments/1irssyr](https://reddit.com/r/onionheadlines/comments/1irssyr) — r/onionheadlines satire headline 'Proposal Renaming "Chicken Kiev" To "Chicken Putin" Shot Down During Ukraine Peace Talks' - the phrase is quoted as a name under discussion in a joke about renaming, not used to refer to a dish. _(pattern: `renam\w*\s+[“"']?chicken\s*kiev`)_

**kazymyr-malevych** (1)

- `youtube` [https://youtube.com/watch?v=fEicHd50tPQ](https://youtube.com/watch?v=fEicHd50tPQ) — Pronunciation-tutorial video ('How to Pronounce ''Kazimir Malevich'' Correctly! (Russian)'); the name is cited as a word to pronounce, not used to refer to the painter. _(pattern: `how to pronounce\s*['"‘’“”]{0,2}\s*kazimir malevich`)_

**volodymyr-the-great** (1)

- `reddit` [https://reddit.com/r/yearofannakarenina/comments/1mc1kxd](https://reddit.com/r/yearofannakarenina/comments/1mc1kxd) — Reading-guide character glossary enumerating aliases side by side: '[Saint Vladimir](wiki), Vladimir the Great, Vladimir I Sviatoslavich, Volodymyr I Sviatoslavych, Old East Slavic: ..., Christian name: Basil' — a name-variant index, not a spelling choice. _(pattern: `vladimir the great,[^.]{0,80}volodymyr`)_

## Suggested drop patterns

Deduplicated per pair, exactly as proposed by the verification agents. These are candidates for `config/pairs.yaml`; run `python -m pipeline.audit.validate_patterns` before applying any of them — a pattern that is safe on a 100-entry exhibit sample can still be catastrophic corpus-wide.

### odesa — 44 pattern(s)

```
\b(?:permian|midland|landman|west\s+texas|oil\s+jobs)\b
\bm\.?\s?v\.?\s+odessa\b
^r/(?:odessatexxx|westtexasgonewild|midessa432wtxslutz|permianplaytime3|midland432m4a|midlandtxxx|trapsgonewild|wtx_pnp|texasswingersofficial|midlandstraight4gaybi|kcm4m|delawarer4r)\b
^r/delaware\b
^r/midessa\b
^r/namenerds\b
band\s+odessa
géographie des ténèbres
historic\s+odessa|odessa,?\s*delaware|women'?s\s+club\s+of\s+odessa
kagura\s+odessa|global\s+kagura
neon\s+love|\bby\s+odessa\b
new\s+odessa
nova\s+odessa
o['’]dessa|sadie\s+sink
odessa,?\s*wash(?:ington)?\b|deutschesfest
odessa,?\s+(?:de|delaware)\b|odessa,\s*de\s+197
odessa[\s-]*doran|odessa\s+canyon
odessa[^.]{0,30}\b(?:1250|model|grill|smoker|pellet)\b
odessa[^a-z]{0,5}(?:fl|florida)\b|tampa[^.]{0,20}odessa
odessa\s*[-–]\s*caribou|caribou[^.]{0,12}odessa
odessa\s+(?:group|network|organi[sz]ation)
odessa\s+(?:mini\s+)?dress
odessa\s+(?:network|group|organi[sz]ation)
odessa\s+(?:v\d|prometheus)
odessa\s+a['’]?zion
odessa\s+bulgar
odessa\s+cubbage
odessa\s+grocery
odessa\s+oliveira
odessa\s+priority\s+emergency\s+room
odessa\s+silverberg
odessa\s+young
onroute|mallorytown
parov\s+stelar
project\s+odessa
project\s+odessa|odessa\s+baby
reasonings\s+with\s+odessa
setlist
smart\s+and\s+strong\s+dogs
the\s+odessa\s+journal
the\s+odessa\s+review
twin\s+peaks\s+odessa
vampirate\s+odessa|odessa\s+delico
williston|bakken|permian
```

### kyiv — 20 pattern(s)

```
\b(?:chicken|pheasant|frog\s+legs?|mushroom|garlic|veggie|vegan|salmon|turkey|beef|cod)\s+(?:a\s*la\s+|ala\s+)?kiev\b
azur\s*lane|azurlane
d[ií]namo\s+de\s+kiev
ghost\s+of\s+kiev\s+ukrainian\s+freedom\s+vodka
giornaledibrescia\.it
green\s+soul\s+kiev|kiev\s+orthopedic
irishdentist\.ie
kiev.{0,40}\bdestroyer\b
kiev[\s-]*class
kiev\s*-?\s*(?:4a?m?|6\s*c?|19m?|60|88)\b
kiev\s*beats
kiev\s+kwentel
kiev\s+station
langa\s*finga|kappalani
lost\s+in\s+kiev
martens\s+kiev|kiev\s+leather\s+backpack
new escort ads|last week'?s new profiles
one\s+5g\s+ace|codename\s+kiev
recruitment\s+agency\s+esca|raise\s+karma
sam\s+hyde
```

### lviv — 29 pattern(s)

```
#lvov\b
/city/lvov/
\balexe[yi]\s+lvov\b
\bberlioz\s+et\s+lvov\b
\bchristine\s+lvov\b
\bdmitri[iy]\s+lvov\b
\bevgeny\s+\w+\s+lvov\b
\bgene\s+lvov\b
\bgeorg\w*\s+lvov\b
\bi\s+lvov\W{0,3}e\b
\blvov(?:'|’)?s\b[^.]{0,40}\bgiggy\b
\blvov-kompaneets\b
\blvov[\s-]sandomir\b
\blvov[\s–-]*warsaw\b
\blvov\s*,?\s*ukraina\b
\blvov\s+vodka\b
\bmikhail\s+lvov\b
\bnikola[iy]\s+(?:\w+\s+)?lvov\b
\bpavel\s+lvov\b
\bprince\s+lvov\b
\bromance\s+in\s+lvov\b
\bsverdlovsk[\s–-]*lvov\b
\bv\.\s*lvov\b
arkady\s+lvov
bohdan\s+lvov
david\s+lvov
dmitri\s+lvov
love\s+song\s+for\s+lemberg\s+lvov
lvovの意味
```

### mykola-hohol — 19 pattern(s)

```
#animeart|animeart
#animeart|animedrawing
,\s*de\s+nikolai\s+gogol
\(amv\)|jester-like design
^r/recomandaricarti_ro
^r/transbr
almas mortas
audiolivro|clássico da literatura russa
carte audio|povestiri
creepypasta reacts|m!y/n
despre vindecarea
islamic video|quranrecitation
klasiket|frymë të vdekura
literatura russa|história mais triste
nikolai\s*gogol[^\n]{0,40}(?:bungou?\s*stray\s*dogs|bungosd|bsd)|(?:bungou?\s*stray\s*dogs|bungosd|bsd)[^\n]{0,40}nikolai\s*gogol
nikolai\s*gogol\s*edit
quanto mais fundo
quizlit
teatru radiofonic
```

### borscht — 27 pattern(s)

```
(?:ev|auto\w*|parts|component)\s+supplier[^.]{0,25}\bborsch\b
\b(?:himar[eë]|dh[eë]rmi|ksamil|albanian?\s+riviera)\b
\b(?:onlyfans|girlcock|tgirl|cumshot|blowjob|pornstar|milf|creampie|nsfw)\b
\bborsch\b(?=[\s\S]{0,400}\b(?:bosch|refrigerator|fridge|freezer|dishwasher|compressor)\b)
\bborsch\b(?=[\s\S]{0,400}\b(?:frigidaire|dishwasher|ikea|appliance)\b)
\bborsch\b(?=[\s\S]{0,400}\b(?:hoover|vacuum|appliance|bosch)\b)
\bdon(?:ald)?\s+borsch\b
^borscht$
anistyn(?:\s+grace)?\s+borsch
borsch\s+bandit
borsch\s+belt
borsch\s+house
borsch\s+man
borschev
dan\s+borsch
frederick\s+borsch
freekarma
jerome(?:\s+francisco)?\s+borsch
jo[ãa]o\s+borsch
johnny\s+yong\s+borsch
katarina\s+borsch
mark\s+borsch
olga\s+borsch
operation\s+borsch
peter\s+borsch
tatiana\s+borsch
thomas\s+borsch
```

### donbas — 16 pattern(s)

```
(?:^|\s)#\w*donbass\w*
(?:union\s+of\s+donbass\s+volunteers|donbass\s+volunteers?\s+union)
\b(?:novyi?|new)\s+donbass\b(?!\s+line)
\breptilians?\b|\benerg(?:y|etic)\s+healer\b
^donbas$
divided\s+donbass
donbass\s*arena
donbass\s+arena
donbass\s+case
donbass\s+dome
donbass\s+media\s+cent(?:re|er)
dw\.com/ru/donbass
giornaledibrescia\.it
irishdentist\.ie
moddb\.com
odnako\.org
```

### luhansk — 9 pattern(s)

```
#lugansk\b
carollism
getmusic\.fm/tags/lugansk
lugansk\.ua
lugansk\s+4-prong
mbbs\s+(?:in\s+russia|abroad)
onlyfans\.com/
urbanhellcirclejerk
wikimedia\.org/wiki/file:\S*lugansk
```

### chornobyl — 26 pattern(s)

```
#tamil
(?:hbo'?s|miniseries|mini[- ]series)\s+chernobyl
(?:re)?watch(?:ing|ed)\s+chernobyl
9\s*lb\s+hammer|skunk\s*#\s*1
chernobyl:?\s*escape\s+from\s+pripyat
chernobyl[\s-]*(?:disaster\s*)?optimi[sz](?:er|ation|ing)
chernobyl[\s-]*(?:disaster\s*)?optimi[sz](?:er|ation|ing)|chernobyl[\s-]inspired\s+optimi
chernobyl[\s-]inspired\s+optimi
chernobyl\.\s*adam\s+nagaitis
chernobyl\s*(?:,|and)\s*(?:i\s+also|mindhunter|severance)
chernobyl\s*2\s*exclu[sz]ion\s+zone
chernobyl\s*\(2019\)
chernobyl\s*\(2019\)|#chernobyl\s*#hboseries
chernobyl\s*\(?\s*2019\s*\)?
chernobyl\s*\(?\s*76\s*sticks\)?|hooligan\s+chase
chernobyl\s+children
chernobyl\s+diaries
chernobyl\s+mini[\s-]?series
chernobyl\s+pack
chernobyl\s+vs\s+edgerunners
chilecomparte\.cl|\[?(?:1080p|720p)\]?.*\bx265\b
heart\s+of\s+cherno?byl
midnight\s+in\s+cherno?byl
mini[\s-]?series\s+chernobyl
renck\s*\(chernobyl\)|\(hbo'?s[^)]*chernobyl\)
shadow\s+of\s+cherno?byl
```

### dynamo-kyiv — 38 pattern(s)

```
(pron[oó]sticos?|apuestas deportivas|futbol europeo|jornada \d)
\b(ao vivo|sem delay|palpites|análises do|jogos do dia)\b
\b(bgmi|1v[s]?4 clutch|spraygod|m416clutch)\b
\b(bgmi|dynamo gaming|dynamogaming)\b
\b(bgmi|dynamo gaming|dynamogaming|1v[s]?4 clutch|m416)\b
\b(equipa|arriscamos|ganhar a primeira liga|já agora)\b
\b(exploit incroyable|qui a marqué|l histoire)\b
\b(golü|sanchezin|maçi|maçı)\b
\b(hasil|tadi malam|ketika|semua pemain|kualifikasi|pertandingan|melawan|mempertemukan|menewaskan|menang lagi)\b
\b(hasil|tadi malam|kualifikasi|jadwal|pertandingan)\b
\b(kariyer önerisi|fm2[0-9]kariyer|türkçe|spiker)\b
\b(konferans ligi|rakipleri|rakibi)\b
\b(le roi oublié|du football)\b
\b(maci biletleri|ne zaman|satisa cikar)\b
\b(maçini|izlerse|fanatik|rezillik|tepki videosu)\b
\b(mempertemukan|melawan|gol salto|pada tanggal)\b
\b(menang lagi|menewaskan|padu mat|kalah)\b
\b(palpites|análises do|ao vivo|sem delay|jogos do dia|o sucesso da equipa)\b
\b(palpites|de quinta|da quinta)\b
\b(pronostics?|pronostic foot|ligue des champions)\b
\b(pronostics?|pronostic foot|ligue europa conference)\b
\b(pronostics?|téléfoot|ligue des champions|ligue europa)\b
\b(rakibi|rakibimizi|tanıyalım|rakipleri)\b
\b(rezillik|tepki videosu|maçi|maçı)\b
\b(semua pemain|klub lokal|dipaksa|tentara|pertandingan)\b
\b(sintesi|andata|ritorno|calcio europeo)\b
\b(téléfoot|1/[24] de finale|coupe des clubs champions)\b
\b(téléfoot|pronostics?|1/[24] de finale|coupe de france)\b
\b(türkçe|spiker|maçi|maçı)\b
\b(victoire importante|pour continuer|l aventure europeenne)\b
\b(özet|maçi|maçı|maçini|rakibi|rakibimizi|rakipleri|golü|türkçe|spiker|kariyer önerisi|tepki videosu|biletleri|kayıtları|satisa|tanıyalım|izlerse)\b
\b(özet|maçi|maçı|rakibi|golü|türkçe|spiker|şov|futbol)\b
\bgnk\s+d[iy]namo\b
\bmeci amical\b
^fc_dynamo_kyiv$
anyone looking for matchworn shirts
free sports pick loaded from
next4k
```

### chicken-kyiv — 13 pattern(s)

```
#ai(?:generated|cat)\b
[а-яё]{4,}
\b(?:tf2|team fortress 2|pootis)\b
\bai[\s-]*(?:magic|generated)\b
arepas,\s*chicken kiev,\s*doner kebab
chicken[\s-]*kiev['’"”]?\s+(?:speech|address)
chicken\s+kiev\s+videos?\s+chicken\s+\w+\s+video
r/(?:tf2|truetf2|tf2fashionadvice|tf2shitposterclub|sfm)\b
r/[A-Za-z]*personals\b
r/kuhinja\b
r/playrust\b
renam\w*\s+[“"']?chicken\s*kiev
viva food:\s*[^.]{0,60}chicken kiev recipe
```

### kharkiv — 23 pattern(s)

```
\barma\s?3\b|milsim
\bkharkov\s+8\b
balroomdancing|ballroomdancing
call\s+of\s+duty
hell\s*let\s*loose|\bhll\b
irishdentist\.ie
jean\s+lopez
kharkov never sleeps
kharkov:?\s*battles\s+before\s+and\s+after
kharkov\\?_\d+-\d+
r/gatesofhellostfront
r/hellletloose|hell\s*let\s*loose
r/natureofpredators
r/thefirerisesmod
r/u_iam_tender_angel
r/victoria3
send\.monobank\.ua
serge[iy]\s+kharkov
simona\s+kharkov
spring\s+prelude
st=kharkov|/l=kharkov
white\s+stork
world\s+of\s+tanks
```

### volodymyr-zelenskyy — 28 pattern(s)

```
(?:russian president|president of russia)\s+vladimir zelensk
,\s*vladimir putin,\s*vladimir zelensky,
/\s*vladimir zelensky
\b(?:ane vale hai|ab .{0,20} india ane)\b
\b(?:ang|ng|sina|nagkita|kasama)\b
\b(?:el enviado especial|calificó de|declaraciones de)\b
\b(?:incontrer|riferendosi|settimane|abbiamo)\b
\b(?:indo à luta|guarda costas)\b
\b(?:ki phir amad|kon sy|par pabandi)\b
\b(?:pani|bata nai thiye|dherai similarities xa)\b
\b(?:presiden ukraina|ukraina|rusia|dengan|yang|untuk)\b
\b(?:presidencial|candidato|pero no lo es también)\b
\b(?:presidentin|deklaroi|takimi|mund të)\b
\b(?:rais wa|amesema|kutoka kwa|akijibu)\b
\bgreat prophecy\b
\|\s*vladimir zelensky\s*\|
^vladimir zelensky$
^volodymyr zelenskyy$
donald trump-\s*vladimir zelensky zelensky
ex-zelensky aide threatens to jail him for life
i stand with ukraine vladimir zelensky
starring ai models:
the flamingo that doesn'?t fly
this is the first gay erotic piece i ever wrote
trump and j\.d\. v[ae]nce glorified president vladimir zelensky
ukraine'?s deaths reach 1 millions
zelensky claimed he ‘never heard of’ ukrainian nazi wwii crimes
•\s*vladimir zelensky \(ukraine\)
```

### ihor-sikorsky — 8 pattern(s)

```
\b(helikopter|penemu|sejarah|kisah|pencipta)\b
\bhelic[oó]ptero\b
igor\s+sikorsky\s+((kyiv|kiev)\s+polytechnic|kpi\b)
igor\s+sikorsky\s+(kyiv|kiev)\s+international\s+airport
igor\s+sikorsky\s+bridge
igor\s+sikorsky\s+historical\s+archives
igor\s+sikorsky\s+memorial
igor\s+sikorsky\s+street
```

### kyivan-rus — 10 pattern(s)

```
(?:analysis|report)\s+from\s+chatgpt
(?:countries|country) (?:i|we) show in this video
\b(?:kasaysayan|paano ito|mula sa|susuriin natin)\b
\|\s*kievan rus\s*\|\s*kievan rus fall\s*\|
^r/(?:chto_ne_skache|the_way_of_the_doommm|the_big_doommm|doommm_links\d*|corona_links\d*|punishment_panic)\b
countries i show in this video
if you like our content, check out our original comics
kievan rus\s+\d{3,4}\s*ad
oxiwyle|com\.oxiwyle\.kievanrus|apps\.apple\.com/[a-z]{2}/app/kievan-rus
sterling silver 925|ebay\.us/m/
```

### babyn-yar — 18 pattern(s)

```
13th symphony[^\n]{0,20}babi yar
babi yar (?:memorial )?park
babi yar colombiano
babi yar commemoration
babi yar concert
babi yar et les
babi yar park
babi yar symphony
babi yar.{0,2}\s*symphony
babi yar\s+symphony
babin yar
in memoriam[^\n]{0,15}babi yar
jurang pembantaian
massacre de babi yar
op\.?\s*113[^\n]{0,25}babi yar
poems?[^\n]{0,25}babi yar
sarabi yar
symphony no\.?\s*13[^\n]{0,25}babi yar
```

### varenyky — 21 pattern(s)

```
#(?:essen|sehr|lecker|deutschland)\b
#(?:tarif|pratik\w*tarif\w*)\b
#cherryculinar\w*
#ke[sş]fet
#retsept\b|kartoshkali
\bkartof\s+p[ie]lmeni\b
\blos\s+vareniki
\bpeynirli\b|\brus\s+mant[ıi]\b
\bravioli\s+russi\b|\bcucina\s+russa\b
\bteigtaschen\b
\bukrainische\b
\bvareniki\s+con\b
\bşirin\s+varenik
\bсталик\b
^(?:\s*#\w+)+\s*$
^\s*vareniki\s*@
^r/u_
https?://[^\s)]*vareniki
ukrayna\s+mant[ıi]s[ıi]
vareniki\s+s\s+[ck]artoshkoi
вареники\s+по\s+рецепту
```

### serhii-korolyov — 8 pattern(s)

```
(fsb|federal security service)[^.]{0,120}sergei korolev
,\s*sergei korolev[^,]{0,25},
pobeda search group,\s*sergei korolev
sergei korolev,\s*first deputy director
sergei korolev,\s*general manager
sergei korolev[^.]{0,60}(russian mafia|organi[sz]ed crime)
sergei korolev\s+space\s*x
space\s+sergei korolev\s+soviet rocket program
```

### oleksandr-usyk — 16 pattern(s)

```
[Ѐ-ӿ]{3,}
\b(analisis|pembalasan|pembuktian|tinju dunia)\b
\b(mengerikan|pertarungan|prediksi|menumbangkan)\b
\b(tinju|kehebatan|mengakui|tertandingi)\b
\bbeobachtet\b|\bbei dem wiegen\b
\bboks\b|\bmmauzbekistan\b
\bbondia\b|\bamepigana\b
\bbondia\b|\bpambano\b|\bndiye\b
\bkekalahan\b
\bkelas berat\b|\bsiapa yang\b
\bkucing\b|\bnamanya\b
\boleksander\b
\btype beat\b
^oleksandr[ _]usyk$
alexander usyk\s+asmr
ballad\s+of\s+alexander\s+usyk
```

### kazymyr-malevych — 13 pattern(s)

```
\(@artistmalevich\)
artista abstracto
customcanvascurators
hanno contribuito significativamente
how to pronounce\s*['"‘’“”]{0,2}\s*kazimir malevich
il dipinto|a causa della
kazimir malevich artist (?:prize|award)
kazimir malevich[ -]*(?:inspired|style)[^\n]{0,80}(?:metal )?wall (?:art|decor|sign)
kazimir malevich\s+maurizio cattelan
malevich pintor ruso
patricia taxxon\s*[-–]\s*kazimir malevich
t[aáạ]c ph[aẩ]m|hinh vuong den
vamos conhecer um pouco de
```

### ternopil — 6 pattern(s)

```
#\s?ternopol
[а-яё]{4,}
\bukraina\b
https?://[^\s)\]]*ternopol
onlyfans\.com
ternopol\s*leather
```

### feodosiia — 7 pattern(s)

```
\(es\)\s*\d{2}\.\d{2}\.\d{4}
\b(?:partlayiş\w*|hücum\w*|vuruldu|neft\s+bazası|şəhərind\w*)\b
\b(?:serangan|ukraina|krimea|jembatan|pelabuhan)\b
feodosiya\s+lapschina
feodosiya\s+mironova
feodosiya\s+to(?:&#39;|['’`])?qliyeva
happy\s+birthday\s+song\b|#makemyday
```

### volodymyr-the-great — 7 pattern(s)

```
\bsejarah\b
^r/kaiserreich
house of huisache|tuatha de acacia|chinacate
klyucharev
taariikhda
vladimir the dirty
vladimir the great,[^.]{0,80}volodymyr
```

### bakhmut — 5 pattern(s)

```
#artemovsk\b
\s-\s*topic:
artemovsk\s+overpass
green\s+revonk|arma\s*3
onlyfans\.com
```

### dnipro-river — 6 pattern(s)

```
(?:bender|tiraspol|transnistria\w*|chisinau|moldova\w*)[^.]{0,150}dnieper|dnieper[^.]{0,150}(?:bender|tiraspol|transnistria\w*)
(?:hit|struck|attack(?:ed|s)?|shell(?:ed|s)?)\s+the\s+dnieper\s+river\b
(?:transnistria\w*|tiraspol|bender|gagauz|moldova\w*)[^.]{0,150}dnieper|dnieper[^.]{0,150}(?:transnistria\w*|tiraspol|bender|gagauz)
\b(?:aur|kya|hain|karte|jante|kaun|zaroor|nahi|ka istemaal|ki tijarat)\b
belarusian\s+dnieper\s+river
captivating\s+destination|why\s+should\s+you\s+visit
```
