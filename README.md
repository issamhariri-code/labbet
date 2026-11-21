# Labbet · Trading Bot

> Från tillförlitliga marknadsdata till spårbara beslut.

**Linux · Docker · Python · PostgreSQL · SQLite**

Lärandeprojekt under aktiv utveckling · Uppdaterat 21 november 2026

[Status](#status-och-fokus) · [Milstolpar](#milstolpar) · [Teknik](#teknik-och-ansvar) · [Projektstruktur](#projektstruktur) · [Verifiering](#tester-och-verifiering) · [Vision](#vision-och-principer) · [Nästa steg](#plan-framåt)

---

Ett långsiktigt lärande- och utvecklingsprojekt inom **Linux, Docker, Python och PostgreSQL**. Målet är att bygga en tradingbot med tillförlitliga data, begripliga beslutsregler, spårbara resultat och oberoende riskkontroll. Arbetet börjar med datainhämtning och robust infrastruktur och går vidare stegvis mot analys, historisk utvärdering och simulerad handel.

Jag skriver normalt koden för hand för att förstå hur delarna fungerar. AI-stöd används för resonemang, arkitektur, avgränsad implementation, granskning och verifiering. Varje nytt steg ska gå att förklara, testa och följa upp.

## Status och fokus

**Projektet har gått från ett första API-till-databas-flöde till en gemensam API-grund med flera kvotfönster, registerstyrda adaptrar och beständig begäransproveniens.** Det senaste utvecklingssteget är historisk prisavläsning med en uttrycklig mottagningsbryttid.

Den senaste samlade verifieringen av utvecklingsversionen, den **21 november 2026**, omfattade **755 unika testmetoder: 0 fel, 0 överhoppade, 0 varningar och exitkod 0**. Lokal TLS, separat PostgreSQL, begränsad databasroll och återställningsprov ingick.

### Publicerat och senaste utvecklingsversion

Den publicerade koden omfattar API-grunden, källbundet tillgänglighetsunderlag och beständig proveniens. Den nyare historiska prisavläsningen och dess nya tester är implementerade och verifierade lokalt, men ingår ännu inte i denna publicerade kodversion. Det är den lokala utvecklingsversionen som omfattas av 755-testersresultatet; den föregående samlade verifieringen för provenienssteget omfattade 726 metoder.

### Drift och granskning

Alpha Vantage-flödet är sedan tidigare godkänt för **kontrollerad manuell hämtning för vald ticker till PostgreSQL**. Verklig lagring av 100 handelsdagar samt beständig API-state, backup och isolerad återläsning har verifierats.

De senare tillgänglighets-, proveniens- och historikändringarna är verifierade i separata testmiljöer. De innebär inget nytt produktionsinförande. Aktivering av nya scheman och funktioner kräver granskning av migrationer, rättigheter, återställning och byggd image.

**Bara Alpha är verkligt registrerad.** En andra adapter används syntetiskt i tester för att pröva att infrastrukturen fungerar för olika klasser, adresser, nycklar och svarformat. Twelve Data är fortsatt kandidat för nästa verkliga källa; dess adapter, publiceringsregler och verkliga åtkomst är ännu inte verifierade. API-arbetet och grinden inför paper trading är fortsatt öppna.

## Milstolpar

Den första grunden var ett sammanhängande dataflöde:

```text
Alpha Vantage → Python → PostgreSQL → separat SQL-läsning
```

Sedan dess har fokus flyttats till kontrollerade anrop, tidsriktiga data och underlag som går att återskapa.

| Period | Milstolpe |
| --- | --- |
| 4 oktober 2026 | Första flödet verifierat med 100 sparade dagsrader för en vald symbol och separat SQL-kontroll. Två isolerade tester provade omvandling och stopp vid saknad data. |
| 8–14 oktober 2026 | Datavalideringen utvecklades stegvis. Steget avslutades efter granskning med 32 godkända testmetoder. |
| 15 oktober 2026 | API-felhantering, profiler, transportskydd, återförsök och lagring verifierades med 197 testmetoder, inklusive lokal HTTPS och separat PostgreSQL. |
| 18 oktober 2026 | Avgränsat driftsgodkännande för manuell hämtning. Verklig lagring av 100 handelsdagar, beständig kvot- och körningshistorik samt backup med isolerad återläsning verifierades. |
| 22–23 oktober 2026 | Explicit källval och kreditbaserad kvotbokföring infördes. Väntetid till tillräcklig kredit, återöppning och oförändrad state vid avslag verifierades; den då aktuella sviten nådde 204 metoder. |
| 28–30 oktober 2026 | Flera samtidiga kvotfönster, avslag för permanent för hög anropskostnad och kompatibel profilmetadata infördes. JSON-profiler, loader och inventory kopplades ihop med verklig kvotbokföring. Samlad verifiering: 277 metoder. |
| 30–31 oktober 2026 | Adaptervalet flyttades till ett gemensamt register för klass, adresser och kontraktskrav. Profil, loader, resolver och klient kopplades ihop med tidiga avslag och bevarade gamla hashvärden. Samlad verifiering: 397 metoder. |
| 1 november 2026 | Frysta typer för sessioners tillgänglighet och källbundet underlag verifierades. UTC, sommartid, handelsdatum, oföränderlighet och stabila fingeravtryck provades separat. |
| 2 november 2026 — tillgänglighet | Käll-, regel- och kontraktsbindning, fryst referenstid och mål-sessioner kopplades in i loader, klient och fallback. Samlad regression: 523 metoder. |
| 2 november 2026 — proveniens | Kompletta kontrakt och begäransunderlag började arkiveras tillsammans med prisrevisioner i samma transaktion. Granskande återläsning, begränsade rättigheter, migrationsordning och syntetisk dump/restore verifierades. Samlad körning: 726 metoder. |
| 21 november 2026 — senaste lokala steg | Historisk prisavläsning vid en explicit mottagningsbryttid verifierades. Alla 726 tidigare test-ID:n ingick och 29 nya tillkom: totalt 755. Återställningsprovet bevarade granskningen av 69 arkiverade begäranden; sex separata historiska urvalsfall gav identiska resultat efter återställning. |

## Vad grunden kan hantera

### Källor, nycklar och anropsbudget

- Registerstyrt adapterval med godkända produktionsadresser och kontraktspar.
- Frysta källprofiler och fasta nyckelversioner per jobb, med rotation och uttrycklig återkallelse.
- Kontroll av även ej valda källor innan tillstånd och autentiseringsuppgifter konstrueras.
- Atomisk kreditbokföring över flera kvotfönster och begränsad samtidighet i beständig SQLite.
- Begränsade återförsök och en gemensam tidsbudget. Godkänd fallback använder reservkällans egen adapter, adress och nyckel.
- HTTPS med kontrollerad origin, DNS-adresser och certifikat samt stopp för omdirigeringar och miljöproxy.

### Tidsbetydelse och arkiv

Börsstängning, tidigaste tillåtna användning och faktisk mottagning hålls isär. En operatörsgranskad spärrtid beskriver en policygräns; den bevisar inte att leverantören publicerade färdiga data vid just den tidpunkten.

När tillgänglighetsfunktionen aktiveras binds underlaget till rätt källa, regelversion och hela datakontraktet. Alla efterfrågade sessioner ska täckas och varje returnerad rad ska klara sin egen tidsspärr. Fallback använder samma frysta begäran och reservkällans eget underlag.

Kontrakt, tillgänglighetsunderlag, referenstid, mål-sessioner och profilhash sparas som versionsbundna dokument. Prisrevisioner och begäransproveniens lagras atomiskt. Granskande återläsning jämför dokumenten med de faktiskt lagrade raderna och deras hashvärden. Äldre format behåller sin verifierade kompatibilitet.

### Senaste lokala steg: vad hade vi tagit emot vid en viss tid?

Den historiska läsaren använder en explicit bryttid och ett bestämt urval handelsdagar. Hämtningar mottagna efter bryttiden utesluts, även när dagens pristabell redan pekar på en senare rättelse. Konflikter, saknade dagar och skadat eller otillräckligt arkivunderlag ger kontrollerade fel.

Resultatet hänvisar till de prisrevisioner och begäranden som användes. Urvalet är reproducerbart för samma arkivinnehåll och bryttid. För att senare återskapa exakt vad ett verkligt beslut använde behöver även beslutets faktiska revisionsreferenser eller motsvarande arkivversion sparas. Det är ett kommande steg inför beslutsfasen.

## Teknik och ansvar

| Del | Roll i projektet |
| --- | --- |
| Linux | Servermiljö, filbehörigheter och driftunderlag. |
| Docker Compose | Separata Python- och PostgreSQL-miljöer, nätverk och beständig lagring. |
| Python | Källbindning, hämtning, datakontrakt, validering och proveniens. |
| PostgreSQL | Prisrevisioner, transaktioner, arkiverat begäransunderlag och granskande återläsning. |
| SQLite | Beständig samordning av kvoter, körningar, spärrar och nyckelversionsmetadata. |

Kodens ansvar delas bland annat mellan [adaptrar](ingestion/api_adapters.py), [profiler](ingestion/api_profiles.py), [API-klient](ingestion/api_client.py), [transport](ingestion/api_transport.py), [datakontrakt](ingestion/api_contracts.py), [tillgänglighet](ingestion/api_availability.py), [proveniens](ingestion/api_provenance.py) och [lagring](ingestion/api_storage.py).

## Projektstruktur

Översikt över den publicerade delen; `*` grupperar närbesläktade filer.

<details>
<summary>Visa projektets mappar och filer</summary>

```text
.
├── README.md
├── .gitignore
├── compose.yaml
├── compose.api.yaml
├── SELECT.sql
├── config/
│   └── api_sources.example.json
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── market_data.py
│   └── api_*.py
├── sql/
│   ├── 001_create_prices.sql
│   ├── 002_api_provenance.sql
│   ├── 003_ingestion_role.sql
│   └── 004_request_provenance.sql
└── tests/
    └── test_*.py
```

</details>

Konfigurationsmallen innehåller platshållare och är inte en driftklar profil. Verklig källkonfiguration, miljöfiler, nycklar, lokal state, databaser och säkerhetskopior hanteras privat. SQL-migrationerna körs som separata, granskade steg; publicering av källkod aktiverar dem inte.

## Tester och verifiering

Testantalet avser respektive körnings omfattning. De 755 metoderna är ett samlat resultat, där de 726 tidigare test-ID:na ingår. Antalen från milstolparna ska därför inte summeras.

| Verifiering | Vad som prövades |
| --- | --- |
| Källbindning och credentials | Fel källval/adress/kontrakt, tidiga avslag, syntetisk andra adapter, fallbackbudget, samtidighet, rotation och återkallelse. |
| Kvoter och state | Samtliga kvotfönster, kreditkostnader, väntetider, scope-isolering, återöppning och full rollback vid avslag. |
| Tillgänglighet och tidsurval | UTC, sommartid, handelsdatum, oförändrad referenstid, sessionstäckning och spärrtid för varje returnerad rad. |
| Lagring och proveniens | Atomisk skrivning, idempotens, konflikter, korruptionsdetektering, äldre format och bevarad yttre transaktion. |
| Transport och rättigheter | Verklig lokal TLS, begränsad inläsningsroll och avslag för otillåtna databasoperationer. |
| Senaste lokala historikprov | Gränstider, sena rättelser, saknade dagar, samtidiga skrivningar och samma urval efter återställning. |

De senaste samlade körningarna använde nätverksisolerad Docker, temporära databaser och syntetiska TLS-fixturer. **Python 3.13.16, Psycopg 3.3.6, tzdata 2026.5 och PostgreSQL 18.6** användes i den dokumenterade 755-körningen. Produktionsdata och riktiga API-nycklar användes inte i dessa isolerade prov.

Verifieringsmiljön kräver förberedda testfixturer och särskilt testdatabasunderlag. Delar av körunderlaget och de detaljerade rapporterna hanteras lokalt. En körning utan detta underlag kan inte jämställas med de dokumenterade fulla integrationstesterna.

Tekniskt godkända tester visar programbeteende och dataintegritet inom testad omfattning. De bevisar inte fullständig marknadsdatatäckning, verklig leverantörsåtkomst, historisk publicering eller en lönsam handelsstrategi. Bredare produktionsaktivering och längre obevakad drift kräver egna verifierade steg.

## Vision och principer

### Flera perspektiv på marknaden

Min ambition är att bygga en bot som kan väga samman flera typer av information och göra sina beslut möjliga att granska. Pris och volym är grunden. På sikt vill jag kunna sätta dem i sammanhang med instrumenthistorik, bolagshändelser, rapporter, finansieringsrisk, marknadsläge och relevanta nyheter.

Flera algoritmer och urvalsregler ska kunna prövas var för sig och tillsammans, med tydliga indata och dokumenterade resultat. Nya datakällor läggs till när deras innehåll, tidsbetydelse och rättigheter fyller ett konkret behov.

### Rätt data vid rätt tidpunkt

Historiken behöver beskriva rätt instrument även efter symbolbyten, bolagshändelser och avnoteringar. Senare rättelser ska ha spårbar historik. Data som kom efter en bryttid får inte smyga in i ett äldre beslutsunderlag.

En fortsatt idé är att låta varje beslut bära hänvisningar till exakt de arkiverade prisrevisioner och övriga underlag som det använde. Då kan en utvärdering förklara både varför ett beslut fattades och vilka fakta som faktiskt var tillgängliga.

### Idéer som går att pröva

Jag vill kunna jämföra idéer med enkla basmodeller och se om de håller på andra tidsperioder än dem de utvecklades på. Avgifter, likviditet och rimliga genomförandekostnader ska ingå i bedömningen. Även försök som inte förbättrar resultatet ska dokumenteras och hjälpa till att styra nästa steg.

### Tydliga ansvar och kontrollerad utveckling

Datainhämtning, analys, risk och orderhantering ska ha tydliga gränser. En framtida gemensam beslutskärna ska kunna användas i historiska tester, paper trading och eventuell senare drift. Riskkontrollen ska kunna stoppa ett förslag oberoende av algoritm eller språkmodell.

Analys, backtesting, mäklarintegration och handel är ännu inte implementerade. Språkmodeller kan senare hjälpa till att strukturera text och belägg; beslut om nya versioner och riskgränser ligger kvar under mänsklig kontroll.

## Plan framåt

1. **Granska senaste historiksteget.** Gå igenom den lokalt verifierade läsaren, dess gränser och återställningsbevis före nästa beroende steg.
2. **Precisera nästa verkliga datakälla.** Granska publiceringstid, prisgrund, prisjusteringar, rättelseordning, åtkomst och datarättigheter. Därefter följer isolerade kontraktsprov, adapterarbete och ett separat godkänt verkligt hämtprov.
3. **Förbereda kontrollerat införande.** Verifiera migrationer, återställningsväg, begränsade rättigheter och byggd image innan ny arkivfunktion aktiveras i drift.
4. **Utöka belastnings- och återhämtningsproven.** Blanda fler samtidiga jobb, sena svar, leverantörsfel, avbrott och återstart. Mät även historikläsningens kostnad på en större datamängd före eventuell indexoptimering.
5. **Bredda datagrunden och bevara beslutsunderlag.** Börja med instrumenthistorik och bolagshändelser. Pröva sedan avgränsade tillskott inom bolagsdata, makro, handelsbarhet och rapporthändelser. Förbered exakta revisionsreferenser inför framtida beslutsåterspelning.
6. **Bygga och utvärdera analys.** Införa begripliga urvalsregler och algoritmer i små steg, följt av reproducerbara historiska tester och paper trading. Mäklaranslutning och verklig handel kräver egna granskade steg.

Ordningen styrs av beroenden, datakvalitet och verifierade resultat. Nya idéer får utvecklas utan att planerat arbete presenteras som färdig funktion.
