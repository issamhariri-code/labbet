# Labbet · Trading Bot

> Från tillförlitliga marknadsdata till spårbara beslut.

**Linux · Docker · Python · PostgreSQL · SQLite**

Lärandeprojekt under aktiv utveckling · Uppdaterat 23 oktober 2026

[Status](#status-och-fokus) · [Milstolpar](#milstolpar) · [Teknik](#teknik-och-ansvar) · [Projektstruktur](#projektstruktur) · [Verifiering](#tester-och-verifiering) · [Vision](#vision-och-principer) · [Nästa steg](#plan-framåt)

---

Ett långsiktigt lärande- och utvecklingsprojekt inom **Linux, Docker, Python och PostgreSQL**. Målet är att bygga en tradingbot med tillförlitliga data, begripliga beslutsregler, spårbara resultat och oberoende riskkontroll. Arbetet börjar med datainhämtning och robust infrastruktur och går vidare stegvis mot analys, historisk utvärdering och simulerad handel.

Jag skriver normalt koden för hand för att förstå hur delarna fungerar. AI-stöd används för resonemang, arkitektur, granskning och verifiering. Varje nytt steg ska gå att förklara, testa och följa upp.

## Milstolpar

Den första grunden var ett sammanhängande dataflöde:

```text
Alpha Vantage → Python → PostgreSQL → separat SQL-läsning
```

| Period | Milstolpe |
| --- | --- |
| 4 oktober 2026 | Första flödet verifierat med 100 sparade dagsrader för en vald symbol och separat SQL-kontroll. Två isolerade tester provade omvandling och stopp vid saknad data. |
| 8–14 oktober 2026 | Datavalideringen utvecklades stegvis och testerna sparades i projektet. Valideringssteget avslutades efter granskning med 32 godkända testmetoder. |
| 15 oktober 2026 | API-felhantering och ytterligare skydd implementerades och granskades. Den utökade sviten nådde 197 godkända testmetoder i isolerad miljö. Driftaktivering återstod vid denna tidpunkt. |
| 18 oktober 2026 | API-implementationen driftaktiverad, granskad och godkänd för kontrollerad manuell hämtning för vald ticker. Verklig lagring av 100 handelsdagar, beständig API-state samt backup och isolerad återläsning verifierade. |
| 22 oktober 2026 | Explicit val av datakälla genom startflödet verifierat i isolerade tester. En gemensam kvotmodell och ett kompatibelt profilgränssnitt verifierade som förberedelse för en andra leverantör. |
| 23 oktober 2026 | Kreditbaserad kvotberäkning och väntetid tills tillräcklig kredit frigörs verifierade. Den aktuella avgränsade regressionssviten passerade med 204 testmetoder, inklusive gränsfall, återöppning och oförändrad state vid avslag. |

## Status och fokus

### Nuvarande fas: datakällor och API-samordning

**Projektet befinner sig i arbetet med datakällor och API-samordning.** Alpha Vantage-flödet är sedan tidigare driftaktiverat, granskat och godkänt för kontrollerad manuell hämtning för vald ticker till PostgreSQL. Nästa källa förbereds stegvis; Twelve Data är vald kandidat, men dess adapter och verkliga åtkomst är ännu inte verifierade.

### Verifierat hittills

- Verklig hämtning och korrekt lagring av 100 handelsdagar.
- Beständig kvot- och körningshistorik samt bekräftad lagring.
- Godkända isolerade felhanterings- och integrationstester.
- Verifierad backup och isolerad återläsning.
- Explicit källval och förberedande profil-/kvotgränssnitt i isolerade tester.
- Kreditbokföring och väntetider för ett kvotfönster, med bevarad historik vid återöppning och avslag utan ny debitering.

### Pågående arbete

Den senaste kvotändringen är testad och granskad. Nästa avgränsade steg är att särskilja anrop vars kostnad överstiger hela kvoten från tillfälligt förbrukad kvot. Flera samtidiga kvotfönster är fortfarande spärrade. Stöd för leverantörens samtliga kvotgränser behöver färdigställas och verifieras före integration och verkliga anrop till den andra källan. Hela API-arbetet är fortsatt öppet.

**Bredare driftsgodkännande återstår:** den andra leverantören, utökade belastningsprov och senare schemalagd, obevakad körning under längre tid.

## Teknik och ansvar

| Del | Roll i projektet |
| --- | --- |
| Linux | Servermiljö och lärande kring tjänster, filer och behörigheter. |
| Docker Compose | Separata containrar för Python och PostgreSQL, med nätverk och beständig datalagring. |
| Python | Hämtning, validering och förberedelse av data för lagring. |
| PostgreSQL | Beständig lagring och oberoende kontroll genom SQL. |
| SQLite | Beständig samordning av API-kvoter, körningar och återhämtning mellan anrop. |

## Projektstruktur

Översikt; `*` grupperar närbesläktade filer.

<details>
<summary>Visa projektets mappar och filer</summary>

```text
.
├── README.md
├── compose.yaml
├── compose.api.yaml
├── .env.example
├── .gitignore
├── SELECT.sql
├── config/
│   └── api_sources.example.json
├── docs/
│   ├── API-SKYDD.md
│   ├── API-DRIFT.md
│   └── API-TESTRESULTAT-2026-10-15.md
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── market_data.py
│   └── api_*.py
├── scripts/
│   └── test_api_isolated.py
├── sql/
│   ├── 001_create_prices.sql
│   └── 002_api_provenance.sql
└── tests/
    ├── test_main.py
    ├── test_market_data.py
    ├── test_*.py
    └── fixtures/
        ├── README.md
        ├── *.pem
        └── *.key
```

</details>

`ingestion/` innehåller hämtning, validering och lagring. `config/` innehåller en konfigurationsmall och `sql/` databasdefinitionerna. `compose.yaml` beskriver grundmiljön; `compose.api.yaml` är ett separat tillägg för API-vägen.

`tests/` innehåller sparade tester, och `fixtures/` syntetiska certifikat och testnycklar för lokal HTTPS. `scripts/` innehåller körskriptet för den isolerade verifieringen. Python körs i Docker.

## Tester och verifiering

### Genomförda kontroller

Den isolerade verifieringen den **15 oktober 2026** godkände **197 testmetoder** i Docker med lokal HTTPS och separat PostgreSQL. Inga verkliga marknads-API-anrop eller produktionsmigreringar ingick i den körningen.

Den **18 oktober 2026** verifierades den första verkliga hämtningen för vald ticker med 100 handelsdagar i PostgreSQL, beständig kvot- och körningshistorik samt lagringsbekräftelse. API-state-backupen verifierades också genom integritets- och innehållskontroller samt isolerad återställning och återöppning i en ny process. Efterverifieringen och återläsningsprovet gjordes utan nya API-anrop; originaldata och backup bevarades.

Den **23 oktober 2026** passerade den aktuella avgränsade regressionssviten med **204 testmetoder**, inga överhoppade och exitkod 0, i nätverksisolerad Docker. Den omfattar bland annat källval, profiler, kreditkostnader, väntetider, tidsgränser, scope-isolering och återöppning av API-state. Kvotavslag kontrolleras mot hela det logiska SQLite-innehållet så att de inte ger nya debiteringar eller jobb. Den körningen använde syntetiska data och egen testlagring, utan externa API-anrop eller produktionsdata.

### Vad resultaten visar

Testantalet avser respektive körnings omfattning. Den senaste regressionen upprepar inte det tidigare provet med lokal HTTPS och separat PostgreSQL och innebär inget nytt godkännande av obevakad drift.

Verifierad hämtning och lagring innebär inte att historiken är fullständig eller att alla marknadsvärden har jämförts med en oberoende källa.

Arbetssättet är att göra avgränsade ändringar, verifiera berörda flöden och dokumentera resultat före nästa steg. [Testprotokollet](docs/API-TESTRESULTAT-2026-10-15.md), [API-översikten](docs/API-SKYDD.md) och [driftrutinen](docs/API-DRIFT.md) innehåller fördjupningen.

## Vision och principer

### Flera perspektiv på marknaden

Min ambition är att bygga en bot som kan väga samman flera typer av information och göra sina beslut möjliga att granska. Pris och volym är grunden. På sikt vill jag kunna sätta dem i sammanhang med bolagsinformation, rapporthändelser, marknadsläge och relevanta nyheter. Flera algoritmer och urvalsregler ska kunna prövas var för sig och tillsammans, med tydliga indata och dokumenterade resultat.

### Rätt data vid rätt tidpunkt

Datakvaliteten styr hur långt analysen får gå. Historiken behöver beskriva rätt instrument även efter symbolbyten, bolagshändelser och avnoteringar. Varje observation behöver också knytas till när informationen faktiskt var tillgänglig. Senare rättelser ska kunna följas utan att tidigare beslutsunderlag skrivs om.

### Idéer som går att pröva

Jag vill kunna jämföra idéer med enkla basmodeller och se om de håller på andra tidsperioder än dem de utvecklades på. Avgifter, likviditet och rimliga genomförandekostnader ska ingå i bedömningen. Även försök som inte förbättrar resultatet ska dokumenteras och hjälpa till att styra nästa steg.

### Tydliga ansvar och kontrollerad utveckling

Den planerade arkitekturen håller datainhämtning, analys, risk och orderhantering åtskilda. En gemensam beslutskärna ska kunna användas i historiska tester, simulerad handel och eventuell framtida drift. Riskkontrollen ska kunna stoppa ett förslag oberoende av vilken algoritm som skapade det. Språkmodeller kan senare få hjälpa till att strukturera text och belägg; beslut om nya versioner och riskgränser ligger kvar under mänsklig kontroll.

Detta är riktningen för kommande utveckling. Analys, backtesting, mäklarintegration och handel är ännu inte implementerade. Systemets tekniska tester visar hur programmet beter sig; eventuell strateginytta behöver utvärderas separat.

## Plan framåt

1. **Färdigställa API-grunden.** Slutföra kvothanteringen inför flera samtidiga tidsfönster, inklusive tydlig hantering av anrop vars kostnad överstiger hela kvoten.
2. **Verifiera en andra datakälla.** Införa separat källprofil och adapter, kontrollera format, marknadsbetydelse och samspel med befintlig källa och därefter göra ett begränsat verkligt hämtprov.
3. **Pröva belastning och återhämtning.** Köra isolerade prov med samtidiga jobb, simulerade leverantörsfel, avbrott och återstart. Kvoter, dataisolering och fullständiga skrivningar ska hålla även när flera saker händer samtidigt.
4. **Bredda datagrunden.** Börja med gemensamma datakontrakt, instrumenthistorik och bolagshändelser. Därefter prövas avgränsade tillskott av bolagsdata, makro och handelsbarhet. Nyhetsarkiv och annan fördjupning kräver visad datakvalitet och rätt att lagra materialet.
5. **Bygga och utvärdera analys.** Införa begripliga urvalsregler och algoritmer i små steg, följt av reproducerbara historiska tester och senare simulerad handel. Eventuell mäklaranslutning och verklig handel kräver egna verifierade steg.

Ordningen styrs av beroenden och granskade resultat. Nya datakällor eller mer komplexa modeller läggs till när de fyller ett tydligt behov och kan utvärderas.
