# labbet

Ett pågående lärandeprojekt inom **Linux, Docker, Python och PostgreSQL**. Projektet utvecklas stegvis med fokus på tillförlitlig datainhämtning, beständig lagring och verifierbara resultat.

## Projektets utveckling

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

## Aktuellt läge

**API-implementationen är driftaktiverad, granskad och godkänd för kontrollerad manuell hämtning för vald ticker från Alpha Vantage till PostgreSQL.**

Det finns belägg för:

- Verklig hämtning och korrekt lagring av 100 handelsdagar.
- Beständig kvot- och körningshistorik samt bekräftad lagring.
- Godkända isolerade felhanterings- och integrationstester.
- Verifierad backup och isolerad återläsning.

**Bredare driftsgodkännande återstår:** den andra leverantören, utökade belastningsprov och senare schemalagd, obevakad körning under längre tid.

Nästa steg är att lägga till en andra API-leverantör med egen nyckel och verifiera integrationen. Därefter följer isolerade belastningsprov med flera samtidiga hämtningar, simulerade svar och fel för att kontrollera botens felhantering, kvotgränser och dataisolering.

## Vad delarna bidrar med

| Del | Roll i projektet |
| --- | --- |
| Linux | Servermiljö och lärande kring tjänster, filer och behörigheter. |
| Docker Compose | Separata containrar för Python och PostgreSQL, med nätverk och beständig datalagring. |
| Python | Hämtning, validering och förberedelse av data för lagring. |
| PostgreSQL | Beständig lagring och oberoende kontroll genom SQL. |

## Projektets filer

Översikt; `*` grupperar närbesläktade filer.

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
    ├── test_api_*.py
    └── fixtures/
        ├── README.md
        ├── *.pem
        └── *.key
```

`ingestion/` innehåller hämtning, validering och lagring. `config/` innehåller en konfigurationsmall och `sql/` databasdefinitionerna. `compose.yaml` beskriver grundmiljön; `compose.api.yaml` är ett separat tillägg för API-vägen.

`tests/` innehåller sparade tester, och `fixtures/` syntetiska certifikat och testnycklar för lokal HTTPS. `scripts/` innehåller körskriptet för den isolerade verifieringen. Python körs i Docker.

## Verifiering

Den isolerade verifieringen den **15 oktober 2026** godkände **197 testmetoder** i Docker med lokal HTTPS och separat PostgreSQL. Inga verkliga marknads-API-anrop eller produktionsmigreringar ingick i den körningen.

Den **18 oktober 2026** verifierades den första verkliga hämtningen för vald ticker med 100 handelsdagar i PostgreSQL, beständig kvot- och körningshistorik samt lagringsbekräftelse. API-state-backupen verifierades också genom integritets- och innehållskontroller samt isolerad återställning och återöppning i en ny process. Efterverifieringen och återläsningsprovet gjordes utan nya API-anrop; originaldata och backup bevarades.

Verifierad hämtning och lagring innebär inte att historiken är fullständig eller att alla marknadsvärden har jämförts med en oberoende källa.

Arbetssättet är att göra avgränsade ändringar, verifiera berörda flöden och dokumentera resultat före nästa steg. [Testprotokollet](docs/API-TESTRESULTAT-2026-10-15.md), [API-översikten](docs/API-SKYDD.md) och [driftrutinen](docs/API-DRIFT.md) innehåller fördjupningen.

## Fortsatt riktning

Datakvalitet och kontrollerat driftinförande kommer först. Analys, backtesting, mäklarintegration och handel är senare steg och är ännu inte implementerade.
