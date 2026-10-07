# labbet

Ett pågående lärandeprojekt inom **Linux, Docker, Python och PostgreSQL**. Bygger förståelse för hur en Linux-miljö, containers och Python-kod kan samverka i ett fungerande flöde för marknadsdata.

Projektet utvecklas stegvis, med fokus på att förstå varje del och verifiera resultatet innan nästa del byggs. Den nuvarande grunden är datainhämtning och beständig lagring. Analys och eventuella framtida handelsfunktioner ligger långt längre fram.

## Var projektet står

Det första dataflödet fungerar:

```text
Alpha Vantage → Python → PostgreSQL → separat SQL-läsning
```

Python hämtar dagliga öppnings-, högsta-, lägsta- och stängningspriser samt volym för en vald aktiesymbol. Data kommer från Alpha Vantage via `TIME_SERIES_DAILY` med `outputsize=compact`, och priserna är ojusterade.

Innan lagring omvandlas datum till `date`, priser till `Decimal` och volym till `int`. Därefter skriver Python raderna till PostgreSQL-tabellen `daily_prices` i en transaktion. Sparade data kan läsas separat med SQL.

Tabellens primärnyckel kombinerar symbol och handelsdatum. Vid en konflikt lämnas den befintliga raden oförändrad genom `ON CONFLICT ... DO NOTHING`. Databasen har också regler för bland annat prisintervall och icke-negativ volym.

## Vad de olika delarna bidrar med

| Del | Roll i projektet |
| --- | --- |
| Linux | Miljön där projektet körs och där arbete med tjänster, filer och behörigheter ingår i lärandet. |
| Docker Compose | Samlar PostgreSQL och Python-jobbet i en gemensam konfiguration med nätverk, miljövariabler och hälsokontroll. |
| Python | Hämtar API-data, omvandlar värden och skriver rader till databasen. |
| PostgreSQL | Lagrar data beständigt och gör det möjligt att kontrollera resultatet med SQL. |

Python körs som ett engångsjobb i en egen container. PostgreSQL är en separat tjänst med en namngiven datavolym. Databasen heter `labbet`; Compose-projektet heter för närvarande `trading-bot`.

## Projektets filer

```text
.
├── README.md
├── compose.yaml
├── .env.example
├── .gitignore
├── SELECT.sql
├── ingestion/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   └── market_data.py
└── sql/
    └── 001_create_prices.sql
```

Ansvaret är uppdelat mellan tre delar: **`market_data.py` hämtar**, **`main.py` omvandlar och sparar**, och **SQL-skriptet skapar tabellen**. Tabellskapandet körs separat och är inte en del av Python-jobbet.

`compose.yaml` definierar tjänsterna och deras nätverk, hälsokontroll och lagring. `Dockerfile` bygger Python 3.13-miljön med Psycopg som databasberoende från `requirements.txt`. `.env.example` innehåller tomma konfigurationsvärden, och `.gitignore` undantar bland annat hemligheter och lokala arbetsfiler. `SELECT.sql` är ett läsexempel där symbolen fylls i för att matcha den som används i Python.

## Vad som har verifierats

Tidigare verifiering den **4 oktober 2026** omfattar:

- **100 sparade dagsrader för en testad symbol**, bekräftade genom en separat SQL-läsning efter datainhämtningen. Det visar att dataflödet och lagringen fungerade vid den körningen.
- **Två godkända isolerade tester av `main.py` med testdubblar.** En komplett påhittad dagsrad omvandlades och skickades till en simulerad insättning. En rad utan stängningspris gav `ValueError` innan någon databasanslutning eller insättning anropades.

Testerna använde inga riktiga API- eller databasanslutningar. Testskriptet kördes tillfälligt och finns inte i repot; någon automatiserad testsvit ingår ännu inte. Kontrollen för saknat stängningspris har testats med testdubblar, men har ännu inte verifierats i en ombyggd Docker-image.

Verifierad lagring innebär inte att alla marknadsvärden har kontrollerats mot en oberoende källa eller att historiken är fullständig.

## Nuvarande begränsningar och fortsatt riktning

Datainhämtningen körs manuellt för en symbol åt gången. Vald ticker fylls i direkt i `main.py`; automatiskt aktieurval saknas. Tom symbol stoppar programmet före API-anropet.

Befintliga rader uppdateras inte när datakällan ändrar sina värden. Valideringen är fortfarande begränsad, och metadata för källa, börs, valuta och justeringsstatus lagras ännu inte. API-åtkomst och anropsgränser beror på den egna nyckeln och datakällans villkor.

Fortsatt utveckling är tänkt att bygga vidare på datakvalitet och verifierbara beräkningar innan analys och backtesting blir aktuella. **Ingen handelsstrategi, backtesting, mäklarintegration eller handel är implementerad.**
