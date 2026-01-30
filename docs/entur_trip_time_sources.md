# Kako dobiti vreme polaska za Entur trip-updates

## Problem

U **GTFS-RT trip-updates** feed-u Entura (npr. `?datasource=RUT`) često su:
- `timestamp` = 0 (feed ne šalje kada je ažuriranje kreirano)
- `start_date` / `start_time` = null (TripDescriptor nije popunjen)

Bez toga ne znamo **za koji polazak** je kašnjenje (trip_id nije jedinstven po danu).

## Zašto je tako

Pri konverziji **SIRI → GTFS-RT** mnogi sistemi (uključujući Entur) ne popunjavaju `start_date` i `start_time` u TripDescriptor-u. To je poznat problem: za ispravno uparivanje vožnje ova polja treba da budu izvučena iz SIRI podataka i mapirana u GTFS-RT.

Izvor: [Current siri and gtfs matching problems](https://github.com/hasadna/open-bus-stride-etl/issues/16), [GTFS-RT TripDescriptor](https://support.google.com/transitpartners/answer/6159841).

---

## Opcija 1: SIRI ET (Estimated Timetable) umesto GTFS-RT

**SIRI ET** sadrži planirana, stvarna i estimirana vremena polaska po stanicama.

| Stavka | Detalj |
|--------|--------|
| Endpoint | `GET https://api.entur.io/realtime/v1/rest/et` |
| Parametri | `datasetId=RUT` (operator), `maxSize=1500` |
| Format | SIRI 2.0 XML |
| Rate limit | 4 zahteva u minuti (za češće – publish/subscribe) |

U XML-u:
- **EstimatedVehicleJourney** – jedna vožnja
- **EstimatedCall** / **RecordedCall** – stanica sa poljima tipa `AimedDepartureTime`, `ExpectedDepartureTime`, `ActualDepartureTime` itd.
- **OriginAimedDepartureTime** (ili ekvivalent u norveškom profilu) – vreme polaska vožnje

**Korak:** Dodati collector ili paralelni pipeline koji povlači SIRI ET (npr. za RUT), parsira XML i upisuje u naš format (trip_id / journey id + vreme polaska + kašnjenja). Dokumentacija: [Entur API Reference – rest/et](https://developer.entur.org/pages-real-time-api/), [Norwegian SIRI profile](https://enturas.atlassian.net/wiki/spaces/PUBLIC/pages/637370420/Norwegian+SIRI+profile).

---

## Opcija 2: Statički GTFS/NeTEx + uparivanje po trip_id

Entur objavljuje **statičke redove vožnje** (NeTEx ili GTFS) sa planiranim vremenima.

| Stavka | Detalj |
|--------|--------|
| Stranica | [Stops- and Timetable data](https://developer.entur.org/stops-and-timetable-data) |
| Formati | NeTEx (pun), GTFS (basic/extended) |
| Ažuriranje | Svakodnevno |

**Ideja:** Preuzeti GTFS (npr. `trips.txt`, `stop_times.txt`, `calendar`/`calendar_dates`) ili NeTEx. Za svaki `trip_id` iz GTFS-RT (npr. `RUT:ServiceJourney:...`) u statičkom skupu naći odgovarajuću vožnju i njeno **planirano vreme polaska** (prvi stop u `stop_times`).  
Napomena: U GTFS-u trip_id može biti drugačiji od NeTEx ServiceJourney ID-a koji Entur koristi u realtime-u – treba proveriti mapiranje ID-ova (npr. u dokumentaciji ili preko Journey Planner API).

---

## Opcija 3: Journey Planner API (GraphQL)

[Journey Planner v3](https://developer.entur.org/pages-journeyplanner-journeyplanner/) nudi upite za „planned and actual departure times”. Pogodan je za pojedinačne upite (npr. jedan polazak), manje za masovno skupljanje celog trip-updates feed-a.

---

## Preporuka

1. **Kratkoročno:** Za analizu kašnjenja koristiti samo zapise sa **validnim `timestamp`** ili **`departure_timestamp`** (kad ga budemo imali); zapise sa `timestamp=0` i bez start_date/start_time isključiti iz agregacije po minutama (što već radimo).
2. **Srednjoročno:** Uvesti **SIRI ET** za operatore koji nas zanimaju (npr. RUT): jedan GET po operatoru, parsiranje XML-a, izvlačenje vremena polaska i kašnjenja, snimanje u isti ili kompatibilan format kao GTFS-RT normalizacija.
3. **Opciono:** Eksperimentisati sa **statičkim GTFS/NeTEx** i uparivanjem po journey/trip ID-u ako nam treba samo planirano vreme bez poziva na realtime ET.

---

## Reference

- [Entur Real-Time API Reference](https://developer.entur.org/pages-real-time-api/)
- [Entur Real-Time Intro (SIRI vs GTFS-RT)](https://developer.entur.org/pages-real-time-intro)
- [Stops- and Timetable data (static)](https://developer.entur.org/stops-and-timetable-data)
- [SIRI-ET Håndbok N801](https://enturas.atlassian.net/wiki/spaces/PUBLIC/pages/637370392/SIRI-ET)
- [GTFS-RT TripDescriptor (start_date, start_time)](https://support.google.com/transitpartners/answer/6159841)
- [SIRI to GTFS-RT conversion (start_date/start_time)](https://github.com/hasadna/open-bus-stride-etl/issues/16)
