"""Build the tiny store `api/test_conformance.py` needs, so those 18 tests can run anywhere.

WHY THIS EXISTS. The conformance suite pins the Python devserver against the Worker contract -
the redistribution gate, `?lang=` fallback, the CSV identity column, local-vs-HTTP bundle
equality. Nothing ran it. CI runs `python -m pytest tests/ -q` (.github/workflows/tests.yml), so
`api/` is never collected; and run by hand the devserver falls back to "the bundled catalog.db",
which exists only in the main checkout at 11.9 GB. In a worktree the suite scored 16 failures out
of 18, and with the real catalogue 10 passed and 8 failed for want of the parquet stores. An
18-test contract that no runner reaches is not coverage, and that is how I came to report "Full
suite: 2,490 passed" for a run that was 18 tests short (R1069).

EVERY VALUE BELOW IS COPIED FROM THE REAL STORE, NOT INVENTED. The first draft of this file made
up plausible-looking values, and plausible is exactly the failure mode a fixture must not have:
it invented a licence id (`oecd-terms`) that exists nowhere in the catalogue, wrote frequencies
as `monthly`/`quarterly`/`annual` where the catalogue stores `M`/`Q`/`A`, put `bls` in a
`prices` category it is not in, and gave four series a catalogue `last_updated` when all five
carry NULL. A fixture whose values cannot occur in production tests a system that does not
exist. The literals here were generated mechanically by reading
`data/catalog.db` and `data/_aqueduct/state.db` read-only, so they are the catalogue's own rows.

The fixture is small because the tests are specific. Six series across six sources, of which
only TWO need data on disk:

    bls:CUUR0000SA0                                   catalogue + parquet
    oecd:GDP_GROWTH_QOQ:USA                           catalogue + parquet
    penn_world_table:rgdpe:USA                        catalogue + a unit_state row
    worldbank:NY.GDP.MKTP.CD:ARB                      catalogue + an ARABIC title
    ilostat:UNE_DEAP_SEX_AGE_RT:AGE_YTHADULT_YGE15:AUS catalogue + es/fr and NO Arabic
    bcrp:BCRP:USDPEN_buy                              catalogue, NO resolver, NO store dir

The redistribution gate needs NO fixture row at all: it answers 451 before existence is
consulted, so its tests read a member from the committed denylist at runtime and probe an id
that exists in no file. No gated id is hard-coded, written to disk, or printed.

The last one is deliberate and must stay that way: ILO publishes no Arabic, so `?lang=ar` has to
fall back to English with no `title_en`, which is the graceful-fallback pin. A fixture that gave
it an Arabic title would turn a real guarantee into a test that cannot fail. That property is
REAL - the catalogue row genuinely carries only es/fr - and `_selfcheck` below asserts it.

THE PARQUET KEYS ARE NOT FREE CHOICES. `test_local_and_http_bundle_row_for_row_identical` asserts
the identity column equals exactly {"CUUR0000SA0", "Q.Y.USA.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102"},
and `test_csv_identity_column_is_native_key` asserts the CATALOGUE id never appears as a key.
Both literals were read out of the resolvers the client actually uses, not guessed:
`_resolve_bls` derives the filename as `bls_id[:2].lower()` -> `cu.parquet` and filters
`series_id == 'CUUR0000SA0'`; `_resolve_oecd` holds a hard-coded `_OECD_TEMPLATES` entry pinning
GDP_GROWTH_QOQ to ONE dataflow filename and the series-key template
`Q.Y.{geo}.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102`. Note the resolver does NOT read the catalogue's
`dataflow` metadata to build the filename, so the two cannot be kept in step by accident.

Paths are overridable, so nothing is written into the real store: `ECONDL_DATA` moves the data
root (`_resolve.default_data_root`) and the devserver takes `--catalog` / `--state`.

    python api/conformance_fixture.py <dest>     # build it and print the three paths
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys

# ---------------------------------------------------------------------------- #
# Literals the TESTS assert verbatim. Each was read out of the resolver that
# derives it (clients/python/econdl/_resolve.py), not chosen here.
# ---------------------------------------------------------------------------- #

BLS_ID = "bls:CUUR0000SA0"
OECD_ID = "oecd:GDP_GROWTH_QOQ:USA"
PWT_ID = "penn_world_table:rgdpe:USA"
WB_ID = "worldbank:NY.GDP.MKTP.CD:ARB"
ILO_ID = "ilostat:UNE_DEAP_SEX_AGE_RT:AGE_YTHADULT_YGE15:AUS"
BCRP_ID = "bcrp:BCRP:USDPEN_buy"     # catalogued, ungated, NO resolver -> the 501 pin

BLS_NATIVE = "CUUR0000SA0"                                        # _resolve_bls filter
OECD_NATIVE = "Q.Y.USA.S1.S1.B1GQ._Z._Z._Z.PC.L.G1.T0102"         # _OECD_TEMPLATES, geo=USA
OECD_FILE = "OECD.SDD.NAD__DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD.parquet"

# ---------------------------------------------------------------------------- #
# Catalogue rows - the real store's own values.
# ---------------------------------------------------------------------------- #

SOURCES = [
    # source_id, name, homepage, license_id, attribution, terms_url
    ('bls', 'Bureau of Labor Statistics', None,
     'us-public-domain', 'Source: U.S. BLS (public domain)', None),
    ('oecd', 'OECD Data Explorer (from 2024-07-01)', None,
     'cc-by-4.0', 'Source: OECD (CC BY 4.0)', None),
    ('penn_world_table', 'Penn World Table 11.0', None,
     'cc-by-4.0', 'Source: Penn World Table 11.0 (CC BY 4.0)', None),
    ('worldbank', 'World Bank Open Data (WDI)', None,
     'cc-by-4.0', 'Source: World Bank, World Development Indicators (CC BY 4.0)', None),
    ('ilostat', 'ILOSTAT (International Labour Organization)', 'https://ilostat.ilo.org/',
     'cc-by-4.0', 'Source: ILOSTAT (CC BY 4.0)', 'https://www.ilo.org/rights-and-permissions'),
    # CATALOGUED BUT NOT MIGRATED - the 501 case, and it is the common case rather than an
    # oddity: 328 sources are catalogued and only 47 have a resolver, so 281 real ungated
    # sources are in exactly this state. `bcrp` is the smallest (3 series). Without a source
    # of this kind the contract's own "501 not_migrated" pin has nothing to fire on.
    ('bcrp', 'Banco Central de Reserva del Perú (BCRP) — Estadísticas / BCRPData',
     'https://estadisticas.bcrp.gob.pe',
     'custom-terms-bcrp', 'Fuente: Banco Central de Reserva del Perú (BCRP)',
     'https://www.bcrp.gob.pe/condiciones-de-uso.html'),
]

# Without this table `_catalog.get_license` raises "no such table: license" and EVERY handler
# that reports a licence answers HTTP 500 - which is what 11 of the 18 tests were failing on.
LICENSES = [
    # license_id, name, reservable, commercial_ok, attribution_required, no_modify, url
    ('cc-by-4.0', 'cc-by-4.0', 1, 1, 1, 0, 'https://creativecommons.org/licenses/by/4.0/'),
    ('us-public-domain', 'us-public-domain', 1, 1, 0, 0, ''),
    # commercial_ok=0, unlike the other two - so the nested licence block is not all-true and
    # a handler that hard-coded `true` would be visible.
    ('custom-terms-bcrp', 'Source-specific terms — see terms_url', 1, 0, 1, 0,
     'https://www.bcrp.gob.pe/condiciones-de-uso.html'),
]

SERIES = [
    # series_id, source_id, title, frequency, unit, geography, category,
    # license_id, start_date, end_date, last_updated, metadata
    #
    # last_updated is NULL on ALL FIVE because it is NULL on all five in the real catalogue.
    # That makes the unit_state fallback the ONLY path to a freshness date, which is what
    # `test_metadata_last_updated_fallback_to_unit_state` pins.
    (BLS_ID, 'bls',
     'CPI-U: All items, US city avg (NSA)',
     'M', None, 'US', 'macro', 'us-public-domain',
     '1913-01-01', '2026-07-01', None,
     '{"bls_id": "CUUR0000SA0", "citation_long": "U.S. Bureau of Labor Statistics. Retrieved'
     ' from https://www.bls.gov. Compiled and redistributed by the Elkassabgi Data Library.",'
     ' "citation_short": "U.S. Bureau of Labor Statistics (BLS).", "description_key":'
     ' ["Vintage rows: some flat-file surveys (e.g. CPI cu) carry multiple rows per (series,'
     ' date) across release vintages; ~96k (series, date) groups differ in value (genuine'
     ' revisions). Take the latest vintage unless you need a back-run.", "Series ids follow'
     ' the BLS scheme; the survey prefix (first 2 chars) selects the file."],'
     ' "description_processing": "Retrieved from the official source, normalized to a long'
     ' {series_key, obs_date, value} schema (period-start dates), de-duplicated, and stored'
     ' as zstd Parquet. Compiled and redistributed by the Elkassabgi Data Library."}'),
    (OECD_ID, 'oecd',
     'GDP, real, growth rate, quarter-on-quarter (%, s.a.) - USA',
     'Q', 'Percent change', 'USA', 'macro', 'cc-by-4.0',
     '1995-01-01', '2026-01-01', None,
     '{"citation_long": "Organisation for Economic Co-operation and Development. Retrieved'
     ' from https://data-explorer.oecd.org. Compiled and redistributed by the Elkassabgi'
     ' Data Library.", "citation_short": "OECD.", "dataflow":'
     ' "OECD.SDD.NAD,DSD_NAMAIN1@DF_QNA_EXPENDITURE_GROWTH_OECD,1.1",'
     ' "description_processing": "Retrieved from the official source, normalized to a long'
     ' {series_key, obs_date, value} schema (period-start dates), de-duplicated, and stored'
     ' as zstd Parquet. Compiled and redistributed by the Elkassabgi Data Library.",'
     ' "indicator": "GDP_GROWTH_QOQ", "ref_area": "USA"}'),
    (PWT_ID, 'penn_world_table',
     'Expenditure-side real GDP at chained PPPs - United States',
     'A', 'mil. 2021US$', 'USA', 'macro', 'cc-by-4.0',
     '1950-12-31', '2023-12-31', None,
     '{"citation_long": "Feenstra, R. C., Inklaar, R., & Timmer, M. P. (2015). The Next'
     ' Generation of the Penn World Table. American Economic Review. Retrieved from'
     ' https://www.rug.nl/ggdc/productivity/pwt. Compiled and redistributed by the'
     ' Elkassabgi Data Library.", "citation_short": "Penn World Table (Feenstra, Inklaar &'
     ' Timmer 2015).", "country": "United States", "definition": "Expenditure-side real GDP'
     ' at chained PPPs", "description_processing": "Retrieved from the official source,'
     ' normalized to a long {series_key, obs_date, value} schema (period-start dates),'
     ' de-duplicated, and stored as zstd Parquet. Compiled and redistributed by the'
     ' Elkassabgi Data Library.", "pwt_version": "11.0", "variable": "rgdpe"}'),
    (WB_ID, 'worldbank',
     'GDP (current US$) - ARB',
     'A', None, 'ARB', 'macro', 'cc-by-4.0',
     '1961-12-31', '2024-12-31', None,
     '{"citation_long": "World Bank (World Development Indicators). Retrieved from'
     ' https://data.worldbank.org. Compiled and redistributed by the Elkassabgi Data'
     ' Library.", "citation_short": "World Bank.", "description_processing": "Retrieved'
     ' from the official source, normalized to a long {series_key, obs_date, value} schema'
     ' (period-start dates), de-duplicated, and stored as zstd Parquet. Compiled and'
     ' redistributed by the Elkassabgi Data Library.", "indicator": "NY.GDP.MKTP.CD",'
     ' "titles": {"ar": "\\u0625\\u062c\\u0645\\u0627\\u0644\\u064a \\u0627\\u0644\\u0646'
     '\\u0627\\u062a\\u062c \\u0627\\u0644\\u0645\\u062d\\u0644\\u064a (\\u0627\\u0644\\u0642'
     '\\u064a\\u0645\\u0629 \\u0627\\u0644\\u062d\\u0627\\u0644\\u064a\\u0629 \\u0628\\u0627'
     '\\u0644\\u062f\\u0648\\u0644\\u0627\\u0631 \\u0627\\u0644\\u0623\\u0645\\u0631\\u064a'
     '\\u0643\\u064a) - \\u0627\\u0644\\u0639\\u0627\\u0644\\u0645 \\u0627\\u0644\\u0639\\u0631'
     '\\u0628\\u064a", "es": "PIB (US$ a precios actuales) - El mundo \\u00e1rabe", "fr":'
     ' "PIB ($\\u00a0US courants) - Le monde arabe", "zh": "GDP\\uff08\\u73b0\\u4ef7\\u7f8e'
     '\\u5143\\uff09 - \\u963f\\u62c9\\u4f2f\\u8054\\u76df\\u56fd\\u5bb6"}}'),
    (ILO_ID, 'ilostat',
     'Unemployment rate, aged 15+ (Total) - Australia',
     'A', '%', 'AUS', 'labour', 'cc-by-4.0',
     '1979-12-31', '2024-12-31', None,
     # es/fr ONLY, and NO 'ar' -- that is the real catalogue row, and it is what makes
     # ?lang=ar exercise the graceful fallback. _selfcheck asserts 'ar' stays absent.
     '{"citation_long": "International Labour Organization, ILOSTAT. Retrieved from'
     ' https://ilostat.ilo.org. Compiled and redistributed by the Elkassabgi Data'
     ' Library.", "citation_short": "International Labour Organization (ILOSTAT).",'
     ' "classif1": "AGE_YTHADULT_YGE15", "description_processing": "Retrieved from the'
     ' official source, normalized to a long {series_key, obs_date, value} schema'
     ' (period-start dates), de-duplicated, and stored as zstd Parquet. Compiled and'
     ' redistributed by the Elkassabgi Data Library.", "indicator": "UNE_DEAP_SEX_AGE_RT",'
     ' "provider": "ILOSTAT", "sex": "SEX_T", "titles": {"es": "Tasa de desocupaci\\u00f3n'
     ' seg\\u00fan sexo y edad (%) - Edad (J\\u00f3venes, adultos): 15+ (Total) -'
     ' Australia", "fr": "Taux de ch\\u00f4mage par sexe et \\u00e2ge (%) - Age (Jeunes,'
     ' adultes): 15+ (Total) - Australie"}}'),
    # The 501 case. Note frequency/unit/geography/category are ALL NULL here - that is the
    # real row, not a shortcut, and it means the metadata route's canonical keys are
    # exercised against a series that fills almost none of them.
    (BCRP_ID, 'bcrp',
     'Tipo de cambio - TC Sistema bancario SBS (S/ por US$) - Compra',
     None, None, None, None, 'custom-terms-bcrp',
     '1997-01-02', '2026-06-22', None,
     '{"citation_long": "Banco Central de Reserva del Per\\u00fa (BCRP) \\u2014'
     ' Estad\\u00edsticas / BCRPData. Retrieved from https://estadisticas.bcrp.gob.pe.'
     ' Compiled and redistributed by the Elkassabgi Data Library.", "citation_short":'
     ' "Banco Central de Reserva del Per\\u00fa (BCRP) \\u2014 Estad\\u00edsticas /'
     ' BCRPData.", "description_processing": "Retrieved from the official source,'
     ' normalized to a long {series_key, obs_date, value} schema (period-start dates),'
     ' de-duplicated, and stored as zstd Parquet. Compiled and redistributed by the'
     ' Elkassabgi Data Library."}'),
]

# ---------------------------------------------------------------------------- #
# State rows. `/v1/last-updates` is unit_state LEFT JOIN source_state, and
# `_next_update_expected` returns null when EITHER input is missing:
# `if not last_success_utc or not cadence: return None` (devserver.py:165).
#
# BE PRECISE ABOUT WHICH MECHANISM IS AT WORK, because an earlier version of this comment was
# not. For `oecd` and `ilostat` BOTH conditions hold - they have no source_state row (so the
# LEFT JOIN yields cadence NULL) AND their unit rows carry last_success_utc NULL. The null is
# therefore OVER-DETERMINED, and an adversarial review showed the consequence: giving either
# one a cadence, on its own, changes no answer, because the missing timestamp still forces the
# null. Saying "the missing source_state row supplies the null branch" named one of two causes
# as if it were the cause.
#
# It is still the real shape - 27 unit rows fleet-wide have a source with no source_state row -
# and it is what lets `test_last_updates_cadence_annual_and_null` see both branches honestly,
# with penn_world_table supplying the annual/365 side. `_selfcheck` pins the cadence-NULL
# mechanism SPECIFICALLY rather than the disjunction, so the LEFT-JOIN-miss path cannot quietly
# disappear behind the half of the OR that would keep the test green.
# ---------------------------------------------------------------------------- #

SOURCE_STATE = [   # source_id, cadence, status, last_success_utc
    ('bls', 'weekly', 'ok', '2026-09-15T11:04:42+00:00'),
    # 'oecd': NO source_state row in the real store -> LEFT JOIN miss -> cadence NULL
    ('penn_world_table', 'annual', 'ok', '2026-07-27T05:40:57+00:00'),
    ('worldbank', 'monthly', 'ok', '2026-08-31T05:26:38+00:00'),
    # 'ilostat': NO source_state row in the real store -> LEFT JOIN miss -> cadence NULL
]

UNIT_STATE = [
    # source_id, unit_id, status, last_success_utc, last_obs_date, obs_count, upstream_vintage
    ('bls', '_all', 'partial', '2026-09-15T11:04:42+00:00', '2026-08-01', 197457576, 'date-tail'),
    # oecd's last_obs_date really is 2100-12-31 in the live state store - an impossible date
    # from the known impossible-date census, copied verbatim rather than quietly corrected so
    # nobody "fixes" the fixture and loses the record of it. No assertion depends on it.
    ('oecd', '_all', 'partial', None, '2100-12-31', 270770922, None),
    ('penn_world_table', '_all', 'no_change', '2026-07-27T05:40:57+00:00', '2023-12-31',
     418397, 'date-tail'),
    ('worldbank', '_all', 'ok', '2026-08-31T05:26:38+00:00', '2025-12-31', 32248, 'date-tail'),
    ('ilostat', '_all', 'partial', None, None, 159899605, None),
]


def build_catalog(path: str) -> None:
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE source (
          source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT,
          attribution TEXT, terms_url TEXT);
        CREATE TABLE license (
          license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER, commercial_ok INTEGER,
          attribution_required INTEGER, no_modify INTEGER DEFAULT 0, url TEXT);
        CREATE TABLE series (
          series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
          geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT,
          last_updated TEXT, metadata TEXT);
        CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title, geography);
    """)
    con.executemany("INSERT INTO source VALUES (?,?,?,?,?,?)", SOURCES)
    con.executemany("INSERT INTO license VALUES (?,?,?,?,?,?,?)", LICENSES)
    con.executemany("INSERT INTO series VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", SERIES)
    con.executemany("INSERT INTO series_fts (series_id, title, geography) VALUES (?,?,?)",
                    [(s[0], s[2], s[5]) for s in SERIES])
    con.commit()
    con.close()


def build_state(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE source_state(
          source_id TEXT PRIMARY KEY, strategy TEXT, cadence TEXT, status TEXT,
          last_success_utc TEXT, last_attempt_utc TEXT, owner TEXT,
          enabled INTEGER DEFAULT 1, note TEXT);
        CREATE TABLE unit_state(
          source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT,
          last_success_utc TEXT, last_attempt_utc TEXT, status TEXT,
          last_obs_date TEXT, obs_count INTEGER DEFAULT 0, attempt_count INTEGER DEFAULT 0,
          last_error TEXT, PRIMARY KEY(source_id, unit_id));
    """)
    for sid, cadence, status, last_success in SOURCE_STATE:
        con.execute("INSERT INTO source_state (source_id, strategy, cadence, status, "
                    "last_success_utc, last_attempt_utc, enabled) VALUES (?,?,?,?,?,?,1)",
                    (sid, "extend_by_date", cadence, status, last_success, last_success))
    for sid, unit, status, last_success, last_obs, obs_count, vintage in UNIT_STATE:
        con.execute("INSERT INTO unit_state (source_id, unit_id, strategy, upstream_vintage, "
                    "last_success_utc, last_attempt_utc, status, last_obs_date, obs_count) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (sid, unit, "extend_by_date", vintage, last_success, last_success,
                     status, last_obs, obs_count))
    con.commit()
    con.close()


def build_store(root: str) -> None:
    """The two parquets, keyed the way the RESOLVERS expect - not the catalogue ids."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    bls_dir = os.path.join(root, "bls")
    oecd_dir = os.path.join(root, "oecd")
    os.makedirs(bls_dir, exist_ok=True)
    os.makedirs(oecd_dir, exist_ok=True)

    # _resolve_bls: filename is bls_id[:2].lower() -> 'cu', key column `series_id`.
    pq.write_table(pa.table({"series_id": [BLS_NATIVE] * 3,
                            "obs_date": ["2026-01-01", "2026-02-01", "2026-03-01"],
                            "value": [300.1, 301.2, 302.3]}),
                   os.path.join(bls_dir, "cu.parquet"))
    # _resolve_oecd: ONE dataflow file per indicator, key column `series_key`, exact match.
    pq.write_table(pa.table({"series_key": [OECD_NATIVE] * 3,
                            "obs_date": ["2026-01-01", "2026-04-01", "2026-07-01"],
                            "value": [0.5, 0.7, 0.6]}),
                   os.path.join(oecd_dir, OECD_FILE))


def _selfcheck(catalog: str, state: str, data_root: str) -> None:
    """Refuse to hand back a fixture that has silently stopped exercising the tests.

    A fixture is the one kind of test input that can make a suite pass by CONSTRUCTION, and
    a value edited here can disable a guarantee without failing anything - the test still
    passes, it just no longer tests. Each assertion below names the test it keeps alive.
    """
    cat = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)
    cat.row_factory = sqlite3.Row
    st = sqlite3.connect(f"file:{state}?mode=ro", uri=True)
    st.row_factory = sqlite3.Row
    try:
        # Every referenced licence must resolve, or the licence-bearing handlers 500.
        missing = [r["license_id"] for r in cat.execute(
            "SELECT DISTINCT s.license_id FROM source s LEFT JOIN license l "
            "ON l.license_id = s.license_id WHERE s.license_id IS NOT NULL "
            "AND l.license_id IS NULL")]
        assert not missing, f"source rows reference licences with no license row: {missing}"

        def titles(series_id):
            row = cat.execute("SELECT metadata FROM series WHERE series_id=?",
                              (series_id,)).fetchone()
            assert row is not None, f"fixture is missing series {series_id}"
            return json.loads(row["metadata"] or "{}").get("titles") or {}

        # test_lang_localizes_title: needs a series WITH an Arabic title.
        assert "ar" in titles(WB_ID), "worldbank lost its Arabic title; ?lang=ar cannot hit"
        # test_lang_graceful_fallback: needs a series WITHOUT one. If 'ar' ever appears
        # here the fallback test passes for the wrong reason and can never fail.
        ilo = titles(ILO_ID)
        assert "ar" not in ilo, "ilostat gained an Arabic title; the fallback pin is now vacuous"
        assert {"es", "fr"} <= set(ilo), f"ilostat lost es/fr: {sorted(ilo)}"

        # test_metadata_last_updated_fallback_to_unit_state: the catalogue must NOT carry a
        # last_updated for pwt, and unit_state MUST carry one, or the fallback is untested.
        row = cat.execute("SELECT last_updated FROM series WHERE series_id=?",
                          (PWT_ID,)).fetchone()
        assert row["last_updated"] is None, "pwt gained a catalogue last_updated"
        row = st.execute("SELECT last_success_utc FROM unit_state WHERE source_id=? "
                         "AND unit_id='_all'", ("penn_world_table",)).fetchone()
        assert row and row["last_success_utc"], "pwt has no unit_state _all timestamp to fall back to"

        # test_last_updates_cadence_annual_and_null: BOTH branches must be reachable.
        joined = st.execute(
            "SELECT u.source_id, u.last_success_utc, s.cadence FROM unit_state u "
            "LEFT JOIN source_state s ON s.source_id = u.source_id").fetchall()
        assert any(r["cadence"] == "annual" and r["last_success_utc"] for r in joined), \
            "no annual unit with a timestamp; the 365-day branch is unreachable"
        # There are TWO ways to reach a null next_update_expected - no cadence, or no
        # timestamp - and the first draft of this assertion accepted either. That let a
        # fixture add source_state rows for every source (killing the LEFT-JOIN-miss path)
        # and still pass, because the no-timestamp path alone kept the OR true. Pin the
        # cadence-NULL mechanism specifically; it is the one the real store exhibits on 27
        # unit rows and the one the test's own comment describes.
        assert any(r["cadence"] is None for r in joined), \
            "every unit's source has a source_state row; the cadence-NULL branch is unreachable"

        # test_sources_nested_shape: needs at least one non-null freshness.
        assert any(r["cadence"] for r in joined), "no source carries freshness"

        # test_status_501_not_migrated: bcrp is in the fixture PRECISELY because it has no
        # resolver. If one is ever written for it, the 501 pin silently turns into a 502 or a
        # 200 and nothing else would say so.
        clients = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                               "clients", "python")
        if clients not in sys.path:
            sys.path.insert(0, clients)
        from econdl import _resolve                                        # noqa: PLC0415
        supported = set(_resolve.supported_sources())
        # supported_sources() swallows every exception (a bare `except Exception: pass`), so
        # an empty answer is a SILENT degradation, not an honest "none". Refuse it: otherwise
        # `"bcrp" not in supported` is true for the wrong reason and both pins below go
        # vacuous at once.
        assert supported, "supported_sources() read EMPTY - it swallows exceptions, so this " \
                          "is a silent failure rather than a real answer"
        assert "bcrp" not in supported, \
            "bcrp gained a resolver; the 501 not_migrated pin is now vacuous"
        assert "bls" in supported, \
            "bls lost its resolver; the data_unavailable-vs-501 distinction is now vacuous"

        # Task #6 identity pin: the parquets must hold the NATIVE keys, and the catalogue
        # ids must NOT appear in them, or "native key != catalog id" proves nothing.
        import pyarrow.parquet as pq
        bls_keys = set(pq.read_table(os.path.join(data_root, "bls", "cu.parquet"),
                                     columns=["series_id"])["series_id"].to_pylist())
        oecd_keys = set(pq.read_table(os.path.join(data_root, "oecd", OECD_FILE),
                                      columns=["series_key"])["series_key"].to_pylist())
        assert bls_keys == {BLS_NATIVE}, f"bls parquet keys are {bls_keys}"
        assert oecd_keys == {OECD_NATIVE}, f"oecd parquet keys are {oecd_keys}"
        assert BLS_ID not in bls_keys and OECD_ID not in oecd_keys, \
            "a catalogue id leaked into a parquet key column; the identity pin is vacuous"
    finally:
        cat.close()
        st.close()


def build(dest: str) -> dict:
    """Build everything under `dest`. Returns the paths the caller must wire up."""
    data_root = os.path.join(dest, "data", "clean_full")
    os.makedirs(data_root, exist_ok=True)
    catalog = os.path.join(dest, "catalog.db")
    state = os.path.join(dest, "data", "_aqueduct", "state.db")
    for p in (catalog, state):
        if os.path.exists(p):
            os.remove(p)
    build_catalog(catalog)
    build_state(state)
    build_store(data_root)
    _selfcheck(catalog, state, data_root)
    return {"catalog": catalog, "state": state, "data_root": data_root}


if __name__ == "__main__":
    out = build(sys.argv[1] if len(sys.argv) > 1 else "_conformance_fixture")
    for k, v in out.items():
        print(f"{k}: {v}")
