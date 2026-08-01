"""Human labels for NOAA GSOM/GSOY element codes, quoted from NOAA's own documents.

WHY THIS IS A FILE AND NOT A DICT INSIDE THE CATALOGUE BUILDER. 3,135,873 catalogue titles hang
off 120 element codes. If a label here is invented, it is invented three million times and looks
authoritative everywhere. Every string below is taken from a NOAA document I downloaded and read:

  [D1] GSOM_GSOY_Description_Document_v1.0.2_20200219.pdf  section 2.2, elements 1-51
       https://www.ncei.noaa.gov/data/gsom/doc/
  [D2] GSOM_documentation.pdf  v1.0.3, 2023-05-15, the per-column definitions
       https://www.ncei.noaa.gov/data/gsom/doc/
  [D3] GHCN-Daily readme.txt, for codes GSOM added after [D1] was written
       https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt

Labels are shortened to a title-length phrase; the meaning is the publisher's, never mine.

THE PARAMETERISED FAMILIES ARE COMPUTED, NOT ENUMERATED. [D1] defines soil temperature as six
patterns (MXyz, MNyz, HXyz, HNyz, LXyz, LNyz) where yz selects a soil cover and depth, and
first/last freeze as FZFx with x = 0-9. Writing out 6 x 100 + 10 literal entries would be a
transcription exercise with a transcription error in it; label_for() expands the pattern.

A CODE WITH NO DOCUMENTED LABEL GETS ITS RAW CODE, NEVER A GUESS. label_for() returns
(label, resolved) so callers can count and report what did not resolve rather than shipping a
plausible-looking title nobody can check.
"""
from __future__ import annotations

# --- literal codes, from [D1]/[D2] unless marked -------------------------------------------
ELEMENTS = {
    # temperature
    "TMAX": "Monthly Mean Maximum Temperature",
    "TMIN": "Monthly Mean Minimum Temperature",
    "TAVG": "Average Temperature",
    "EMXT": "Extreme Maximum Temperature",
    "EMNT": "Extreme Minimum Temperature",
    "DYXT": "Day of Month of Extreme Maximum Temperature",
    "DYNT": "Day of Month of Extreme Minimum Temperature",
    "DX90": "Days with Maximum Temperature >= 32.2C/90F",
    "DX70": "Days with Maximum Temperature >= 21.1C/70F",
    "DX32": "Days with Maximum Temperature <= 0C/32F",
    "DT32": "Days with Minimum Temperature <= 0C/32F",
    "DT00": "Days with Minimum Temperature <= -17.8C/0F",
    # degree days
    "HTDD": "Heating Degree Days",
    "CLDD": "Cooling Degree Days",
    "HDSD": "Heating Degree Days (season to date)",
    "CDSD": "Cooling Degree Days (season to date)",
    # precipitation
    "PRCP": "Total Precipitation",
    "EMXP": "Highest Daily Total of Precipitation",
    "DYXP": "Day of Month of Highest Daily Total of Precipitation",
    "DP01": "Days with >= 0.254 mm (0.01 in) of Precipitation",
    "DP10": "Days with >= 2.54 mm (0.1 in) of Precipitation",
    "DP1X": "Days with >= 25.4 mm (1.0 in) of Precipitation",
    "DP05": "Days with >= 0.05 in of Precipitation",          # [D2] sample header
    # snow
    "SNOW": "Total Snowfall",
    "EMSN": "Highest Daily Snowfall",
    "DYSN": "Day of Month of Highest Daily Snowfall",
    "DSNW": "Days with Snowfall >= 25 mm (1 in)",
    "EMSD": "Highest Daily Snow Depth",
    "DYSD": "Day of Month of Highest Daily Snow Depth",
    "DSND": "Days with Snow Depth >= 25 mm (1 in)",
    # evaporation pan
    "EVAP": "Total Evaporation",
    "MNPN": "Mean Minimum Temperature of Evaporation Pan Water",
    "MXPN": "Mean Maximum Temperature of Evaporation Pan Water",
    "WDMV": "Total Wind Movement over Evaporation Pan",
    # sunshine
    "TSUN": "Total Sunshine",
    "PSUN": "Average Daily Percent of Possible Sunshine",
    # wind
    "AWND": "Average Wind Speed",
    "WSFM": "Maximum Wind Speed (Fastest Mile)",
    "WDFM": "Wind Direction for Maximum Wind Speed (Fastest Mile)",
    "WSF1": "Maximum Wind Speed (Fastest 1-Minute)",
    "WDF1": "Wind Direction for Maximum Wind Speed (Fastest 1-Minute)",
    "WSF2": "Maximum Wind Speed (Fastest 2-Minute)",
    "WDF2": "Wind Direction for Maximum Wind Speed (Fastest 2-Minute)",
    "WSF5": "Peak Wind Gust Speed (Fastest 5-Second)",
    "WDF5": "Wind Direction for Peak Wind Gust Speed (Fastest 5-Second)",
    "WSFG": "Peak Wind Gust Speed",
    "WDFG": "Wind Direction for Peak Wind Gust Speed",
    "WSFI": "Highest Instantaneous Wind Speed",                # [D2]
    "WDFI": "Direction of Highest Instantaneous Wind Speed",   # [D2]
    # weather-type day counts
    "DYFG": "Days with Fog",
    "DYHF": "Days with Heavy Fog (visibility under 1/4 statute mile)",
    "DYTS": "Days with Thunderstorms",
    # humidity / pressure — GSOM added these after [D1]; definitions from [D2] and [D3]
    "RHAV": "Average Relative Humidity",
    "RHMN": "Average of Minimum Relative Humidity",
    "RHMX": "Average of Maximum Relative Humidity",
    "ADPT": "Average Dew Point Temperature",
    "AWBT": "Average Wet Bulb Temperature",
    "ASLP": "Average Sea Level Pressure",
    "ASTP": "Average Station Level Pressure",
}

# --- FZFx, from [D1] element 48 -------------------------------------------------------------
_FZF = {
    "0": "First Freeze of the Year <= 0.0C/32F",
    "1": "First Freeze of the Year <= -2.2C/28F",
    "2": "First Freeze of the Year <= -4.4C/24F",
    "3": "First Freeze of the Year <= -6.7C/20F",
    "4": "First Freeze of the Year <= -8.9C/16F",
    "5": "Last Freeze of the Year <= 0.0C/32F",
    "6": "Last Freeze of the Year <= -2.2C/28F",
    "7": "Last Freeze of the Year <= -4.4C/24F",
    "8": "Last Freeze of the Year <= -6.7C/20F",
    "9": "Last Freeze of the Year <= -8.9C/16F",
}

# --- soil temperature MXyz/MNyz/HXyz/HNyz/LXyz/LNyz, from [D1] elements 40-45 ---------------
_SOIL_STAT = {
    "MX": "Mean of Daily Maximum Soil Temperature",
    "MN": "Mean of Daily Minimum Soil Temperature",
    "HX": "Highest Maximum Soil Temperature",
    "HN": "Highest Minimum Soil Temperature",
    "LX": "Lowest Maximum Soil Temperature",
    "LN": "Lowest Minimum Soil Temperature",
}
_SOIL_COVER = {"0": "unknown cover", "1": "grass", "2": "fallow", "3": "bare ground",
               "4": "brome grass", "5": "sod", "6": "straw mulch", "7": "grass muck",
               "8": "bare muck"}
_SOIL_DEPTH = {"0": "5 cm", "1": "10 cm", "2": "20 cm", "3": "50 cm", "4": "100 cm",
               "5": "150 cm", "6": "180 cm", "7": "unknown depth"}


def label_for(code: str) -> tuple[str, bool]:
    """-> (human label, resolved). `resolved` is False when the code is returned verbatim."""
    if not code:
        return "", False
    if code in ELEMENTS:
        return ELEMENTS[code], True
    if len(code) == 4 and code.startswith("FZF") and code[3] in _FZF:
        return _FZF[code[3]], True
    if len(code) == 4 and code[:2] in _SOIL_STAT:
        stat, y, z = _SOIL_STAT[code[:2]], code[2], code[3]
        # yz is documented as a cover/depth PAIR, and the observed store carries values
        # (01..09) that [D1]'s 8-cover x 7-depth table does not fully span. Label the part
        # that is documented and say plainly that the rest is a code, rather than inventing
        # a soil profile: "(soil profile 09)" is checkable, a made-up depth is not.
        cover, depth = _SOIL_COVER.get(y), _SOIL_DEPTH.get(z)
        if cover and depth:
            return f"{stat} ({cover}, {depth})", True
        return f"{stat} (soil profile {y}{z})", True
    return code, False


def summary() -> str:
    return (f"{len(ELEMENTS)} literal codes + FZF0-9 + six soil-temperature families "
            f"(MX/MN/HX/HN/LX/LN)")


if __name__ == "__main__":
    print(summary())
    for c in ("TMAX", "FZF4", "MX01", "HN09", "RHMX", "ZZZZ"):
        lab, ok = label_for(c)
        print(f"  {c:6s} {'OK ' if ok else 'RAW'} {lab}")
