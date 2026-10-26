"""hagstofa QUARANTINE - tables whose STORED rows already hold measured damage that no fetch-time guard
can see (review AR-379, ledger R1673). Read by hagstofa._guard_changed: a table listed here never enters the
changed set (fail closed) and is named in the note, like the scheme holds.

WHY A LIST. The fetcher compares a fetch with the STORE. Damage merged before the shift and re-code guards
existed (R1124, R1130, R1140) sits in the store itself, so every later merge into such a table would publish
it through Route B's changed set. It shows only when the store is compared with the SERVED copy, which the
fetcher never reads. The 37 entries are the re-derive planner's E3 set (instrument 3: store vs served revised
cells, 2026-10-24 pin 20261024T032144Z, R2 store copy md5 == ETag): OFFSET = revised values equal to a
neighbour's served value (SKO02102: 1,143 of 1,143 at school code -1, the R1124 renumbering); RESCALE = a
uniform store/served ratio (UMH31220, UMH31230: median exactly 2.000 at 2023-12-31); SPLICE = a large median
change confined to the boundary period; JUMP = series opening with a >3x break from their served history.
SPLICE and JUMP are the planner's thresholds and may hold some ordinary revisions: holding one costs a stale
CSV, publishing a damaged one costs wrong data.

An entry is REMOVED only by a review that clears the table (or after its clean re-pull, R22), in the same PR
as the evidence. Never add or remove one without the measurement that justifies it.
"""

QUARANTINE: dict[str, str] = {
    'ICE:Atvinnuvegir:fyrirtaeki:launakostnadur:launakostnadur:VIN02091.px':
        'E3 2026-10-24: 123 revised cells vs served; SPLICE 8.3% / 102',
    'ICE:Atvinnuvegir:sjavarutvegur:aflatolur:afli_manudir:SJA01101.px':
        'E3 2026-10-24: 894 revised cells vs served; SPLICE 25.1% / 889',
    'ICE:Atvinnuvegir:sjavarutvegur:aflatolur:afli_manudir:SJA01102.px':
        'E3 2026-10-24: 731 revised cells vs served; SPLICE 20.4% / 730',
    'ICE:Efnahagur:fjaropinber:fjarmal_fraedsla:1_utgjold_fraedsla:THJ05611.px':
        'E3 2026-10-24: 21 revised cells vs served; SPLICE 7.4% / 21',
    'ICE:Efnahagur:fjaropinber:fjarmal_fraedsla:1_utgjold_fraedsla:THJ05614.px':
        'E3 2026-10-24: 9 revised cells vs served; SPLICE 7.5% / 8',
    'ICE:Efnahagur:fjaropinber:fjarmal_heilbr:THJ05511.px':
        'E3 2026-10-24: 31 revised cells vs served; SPLICE 7.5% / 28; JUMP 4 of 60',
    'ICE:Efnahagur:fjaropinber:fjarmal_heilbr:THJ05532.px':
        'E3 2026-10-24: 29 revised cells vs served; JUMP 4 of 44',
    'ICE:Efnahagur:fjaropinber:fjarmal_opinber:fjarmal_arsfj:THJ05811.px':
        'E3 2026-10-24: 21 revised cells vs served; SPLICE 9.1% / 20',
    'ICE:Efnahagur:fjaropinber:fjarmal_opinber:fjarmal_opinber:THJ05151.px':
        'E3 2026-10-24: 48 revised cells vs served; JUMP 5 of 101',
    'ICE:Efnahagur:fjaropinber:fjarmal_opinber:fjarmal_opinber:THJ05171.px':
        'E3 2026-10-24: 31 revised cells vs served; JUMP 3 of 86',
    'ICE:Efnahagur:fjaropinber:fjarmal_opinber:fjarmal_opinber:THJ05181.px':
        'E3 2026-10-24: 28 revised cells vs served; SPLICE 9.1% / 26',
    'ICE:Efnahagur:fjaropinber:fjarmal_rikissjods:THJ05221.px':
        'E3 2026-10-24: 50 revised cells vs served; SPLICE 5.1% / 48',
    'ICE:Efnahagur:fjaropinber:fjarmal_rikissjods:THJ05251.px':
        'E3 2026-10-24: 0 revised cells vs served; JUMP 3 of 96',
    'ICE:Efnahagur:fjaropinber:fjarmal_rikissjods:THJ05267.px':
        'E3 2026-10-24: 0 revised cells vs served; JUMP 4 of 90',
    'ICE:Efnahagur:fjaropinber:fjarmal_rikissjods:THJ05271.px':
        'E3 2026-10-24: 0 revised cells vs served; JUMP 5 of 82',
    'ICE:Efnahagur:fjaropinber:fjarmal_rikissjods:THJ05281.px':
        'E3 2026-10-24: 28 revised cells vs served; SPLICE 9.4% / 26',
    'ICE:Efnahagur:thjodhagsreikningar:fjarmalareikningar:fjarmalareikningar:THJ10001.px':
        'E3 2026-10-24: 691 revised cells vs served; SPLICE 12.1% / 551',
    'ICE:Efnahagur:thjodhagsreikningar:fjarmalareikningar:fjarmalareikningar:THJ10002.px':
        'E3 2026-10-24: 138 revised cells vs served; SPLICE 6.2% / 132',
    'ICE:Efnahagur:thjodhagsreikningar:fjarmunamyndun_fjarmunaeign:fjarmunamyndun:THJ03105.px':
        'E3 2026-10-24: 155 revised cells vs served; SPLICE 6.2% / 155',
    'ICE:Efnahagur:thjodhagsreikningar:fjarmunamyndun_fjarmunaeign:fjarmunamyndun_arsfj:THJ03111.px':
        'E3 2026-10-24: 20 revised cells vs served; SPLICE 10.7% / 19',
    'ICE:Efnahagur:thjodhagsreikningar:tekjuskipting:tekjuskipting:THJ06021.px':
        'E3 2026-10-24: 18 revised cells vs served; SPLICE 5.0% / 18',
    'ICE:Efnahagur:utanrikisverslun:1_voruvidskipti:01_voruskipti:UTA06003.px':
        'E3 2026-10-24: 12 revised cells vs served; JUMP 14 of 506',
    'ICE:Efnahagur:utanrikisverslun:1_voruvidskipti:01_voruskipti:UTA06203.px':
        'E3 2026-10-24: 9 revised cells vs served; SPLICE 6.0% / 6',
    'ICE:Efnahagur:utanrikisverslun:2_thjonusta:thjonusta:UTA04010.px':
        'E3 2026-10-24: 0 revised cells vs served; JUMP 38 of 1404',
    'ICE:Samfelag:launogtekjur:2_lvt:2_arsfjordungstolur:LAU04109.px':
        'E3 2026-10-24: 46 revised cells vs served; SPLICE 18.2% / 38',
    'ICE:Samfelag:skolamal:2_grunnskolastig:0_gsNemendur:SKO02102.px':
        'E3 2026-10-24: 1143 revised cells vs served; OFFSET 1143 of 1143; SPLICE 51.6% / 959; JUMP 306 of 2191',
    'ICE:Samfelag:vinnumarkadur:vinnumarkadsrannsokn:2_arsfjordungstolur:VIN01206.px':
        'E3 2026-10-24: 12 revised cells vs served; SPLICE 5.7% / 10',
    'ICE:Umhverfi:1_natturufar:1_landlysing:UMH01111.px':
        'E3 2026-10-24: 24 revised cells vs served; SPLICE 7.9% / 22; JUMP 3 of 28',
    'ICE:Umhverfi:2_losunlofttegunda:1_losunlofttegunda_nir:UMH31101.px':
        'E3 2026-10-24: 35 revised cells vs served; SPLICE 8.0% / 34; AR-381: vintage splice, LULUCF_WETLAND '
        'CH4 2022 stored 37,746.48 vs publisher 667.09 (a 56x break at 2022/2023 the publisher does not have)',
    'ICE:Umhverfi:2_losunlofttegunda:1_losunlofttegunda_nir:UMH31107.px':
        'E3 2026-10-24: 25 revised cells vs served; SPLICE 97.9% / 22; JUMP 15 of 35; AR-381: category relabel, '
        '2.A 2022 stored 1,871.92 vs publisher 0.94 (two categories in one series)',
    'ICE:Umhverfi:2_losunlofttegunda:1_losunlofttegunda_nir:UMH31108.px':
        'E3 2026-10-24: 118 revised cells vs served; SPLICE 8.4% / 112; JUMP 12 of 230',
    'ICE:Umhverfi:2_losunlofttegunda:2_losunlofttegunda_aea:UMH31109.px':
        'E3 2026-10-24: 67 revised cells vs served; SPLICE 18.7% / 22; JUMP 5 of 78',
    'ICE:Umhverfi:2_losunlofttegunda:2_losunlofttegunda_aea:UMH31110.px':
        'E3 2026-10-24: 72 revised cells vs served; SPLICE 18.2% / 67',
    'ICE:Umhverfi:2_losunlofttegunda:2_losunlofttegunda_aea:UMH31118.px':
        'E3 2026-10-24: 380 revised cells vs served; SPLICE 18.1% / 242; JUMP 13 of 428',
    'ICE:Umhverfi:2_losunlofttegunda:2_losunlofttegunda_aea:UMH31119.px':
        'E3 2026-10-24: 375 revised cells vs served; SPLICE 18.7% / 203; JUMP 20 of 405',
    'ICE:Umhverfi:2_losunlofttegunda:3_losunlofttegunda_lrtap:UMH31220.px':
        'E3 2026-10-24: 75 revised cells vs served; RESCALE x2 / 41 of 59; SPLICE 100.0% / 59',
    'ICE:Umhverfi:2_losunlofttegunda:3_losunlofttegunda_lrtap:UMH31230.px':
        'E3 2026-10-24: 256 revised cells vs served; RESCALE x2 / 127 of 177; SPLICE 100.0% / 177; JUMP 11 of 278',
}
