"""
check_tstor_calc_inputs.py

For each river/reach listed in a river datasheet CSV, checks that the four
CSV inputs required for Tstor calculation exist, have the required columns,
and contain at least one row of data.

Checked inputs:
    #2  Channel belt areas    {wd}/ChannelBelts/Extracted_ChannelBelts/{river}/{river}_channelbelt_areas.csv
    #3  Transit lengths       {wd}/RiverMapping/Mobility/{river}/{river}_transit_lengths.csv
    #4  Mobility metrics      {wd}/RiverMapping/Mobility/{river}/{river}_mobility_metrics.csv
    #5  Aw distributions      {wd}/RiverMapping/Mobility/{river}/Aw_distributions/Reach_{n}_aw_dist.csv

Usage:
    python check_tstor_calc_inputs.py <river_datasheet.csv> <output_report.csv>
"""

import sys
import os
import re
import pandas as pd


# ── helpers ───────────────────────────────────────────────────────────────────

def parse_reach_range(reach_range_raw, available_reaches):
    """Parse the reach_range field from the datasheet into a list of ints."""
    if isinstance(reach_range_raw, (int, float)):
        return [int(reach_range_raw)]

    s = str(reach_range_raw).strip()

    if s == "All":
        return list(available_reaches)

    if s.isdigit():
        return [int(s)]

    match = re.match(r'^\(\s*(\d+)\s*,\s*(\d+)\s*\)$', s)
    if match:
        start, end = int(match.group(1)), int(match.group(2))
        return [r for r in available_reaches if start <= r <= end]

    raise ValueError(f"Cannot parse reach_range: {reach_range_raw!r}")


def check_csv(path, required_cols, ds_order=None):
    """
    Check a CSV for existence, required columns, non-empty data, and
    optionally that a specific ds_order row is present.

    Returns a short status string: 'OK' or a descriptive error.
    """
    if not os.path.isfile(path):
        return "FILE_MISSING"

    try:
        df = pd.read_csv(path)
    except Exception as e:
        return f"READ_ERROR: {e}"

    if df.empty:
        return "EMPTY"

    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        return f"MISSING_COLS: {missing_cols}"

    if ds_order is not None and ds_order not in df['ds_order'].values:
        return f"REACH_{ds_order}_NOT_IN_FILE"

    return "OK"


def infer_available_reaches(paths):
    """
    Try to infer the list of available reaches from any existing river-level
    CSV that has a ds_order column. Falls back to 1–100.
    """
    for p in paths:
        if os.path.isfile(p):
            try:
                df = pd.read_csv(p)
                if 'ds_order' in df.columns:
                    return sorted(df['ds_order'].dropna().astype(int).tolist())
            except Exception:
                pass
    return list(range(1, 101))


# ── main ──────────────────────────────────────────────────────────────────────

def run_checks(datasheet_path, output_path):
    config = pd.read_csv(datasheet_path)

    required_ds_cols = ['river_name', 'working_directory', 'reach_range']
    missing_ds_cols = [c for c in required_ds_cols if c not in config.columns]
    if missing_ds_cols:
        raise ValueError(f"Datasheet is missing required columns: {missing_ds_cols}")

    rows = []

    for _, row in config.iterrows():
        river = row['river_name']
        wd    = row['working_directory']
        rr    = row['reach_range']

        # Build paths for river-level files
        cb_path  = os.path.join(wd, 'ChannelBelts', 'Extracted_ChannelBelts', river, f"{river}_channelbelt_areas.csv")
        tl_path  = os.path.join(wd, 'RiverMapping', 'Mobility', river, f"{river}_transit_lengths.csv")
        mob_path = os.path.join(wd, 'RiverMapping', 'Mobility', river, f"{river}_mobility_metrics.csv")
        aw_dir   = os.path.join(wd, 'RiverMapping', 'Mobility', river, 'Aw_distributions')

        available_reaches = infer_available_reaches([cb_path, mob_path, tl_path])

        try:
            reaches = parse_reach_range(rr, available_reaches)
        except ValueError as e:
            rows.append({
                'river_name': river, 'ds_order': 'N/A',
                'channelbelt_areas': f"REACH_RANGE_ERROR: {e}",
                'transit_lengths':   f"REACH_RANGE_ERROR: {e}",
                'mobility_metrics':  f"REACH_RANGE_ERROR: {e}",
                'aw_distribution':   f"REACH_RANGE_ERROR: {e}",
                'all_ok': False,
            })
            continue

        for reach in reaches:
            cb_status  = check_csv(cb_path,  ['ds_order', 'area_sq_km'],       ds_order=reach)
            tl_status  = check_csv(tl_path,  ['ds_order', 'n_stor'],           ds_order=reach)
            mob_status = check_csv(mob_path, ['ds_order', 'Tw_yr', 'Pswitch'], ds_order=reach)
            aw_path    = os.path.join(aw_dir, f"Reach_{reach}_aw_dist.csv")
            aw_status  = check_csv(aw_path,  ['A_w_m2'])

            all_ok = all(s == 'OK' for s in [cb_status, tl_status, mob_status, aw_status])

            rows.append({
                'river_name':        river,
                'ds_order':          reach,
                'channelbelt_areas': cb_status,
                'transit_lengths':   tl_status,
                'mobility_metrics':  mob_status,
                'aw_distribution':   aw_status,
                'all_ok':            all_ok,
            })

    report = pd.DataFrame(rows)
    report.to_csv(output_path, index=False)

    total = len(report)
    ok    = report['all_ok'].sum()
    print(f"Report saved to: {output_path}")
    print(f"{ok}/{total} reaches have all inputs OK.")

    # Print a brief breakdown of any failures
    failures = report[~report['all_ok']]
    if not failures.empty:
        status_cols = ['channelbelt_areas', 'transit_lengths', 'mobility_metrics', 'aw_distribution']
        for col in status_cols:
            bad = failures[failures[col] != 'OK'][['river_name', 'ds_order', col]]
            if not bad.empty:
                print(f"\n  {col} issues ({len(bad)}):")
                for _, r in bad.iterrows():
                    print(f"    {r['river_name']} reach {r['ds_order']}: {r[col]}")

    return report


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python check_tstor_calc_inputs.py <river_datasheet.csv> <output_report.csv>")
        sys.exit(1)

    run_checks(sys.argv[1], sys.argv[2])
