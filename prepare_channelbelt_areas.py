"""
prepare_channelbelt_areas.py

Extracts channel belt areas from the compiled river data CSV and writes
one channelbelt_areas CSV per river in the format expected by
Mobility_SedStorage_Modeling.py (get_tstor_distributions).

Output per river:
    {working_directory}/ChannelBelts/Extracted_ChannelBelts/{river_name}/
        {river_name}_channelbelt_areas.csv
    Columns: ds_order, area_sq_km

Usage:
    python prepare_channelbelt_areas.py <river_datasheet.csv> <compiled_data.csv>
"""

import sys
import os
import pandas as pd


def run(datasheet_path, compiled_path):
    datasheet = pd.read_csv(datasheet_path)
    compiled  = pd.read_csv(compiled_path)

    # Validate required columns
    for col in ['river_name', 'working_directory']:
        if col not in datasheet.columns:
            raise ValueError(f"Datasheet missing column: '{col}'")
    for col in ['river_name', 'ds_order', 'Acb_km2']:
        if col not in compiled.columns:
            raise ValueError(f"Compiled CSV missing column: '{col}'")

    # Rivers to process — exact match only
    datasheet_rivers = set(datasheet['river_name'].unique())
    compiled_rivers  = set(compiled['river_name'].unique())

    matched   = datasheet_rivers & compiled_rivers
    unmatched = datasheet_rivers - compiled_rivers

    if unmatched:
        print(f"WARNING: {len(unmatched)} river(s) in datasheet have no exact match in compiled CSV:")
        for r in sorted(unmatched):
            print(f"  {r}")
        print()

    # Build river_name → working_directory lookup from datasheet
    wd_lookup = datasheet.drop_duplicates('river_name').set_index('river_name')['working_directory']

    saved = 0
    for river_name in sorted(matched):
        working_dir = wd_lookup[river_name]

        # Extract and rename
        subset = (
            compiled[compiled['river_name'] == river_name][['ds_order', 'Acb_km2']]
            .rename(columns={'Acb_km2': 'area_sq_km'})
            .sort_values('ds_order')
            .reset_index(drop=True)
        )

        # Build output path and save
        out_dir  = os.path.join(working_dir, 'ChannelBelts', 'Extracted_ChannelBelts', river_name)
        out_file = os.path.join(out_dir, f"{river_name}_channelbelt_areas.csv")
        os.makedirs(out_dir, exist_ok=True)
        subset.to_csv(out_file, index=False)
        print(f"Saved: {out_file}  ({len(subset)} reach(es))")
        saved += 1

    print(f"\nDone. {saved} river(s) written, {len(unmatched)} skipped (no match).")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python prepare_channelbelt_areas.py <river_datasheet.csv> <compiled_data.csv>")
        sys.exit(1)

    run(sys.argv[1], sys.argv[2])
