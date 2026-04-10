import os
import re
import shutil
import zipfile

import pandas as pd


ORDERED_TIME_LABELS = [
    "AMRush",
    "Midday",
    "PMRush",
    "EarlyEvening",
    "LateEvening",
    "Midnight",
    "Weekday",
    "WkndMidnight",
]


def save_uploaded_file(uploaded_file, destination_path):
    with open(destination_path, "wb") as f:
        f.write(uploaded_file.getbuffer())


def zip_directory(folder_path, zip_path):
    folder_path = os.path.abspath(folder_path)
    zip_path = os.path.abspath(zip_path)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_path):
            for file_name in files:
                full_path = os.path.join(root, file_name)
                arcname = os.path.relpath(full_path, folder_path)
                zipf.write(full_path, arcname)


def normalize_text(value):
    if pd.isna(value):
        return ""
    return str(value).strip()


def normalize_col(col):
    col = str(col).strip().lower()
    col = col.replace("\n", " ")
    col = re.sub(r"\s+", " ", col)
    return col


def drop_unnamed_columns(df):
    return df.drop(
        columns=[c for c in df.columns if "unnamed" in str(c).lower()],
        errors="ignore",
    )


def find_header_row(df_raw, required_terms, max_scan_rows=25):
    required_terms = [normalize_col(x) for x in required_terms]
    scan_limit = min(len(df_raw), max_scan_rows)

    for i in range(scan_limit):
        row_values = [normalize_col(v) for v in df_raw.iloc[i].tolist()]
        row_text = " | ".join(row_values)

        if all(term in row_text for term in required_terms):
            return i

    return None


def read_sheet_auto_header(file_path, sheet_name, required_terms, max_scan_rows=25):
    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    header_row = find_header_row(df_raw, required_terms, max_scan_rows=max_scan_rows)

    if header_row is None:
        raise ValueError(
            f"Could not detect header row in {file_path} | "
            f"sheet={sheet_name} | required={required_terms}"
        )

    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
    df.columns = [str(c).strip().replace("\n", " ") for c in df.columns]
    df = drop_unnamed_columns(df)
    df = df.dropna(how="all")
    return df


def find_matching_column(df, candidates):
    normalized_map = {normalize_col(col): col for col in df.columns}

    for candidate in candidates:
        norm = normalize_col(candidate)
        if norm in normalized_map:
            return normalized_map[norm]

    return None


def extract_train_line(master_filename):
    base = os.path.basename(master_filename)
    base = base.replace("_Master.xls", "")
    return base.split("_")[-1].strip()


def extract_find_audio_pairs(columns):
    cols = list(columns)
    pairs = []

    for i in range(len(cols) - 1):
        left = normalize_col(cols[i])
        right = normalize_col(cols[i + 1])

        if "find" in left and "transfer audio file" in right:
            pairs.append((cols[i], cols[i + 1]))

    if pairs:
        return pairs

    find_cols = [c for c in cols if "find" in normalize_col(c)]
    audio_cols = [c for c in cols if "transfer audio file" in normalize_col(c)]

    for i in range(min(len(find_cols), len(audio_cols))):
        pairs.append((find_cols[i], audio_cols[i]))

    return pairs


def load_transcriptions_from_raw_audio_list(audio_list_path):
    xl = pd.ExcelFile(audio_list_path)
    frames = []

    for sheet_name in xl.sheet_names:
        try:
            df = read_sheet_auto_header(
                audio_list_path,
                sheet_name=sheet_name,
                required_terms=["file name", "audio contents"],
                max_scan_rows=20,
            )
        except Exception:
            continue

        file_name_col = find_matching_column(df, ["File Name"])
        audio_contents_col = find_matching_column(df, ["Audio Contents"])

        if not file_name_col or not audio_contents_col:
            continue

        slim = df[[file_name_col, audio_contents_col]].copy()
        slim.columns = ["File Name", "Audio Contents"]
        slim = slim.dropna(subset=["File Name"])
        slim["File Name"] = slim["File Name"].astype(str).str.strip()
        slim["Audio Contents"] = (
            slim["Audio Contents"].fillna("").astype(str).str.strip()
        )

        frames.append(slim)

    if not frames:
        return pd.DataFrame(
            columns=["File Name", "Audio Contents", "File Name Normalized"]
        )

    df_transcribe = pd.concat(frames, ignore_index=True)
    df_transcribe = df_transcribe.drop_duplicates(subset=["File Name"])
    df_transcribe["File Name Normalized"] = df_transcribe["File Name"].str.lower()

    return df_transcribe


def build_audio_index(audio_root):
    index = {}

    for root, _, files in os.walk(audio_root):
        for f in files:
            index[f.lower()] = os.path.join(root, f)

    return index


def build_final_dataset(excel_root, df_transcribe):
    results = []

    for root, _, files in os.walk(excel_root):
        master_files = [f for f in files if f.endswith("_Master.xls") or f.endswith("_Master.xlsx")]

        for master_file in master_files:
            master_path = os.path.join(root, master_file)
            train_line = extract_train_line(master_file)

            try:
                xl_master = pd.ExcelFile(master_path)
                if "Station Master" not in xl_master.sheet_names:
                    continue

                df_station = read_sheet_auto_header(
                    master_path,
                    sheet_name="Station Master",
                    required_terms=["station code", "station name"],
                    max_scan_rows=20,
                )
            except Exception:
                continue

            station_code_col = find_matching_column(df_station, ["Station Code"])
            station_name_col = find_matching_column(df_station, ["Station Name"])

            if not station_code_col or not station_name_col:
                continue

            transfer_files = [
                os.path.join(root, f)
                for f in files
                if "Transfer" in f and train_line in f and (f.endswith(".xls") or f.endswith(".xlsx"))
            ]

            for transfer_file in transfer_files:
                try:
                    df_transfer = read_sheet_auto_header(
                        transfer_file,
                        sheet_name="Transfer",
                        required_terms=["station code", "direction"],
                        max_scan_rows=20,
                    )
                except Exception:
                    continue

                transfer_station_code_col = find_matching_column(
                    df_transfer,
                    ["Station Code"],
                )
                transfer_direction_col = find_matching_column(
                    df_transfer,
                    ["Direction", "Transfer Direction"],
                )

                if not transfer_station_code_col or not transfer_direction_col:
                    continue

                pairs = extract_find_audio_pairs(df_transfer.columns)

                for _, station_row in df_station.iterrows():
                    code = normalize_text(station_row.get(station_code_col))
                    name = normalize_text(station_row.get(station_name_col))

                    if not code:
                        continue

                    for direction in ["Northbound", "Southbound"]:
                        match_rows = df_transfer[
                            (
                                df_transfer[transfer_station_code_col]
                                .astype(str)
                                .str.strip()
                                == code
                            )
                            & (
                                df_transfer[transfer_direction_col]
                                .astype(str)
                                .str.strip()
                                == direction
                            )
                        ]

                        if match_rows.empty:
                            continue

                        for _, transfer_row in match_rows.iterrows():
                            for idx, (find_col, audio_col) in enumerate(pairs):
                                if idx >= len(ORDERED_TIME_LABELS):
                                    continue

                                audio_file = transfer_row.get(audio_col)
                                if pd.isna(audio_file):
                                    continue

                                audio_file = normalize_text(audio_file)
                                find_label = normalize_text(transfer_row.get(find_col))
                                time_slot = ORDERED_TIME_LABELS[idx]

                                transcript_match = df_transcribe[
                                    df_transcribe["File Name Normalized"]
                                    == audio_file.lower()
                                ]

                                transcript = (
                                    transcript_match.iloc[0]["Audio Contents"]
                                    if not transcript_match.empty
                                    else ""
                                )

                                results.append(
                                    {
                                        "Train Line": train_line,
                                        "Station Code": code,
                                        "Station Name": name,
                                        "Direction": direction,
                                        "Time Slot": time_slot,
                                        "Find": find_label,
                                        "Voice File": audio_file,
                                        "Transcript": transcript,
                                    }
                                )

    df_final = pd.DataFrame(results)

    if df_final.empty:
        return df_final

    df_final = df_final.drop_duplicates()
    df_final = df_final.sort_values(
        by=["Train Line", "Station Code", "Direction", "Time Slot", "Voice File"],
        kind="stable",
    ).reset_index(drop=True)

    return df_final


def organize_files_by_time(df_final, organized_base, audio_index):
    if df_final.empty:
        return

    for _, row in df_final.iterrows():
        code = normalize_text(row["Station Code"])
        direction = normalize_text(row["Direction"])
        line = normalize_text(row["Train Line"])
        station_name = normalize_text(row["Station Name"])
        time_slot = normalize_text(row["Time Slot"])
        voice_file = normalize_text(row["Voice File"])

        folder_name = f"{code}_{direction}_{line}_{station_name}".replace(" ", "_")
        folder_path = os.path.join(organized_base, folder_name, time_slot)
        os.makedirs(folder_path, exist_ok=True)

        csv_path = os.path.join(folder_path, "transcriptions.csv")

        entry_df = pd.DataFrame(
            [
                {
                    "Line": line,
                    "Station Code": code,
                    "Direction": direction,
                    "Station": station_name,
                    "Voice File": voice_file,
                    "Transcript": normalize_text(row["Transcript"]),
                }
            ]
        )

        if os.path.exists(csv_path):
            try:
                existing = pd.read_csv(csv_path)
                duplicate = existing[
                    (existing["Voice File"].astype(str).str.lower() == voice_file.lower())
                    & (existing["Station Code"].astype(str) == str(code))
                    & (existing["Line"].astype(str) == str(line))
                    & (existing["Direction"].astype(str) == str(direction))
                ]
                if duplicate.empty:
                    entry_df.to_csv(csv_path, mode="a", header=False, index=False)
            except Exception:
                entry_df.to_csv(
                    csv_path,
                    mode="a",
                    header=not os.path.exists(csv_path),
                    index=False,
                )
        else:
            entry_df.to_csv(csv_path, index=False)

        actual_audio_path = audio_index.get(voice_file.lower())
        if actual_audio_path:
            dst = os.path.join(folder_path, voice_file)
            if not os.path.exists(dst):
                shutil.copy(actual_audio_path, dst)


def run_pipeline(
    raw_audio_list_path,
    excel_root,
    voice_folder,
    output_csv,
    organized_base,
    make_organized_folders=True,
):
    df_transcribe = load_transcriptions_from_raw_audio_list(raw_audio_list_path)
    audio_index = build_audio_index(voice_folder)

    df_final = build_final_dataset(excel_root, df_transcribe)

    if df_final.empty:
        return df_final

    df_final.to_csv(output_csv, index=False)

    if make_organized_folders:
        organize_files_by_time(df_final, organized_base, audio_index)

    return df_final