import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from processor import (
    run_pipeline,
    save_uploaded_file,
    zip_directory,
)

st.set_page_config(page_title="Voice File Matrix Generator", layout="wide")


def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.title("Enter Password")
    password = st.text_input("Password", type="password")

    if st.button("Enter"):
        if password == "P4@admin":
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Wrong password")

    return False


def render_lookup_tool(df_final):
    st.subheader("Lookup Tool")

    base_df = df_final.copy()
    lookup_df = base_df.copy()

    col1, col2 = st.columns(2)

    with col1:
        line_options = ["All"] + sorted(
            base_df["Train Line"].dropna().astype(str).unique().tolist()
        )
        selected_line = st.selectbox("Train Line", line_options, key="selected_line")

        station_source_df = base_df.copy()
        if selected_line != "All":
            station_source_df = station_source_df[
                station_source_df["Train Line"].astype(str) == selected_line
            ]

        station_options = ["All"] + sorted(
            station_source_df["Station Name"].dropna().astype(str).unique().tolist()
        )
        selected_station = st.selectbox(
            "Station Name",
            station_options,
            key="selected_station",
        )

        station_code_search = st.text_input(
            "Search by station code",
            key="station_code_search",
        )

    with col2:
        direction_source_df = base_df.copy()
        if selected_line != "All":
            direction_source_df = direction_source_df[
                direction_source_df["Train Line"].astype(str) == selected_line
            ]
        if selected_station != "All":
            direction_source_df = direction_source_df[
                direction_source_df["Station Name"].astype(str) == selected_station
            ]

        direction_options = ["All"] + sorted(
            direction_source_df["Direction"].dropna().astype(str).unique().tolist()
        )
        selected_direction = st.selectbox(
            "Direction",
            direction_options,
            key="selected_direction",
        )

        time_source_df = base_df.copy()
        if selected_line != "All":
            time_source_df = time_source_df[
                time_source_df["Train Line"].astype(str) == selected_line
            ]
        if selected_station != "All":
            time_source_df = time_source_df[
                time_source_df["Station Name"].astype(str) == selected_station
            ]
        if selected_direction != "All":
            time_source_df = time_source_df[
                time_source_df["Direction"].astype(str) == selected_direction
            ]

        time_options = ["All"] + sorted(
            time_source_df["Time Slot"].dropna().astype(str).unique().tolist()
        )
        selected_time = st.selectbox("Time Slot", time_options, key="selected_time")

        voice_search = st.text_input("Search by voice file", key="voice_search")

    transcript_search = st.text_input("Search transcript", key="transcript_search")

    if selected_line != "All":
        lookup_df = lookup_df[lookup_df["Train Line"].astype(str) == selected_line]

    if selected_station != "All":
        lookup_df = lookup_df[lookup_df["Station Name"].astype(str) == selected_station]

    if selected_direction != "All":
        lookup_df = lookup_df[lookup_df["Direction"].astype(str) == selected_direction]

    if selected_time != "All":
        lookup_df = lookup_df[lookup_df["Time Slot"].astype(str) == selected_time]

    if station_code_search.strip():
        lookup_df = lookup_df[
            lookup_df["Station Code"].astype(str).str.contains(
                station_code_search.strip(),
                case=False,
                na=False,
            )
        ]

    if voice_search.strip():
        lookup_df = lookup_df[
            lookup_df["Voice File"].astype(str).str.contains(
                voice_search.strip(),
                case=False,
                na=False,
            )
        ]

    if transcript_search.strip():
        lookup_df = lookup_df[
            lookup_df["Transcript"].astype(str).str.contains(
                transcript_search.strip(),
                case=False,
                na=False,
            )
        ]

    st.write(f"Matches: {len(lookup_df)}")
    st.dataframe(lookup_df, use_container_width=True)

    filtered_csv = lookup_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download filtered results",
        data=filtered_csv,
        file_name="filtered_lookup_results.csv",
        mime="text/csv",
    )


def main():
    if not check_password():
        st.stop()

    if "processed_df" not in st.session_state:
        st.session_state.processed_df = None

    if "processed_csv_bytes" not in st.session_state:
        st.session_state.processed_csv_bytes = None

    if "organized_zip_bytes" not in st.session_state:
        st.session_state.organized_zip_bytes = None

    st.title("Voice File Matrix Generator")
    st.write(
        "Upload the raw audio list, all master/transfer Excel files, and optionally audio files."
    )

    raw_audio_list_file = st.file_uploader(
        "Upload raw audio list (.xls or .xlsx)",
        type=["xls", "xlsx"],
        key="raw_audio_list",
    )

    excel_files = st.file_uploader(
        "Upload all master/transfer Excel files (.xls or .xlsx)",
        type=["xls", "xlsx"],
        accept_multiple_files=True,
        key="excel_files",
    )

    audio_files = st.file_uploader(
        "Upload all audio files (optional)",
        accept_multiple_files=True,
        key="audio_files",
    )

    make_organized_folders = st.checkbox(
        "Also create organized folders with copied audio + transcriptions",
        value=False,
    )

    col_a, col_b = st.columns([1, 1])

    with col_a:
        run_clicked = st.button("Run Processing", type="primary")

    with col_b:
        if st.button("Clear Results"):
            st.session_state.processed_df = None
            st.session_state.processed_csv_bytes = None
            st.session_state.organized_zip_bytes = None
            for key in [
                "selected_line",
                "selected_station",
                "selected_direction",
                "selected_time",
                "station_code_search",
                "voice_search",
                "transcript_search",
            ]:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()

    if run_clicked:
        if raw_audio_list_file is None:
            st.error("Please upload the raw audio list file.")
            return

        if not excel_files:
            st.error("Please upload the master/transfer Excel files.")
            return

        if make_organized_folders and not audio_files:
            st.error("You turned on organized folders, so audio files are required.")
            return

        with st.spinner("Processing files..."):
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir = Path(temp_dir)

                excel_root = temp_dir / "excel_root"
                voice_folder = temp_dir / "voice_folder"
                output_dir = temp_dir / "output"
                organized_base = temp_dir / "organized_files_with_time"

                excel_root.mkdir(parents=True, exist_ok=True)
                voice_folder.mkdir(parents=True, exist_ok=True)
                output_dir.mkdir(parents=True, exist_ok=True)
                organized_base.mkdir(parents=True, exist_ok=True)

                raw_audio_list_path = temp_dir / raw_audio_list_file.name
                save_uploaded_file(raw_audio_list_file, raw_audio_list_path)

                for uploaded_excel in excel_files:
                    save_uploaded_file(uploaded_excel, excel_root / uploaded_excel.name)

                if audio_files:
                    for uploaded_audio in audio_files:
                        save_uploaded_file(
                            uploaded_audio,
                            voice_folder / uploaded_audio.name,
                        )

                output_csv = output_dir / "final_voice_file_matrix.csv"

                try:
                    df_final = run_pipeline(
                        raw_audio_list_path=str(raw_audio_list_path),
                        excel_root=str(excel_root),
                        voice_folder=str(voice_folder),
                        output_csv=str(output_csv),
                        organized_base=str(organized_base),
                        make_organized_folders=make_organized_folders,
                    )

                    if df_final.empty:
                        st.warning("No records found.")
                        st.session_state.processed_df = None
                        st.session_state.processed_csv_bytes = None
                        st.session_state.organized_zip_bytes = None
                        return

                    st.session_state.processed_df = df_final.copy()

                    with open(output_csv, "rb") as f:
                        st.session_state.processed_csv_bytes = f.read()

                    if make_organized_folders:
                        zip_path = output_dir / "organized_files_with_time.zip"
                        zip_directory(organized_base, zip_path)
                        with open(zip_path, "rb") as f:
                            st.session_state.organized_zip_bytes = f.read()
                    else:
                        st.session_state.organized_zip_bytes = None

                    st.success(f"Done. Total rows: {len(df_final)}")

                except Exception as e:
                    st.exception(e)
                    return

    if st.session_state.processed_df is not None:
        df_final = st.session_state.processed_df

        st.subheader("Full Output")
        st.dataframe(df_final, use_container_width=True)

        if st.session_state.processed_csv_bytes is not None:
            st.download_button(
                label="Download final CSV",
                data=st.session_state.processed_csv_bytes,
                file_name="final_voice_file_matrix.csv",
                mime="text/csv",
            )

        if st.session_state.organized_zip_bytes is not None:
            st.download_button(
                label="Download organized folders ZIP",
                data=st.session_state.organized_zip_bytes,
                file_name="organized_files_with_time.zip",
                mime="application/zip",
            )

        render_lookup_tool(df_final)


if __name__ == "__main__":
    main()
