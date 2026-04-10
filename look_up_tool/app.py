import tempfile
from pathlib import Path

import streamlit as st

from auth import check_password
from processor import (
    run_pipeline,
    save_uploaded_file,
    zip_directory,
)

st.set_page_config(page_title="Voice File Matrix Generator", layout="wide")


def main():
    if not check_password():
        st.stop()

    st.title("Voice File Matrix Generator")
    st.write(
        "Upload the raw audio list, all master/transfer Excel files, and all audio files."
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
        "Upload all audio files",
        accept_multiple_files=True,
        key="audio_files",
    )

    make_organized_folders = st.checkbox(
        "Also create organized folders with copied audio + transcriptions",
        value=True,
    )

    if st.button("Run Processing", type="primary"):
        if raw_audio_list_file is None:
            st.error("Please upload the raw audio list file.")
            return

        if not excel_files:
            st.error("Please upload the master/transfer Excel files.")
            return

        if not audio_files:
            st.error("Please upload the audio files.")
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

                for uploaded_audio in audio_files:
                    save_uploaded_file(uploaded_audio, voice_folder / uploaded_audio.name)

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
                        return

                    st.success(f"Done. Total rows: {len(df_final)}")

                    st.subheader("Preview")
                    st.dataframe(df_final, use_container_width=True)

                    with open(output_csv, "rb") as f:
                        st.download_button(
                            label="Download final CSV",
                            data=f.read(),
                            file_name="final_voice_file_matrix.csv",
                            mime="text/csv",
                        )

                    if make_organized_folders:
                        zip_path = output_dir / "organized_files_with_time.zip"
                        zip_directory(organized_base, zip_path)

                        with open(zip_path, "rb") as f:
                            st.download_button(
                                label="Download organized folders ZIP",
                                data=f.read(),
                                file_name="organized_files_with_time.zip",
                                mime="application/zip",
                            )

                except Exception as e:
                    st.exception(e)


if __name__ == "__main__":
    main()