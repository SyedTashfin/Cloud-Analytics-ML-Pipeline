from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path
from typing import Iterable, List

KAGGLE_CRED_PATH = Path("~/.kaggle/kaggle.json").expanduser()


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for downloading Kaggle data."""
    parser = argparse.ArgumentParser(
        description="Download Kaggle dataset files into the raw data directory."
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Kaggle dataset slug, e.g. 'retailrocket/ecommerce-dataset'",
    )
    parser.add_argument(
        "--months",
        nargs="*",
        default=[],
        help="Optional list of month tokens to filter files, e.g. 2021-01 2021-02",
    )
    parser.add_argument(
        "--outdir",
        default="data/raw",
        help="Output directory for downloaded files",
    )
    return parser.parse_args()


def normalize_months(months: Iterable[str]) -> List[str]:
    """Normalize month arguments, supporting space or comma-delimited values."""
    normalized: List[str] = []
    for entry in months:
        for piece in entry.split(","):
            piece = piece.strip()
            if piece:
                normalized.append(piece)
    return normalized


def credentials_available() -> bool:
    """Return True if Kaggle API credentials are available on disk."""
    return KAGGLE_CRED_PATH.is_file()


def print_credentials_instructions() -> None:
    """Print instructions for setting up Kaggle API credentials."""
    message = (
        "Kaggle API credentials not found at ~/.kaggle/kaggle.json.\n"
        "Create them by:\n"
        "1) Visit https://www.kaggle.com -> Account -> API -> Create New API Token\n"
        "2) Move the downloaded kaggle.json to ~/.kaggle/kaggle.json\n"
        "3) Run: chmod 600 ~/.kaggle/kaggle.json\n"
        "Keep the credentials outside this repository and never commit them."
    )
    print(message)


def load_kaggle_api():
    """Load and authenticate the Kaggle API client."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ModuleNotFoundError:
        print("Missing dependency 'kaggle'. Install with: pip install kaggle")
        sys.exit(1)

    api = KaggleApi()
    try:
        api.authenticate()
    except Exception as exc:  # pragma: no cover - Kaggle handles auth in runtime
        print(f"Failed to authenticate Kaggle API: {exc}")
        sys.exit(1)
    return api


def list_dataset_files(api, dataset: str) -> List[str]:
    """List file names available for a Kaggle dataset."""
    response = api.dataset_list_files(dataset)
    files = getattr(response, "files", []) or []
    names = [file.name for file in files if getattr(file, "name", None)]
    if not names:
        raise RuntimeError(
            "No files returned for dataset. Check the dataset slug and access."
        )
    return names


def download_file(api, dataset: str, file_name: str, outdir: Path) -> None:
    """Download a single dataset file and unzip if needed."""
    outdir.mkdir(parents=True, exist_ok=True)
    try:
        api.dataset_download_file(
            dataset,
            file_name=file_name,
            path=str(outdir),
            force=False,
            quiet=False,
            unzip=True,
        )
        return
    except TypeError:
        pass

    download_path = api.dataset_download_file(
        dataset,
        file_name=file_name,
        path=str(outdir),
        force=False,
        quiet=False,
    )

    zip_path = Path(download_path) if download_path else None
    if zip_path is None or not zip_path.exists():
        candidates = list(outdir.glob(f"{Path(file_name).name}*.zip"))
        zip_path = candidates[0] if candidates else None

    if zip_path and zip_path.exists() and zipfile.is_zipfile(zip_path):
        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(outdir)


def validate_downloads(outdir: Path, file_names: List[str]) -> None:
    """Validate that expected files exist after download."""
    missing: List[str] = []
    for name in file_names:
        name_path = Path(name)
        expected = outdir / name_path
        if expected.exists():
            continue

        zip_candidate = outdir / f"{name_path.name}.zip"
        if zip_candidate.exists():
            continue

        found = next(outdir.rglob(name_path.name), None)
        if found is None:
            missing.append(name)

    if missing:
        missing_list = ", ".join(missing)
        raise FileNotFoundError(
            f"Downloaded files missing from {outdir}: {missing_list}"
        )


def main() -> None:
    """Download Kaggle files into the raw data directory."""
    args = parse_args()
    outdir = Path(args.outdir)
    months = normalize_months(args.months)

    if not credentials_available():
        print_credentials_instructions()
        sys.exit(1)

    api = load_kaggle_api()
    all_files = list_dataset_files(api, args.dataset)

    if months:
        target_files = [
            name
            for name in all_files
            if any(month in name for month in months)
        ]
        if not target_files:
            raise ValueError(
                "No dataset files matched the requested months: "
                f"{', '.join(months)}"
            )
    else:
        target_files = all_files

    for file_name in target_files:
        download_file(api, args.dataset, file_name, outdir)

    validate_downloads(outdir, target_files)
    print(f"Downloaded {len(target_files)} file(s) to {outdir}")


if __name__ == "__main__":
    main()
