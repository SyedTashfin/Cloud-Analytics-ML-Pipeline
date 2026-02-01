from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

PathLike = Union[str, Path]

DATA_DIR_ENV = os.getenv("DASHBOARD_DATA_DIR")
if DATA_DIR_ENV:
    DATA_DIR: PathLike = DATA_DIR_ENV.rstrip("/")
else:
    DATA_DIR = Path(__file__).resolve().parents[1] / "reports" / "dashboard"

REPORTS_ROOT_ENV = os.getenv("REPORTS_ROOT")
if REPORTS_ROOT_ENV:
    REPORTS_ROOT: PathLike = REPORTS_ROOT_ENV.rstrip("/")
elif str(DATA_DIR).endswith("/dashboard"):
    REPORTS_ROOT = str(DATA_DIR).rsplit("/", 1)[0]
else:
    REPORTS_ROOT = Path(DATA_DIR).parent if isinstance(DATA_DIR, Path) else DATA_DIR

FONT_BODY = "IBM Plex Sans"
FONT_DISPLAY = "IBM Plex Serif"

COLOR_TEXT = "#111111"
COLOR_MUTED = "#4B5563"
COLOR_ACCENT = "#0F766E"
COLOR_ACCENT_2 = "#EA580C"
COLOR_ACCENT_3 = "#1D4ED8"
COLOR_ACCENT_4 = "#B45309"

LEAKAGE_HINTS = (
    "purchase",
    "event_type_purchase",
    "has_purchase",
    "purchase_count",
    "event_purchase_count",
)


def is_gcs_path(value: PathLike) -> bool:
    """Return True when the path points to GCS."""
    return str(value).startswith("gs://")


def normalize_gcs_path(value: str) -> str:
    """Normalize a GCS path for gcsfs."""
    return value.replace("gs://", "", 1) if value.startswith("gs://") else value


@st.cache_resource(show_spinner=False)
def get_gcs_fs():
    """Create a cached GCS filesystem client."""
    import gcsfs

    return gcsfs.GCSFileSystem()


def path_exists(path: PathLike) -> bool:
    """Check whether a local or GCS path exists."""
    if is_gcs_path(path):
        fs = get_gcs_fs()
        return fs.exists(normalize_gcs_path(str(path)))
    return Path(path).exists()


def join_path(base: PathLike, *parts: str) -> PathLike:
    """Join path components for local or GCS paths."""
    if is_gcs_path(base):
        return "/".join([str(base).rstrip("/"), *parts])
    return Path(base, *parts)


def data_path(*parts: str) -> PathLike:
    """Build a path relative to the dashboard data root."""
    return join_path(DATA_DIR, *parts)


def reports_path(*parts: str) -> PathLike:
    """Build a path relative to the reports root."""
    return join_path(REPORTS_ROOT, *parts)

px.defaults.template = "plotly_white"
px.defaults.color_discrete_sequence = [
    COLOR_ACCENT,
    COLOR_ACCENT_2,
    COLOR_ACCENT_3,
    COLOR_ACCENT_4,
]


def format_number(value: Optional[float]) -> str:
    """Format large numbers with suffixes."""
    if value is None:
        return "0"
    value = float(value)
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.0f}"


def format_currency(value: Optional[float], currency: str) -> str:
    """Format currency values for display."""
    if value is None:
        return f"0 {currency}"
    return f"{value:,.2f} {currency}"


def format_percent(value: Optional[float], decimals: int = 1) -> str:
    """Format a fraction as a percentage string."""
    if value is None:
        return "0%"
    return f"{float(value) * 100:.{decimals}f}%"


def format_optional_percent(value: Optional[float], decimals: int = 1) -> str:
    """Format a percentage when available."""
    if value is None:
        return "Unavailable"
    return format_percent(value, decimals)


def format_optional_score(value: Optional[float], decimals: int = 3) -> str:
    """Format a score when available."""
    if value is None:
        return "Unavailable"
    return f"{float(value):.{decimals}f}"


def format_optional_text(value: Optional[str], fallback: str = "Unavailable") -> str:
    """Format a text value when available."""
    return value if value else fallback


def weighted_average(values: pd.Series, weights: pd.Series) -> Optional[float]:
    """Compute a weighted average, returning None when unavailable."""
    if values.empty or weights.empty:
        return None
    total = float(weights.sum())
    if total == 0:
        return 0.0
    return float((values * weights).sum() / total)


def to_float(value: object) -> Optional[float]:
    """Convert a value to float when possible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def find_leakage_features(features: Iterable[str]) -> List[str]:
    """Return feature names that look like label leakage."""
    flagged = []
    for feature in features:
        feature_lower = feature.lower()
        if any(token in feature_lower for token in LEAKAGE_HINTS):
            flagged.append(feature)
    return flagged


@st.cache_data(show_spinner=False)
def load_json(path: PathLike) -> Dict[str, object]:
    """Load a JSON file if it exists."""
    if not path_exists(path):
        return {}
    if is_gcs_path(path):
        fs = get_gcs_fs()
        with fs.open(normalize_gcs_path(str(path)), "r") as handle:
            return json.load(handle)
    return json.loads(Path(path).read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def load_parquet(path: PathLike) -> pd.DataFrame:
    """Load a parquet file if it exists."""
    if not path_exists(path):
        return pd.DataFrame()
    return pd.read_parquet(str(path))


def parse_iso_date(value: Optional[str]) -> Optional[date]:
    """Parse an ISO date string to a date object."""
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def infer_date_bounds(
    kpis: Dict[str, object],
    data_sources: Iterable[Tuple[pd.DataFrame, str]],
) -> Tuple[date, date]:
    """Infer date bounds from available datasets."""
    min_dates = []
    max_dates = []

    for df, col in data_sources:
        if df.empty or col not in df.columns:
            continue
        series = pd.to_datetime(df[col], errors="coerce")
        if series.notna().any():
            min_dates.append(series.min().date())
            max_dates.append(series.max().date())

    min_kpi = parse_iso_date(kpis.get("min_event_date"))
    max_kpi = parse_iso_date(kpis.get("max_event_date"))
    if min_kpi:
        min_dates.append(min_kpi)
    if max_kpi:
        max_dates.append(max_kpi)

    today = date.today()
    min_date = min(min_dates) if min_dates else today
    max_date = max(max_dates) if max_dates else today
    if min_date > max_date:
        return max_date, min_date
    return min_date, max_date


def filter_by_date(
    df: pd.DataFrame, date_col: str, start: date, end: date
) -> pd.DataFrame:
    """Filter a DataFrame by a date range."""
    if df.empty or date_col not in df.columns:
        return df
    series = pd.to_datetime(df[date_col], errors="coerce")
    mask = (series.dt.date >= start) & (series.dt.date <= end)
    return df.loc[mask].copy()


def find_price_bounds(datasets: Iterable[pd.DataFrame]) -> Optional[Tuple[float, float]]:
    """Return global price bounds from available datasets."""
    min_prices = []
    max_prices = []
    for df in datasets:
        if df.empty:
            continue
        if "min_price" in df.columns:
            value = df["min_price"].min()
            if pd.notna(value):
                min_prices.append(float(value))
        if "max_price" in df.columns:
            value = df["max_price"].max()
            if pd.notna(value):
                max_prices.append(float(value))
        if "avg_price" in df.columns:
            value = df["avg_price"].min()
            if pd.notna(value):
                min_prices.append(float(value))
            value = df["avg_price"].max()
            if pd.notna(value):
                max_prices.append(float(value))
    if not min_prices or not max_prices:
        return None
    return min(min_prices), max(max_prices)


def filter_by_price(
    df: pd.DataFrame, price_min: float, price_max: float
) -> pd.DataFrame:
    """Filter a DataFrame using an average price range when present."""
    if df.empty:
        return df
    if "avg_price" in df.columns:
        mask = (df["avg_price"] >= price_min) & (df["avg_price"] <= price_max)
        return df.loc[mask].copy()
    return df


def safe_divide(numerator: float, denominator: float) -> float:
    """Safely divide two numeric values."""
    if denominator in (0, 0.0) or pd.isna(denominator):
        return 0.0
    return float(numerator) / float(denominator)


def style_figure(
    fig: go.Figure,
    title: str,
    x_title: Optional[str] = None,
    y_title: Optional[str] = None,
) -> go.Figure:
    """Apply consistent styling to Plotly figures."""
    fig.update_layout(
        title=dict(text=title, x=0, font=dict(family=FONT_DISPLAY, size=18)),
        font=dict(family=FONT_BODY, color=COLOR_TEXT, size=14),
        margin=dict(l=10, r=10, t=50, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hoverlabel=dict(font=dict(family=FONT_BODY, size=12)),
        autosize=True,
    )
    if x_title:
        fig.update_xaxes(title_text=x_title)
    if y_title:
        fig.update_yaxes(title_text=y_title)
    fig.update_xaxes(tickfont=dict(color=COLOR_TEXT, size=12))
    fig.update_yaxes(tickfont=dict(color=COLOR_TEXT, size=12))
    return fig


def build_highlights(
    funnel_daily: pd.DataFrame,
    purchases_by_hour: pd.DataFrame,
    top_categories: pd.DataFrame,
    conversion_by_weekday: pd.DataFrame,
) -> List[Tuple[str, str]]:
    """Create narrative highlights from aggregated data."""
    highlights: List[Tuple[str, str]] = []

    if not purchases_by_hour.empty:
        hourly = (
            purchases_by_hour.groupby("hour", as_index=False)["purchase_count"]
            .sum()
            .sort_values("purchase_count", ascending=False)
        )
        if not hourly.empty:
            peak_hour = int(hourly.iloc[0]["hour"])
            highlights.append(("Peak purchase hour", f"{peak_hour:02d}:00"))

    if not top_categories.empty:
        categories = (
            top_categories.groupby("category", as_index=False)["revenue"]
            .sum()
            .sort_values("revenue", ascending=False)
        )
        if not categories.empty:
            top_category = str(categories.iloc[0]["category"])
            highlights.append(("Top category", top_category))

    if not conversion_by_weekday.empty:
        weekday = (
            conversion_by_weekday.groupby("day_of_week", as_index=False)
            .agg({"sessions": "sum", "purchases": "sum"})
            .assign(rate=lambda df: df["purchases"] / df["sessions"].replace(0, pd.NA))
            .sort_values("rate", ascending=False)
        )
        if not weekday.empty:
            weekday_map = {
                1: "Sun",
                2: "Mon",
                3: "Tue",
                4: "Wed",
                5: "Thu",
                6: "Fri",
                7: "Sat",
            }
            best_day = weekday.iloc[0]
            day_label = weekday_map.get(int(best_day["day_of_week"]), "N/A")
            rate_value = float(best_day["rate"]) if pd.notna(best_day["rate"]) else 0.0
            highlights.append(("Best weekday", f"{day_label} ({format_percent(rate_value)})"))

    return highlights


def build_key_insights(
    actual_session_conversion: Optional[float],
    view_to_purchase_rate: float,
    filtered_revenue: float,
    filtered_purchases: float,
    avg_order_value: float,
    currency: str,
    highlights: List[Tuple[str, str]],
) -> List[str]:
    """Build key insights based on current filters."""
    insights: List[str] = []
    if actual_session_conversion is not None:
        insights.append(
            "Session conversion rate: "
            f"{format_percent(actual_session_conversion, 2)} of sessions purchase."
        )
    else:
        insights.append("Session conversion rate unavailable for current filters.")

    insights.append(
        "View-to-purchase rate: "
        f"{format_percent(view_to_purchase_rate, 2)} of product views convert."
    )

    insights.append(
        "Revenue: "
        f"{format_currency(filtered_revenue, currency)} from "
        f"{format_number(filtered_purchases)} purchases (AOV "
        f"{format_currency(avg_order_value, currency)})."
    )

    highlight_map = {label: value for label, value in highlights}
    if "Top category" in highlight_map:
        insights.append(f"Top category by revenue: {highlight_map['Top category']}.")
    if "Peak purchase hour" in highlight_map:
        insights.append(f"Peak purchase hour: {highlight_map['Peak purchase hour']}.")
    if "Best weekday" in highlight_map and len(insights) < 5:
        insights.append(f"Best weekday for conversion: {highlight_map['Best weekday']}.")

    while len(insights) < 5:
        insights.append("Expand the date range or clear filters for more insights.")

    return insights[:5]


def render_kpi_row(items: List[Tuple[str, str, str]], columns: int = 4) -> None:
    """Render KPI cards using Streamlit metrics."""
    row = st.columns(columns)
    for index, (label, value, detail) in enumerate(items):
        if index >= len(row):
            break
        with row[index]:
            st.metric(label, value)
            st.caption(detail)


def render_empty_state(message: str) -> None:
    """Render a friendly empty-state message."""
    st.info(message)


st.set_page_config(page_title="Clickstream Dashboard", page_icon="📊", layout="wide")

st.markdown(
    """
    <style>
    @import url("https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=IBM+Plex+Serif:wght@600;700&display=swap");

    :root {
        --bg: #ffffff;
        --text: #111111;
        --muted: #1f2937;
        --border: #e5e7eb;
        --card-shadow: 0 6px 16px rgba(17, 17, 17, 0.08);
    }

    html, body {
        font-size: 16px;
        color: var(--text);
    }

    .stApp {
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        color: var(--text);
        font-family: "IBM Plex Sans", sans-serif;
        line-height: 1.55;
    }

    h1, h2, h3, h4 {
        font-family: "IBM Plex Serif", serif;
        color: var(--text);
        letter-spacing: 0.2px;
    }

    [data-testid="stSidebar"] {
        background: #f3f4f6;
        border-right: 1px solid var(--border);
    }

    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] p {
        color: var(--text);
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: var(--text);
    }

    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] li,
    [data-testid="stMarkdownContainer"] span {
        color: var(--text);
        font-size: 1rem;
    }

    [data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 12px 14px;
        box-shadow: var(--card-shadow);
        animation: rise 0.4s ease-out both;
    }

    [data-testid="stMetricLabel"] {
        color: var(--muted);
        font-weight: 600;
    }

    [data-testid="stMetricValue"] {
        color: var(--text);
        font-size: 1.5rem;
        font-weight: 700;
    }

    [data-testid="stCaptionContainer"] p {
        color: var(--muted);
        font-size: 0.95rem;
        line-height: 1.5;
    }

    [data-testid="stAlert"] {
        border-radius: 12px;
        border: 1px solid var(--border);
        animation: rise 0.4s ease-out both;
    }

    [data-testid="stAlert"] p {
        color: var(--text);
    }

    [data-testid="stExpander"] summary {
        color: var(--text);
        font-weight: 600;
    }

    button[data-baseweb="tab"] {
        font-weight: 600;
    }

    [data-testid="stPlotlyChart"] {
        animation: rise 0.4s ease-out both;
    }

    @keyframes rise {
        from {
            opacity: 0;
            transform: translateY(6px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    @media (max-width: 900px) {
        [data-testid="stHorizontalBlock"] {
            flex-direction: column;
        }

        [data-testid="column"] {
            width: 100% !important;
            flex: 1 1 100% !important;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if not path_exists(DATA_DIR):
    st.error("Dashboard aggregates not found. Run make dashboard_data first.")
    st.stop()

reports_root = REPORTS_ROOT
kpis = load_json(data_path("kpis.json"))
model_metrics_path = data_path("model_metrics.json")
model_metrics = load_json(model_metrics_path)
metrics_fallback = reports_path("metrics.json")
if not model_metrics and path_exists(metrics_fallback):
    model_metrics = load_json(metrics_fallback)
model_metrics_available = bool(model_metrics)
propensity_drivers = load_json(data_path("propensity_drivers.json"))
propensity_kpis = load_json(data_path("propensity_kpis.json"))

funnel_daily = load_parquet(data_path("funnel_daily.parquet"))
purchases_by_hour = load_parquet(data_path("purchases_by_hour.parquet"))
conversion_by_weekday = load_parquet(data_path("conversion_by_weekday.parquet"))
top_categories = load_parquet(data_path("top_categories.parquet"))
top_brands = load_parquet(data_path("top_brands.parquet"))
session_distributions = load_parquet(data_path("session_distributions.parquet"))
propensity_daily = load_parquet(data_path("propensity_daily.parquet"))
propensity_hist = load_parquet(data_path("propensity_hist.parquet"))
propensity_threshold = load_parquet(data_path("propensity_threshold.parquet"))
calibration_bins = load_parquet(data_path("calibration_bins.parquet"))
threshold_curves = load_parquet(data_path("threshold_curves.parquet"))
propensity_daily_raw = propensity_daily.copy()

min_date, max_date = infer_date_bounds(
    kpis,
    [
        (funnel_daily, "event_date"),
        (purchases_by_hour, "event_date"),
        (conversion_by_weekday, "session_date"),
        (top_categories, "event_date"),
        (top_brands, "event_date"),
        (session_distributions, "session_date"),
        (propensity_daily, "date"),
    ],
)

with st.sidebar:
    st.header("Filters")
    quick_range = st.selectbox(
        "Date range",
        ["All time", "Last 7 days", "Last 30 days", "Custom"],
        index=0,
        help="Filter all charts by event date.",
    )

    if quick_range == "Custom":
        selected_range = st.date_input(
            "Custom range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Pick a start and end date for the dashboard.",
        )
        if isinstance(selected_range, tuple):
            start_date, end_date = selected_range
        else:
            start_date = end_date = selected_range
    else:
        end_date = max_date
        if quick_range == "Last 7 days":
            start_date = max(end_date - timedelta(days=6), min_date)
        elif quick_range == "Last 30 days":
            start_date = max(end_date - timedelta(days=29), min_date)
        else:
            start_date = min_date

    category_options = (
        sorted(top_categories["category"].dropna().unique())
        if not top_categories.empty and "category" in top_categories.columns
        else []
    )
    selected_categories = st.multiselect(
        "Category",
        category_options,
        help="Filter purchase-based charts by product category.",
    )

    price_bounds = find_price_bounds([top_categories, top_brands, purchases_by_hour])
    price_filter = None
    if price_bounds:
        price_min, price_max = price_bounds
        if price_min != price_max:
            price_filter = st.slider(
                "Average purchase price",
                min_value=float(price_min),
                max_value=float(price_max),
                value=(float(price_min), float(price_max)),
                help="Filter purchase-based charts by average purchase price.",
            )

    st.caption(f"Available data: {min_date.isoformat()} to {max_date.isoformat()}")

funnel_daily = filter_by_date(funnel_daily, "event_date", start_date, end_date)
purchases_by_hour = filter_by_date(
    purchases_by_hour, "event_date", start_date, end_date
)
conversion_by_weekday = filter_by_date(
    conversion_by_weekday, "session_date", start_date, end_date
)
top_categories = filter_by_date(top_categories, "event_date", start_date, end_date)
top_brands = filter_by_date(top_brands, "event_date", start_date, end_date)
session_distributions = filter_by_date(
    session_distributions, "session_date", start_date, end_date
)
propensity_daily = filter_by_date(propensity_daily, "date", start_date, end_date)

if selected_categories and not top_categories.empty:
    top_categories = top_categories.loc[
        top_categories["category"].isin(selected_categories)
    ].copy()

if price_filter:
    price_min, price_max = price_filter
    top_categories = filter_by_price(top_categories, price_min, price_max)
    top_brands = filter_by_price(top_brands, price_min, price_max)
    purchases_by_hour = filter_by_price(purchases_by_hour, price_min, price_max)

currency = str(kpis.get("currency", "USD"))

filtered_views = float(funnel_daily["views"].sum()) if not funnel_daily.empty else 0.0
filtered_carts = float(funnel_daily["carts"].sum()) if not funnel_daily.empty else 0.0
if not top_categories.empty:
    filtered_purchases = float(top_categories["purchase_count"].sum())
elif selected_categories:
    filtered_purchases = 0.0
elif price_filter and not purchases_by_hour.empty:
    filtered_purchases = float(purchases_by_hour["purchase_count"].sum())
else:
    filtered_purchases = (
        float(funnel_daily["purchases"].sum()) if not funnel_daily.empty else 0.0
    )
filtered_events = filtered_views + filtered_carts + filtered_purchases
filtered_sessions = (
    float(funnel_daily["sessions"].sum()) if not funnel_daily.empty else 0.0
)
if not top_categories.empty:
    filtered_revenue = float(top_categories["revenue"].sum())
elif selected_categories:
    filtered_revenue = 0.0
else:
    filtered_revenue = (
        float(purchases_by_hour["revenue"].sum())
        if not purchases_by_hour.empty
        else 0.0
    )
filtered_avg_order = safe_divide(filtered_revenue, filtered_purchases)
filtered_conversion = safe_divide(filtered_purchases, filtered_views)

propensity_required_cols = {"mean_pred_prob", "sessions", "purchases"}
propensity_daily_has_data = (
    not propensity_daily_raw.empty
    and propensity_required_cols.issubset(propensity_daily_raw.columns)
)
propensity_daily_filtered_has_data = (
    not propensity_daily.empty
    and propensity_required_cols.issubset(propensity_daily.columns)
)
propensity_kpis_available = bool(propensity_kpis) and bool(
    propensity_kpis.get("available")
)
propensity_ready = propensity_kpis_available or propensity_daily_has_data

predicted_conversion = None
actual_session_conversion = None
if propensity_daily_filtered_has_data:
    propensity_daily["sessions"] = pd.to_numeric(
        propensity_daily["sessions"], errors="coerce"
    ).fillna(0)
    propensity_daily["purchases"] = pd.to_numeric(
        propensity_daily["purchases"], errors="coerce"
    ).fillna(0)
    propensity_daily["mean_pred_prob"] = pd.to_numeric(
        propensity_daily["mean_pred_prob"], errors="coerce"
    ).fillna(0)
    propensity_sessions = float(propensity_daily["sessions"].sum())
    predicted_conversion = weighted_average(
        propensity_daily["mean_pred_prob"], propensity_daily["sessions"]
    )
    actual_session_conversion = safe_divide(
        float(propensity_daily["purchases"].sum()), propensity_sessions
    )
elif propensity_kpis_available:
    predicted_conversion = to_float(propensity_kpis.get("predicted_conversion_rate"))
    actual_session_conversion = to_float(
        propensity_kpis.get("actual_session_conversion_rate")
    )
else:
    actual_session_conversion = safe_divide(filtered_purchases, filtered_sessions)

calibration_gap = None
if predicted_conversion is not None and actual_session_conversion is not None:
    calibration_gap = predicted_conversion - actual_session_conversion
elif propensity_kpis_available:
    calibration_gap = to_float(propensity_kpis.get("calibration_gap"))

threshold_curves_available = (
    not threshold_curves.empty
    and {"threshold", "precision", "recall", "f1", "pct_flagged"}.issubset(
        threshold_curves.columns
    )
)
base_rate = actual_session_conversion if actual_session_conversion is not None else 0.1
if base_rate <= 0:
    base_rate = 0.1
default_threshold = min(max(float(base_rate), 0.01), 0.99)
selected_threshold = float(
    st.session_state.get("propensity_threshold", default_threshold)
)

threshold_value = None
high_propensity_share = to_float(propensity_kpis.get("high_propensity_share"))
precision_at_threshold = None
recall_at_threshold = None
f1_at_threshold = None

if threshold_curves_available:
    threshold_curves = threshold_curves.copy()
    threshold_curves["threshold"] = pd.to_numeric(
        threshold_curves["threshold"], errors="coerce"
    )
    threshold_curves = threshold_curves.dropna(subset=["threshold"]).sort_values(
        "threshold"
    )
    if not threshold_curves.empty:
        row = threshold_curves.iloc[
            (threshold_curves["threshold"] - selected_threshold).abs().idxmin()
        ]
        threshold_value = float(row["threshold"])
        high_propensity_share = to_float(row.get("pct_flagged"))
        precision_at_threshold = to_float(row.get("precision"))
        recall_at_threshold = to_float(row.get("recall"))
        f1_at_threshold = to_float(row.get("f1"))

if threshold_value is None:
    threshold_value = to_float(propensity_kpis.get("threshold")) or 0.5
    if high_propensity_share is None and not propensity_threshold.empty:
        threshold_df = propensity_threshold.copy()
        threshold_df["threshold"] = pd.to_numeric(
            threshold_df["threshold"], errors="coerce"
        )
        threshold_df = threshold_df.dropna(subset=["threshold"])
        if not threshold_df.empty:
            row = threshold_df.iloc[(threshold_df["threshold"] - 0.5).abs().idxmin()]
            threshold_value = float(row["threshold"])
            high_propensity_share = to_float(row.get("pct_sessions_above"))

precision_metric = to_float(model_metrics.get("precision"))
recall_metric = to_float(model_metrics.get("recall"))
auc_metric = to_float(model_metrics.get("auc"))
f1_metric = to_float(model_metrics.get("f1"))
if f1_metric is None and precision_metric is not None and recall_metric is not None:
    f1_metric = (
        2 * precision_metric * recall_metric / (precision_metric + recall_metric)
        if (precision_metric + recall_metric)
        else None
    )

if propensity_ready:
    dashboard_title = "Session Purchase Propensity Dashboard"
    dashboard_subtitle = (
        "Predicts how likely a user session is to end in a purchase, based on "
        "clickstream behavior."
    )
    dashboard_intro = (
        "What this dashboard answers: Conversion funnel, propensity trends, "
        "top categories/brands, session behavior, and model performance."
    )
else:
    dashboard_title = "Conversion Analytics + Model Performance Dashboard"
    dashboard_subtitle = (
        "Explore conversion behavior and model performance. Train the model to "
        "unlock propensity insights."
    )
    dashboard_intro = (
        "What this dashboard answers: Conversion funnel, time trends, "
        "top categories/brands, session behavior, and model performance."
    )

st.title(dashboard_title)
st.caption(dashboard_subtitle)

st.info(dashboard_intro)
if not propensity_ready:
    st.warning(
        "Run make train && make evaluate && make dashboard_data to enable "
        "propensity insights."
    )

filter_summary = (
    f"Active view: {start_date.isoformat()} to {end_date.isoformat()} | "
    f"Data range: {min_date.isoformat()} to {max_date.isoformat()}"
)
if selected_categories:
    filter_summary += f" | Categories: {len(selected_categories)} selected"
if price_filter:
    filter_summary += f" | Avg price: {price_min:.2f} to {price_max:.2f} {currency}"

st.caption(filter_summary)

if propensity_ready:
    st.subheader("Propensity KPIs")

    propensity_scope_label = (
        "selected dates" if propensity_daily_filtered_has_data else "all scored sessions"
    )
    if predicted_conversion is None and actual_session_conversion is None:
        render_empty_state("No propensity KPIs available for the selected range.")
    else:
        render_kpi_row(
            [
                (
                    "Session conversion rate (purchases / sessions)",
                    format_optional_percent(actual_session_conversion, 2),
                    f"Purchases divided by sessions within {propensity_scope_label}.",
                ),
                (
                    "Predicted conversion rate",
                    format_optional_percent(predicted_conversion, 2),
                    f"Mean predicted probability across {propensity_scope_label}.",
                ),
                (
                    "Mean calibration gap (mean predicted - actual)",
                    format_optional_percent(calibration_gap, 3),
                    "Positive means over-predicting; negative means under-predicting.",
                ),
                (
                    "High-propensity sessions share",
                    format_optional_percent(high_propensity_share, 2),
                    f"Sessions with predicted probability >= {threshold_value:.2f}.",
                ),
            ]
        )

        render_kpi_row(
            [
                (
                    "Precision",
                    format_optional_percent(precision_metric, 1),
                    "Test-set precision.",
                ),
                ("Recall", format_optional_percent(recall_metric, 1), "Test-set recall."),
                ("F1 score", format_optional_percent(f1_metric, 1), "Test-set balance."),
                ("AUC", format_optional_score(auc_metric, 3), "Test-set ROC area."),
            ]
        )

st.subheader("Behavior KPIs")

render_kpi_row(
    [
        ("Total events", format_number(filtered_events), "Based on active dates"),
        ("Sessions", format_number(filtered_sessions), "Based on active dates"),
        ("Purchases", format_number(filtered_purchases), "Filtered by category/price"),
        (
            "Revenue",
            format_currency(filtered_revenue, currency),
            "Filtered by category/price",
        ),
    ]
)
render_kpi_row(
    [
        (
            "Avg order value",
            format_currency(filtered_avg_order, currency),
            "Filtered by category/price",
        ),
        (
            "View-to-purchase rate (purchases / views)",
            format_percent(filtered_conversion, 2),
            "Purchases divided by views.",
        ),
        ("Total users", format_number(kpis.get("total_users")), "Export range"),
        (
            "Avg session duration",
            f"{float(kpis.get('avg_session_duration_seconds') or 0):.0f} sec",
            "Export range",
        ),
    ]
)

highlights = build_highlights(
    funnel_daily, purchases_by_hour, top_categories, conversion_by_weekday
)
if highlights:
    st.subheader("Highlights")
    highlight_cols = st.columns(min(4, len(highlights)))
    for col, (label, value) in zip(highlight_cols, highlights):
        with col:
            st.metric(label, value)

key_insights = build_key_insights(
    actual_session_conversion,
    filtered_conversion,
    filtered_revenue,
    filtered_purchases,
    filtered_avg_order,
    currency,
    highlights,
)
st.subheader("Key insights (current filters)")
st.info("\n".join(f"- {insight}" for insight in key_insights))

if all(
    df.empty
    for df in [
        funnel_daily,
        purchases_by_hour,
        conversion_by_weekday,
        top_categories,
        top_brands,
        session_distributions,
        propensity_daily,
    ]
):
    render_empty_state(
        "No data matches the current filters. Try expanding the date range or clearing filters."
    )

overview_tab, commerce_tab, sessions_tab, model_tab = st.tabs(
    ["Overview", "Commerce", "Sessions", "Model"]
)

with overview_tab:
    st.subheader("Purpose")
    if propensity_ready:
        st.markdown("### Session Purchase Propensity Dashboard")
        st.markdown(
            "Predicts the likelihood that a user session ends in a purchase, "
            "based on clickstream behavior."
        )
        st.caption(
            "Session = a user's activity window. Propensity = predicted probability. "
            "Conversion = the session ends with a purchase."
        )
        so_what = (
            "So what: Compare predicted propensity with actual conversion to understand "
            "demand and model quality."
        )
        how_to_use = (
            "- Filter date/category/price\n"
            "- Explore funnel + trends\n"
            "- Review model performance + propensity drivers"
        )
    else:
        st.markdown("### Conversion Analytics Dashboard")
        st.markdown(
            "Tracks how sessions convert to purchases and where revenue concentrates."
        )
        st.caption(
            "Session = a user's activity window. Conversion = the session ends with a "
            "purchase."
        )
        so_what = (
            "So what: Monitor conversion health and revenue drivers before deploying "
            "a propensity model."
        )
        how_to_use = (
            "- Filter date/category/price\n"
            "- Explore funnel + trends\n"
            "- Run training/evaluation to unlock propensity insights"
        )

    with st.expander("How to use this dashboard"):
        st.markdown(how_to_use)

    st.caption(so_what)

    if propensity_ready:
        st.subheader("Propensity over time")
        if propensity_daily.empty or "mean_pred_prob" not in propensity_daily.columns:
            render_empty_state("No propensity data for the selected range.")
        else:
            propensity_time = propensity_daily.copy()
            propensity_time["mean_pred_prob"] = pd.to_numeric(
                propensity_time["mean_pred_prob"], errors="coerce"
            ).fillna(0)
            if "actual_session_conversion_rate" not in propensity_time.columns and {
                "sessions",
                "purchases",
            }.issubset(propensity_time.columns):
                propensity_time["actual_session_conversion_rate"] = propensity_time.apply(
                    lambda row: safe_divide(row["purchases"], row["sessions"]), axis=1
                )
            if "actual_session_conversion_rate" in propensity_time.columns:
                propensity_time["actual_session_conversion_rate"] = pd.to_numeric(
                    propensity_time["actual_session_conversion_rate"], errors="coerce"
                ).fillna(0)
            propensity_time["date"] = pd.to_datetime(
                propensity_time["date"], errors="coerce"
            )
            propensity_time = propensity_time.sort_values("date")

            propensity_fig = go.Figure()
            propensity_fig.add_trace(
                go.Scatter(
                    x=propensity_time["date"],
                    y=propensity_time["mean_pred_prob"],
                    mode="lines+markers",
                    name="Predicted propensity",
                    line=dict(color=COLOR_ACCENT, width=3),
                    hovertemplate=(
                        "%{x|%b %d, %Y}<br>Predicted: %{y:.1%}<extra></extra>"
                    ),
                )
            )
            propensity_fig.add_trace(
                go.Scatter(
                    x=propensity_time["date"],
                    y=propensity_time["actual_session_conversion_rate"],
                    mode="lines+markers",
                    name="Actual conversion",
                    line=dict(color=COLOR_ACCENT_3, width=3),
                    hovertemplate="%{x|%b %d, %Y}<br>Actual: %{y:.1%}<extra></extra>",
                )
            )
            propensity_fig.update_yaxes(tickformat=".0%")
            st.plotly_chart(
                style_figure(
                    propensity_fig,
                    "Predicted propensity vs actual conversion",
                    x_title="Date",
                    y_title="Rate (%)",
                ),
                use_container_width=True,
            )

        st.subheader("Decision threshold")
        if threshold_curves_available and not threshold_curves.empty:
            selected_threshold = st.slider(
                "Propensity threshold",
                min_value=0.0,
                max_value=1.0,
                value=float(selected_threshold),
                step=0.01,
                help=(
                    "Adjust the predicted probability cutoff used to flag "
                    "high-propensity sessions."
                ),
                key="propensity_threshold",
            )
            st.caption(
                "Threshold metrics are computed from scored sessions across all dates "
                "using aggregated thresholds."
            )
            render_kpi_row(
                [
                    (
                        "Precision @ threshold",
                        format_optional_percent(precision_at_threshold, 2),
                        "Share of flagged sessions that convert.",
                    ),
                    (
                        "Recall @ threshold",
                        format_optional_percent(recall_at_threshold, 2),
                        "Share of conversions captured.",
                    ),
                    (
                        "F1 @ threshold",
                        format_optional_percent(f1_at_threshold, 2),
                        "Balance of precision/recall.",
                    ),
                    (
                        "Sessions flagged",
                        format_optional_percent(high_propensity_share, 2),
                        "Pct of sessions above the threshold.",
                    ),
                ],
                columns=4,
            )

            curve_df = threshold_curves.copy()
            curve_df["threshold"] = pd.to_numeric(
                curve_df["threshold"], errors="coerce"
            )
            curve_df = curve_df.dropna(subset=["threshold"]).sort_values("threshold")
            curve_df["precision"] = pd.to_numeric(
                curve_df["precision"], errors="coerce"
            )
            curve_df["recall"] = pd.to_numeric(curve_df["recall"], errors="coerce")
            curve_df["f1"] = pd.to_numeric(curve_df["f1"], errors="coerce")
            pr_fig = px.line(
                curve_df,
                x="threshold",
                y=["precision", "recall", "f1"],
                labels={"value": "Score", "threshold": "Threshold"},
                color_discrete_sequence=[
                    COLOR_ACCENT,
                    COLOR_ACCENT_3,
                    COLOR_ACCENT_2,
                ],
            )
            pr_fig.update_traces(
                hovertemplate="Threshold %{x:.2f}<br>%{y:.2f}<extra></extra>"
            )
            pr_fig.update_yaxes(range=[0, 1])
            st.plotly_chart(
                style_figure(
                    pr_fig,
                    "Threshold vs precision/recall",
                    x_title="Threshold",
                    y_title="Score",
                ),
                use_container_width=True,
            )
        else:
            render_empty_state(
                "Threshold curve unavailable. Run make evaluate / dashboard_data."
            )

        st.subheader("Predicted propensity distribution")
        if propensity_hist.empty or "sessions_count" not in propensity_hist.columns:
            render_empty_state("No propensity distribution for the selected range.")
        else:
            hist_df = propensity_hist.copy()
            hist_df["sessions_count"] = pd.to_numeric(
                hist_df["sessions_count"], errors="coerce"
            ).fillna(0)
            if "probability_bin" in hist_df.columns:
                hist_df["bucket_label"] = hist_df["probability_bin"].astype(str)
            else:
                if not {"bucket_low", "bucket_high"}.issubset(hist_df.columns):
                    render_empty_state(
                        "Propensity distribution missing bucket ranges."
                    )
                    hist_df = pd.DataFrame()
                else:
                    hist_df["bucket_low"] = pd.to_numeric(
                        hist_df["bucket_low"], errors="coerce"
                    ).fillna(0)
                    hist_df["bucket_high"] = pd.to_numeric(
                        hist_df["bucket_high"], errors="coerce"
                    ).fillna(0)
                    hist_df["bucket_label"] = hist_df.apply(
                        lambda row: f"{row['bucket_low']:.1f}-{row['bucket_high']:.1f}",
                        axis=1,
                    )
            if not hist_df.empty and "bucket_low" in hist_df.columns:
                hist_df = hist_df.sort_values("bucket_low")
            if not hist_df.empty:
                hist_fig = px.bar(
                    hist_df,
                    x="bucket_label",
                    y="sessions_count",
                    labels={
                        "bucket_label": "Predicted probability",
                        "sessions_count": "Sessions",
                    },
                    color_discrete_sequence=[COLOR_ACCENT_2],
                )
                hist_fig.update_traces(
                    hovertemplate="Prob %{x}<br>Sessions: %{y:,.0f}<extra></extra>"
                )
                st.plotly_chart(
                    style_figure(
                        hist_fig,
                        "Predicted propensity distribution",
                        x_title="Predicted probability",
                        y_title="Sessions",
                    ),
                    use_container_width=True,
                )

        st.subheader("Calibration reliability")
        if calibration_bins.empty or not {
            "bin_low",
            "bin_high",
            "mean_pred_prob",
            "actual_rate",
            "sessions",
        }.issubset(calibration_bins.columns):
            render_empty_state(
                "Calibration bins unavailable. Run make evaluate / dashboard_data."
            )
        else:
            calib_df = calibration_bins.copy()
            calib_df["bin_low"] = pd.to_numeric(
                calib_df["bin_low"], errors="coerce"
            )
            calib_df["bin_high"] = pd.to_numeric(
                calib_df["bin_high"], errors="coerce"
            )
            calib_df["mean_pred_prob"] = pd.to_numeric(
                calib_df["mean_pred_prob"], errors="coerce"
            )
            calib_df["actual_rate"] = pd.to_numeric(
                calib_df["actual_rate"], errors="coerce"
            )
            calib_df["sessions"] = pd.to_numeric(
                calib_df["sessions"], errors="coerce"
            ).fillna(0)
            calib_df = calib_df.dropna(
                subset=["mean_pred_prob", "actual_rate", "bin_low", "bin_high"]
            )
            calib_df["bin_label"] = calib_df.apply(
                lambda row: f"{row['bin_low']:.2f}-{row['bin_high']:.2f}",
                axis=1,
            )

            if calib_df.empty:
                render_empty_state("Calibration bins unavailable for this selection.")
            else:
                calib_fig = go.Figure()
                max_sessions = calib_df["sessions"].max()
                marker_sizes = (
                    (calib_df["sessions"] / max_sessions * 12)
                    if max_sessions
                    else pd.Series([6] * len(calib_df))
                )
                calib_fig.add_trace(
                    go.Scatter(
                        x=calib_df["mean_pred_prob"],
                        y=calib_df["actual_rate"],
                        mode="lines+markers",
                        name="Calibration",
                        marker=dict(
                            size=marker_sizes.fillna(6).clip(lower=6, upper=16),
                            color=COLOR_ACCENT,
                        ),
                        hovertemplate=(
                            "Bin %{text}<br>Predicted: %{x:.2%}<br>"
                            "Actual: %{y:.2%}<br>Sessions: %{customdata:,.0f}"
                            "<extra></extra>"
                        ),
                        text=calib_df["bin_label"],
                        customdata=calib_df["sessions"],
                    )
                )
                calib_fig.add_trace(
                    go.Scatter(
                        x=[0, 1],
                        y=[0, 1],
                        mode="lines",
                        name="Perfect calibration",
                        line=dict(color=COLOR_MUTED, dash="dash"),
                        hoverinfo="skip",
                    )
                )
                calib_fig.update_xaxes(range=[0, 1], tickformat=".0%")
                calib_fig.update_yaxes(range=[0, 1], tickformat=".0%")
                st.plotly_chart(
                    style_figure(
                        calib_fig,
                        "Calibration: predicted vs actual",
                        x_title="Mean predicted probability",
                        y_title="Actual conversion rate",
                    ),
                    use_container_width=True,
                )

        st.subheader("Top propensity drivers")
        driver_source = propensity_drivers.get("source")
        score_label = propensity_drivers.get("score_label", "weight").replace("_", " ")
        source_label = {
            "model_coefficients": "model coefficients",
            "feature_correlation": "feature correlations",
        }.get(driver_source, "scores")
        positive_drivers = propensity_drivers.get("positive", [])
        negative_drivers = propensity_drivers.get("negative", [])

        if not positive_drivers and not negative_drivers:
            render_empty_state("Driver coefficients unavailable for this model.")
        else:
            if driver_source != "model_coefficients":
                st.info(
                    "Model coefficients unavailable; showing proxy correlations instead."
                )
            st.caption(
                f"Drivers based on {source_label} ({score_label}). Positive drivers raise "
                "propensity; negative drivers lower it."
            )
            pos_df = pd.DataFrame(positive_drivers)
            neg_df = pd.DataFrame(negative_drivers)
            col_pos, col_neg = st.columns(2)
            with col_pos:
                st.markdown("**Positive drivers**")
                if pos_df.empty:
                    st.caption("No positive drivers available.")
                else:
                    score_col = "weight" if "weight" in pos_df.columns else "score"
                    if "description" not in pos_df.columns:
                        pos_df["description"] = "Derived session feature."
                    pos_df = pos_df.sort_values(score_col, ascending=False).head(8)
                    st.dataframe(
                        pos_df.rename(
                            columns={
                                "feature": "Feature",
                                score_col: score_label.title(),
                                "description": "Description",
                            }
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )
            with col_neg:
                st.markdown("**Negative drivers**")
                if neg_df.empty:
                    st.caption("No negative drivers available.")
                else:
                    score_col = "weight" if "weight" in neg_df.columns else "score"
                    if "description" not in neg_df.columns:
                        neg_df["description"] = "Derived session feature."
                    neg_df = neg_df.sort_values(score_col).head(8)
                    st.dataframe(
                        neg_df.rename(
                            columns={
                                "feature": "Feature",
                                score_col: score_label.title(),
                                "description": "Description",
                            }
                        ),
                        use_container_width=True,
                        hide_index=True,
                    )

    st.subheader("Conversion funnel")
    if funnel_daily.empty:
        render_empty_state("No funnel data available for the selected range.")
    else:
        funnel_totals = {
            "View": float(funnel_daily["views"].sum()),
            "Cart": float(funnel_daily["carts"].sum()),
            "Purchase": float(funnel_daily["purchases"].sum()),
        }
        funnel_df = pd.DataFrame(
            {"Stage": list(funnel_totals.keys()), "Count": list(funnel_totals.values())}
        )
        funnel_fig = go.Figure(
            go.Funnel(
                y=funnel_df["Stage"],
                x=funnel_df["Count"],
                textinfo="value+percent initial",
                marker=dict(color=[COLOR_ACCENT, COLOR_ACCENT_2, COLOR_ACCENT_3]),
                hovertemplate="Stage: %{y}<br>Count: %{x:,.0f}<extra></extra>",
            )
        )
        st.plotly_chart(
            style_figure(funnel_fig, "Conversion funnel", y_title="Stage", x_title="Count"),
            use_container_width=True,
        )

    st.subheader("Daily activity")
    if funnel_daily.empty:
        render_empty_state("No daily activity data available for the selected range.")
    else:
        daily_fig = px.area(
            funnel_daily.sort_values("event_date"),
            x="event_date",
            y=["views", "carts", "purchases"],
            labels={"value": "Events", "event_date": "Date"},
            color_discrete_sequence=[COLOR_ACCENT, COLOR_ACCENT_4, COLOR_ACCENT_2],
        )
        daily_fig.update_layout(hovermode="x unified")
        daily_fig.for_each_trace(
            lambda trace: trace.update(
                hovertemplate="%{x|%b %d, %Y}<br>%{y:,.0f} events<extra>%{fullData.name}</extra>"
            )
        )
        st.plotly_chart(
            style_figure(daily_fig, "Daily activity", x_title="Date", y_title="Events"),
            use_container_width=True,
        )

    st.subheader("Conversion by weekday")
    if conversion_by_weekday.empty:
        render_empty_state("No conversion data available for the selected range.")
    else:
        weekday_map = {
            1: "Sun",
            2: "Mon",
            3: "Tue",
            4: "Wed",
            5: "Thu",
            6: "Fri",
            7: "Sat",
        }
        weekday = (
            conversion_by_weekday.groupby("day_of_week", as_index=False)
            .agg({"sessions": "sum", "purchases": "sum"})
            .assign(
                conversion_rate=lambda df: (
                    df["purchases"] / df["sessions"].replace(0, pd.NA)
                ).fillna(0),
                weekday=lambda df: df["day_of_week"].map(weekday_map),
            )
            .sort_values("day_of_week")
        )
        weekday_fig = px.bar(
            weekday,
            x="weekday",
            y="conversion_rate",
            labels={"weekday": "Day", "conversion_rate": "Conversion rate"},
            color_discrete_sequence=[COLOR_ACCENT],
        )
        weekday_fig.update_yaxes(tickformat=".0%")
        weekday_fig.update_traces(
            hovertemplate="%{x}<br>Conversion: %{y:.1%}<extra></extra>"
        )
        st.plotly_chart(
            style_figure(
                weekday_fig, "Conversion by weekday", x_title="Day", y_title="Rate (%)"
            ),
            use_container_width=True,
        )

with commerce_tab:
    st.caption("So what: Identify revenue drivers and when customers buy.")

    col_left, col_right = st.columns(2)
    with col_left:
        if top_categories.empty:
            render_empty_state("No category data available for the selected range.")
        else:
            categories = (
                top_categories.groupby("category", as_index=False)
                .agg({"purchase_count": "sum", "revenue": "sum", "avg_price": "mean"})
                .sort_values("revenue", ascending=False)
                .head(10)
            )
            cat_fig = px.bar(
                categories,
                y="category",
                x="revenue",
                orientation="h",
                labels={"revenue": f"Revenue ({currency})", "category": "Category"},
                color_discrete_sequence=[COLOR_ACCENT],
            )
            cat_fig.update_traces(
                hovertemplate=f"%{{y}}<br>Revenue: %{{x:,.2f}} {currency}<extra></extra>"
            )
            st.plotly_chart(
                style_figure(
                    cat_fig,
                    "Top categories",
                    x_title=f"Revenue ({currency})",
                    y_title="Category",
                ),
                use_container_width=True,
            )

    with col_right:
        if top_brands.empty:
            render_empty_state("No brand data available for the selected range.")
        else:
            brands = (
                top_brands.groupby("brand", as_index=False)
                .agg({"purchase_count": "sum", "revenue": "sum", "avg_price": "mean"})
                .sort_values("revenue", ascending=False)
                .head(10)
            )
            brand_fig = px.bar(
                brands,
                y="brand",
                x="revenue",
                orientation="h",
                labels={"revenue": f"Revenue ({currency})", "brand": "Brand"},
                color_discrete_sequence=[COLOR_ACCENT_3],
            )
            brand_fig.update_traces(
                hovertemplate=f"%{{y}}<br>Revenue: %{{x:,.2f}} {currency}<extra></extra>"
            )
            st.plotly_chart(
                style_figure(
                    brand_fig,
                    "Top brands",
                    x_title=f"Revenue ({currency})",
                    y_title="Brand",
                ),
                use_container_width=True,
            )

    if purchases_by_hour.empty:
        render_empty_state("No purchase timing data available for the selected range.")
    else:
        hourly = (
            purchases_by_hour.groupby("hour", as_index=False)
            .agg({"purchase_count": "sum", "revenue": "sum"})
            .sort_values("hour")
        )
        hourly_fig = px.bar(
            hourly,
            x="hour",
            y="purchase_count",
            labels={"hour": "Hour of day", "purchase_count": "Purchases"},
            color_discrete_sequence=[COLOR_ACCENT_2],
        )
        hourly_fig.update_traces(
            hovertemplate="Hour %{x}:00<br>Purchases: %{y:,.0f}<extra></extra>"
        )
        st.plotly_chart(
            style_figure(
                hourly_fig,
                "Purchases by hour",
                x_title="Hour of day",
                y_title="Purchases",
            ),
            use_container_width=True,
        )

with sessions_tab:
    st.caption("So what: Understand session depth and behavior distributions.")

    if session_distributions.empty:
        render_empty_state("No session distribution data available for the selected range.")
    else:
        metrics = sorted(session_distributions["metric"].unique())
        selected_metric = st.selectbox(
            "Session metric",
            metrics,
            help="Choose which session metric to visualize.",
        )
        dist = session_distributions.loc[
            session_distributions["metric"] == selected_metric
        ].copy()
        dist = (
            dist.groupby(["bin_start", "bin_end"], as_index=False)["count"]
            .sum()
            .sort_values("bin_start")
        )
        dist["bin_label"] = dist.apply(
            lambda row: f"{row['bin_start']:.0f}-{row['bin_end']:.0f}", axis=1
        )
        dist_fig = px.bar(
            dist,
            x="bin_label",
            y="count",
            labels={"bin_label": "Bin", "count": "Sessions"},
            color_discrete_sequence=[COLOR_ACCENT_4],
        )
        dist_fig.update_traces(
            hovertemplate="Bin %{x}<br>Sessions: %{y:,.0f}<extra></extra>"
        )
        dist_fig.update_layout(xaxis_tickangle=-30)
        st.plotly_chart(
            style_figure(
                dist_fig,
                "Session distribution",
                x_title="Bin",
                y_title="Sessions",
            ),
            use_container_width=True,
        )

    st.markdown(
        "Use this view to compare how session behavior shifts across the selected date range."
    )

with model_tab:
    st.caption("So what: Track predictive quality and classification behavior.")

    if not model_metrics_available:
        render_empty_state("Model metrics unavailable. Run make evaluate.")
    else:
        metric_keys = ["auc", "accuracy", "precision", "recall", "f1"]
        metric_values = [model_metrics.get(key) for key in metric_keys]
        metrics_df = pd.DataFrame(
            {"metric": metric_keys, "value": metric_values}
        ).dropna()

        if not metrics_df.empty:
            metrics_fig = px.bar(
                metrics_df,
                x="metric",
                y="value",
                labels={"metric": "Metric", "value": "Score"},
                color_discrete_sequence=[COLOR_ACCENT_3],
            )
            metrics_fig.update_yaxes(range=[0, 1])
            metrics_fig.update_traces(
                hovertemplate="%{x}<br>Score: %{y:.3f}<extra></extra>"
            )
            st.plotly_chart(
                style_figure(
                    metrics_fig, "Model scores", x_title="Metric", y_title="Score"
                ),
                use_container_width=True,
            )

        st.caption("Metrics shown are computed on the test set only.")

        st.subheader("Model validity checks")
        split_method = format_optional_text(
            model_metrics.get("split_method"), "Unknown (add in evaluate pipeline)"
        )
        pct_positive_train = to_float(model_metrics.get("pct_positive_train"))
        pct_positive_test = to_float(model_metrics.get("pct_positive_test"))

        render_kpi_row(
            [
                ("Split method", split_method, "Evaluation split strategy."),
                (
                    "Train positive rate",
                    format_optional_percent(pct_positive_train, 2),
                    "Purchase sessions in training set.",
                ),
                (
                    "Test positive rate",
                    format_optional_percent(pct_positive_test, 2),
                    "Purchase sessions in test set.",
                ),
            ],
            columns=3,
        )

        train_start = model_metrics.get("train_start")
        train_end = model_metrics.get("train_end")
        test_start = model_metrics.get("test_start")
        test_end = model_metrics.get("test_end")
        if train_start or train_end or test_start or test_end:
            st.caption(
                "Train window: "
                f"{format_optional_text(train_start, 'unknown')} to "
                f"{format_optional_text(train_end, 'unknown')} | Test window: "
                f"{format_optional_text(test_start, 'unknown')} to "
                f"{format_optional_text(test_end, 'unknown')}"
            )
        else:
            st.caption("Train/test date windows unavailable.")

        if model_metrics.get("suspiciously_perfect_metrics"):
            st.warning(
                "Suspiciously perfect metrics detected. Check for leakage or "
                "overlapping train/test windows."
            )

        features_used = model_metrics.get("features_used") or []
        leakage_features = find_leakage_features(features_used)
        if leakage_features:
            st.error("Possible label leakage: feature encodes purchase outcome.")
            st.caption("Flagged features: " + ", ".join(leakage_features))
        else:
            st.success("No leakage-prone feature names detected.")

        with st.expander("Features used in training"):
            if features_used:
                st.dataframe(
                    pd.DataFrame({"feature": features_used}),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.caption("Feature list unavailable. Add features_used to metrics.json.")

        roc_curve = model_metrics.get("roc_curve")
        if isinstance(roc_curve, dict) and {"fpr", "tpr"}.issubset(roc_curve.keys()):
            roc_df = pd.DataFrame({"fpr": roc_curve["fpr"], "tpr": roc_curve["tpr"]})
            roc_fig = px.line(
                roc_df,
                x="fpr",
                y="tpr",
                labels={"fpr": "False positive rate", "tpr": "True positive rate"},
                color_discrete_sequence=[COLOR_ACCENT],
            )
            roc_fig.add_shape(
                type="line",
                x0=0,
                y0=0,
                x1=1,
                y1=1,
                line=dict(dash="dot", color=COLOR_MUTED),
            )
            roc_fig.update_traces(
                hovertemplate="FPR %{x:.3f}<br>TPR %{y:.3f}<extra></extra>"
            )
            st.plotly_chart(
                style_figure(
                    roc_fig,
                    "ROC curve",
                    x_title="False positive rate",
                    y_title="True positive rate",
                ),
                use_container_width=True,
            )

        confusion = model_metrics.get("confusion_matrix")
        cm_data = None
        if isinstance(confusion, dict) and {"tn", "fp", "fn", "tp"}.issubset(
            confusion.keys()
        ):
            cm_data = [
                [confusion["tn"], confusion["fp"]],
                [confusion["fn"], confusion["tp"]],
            ]
        elif isinstance(confusion, list) and len(confusion) == 2:
            cm_data = confusion

        if cm_data is not None:
            cm_fig = px.imshow(
                cm_data,
                text_auto=True,
                color_continuous_scale="Blues",
                labels=dict(x="Predicted", y="Actual", color="Count"),
                x=["Negative", "Positive"],
                y=["Negative", "Positive"],
            )
            cm_fig.update_traces(
                hovertemplate="Actual %{y}<br>Predicted %{x}<br>Count %{z}<extra></extra>"
            )
            st.plotly_chart(
                style_figure(cm_fig, "Confusion matrix"),
                use_container_width=True,
            )
