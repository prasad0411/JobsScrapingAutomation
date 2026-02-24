#!/usr/bin/env python3
"""
Job Hunt Dashboard — Streamlit App

Run locally:
    streamlit run dashboard/app.py

Deploy on Streamlit Cloud:
    1. Push to GitHub
    2. Go to share.streamlit.io
    3. Connect repo, set main file to dashboard/app.py
    4. Add secrets (Google Sheets credentials)
"""

import streamlit as st
import pandas as pd
import gspread
import json
import os
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import plotly.express as px
import plotly.graph_objects as go

# ─── Page Config ───────────────────────────────────────────────

st.set_page_config(
    page_title="Job Hunt Analytics",
    page_icon="--",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── Custom CSS ────────────────────────────────────────────────

st.markdown(
    """
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&family=Space+Grotesk:wght@400;500;600;700&display=swap');

    .stApp {
        background-color: #0E1117;
        color: #FAFAFA;
    }

    h1, h2, h3 {
        font-family: 'Space Grotesk', sans-serif !important;
        color: #FAFAFA !important;
    }

    p, span, div, label {
        font-family: 'DM Sans', sans-serif !important;
    }

    /* Metric cards */
    .metric-card {
        background: linear-gradient(135deg, #1a1f2e 0%, #151922 100%);
        border: 1px solid #2a3040;
        border-radius: 12px;
        padding: 24px;
        text-align: center;
        transition: transform 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        border-color: #4a90d9;
    }
    .metric-value {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 42px;
        font-weight: 700;
        color: #4a90d9;
        margin: 8px 0;
    }
    .metric-label {
        font-family: 'DM Sans', sans-serif;
        font-size: 14px;
        color: #8892a0;
        text-transform: uppercase;
        letter-spacing: 1.2px;
    }
    .metric-sub {
        font-family: 'DM Sans', sans-serif;
        font-size: 13px;
        color: #5a6270;
        margin-top: 4px;
    }

    /* Status pills */
    .status-applied { color: #4CAF50; }
    .status-rejected { color: #f44336; }
    .status-oa { color: #FF9800; }

    /* Header */
    .dashboard-header {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 36px;
        font-weight: 700;
        color: #FAFAFA;
        margin-bottom: 4px;
    }
    .dashboard-sub {
        font-family: 'DM Sans', sans-serif;
        font-size: 16px;
        color: #6a7380;
        margin-bottom: 32px;
    }

    /* Section headers */
    .section-header {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 20px;
        font-weight: 600;
        color: #c0c8d4;
        margin-top: 32px;
        margin-bottom: 16px;
        padding-bottom: 8px;
        border-bottom: 1px solid #2a3040;
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* Plotly chart backgrounds */
    .js-plotly-plot .plotly .bg {
        fill: transparent !important;
    }
</style>
""",
    unsafe_allow_html=True,
)


# ─── Data Loading ──────────────────────────────────────────────


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load data from Google Sheets."""
    creds_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".local",
        "credentials.json",
    )

    if os.path.exists(creds_path):
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            creds_path,
            [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
    else:
        st.error(
            "No credentials found. Add .local/credentials.json or Streamlit secrets."
        )
        return None, None, None

    gc = gspread.authorize(creds)
    ss = gc.open("H1B visa")

    # Load Valid Entries
    try:
        valid_ws = ss.worksheet("Valid Entries")
        valid_data = valid_ws.get_all_records()
        valid_df = pd.DataFrame(valid_data)
    except Exception as e:
        st.error(f"Failed to load Valid Entries: {e}")
        valid_df = pd.DataFrame()

    # Load Discarded Entries
    try:
        disc_ws = ss.worksheet("Discarded Entries")
        disc_data = disc_ws.get_all_records()
        disc_df = pd.DataFrame(disc_data)
    except Exception:
        disc_df = pd.DataFrame()

    # Load Outreach Tracker
    try:
        out_ws = ss.worksheet("Outreach Tracker")
        out_data = out_ws.get_all_records()
        out_df = pd.DataFrame(out_data)
    except Exception:
        out_df = pd.DataFrame()

    return valid_df, disc_df, out_df


# ─── Helper Functions ──────────────────────────────────────────


def parse_status(status_str):
    """Normalize status strings."""
    if not status_str or not isinstance(status_str, str):
        return "Unknown"
    s = status_str.strip().lower()
    if "applied" in s and "not" not in s:
        return "Applied"
    elif "not applied" in s:
        return "Not Applied"
    elif "rejected" in s:
        return "Rejected"
    elif "oa" in s:
        return "OA Round"
    elif "offer" in s:
        return "Offer"
    elif "interview" in s:
        return "Interview"
    return status_str.strip()


def parse_date(date_str):
    """Parse various date formats from the sheet."""
    if not date_str or not isinstance(date_str, str):
        return None
    date_str = date_str.strip()
    formats = [
        "%d %B, %Y",
        "%d %B, %I:%M %p",
        "%d %B, %I:%M %p",
        "%B %d, %Y",
        "%d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    # Try partial: "05 February, 10:35 PM" → add current year
    try:
        dt = datetime.strptime(date_str, "%d %B, %I:%M %p")
        return dt.replace(year=2026)
    except ValueError:
        pass
    return None


def metric_card(label, value, sub=""):
    """Render a styled metric card."""
    sub_html = f'<div class="metric-sub">{sub}</div>' if sub else ""
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {sub_html}
    </div>
    """


# ─── Main App ─────────────────────────────────────────────────


def main():
    # Header
    st.markdown(
        '<div class="dashboard-header">Job Hunt Analytics</div>', unsafe_allow_html=True
    )
    st.markdown(
        '<div class="dashboard-sub">Prasad Kanade | MS CS Northeastern | Summer 2026 Internships</div>',
        unsafe_allow_html=True,
    )

    # Load data
    with st.spinner("Loading live data from Google Sheets..."):
        valid_df, disc_df, out_df = load_data()

    if valid_df is None or valid_df.empty:
        st.warning("No data found in Valid Entries sheet.")
        return

    # ─── Parse and clean data ──────────────────────────────────

    # Normalize status column
    status_col = "Status" if "Status" in valid_df.columns else valid_df.columns[1]
    valid_df["status_clean"] = valid_df[status_col].apply(parse_status)

    # Parse dates
    date_col = None
    for col in [
        "Date Applied",
        "Entry Date",
        valid_df.columns[4] if len(valid_df.columns) > 4 else "",
    ]:
        if col in valid_df.columns:
            date_col = col
            break

    if date_col:
        valid_df["date_parsed"] = valid_df[date_col].apply(parse_date)
        valid_df["week"] = valid_df["date_parsed"].apply(
            lambda d: d.isocalendar()[1] if d is not None and not pd.isna(d) else None
        )
        valid_df["date_only"] = valid_df["date_parsed"].apply(
            lambda d: d.date() if d is not None and not pd.isna(d) else None
        )

    # Source column
    source_col = None
    for col in ["Source", valid_df.columns[-1] if len(valid_df.columns) > 10 else ""]:
        if col in valid_df.columns and valid_df[col].dtype == object:
            unique_vals = valid_df[col].dropna().unique()
            if any(
                s in str(unique_vals) for s in ["SWE", "Jobright", "Simplify", "Manual"]
            ):
                source_col = col
                break

    # Company column
    company_col = "Company" if "Company" in valid_df.columns else valid_df.columns[2]

    # ─── Top Metrics Row ───────────────────────────────────────

    total = len(valid_df)
    applied = len(valid_df[valid_df["status_clean"] == "Applied"])
    rejected = len(valid_df[valid_df["status_clean"] == "Rejected"])
    oa_rounds = len(valid_df[valid_df["status_clean"] == "OA Round"])
    not_applied = len(valid_df[valid_df["status_clean"] == "Not Applied"])
    discarded_count = len(disc_df) if disc_df is not None and not disc_df.empty else 0

    # Calculate this week's applications
    this_week = 0
    if "date_parsed" in valid_df.columns:
        now = datetime.now()
        week_start = now - timedelta(days=now.weekday())
        this_week = len(
            valid_df[
                valid_df["date_parsed"].apply(
                    lambda d: d is not None and d >= week_start
                )
            ]
        )

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    with col1:
        st.markdown(
            metric_card("Total Jobs", total, f"{discarded_count} discarded"),
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            metric_card("Applied", applied, f"{applied*100//max(total,1)}% of total"),
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(metric_card("Not Applied", not_applied), unsafe_allow_html=True)
    with col4:
        st.markdown(
            metric_card(
                "Rejected", rejected, f"{rejected*100//max(applied,1)}% rejection rate"
            ),
            unsafe_allow_html=True,
        )
    with col5:
        st.markdown(metric_card("OA Rounds", oa_rounds), unsafe_allow_html=True)
    with col6:
        st.markdown(
            metric_card("This Week", this_week, "applications"), unsafe_allow_html=True
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ─── Outreach Metrics ──────────────────────────────────────

    if out_df is not None and not out_df.empty:
        notes_col_name = (
            "Notes"
            if "Notes" in out_df.columns
            else (out_df.columns[-1] if len(out_df.columns) > 12 else "")
        )
        hm_email_col = "HM Email" if "HM Email" in out_df.columns else ""
        rec_email_col = "Recruiter Email" if "Recruiter Email" in out_df.columns else ""

        emails_found = 0
        emails_delivered = 0
        emails_bounced = 0

        if hm_email_col:
            emails_found += (
                out_df[hm_email_col]
                .apply(lambda x: bool(str(x).strip() and "@" in str(x)))
                .sum()
            )
        if rec_email_col:
            emails_found += (
                out_df[rec_email_col]
                .apply(lambda x: bool(str(x).strip() and "@" in str(x)))
                .sum()
            )

        if notes_col_name and notes_col_name in out_df.columns:
            emails_delivered = (
                out_df[notes_col_name].apply(lambda x: "Delivered" in str(x)).sum()
            )
            emails_bounced = (
                out_df[notes_col_name]
                .apply(lambda x: "bounced" in str(x).lower())
                .sum()
            )

        st.markdown(
            '<div class="section-header">Outreach Pipeline</div>',
            unsafe_allow_html=True,
        )
        oc1, oc2, oc3, oc4 = st.columns(4)
        with oc1:
            st.markdown(
                metric_card("Emails Found", emails_found), unsafe_allow_html=True
            )
        with oc2:
            st.markdown(
                metric_card("Delivered", emails_delivered), unsafe_allow_html=True
            )
        with oc3:
            st.markdown(metric_card("Bounced", emails_bounced), unsafe_allow_html=True)
        with oc4:
            pending = len(out_df) - emails_found // 2  # Rough estimate
            st.markdown(
                metric_card("Pending Discovery", max(0, pending)),
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # ─── Charts Row 1: Status + Source ─────────────────────────

    st.markdown(
        '<div class="section-header">Application Breakdown</div>',
        unsafe_allow_html=True,
    )

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        # Status distribution
        status_counts = valid_df["status_clean"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]

        color_map = {
            "Applied": "#4CAF50",
            "Not Applied": "#607D8B",
            "Rejected": "#f44336",
            "OA Round": "#FF9800",
            "Interview": "#2196F3",
            "Offer": "#9C27B0",
            "Unknown": "#455A64",
        }

        fig_status = px.pie(
            status_counts,
            values="Count",
            names="Status",
            color="Status",
            color_discrete_map=color_map,
            hole=0.5,
        )
        fig_status.update_layout(
            title=dict(
                text="Applications by Status",
                font=dict(family="Space Grotesk", size=18, color="#c0c8d4"),
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="DM Sans", color="#8892a0"),
            legend=dict(font=dict(size=12)),
            margin=dict(t=50, b=20, l=20, r=20),
            height=400,
        )
        fig_status.update_traces(textinfo="value+percent", textfont_size=12)
        st.plotly_chart(fig_status, use_container_width=True)

    with chart_col2:
        # Source distribution
        if source_col:
            source_counts = valid_df[source_col].value_counts().head(10).reset_index()
            source_counts.columns = ["Source", "Count"]

            fig_source = px.bar(
                source_counts,
                x="Count",
                y="Source",
                orientation="h",
                color="Count",
                color_continuous_scale=["#1a3a5c", "#4a90d9", "#7ab8f5"],
            )
            fig_source.update_layout(
                title=dict(
                    text="Jobs by Source",
                    font=dict(family="Space Grotesk", size=18, color="#c0c8d4"),
                ),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="DM Sans", color="#8892a0"),
                xaxis=dict(gridcolor="#1e2530"),
                yaxis=dict(gridcolor="#1e2530", autorange="reversed"),
                coloraxis_showscale=False,
                margin=dict(t=50, b=20, l=20, r=20),
                height=400,
            )
            st.plotly_chart(fig_source, use_container_width=True)
        else:
            st.info("Source column not detected in sheet")

    # ─── Charts Row 2: Timeline + Top Companies ───────────────

    st.markdown(
        '<div class="section-header">Trends and Top Companies</div>',
        unsafe_allow_html=True,
    )

    chart_col3, chart_col4 = st.columns(2)

    with chart_col3:
        # Applications over time
        if "date_only" in valid_df.columns:
            daily = (
                valid_df.dropna(subset=["date_only"])
                .groupby("date_only")
                .size()
                .reset_index()
            )
            daily.columns = ["Date", "Count"]
            daily["Cumulative"] = daily["Count"].cumsum()
            daily["Date"] = pd.to_datetime(daily["Date"])

            fig_timeline = go.Figure()
            fig_timeline.add_trace(
                go.Bar(
                    x=daily["Date"],
                    y=daily["Count"],
                    name="Daily",
                    marker_color="#2a5a8a",
                    opacity=0.6,
                )
            )
            fig_timeline.add_trace(
                go.Scatter(
                    x=daily["Date"],
                    y=daily["Cumulative"],
                    name="Cumulative",
                    line=dict(color="#4a90d9", width=3),
                    yaxis="y2",
                )
            )
            fig_timeline.update_layout(
                title=dict(
                    text="Applications Over Time",
                    font=dict(family="Space Grotesk", size=18, color="#c0c8d4"),
                ),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="DM Sans", color="#8892a0"),
                xaxis=dict(gridcolor="#1e2530"),
                yaxis=dict(title="Daily", gridcolor="#1e2530"),
                yaxis2=dict(
                    title="Cumulative",
                    overlaying="y",
                    side="right",
                    gridcolor="#1e2530",
                ),
                legend=dict(x=0.01, y=0.99, font=dict(size=11)),
                margin=dict(t=50, b=20, l=20, r=40),
                height=400,
                hovermode="x unified",
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.info("Date column not detected")

    with chart_col4:
        # Top companies
        company_counts = valid_df[company_col].value_counts().head(15).reset_index()
        company_counts.columns = ["Company", "Applications"]

        fig_companies = px.bar(
            company_counts,
            x="Applications",
            y="Company",
            orientation="h",
            color="Applications",
            color_continuous_scale=["#1a3a5c", "#4a90d9", "#7ab8f5"],
        )
        fig_companies.update_layout(
            title=dict(
                text="Top Companies (by # of roles)",
                font=dict(family="Space Grotesk", size=18, color="#c0c8d4"),
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="DM Sans", color="#8892a0"),
            xaxis=dict(gridcolor="#1e2530"),
            yaxis=dict(gridcolor="#1e2530", autorange="reversed"),
            coloraxis_showscale=False,
            margin=dict(t=50, b=20, l=20, r=20),
            height=400,
        )
        st.plotly_chart(fig_companies, use_container_width=True)

    # ─── Weekly Trend ──────────────────────────────────────────

    if "week" in valid_df.columns:
        st.markdown(
            '<div class="section-header">Weekly Application Pace</div>',
            unsafe_allow_html=True,
        )

        weekly = valid_df.dropna(subset=["week"]).groupby("week").size().reset_index()
        weekly.columns = ["Week", "Applications"]

        fig_weekly = px.bar(
            weekly,
            x="Week",
            y="Applications",
            color="Applications",
            color_continuous_scale=["#1a3a5c", "#4a90d9"],
        )
        fig_weekly.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="DM Sans", color="#8892a0"),
            xaxis=dict(title="Week Number", gridcolor="#1e2530"),
            yaxis=dict(title="Applications", gridcolor="#1e2530"),
            coloraxis_showscale=False,
            margin=dict(t=20, b=20, l=20, r=20),
            height=300,
        )
        st.plotly_chart(fig_weekly, use_container_width=True)

    # ─── Footer ────────────────────────────────────────────────

    st.markdown("---")
    st.markdown(
        '<div style="text-align: center; color: #4a5568; font-size: 13px; font-family: DM Sans;">'
        "Data refreshes every 5 minutes from Google Sheets | "
        "Built with Streamlit + Plotly"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
