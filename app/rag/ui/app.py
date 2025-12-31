import os
import json
import requests
import streamlit as st

st.set_page_config(page_title="Streaming RAG UI", layout="wide")

# If UI runs on your Windows host: http://localhost:8000
# If UI runs inside Docker network: http://wikimedia-rag-api:8000
API_BASE = os.getenv("RAG_API_URL", "http://localhost:8000").rstrip("/")
QUERY_URL = f"{API_BASE}/query"
HEALTH_URL = f"{API_BASE}/health"

st.title("Streaming RAG — Query UI")

with st.sidebar:
    st.subheader("API Connectivity")
    st.write(f"**RAG_API_URL:** `{API_BASE}`")

    if st.button("Ping /health"):
        try:
            r = requests.get(HEALTH_URL, timeout=5)
            st.success(f"UP ✅ ({r.status_code})")
            st.code(r.text)
        except Exception as e:
            st.error(f"Health check failed: {e}")

query = st.text_area("Query", value="recent wikimedia edit about what", height=120)
top_k = st.slider("top_k", min_value=1, max_value=20, value=3)

run = st.button("Run Query", type="primary")

if run:
    if not query.strip():
        st.warning("Query cannot be empty.")
        st.stop()

    payload = {"query": query.strip(), "top_k": int(top_k)}

    with st.spinner("Calling RAG API..."):
        try:
            resp = requests.post(QUERY_URL, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.HTTPError:
            st.error(f"API error: {resp.status_code}")
            st.code(resp.text)
            st.stop()
        except Exception as e:
            st.error(f"Request failed: {e}")
            st.stop()

    st.subheader("Results")
    results = data.get("results", [])

    if isinstance(results, list) and results:
        rows = []
        for r in results:
            rows.append({
                "score": r.get("score"),
                "title": r.get("title") or r.get("page_title") or r.get("id"),
                "url": r.get("url") or r.get("page_url"),
                "timestamp": r.get("timestamp") or r.get("ts"),
                "snippet": (r.get("content") or r.get("text") or "")[:240],
            })
        st.dataframe(rows, use_container_width=True)
    else:
        st.info("No results returned.")

    with st.expander("Raw JSON response"):
        st.code(json.dumps(data, indent=2))
