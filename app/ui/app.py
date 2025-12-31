import os
import requests
import streamlit as st

st.set_page_config(page_title="Wikimedia Streaming RAG", layout="wide")

RAG_API_URL = os.getenv("RAG_API_URL", "http://wikimedia-rag-api:8000").rstrip("/")

st.title("Wikimedia Streaming RAG")

# --- API health ---
with st.expander("API Health", expanded=True):
    try:
        r = requests.get(f"{RAG_API_URL}/health", timeout=5)
        st.write("Status:", r.status_code)
        st.code(r.text)
    except Exception as e:
        st.error(f"API not reachable: {e}")

st.divider()

# --- Query UI ---
st.subheader("Ask the index")
query = st.text_input("Query", value="recent wikimedia edit about what")
top_k = st.slider("top_k", min_value=1, max_value=10, value=3)

if st.button("Run Query"):
    try:
        payload = {"query": query, "top_k": top_k}
        resp = requests.post(f"{RAG_API_URL}/query", json=payload, timeout=30)
        st.write("Status:", resp.status_code)
        data = resp.json()

        results = data.get("results", [])
        st.write(f"Results: {len(results)}")

        for i, item in enumerate(results, start=1):
            st.markdown(f"### {i}")
            st.json(item)

    except Exception as e:
        st.error(f"Query failed: {e}")
