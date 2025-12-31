import os
import time
import requests
import streamlit as st

RAG_API_URL = os.getenv("RAG_API_URL", "http://wikimedia-rag-api:8000").rstrip("/")

st.set_page_config(page_title="Wikimedia Streaming RAG", layout="wide")
st.title("Live Sentiment Score")

# --- Helper: cheap heuristic sentiment (no extra libs, no paid models) ---
POS = {"good","great","excellent","positive","win","success","improve","improved","improvement","fix","fixed","clean","stable"}
NEG = {"bad","terrible","awful","negative","fail","failure","break","broken","error","crash","revert","reverted","vandal","spam"}

def heuristic_sentiment(text: str) -> float:
    if not text:
        return 0.0
    t = text.lower()
    pos = sum(w in t for w in POS)
    neg = sum(w in t for w in NEG)
    denom = max(1, pos + neg)
    return (pos - neg) / denom  # -1..+1

def get_health():
    try:
        r = requests.get(f"{RAG_API_URL}/health", timeout=5)
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        return False, str(e)

colA, colB = st.columns([1, 2])
with colA:
    ok, payload = get_health()
    st.metric("API Health", "OK" if ok else "DOWN")
with colB:
    st.caption(f"RAG_API_URL = {RAG_API_URL}")
    if not ok:
        st.error(payload)

st.divider()

# --- Query panel ---
left, right = st.columns([2, 1])

with left:
    query = st.text_input("Ask something (RAG query)", value="recent wikimedia edit about what")
    top_k = st.slider("top_k", 1, 10, 3)

    if st.button("Run Query", type="primary", use_container_width=True):
        try:
            r = requests.post(
                f"{RAG_API_URL}/query",
                json={"query": query, "top_k": int(top_k)},
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
            st.session_state["last_query"] = data
        except Exception as e:
            st.error(f"Query failed: {e}")

with right:
    st.subheader("Auto-refresh")
    auto = st.toggle("Enable", value=False)
    interval = st.slider("Refresh seconds", 2, 15, 5)

if auto:
    time.sleep(0.2)
    st.experimental_rerun()

data = st.session_state.get("last_query")
if not data:
    st.info("Run a query to populate results.")
    st.stop()

st.subheader("Results")
results = data.get("results", [])
if not results:
    st.warning("No results returned.")
    st.stop()

# --- Sentiment score from returned snippets/content ---
texts = []
for item in results:
    # tolerate different schemas
    txt = (
        item.get("content")
        or item.get("text")
        or item.get("document")
        or item.get("title")
        or ""
    )
    texts.append(txt)

scores = [heuristic_sentiment(t) for t in texts]
avg = sum(scores) / max(1, len(scores))

c1, c2, c3 = st.columns(3)
c1.metric("Docs Returned", str(len(results)))
c2.metric("Avg Sentiment (heuristic)", f"{avg:+.2f}")
c3.metric("Signal Quality", "LOW (heuristic)" if len(POS) < 50 else "OK")

for i, item in enumerate(results, start=1):
    title = item.get("title") or item.get("id") or f"Result {i}"
    url = item.get("url") or ""
    content = item.get("content") or item.get("text") or ""

    with st.expander(f"{i}. {title}  |  sentiment={scores[i-1]:+.2f}", expanded=(i <= 1)):
        if url:
            st.write(url)
        st.write(content[:2000] if content else item)
