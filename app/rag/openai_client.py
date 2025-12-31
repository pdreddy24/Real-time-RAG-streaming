from rag.config import OPENAI_API_KEY, OPENAI_CHAT_MODEL, OPENAI_EMBED_MODEL

# Compatibility wrapper:
# - New SDK (v1.x): from openai import OpenAI
# - Old SDK (0.x): import openai
try:
    from openai import OpenAI  # type: ignore
    _client = OpenAI(api_key=OPENAI_API_KEY)

    def embed(text: str) -> list[float]:
        r = _client.embeddings.create(model=OPENAI_EMBED_MODEL, input=text)
        return r.data[0].embedding

    def chat(messages: list[dict], max_tokens: int = 350) -> str:
        r = _client.chat.completions.create(
            model=OPENAI_CHAT_MODEL,
            messages=messages,
            temperature=0.0,
            max_tokens=max_tokens,
        )
        return r.choices[0].message.content.strip()

except Exception:
    import openai  # type: ignore
    openai.api_key = OPENAI_API_KEY

    def embed(text: str) -> list[float]:
        r = openai.Embedding.create(model=OPENAI_EMBED_MODEL, input=text)
        return r["data"][0]["embedding"]

    def chat(messages: list[dict], max_tokens: int = 350) -> str:
        r = openai.ChatCompletion.create(
            model=OPENAI_CHAT_MODEL,
            messages=messages,
            temperature=0.0,
            max_tokens=max_tokens,
        )
        return r["choices"][0]["message"]["content"].strip()
