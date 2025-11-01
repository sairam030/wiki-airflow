# How to Switch Between Ollama and Groq API

## Quick Switch Guide

### Option 1: Use Groq API (⚡ 10-20x Faster)

**Edit `plugins/config.py`:**

```python
# Change this line:
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "groq")  # Changed from "ollama" to "groq"

# Add your Groq API key:
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "gsk_YOUR_API_KEY_HERE")
```

**Get your free API key:**
1. Go to https://console.groq.com
2. Sign up (free)
3. Go to API Keys section
4. Create new API key
5. Copy and paste it in `config.py`

**Free tier limits:**
- 30 requests/minute
- 6,000 requests/day
- Perfect for 995 pages!

---

### Option 2: Use Ollama (🔒 Private, GPU-accelerated)

**Edit `plugins/config.py`:**

```python
# Change this line:
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")  # Keep as "ollama"
```

That's it! Your local GPU will be used.

---

## After Changing

**Always restart Airflow after editing config:**

```bash
# Clear cache
find /home/ram/wiki-trends-pipeline -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null

# Restart Airflow
docker restart airflow-scheduler airflow-webserver
```

---

## Performance Comparison

| Provider | Speed/Page | 100 Pages | 995 Pages | Privacy | Cost |
|----------|-----------|-----------|-----------|---------|------|
| **Groq** | 0.5-1s | ~1-2 min | ~8-17 min | ❌ Cloud | ✅ Free |
| **Ollama** | 8-16s | ~13-27 min | ~130-265 min | ✅ Local | ✅ Free |

---

## Current Settings

The code checks `LLM_PROVIDER` in `plugins/config.py`:
- Default: `"ollama"` (local GPU)
- To switch: Change to `"groq"` (cloud API)

That's it! No other code changes needed.
