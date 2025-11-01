# Get Your FREE Hugging Face API Key (30 seconds)

## Steps:

1. **Go to**: https://huggingface.co/join
2. **Sign up** (use GitHub/Google for instant signup)
3. **Go to**: https://huggingface.co/settings/tokens
4. **Click**: "New token"
5. **Name it**: "wiki-pipeline" 
6. **Role**: Select "Read"
7. **Click**: "Generate"
8. **Copy** the token (starts with `hf_...`)

## Add to Your Config:

Edit `plugins/config.py`:

```python
HUGGINGFACE_API_KEY = os.getenv("HUGGINGFACE_API_KEY", "hf_YOUR_KEY_HERE")
```

## Why Hugging Face?

✅ **Completely FREE** - No credit card needed  
✅ **No strict rate limits** - Much more generous than Groq  
✅ **Fast inference** - ~2-4 seconds per page  
✅ **Good models** - Llama 3.2 3B is very accurate  

## Expected Performance:

- **100 pages**: ~3-5 minutes
- **995 pages**: ~30-50 minutes
- **5 concurrent workers** = No rate limit issues!

Much better than Groq's 30 req/min limit! 🚀
