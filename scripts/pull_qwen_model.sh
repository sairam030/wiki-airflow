#!/bin/bash
# Script to pull Qwen2.5:3b model and remove old LLaMA model

echo "🔄 Pulling Qwen2.5:3b model..."
docker exec -it ollama ollama pull qwen2.5:3b

echo ""
echo "✅ Qwen2.5:3b model pulled successfully!"
echo ""

# echo "🗑️  Removing old llama3.2:3b model to save space..."
# docker exec -it ollama ollama rm llama3.2:1b

echo ""
echo "✨ Done! Available models:"
docker exec -it ollama ollama list
