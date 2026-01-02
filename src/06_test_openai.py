from openai import OpenAI
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Very small, cheap test call
response = client.responses.create(
    model="gpt-4.1-mini",
    input="Reply with exactly the words: API test successful."
)

print(response.output_text)
